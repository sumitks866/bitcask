package kv

import (
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"
)

type KVStore struct {
	dataDir     string
	maxFileSize int64
	maxFileID   int64
	activeFile  *DataFile
	syncOnPut   bool
	keyDir      *KeyDir
	lock        sync.RWMutex

	fileCache map[int64]*DataFile // cached read-only file handles

	compactionInterval time.Duration
	compactionStop     chan struct{} // closed to signal worker to stop
}

func NewKVStore(cfg *KVStoreConfig) (*KVStore, error) {
	if cfg == nil {
		cfg = &KVStoreConfig{}
	}
	cfg.applyDefaults()

	log.Printf("INFO: Initializing KVStore (dataDir=%s)", *cfg.DataDir)

	activeFile, err := getActiveFile(*cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize active file: %w", err)
	}
	log.Printf("INFO: KVStore initialized with active file ID %d", activeFile.ID)

	maxFileID, err := getMaxFileID(*cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get max file ID: %w", err)
	}

	kv := &KVStore{
		dataDir:            *cfg.DataDir,
		maxFileSize:        *cfg.MaxFileSize,
		syncOnPut:          *cfg.SyncOnPut,
		compactionInterval: *cfg.CompactionInterval,

		activeFile:     activeFile,
		maxFileID:      maxFileID,
		keyDir:         NewKeyDir(),
		fileCache:      make(map[int64]*DataFile),
		compactionStop: make(chan struct{}),
	}

	if err := kv.loadIndex(); err != nil {
		return nil, fmt.Errorf("failed to load index: %w", err)
	}

	go kv.compactionWorker()

	return kv, nil
}

// compactionWorker runs Compact() on a fixed interval until Close() is called.
func (kv *KVStore) compactionWorker() {
	ticker := time.NewTicker(kv.compactionInterval)
	defer ticker.Stop()
	log.Printf("INFO: Compaction worker started (interval: %s)", kv.compactionInterval)
	for {
		select {
		case <-ticker.C:
			log.Printf("INFO: Compaction worker triggering compact")
			if err := kv.Compact(); err != nil {
				log.Printf("ERROR: Compaction failed: %v", err)
			}
		case <-kv.compactionStop:
			log.Printf("INFO: Compaction worker stopped")
			return
		}
	}
}

// Close stops the compaction worker and releases all resources.
func (kv *KVStore) Close() {
	close(kv.compactionStop)
	kv.lock.Lock()
	defer kv.lock.Unlock()
	if err := kv.activeFile.File.Close(); err != nil {
		log.Printf("ERROR: Failed to close active file: %v", err)
	}
	for id, f := range kv.fileCache {
		if err := f.File.Close(); err != nil {
			log.Printf("ERROR: Failed to close cached file %d: %v", id, err)
		}
	}
	kv.fileCache = nil
}

func (kv *KVStore) Put(key []byte, value []byte) error {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	entry := &Entry{
		Key:       key,
		Value:     value,
		Timestamp: time.Now().Unix(),
	}

	fileId, entryOffset, entrySize, err := kv.storeEntry(entry)
	if err != nil {
		return err
	}

	if len(value) == 0 {
		kv.keyDir.Delete(string(key))
		return nil
	}

	kv.keyDir.Put(string(key), &IndexItem{
		FileId:    fileId,
		Offset:    entryOffset,
		Size:      int32(entrySize),
		Timestamp: entry.Timestamp,
	})

	// log.Printf("INFO: KVStore.Put key=%q file offset after write: %d", string(key), kv.activeFile.offset)
	return nil
}

func (kv *KVStore) Get(key []byte) ([]byte, error) {
	kv.lock.RLock()
	defer kv.lock.RUnlock()

	item, ok := kv.keyDir.Get(string(key))
	if !ok {
		return nil, ErrKeyNotFound
	}

	file, err := kv.getCachedFile(item.FileId)
	if err != nil {
		return nil, fmt.Errorf("Failed to open data file ID %d: %w", item.FileId, err)
	}

	entry, _, _, err := ReadEntry(file.File, item.Offset)
	if err != nil {
		return nil, fmt.Errorf("Failed to read entry from file ID %d at offset %d: %w", item.FileId, item.Offset, err)
	}

	return entry.Value, nil
}

// getCachedFile returns an open DataFile from the cache, opening it on first access.
// Must be called while holding at least kv.lock.RLock().
// Note: ReadAt is goroutine-safe, so concurrent readers sharing a cached handle is fine.
func (kv *KVStore) getCachedFile(id int64) (*DataFile, error) {
	if f, ok := kv.fileCache[id]; ok {
		return f, nil
	}

	// Upgrade to write lock — we need to mutate the cache.
	// We must re-check after acquiring the write lock because another goroutine
	// may have populated the cache between our RUnlock and Lock.
	kv.lock.RUnlock()
	kv.lock.Lock()
	defer func() {
		// Downgrade back to read lock before returning to the RLock-holding caller.
		kv.lock.Unlock()
		kv.lock.RLock()
	}()

	if f, ok := kv.fileCache[id]; ok {
		return f, nil
	}

	f, err := openDataFile(kv.dataDir, id)
	if err != nil {
		return nil, err
	}
	kv.fileCache[id] = f
	return f, nil
}

func (kv *KVStore) Delete(key []byte) error {
	return kv.Put(key, Tombstone)
}

// stores entry in active file and returns file id, offset and size
func (kv *KVStore) storeEntry(entry *Entry) (int64, int64, int, error) {

	encodedEntry, err := EncodeEntry(entry)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("Failed to encode entry: %w", err)
	}

	if kv.activeFile.offset+int64(len(encodedEntry)) > kv.maxFileSize {
		log.Printf("INFO: Active file ID %d exceeded max size. Rotating file.", kv.activeFile.ID)
		err := kv.rotateActiveFile()
		if err != nil {
			return 0, 0, 0, fmt.Errorf("Failed to rotate active file: %w", err)
		}
		log.Printf("INFO: Rotation complete. New active file ID %d", kv.activeFile.ID)
	}

	// capture start offset before append
	startOffset := kv.activeFile.offset

	err = kv.activeFile.Append(encodedEntry, kv.syncOnPut)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("Failed to append entry: %w", err)
	}

	// ensure maxFileID reflects the active file
	kv.maxFileID = kv.activeFile.ID

	return kv.activeFile.ID, startOffset, len(encodedEntry), nil
}

func (kv *KVStore) rotateActiveFile() error {
	// close current active file
	err := kv.activeFile.File.Close()
	if err != nil {
		return fmt.Errorf("Failed to close active file ID %d: %w", kv.activeFile.ID, err)
	}

	// derive new file id from kv.maxFileID to ensure monotonic progression
	newFileId := kv.maxFileID + 1
	// fallback: if maxFileID wasn't up-to-date, ensure it's greater than activeFile.ID
	if newFileId <= kv.activeFile.ID {
		newFileId = kv.activeFile.ID + 1
	}
	newActiveFile, err := createNewDataFile(kv.dataDir, newFileId)
	if err != nil {
		return fmt.Errorf("Failed to create new data file ID %d: %w", newFileId, err)
	}
	kv.activeFile = newActiveFile
	kv.maxFileID = newFileId
	return nil
}

// loadIndex builds the in-memory keydir by scanning data files on startup.
// For files that have a companion .hint file (produced by compaction), the hint
// file is used instead — this skips reading values and is much faster.
func (kv *KVStore) loadIndex() error {
	fileIds, err := getSortedDataFileIds(kv.dataDir)
	if err != nil {
		return fmt.Errorf("Failed to get data file IDs: %w", err)
	}

	for _, id := range fileIds {
		if kv.loadIndexFromHint(id) {
			continue
		}
		if err := kv.loadIndexFromDataFile(id); err != nil {
			return err
		}
	}

	return nil
}

// loadIndexFromHint attempts to populate the keydir from a .hint file.
// Returns true if the hint file was found and loaded successfully.
func (kv *KVStore) loadIndexFromHint(fileId int64) bool {
	items, err := readHintFile(kv.dataDir, fileId)
	if err != nil {
		// hint file doesn't exist or is unreadable — fall back to data file
		return false
	}

	for _, h := range items {
		key := string(h.Key)
		entrySize := headerSize + h.KeySize + h.ValueSize

		if h.ValueSize == 0 {
			kv.keyDir.Delete(key)
		} else {
			kv.keyDir.Put(key, &IndexItem{
				FileId:    fileId,
				Offset:    h.Offset,
				Size:      int32(entrySize),
				Timestamp: h.Timestamp,
			})
		}
	}
	return true
}

// loadIndexFromDataFile scans every entry in a data file to populate the keydir.
func (kv *KVStore) loadIndexFromDataFile(id int64) error {
	file, err := openDataFile(kv.dataDir, id)
	if err != nil {
		return fmt.Errorf("Failed to open data file ID %d: %w", id, err)
	}
	defer file.File.Close()

	offset := int64(0)
	for {
		entry, _, bytesRead, err := ReadEntry(file.File, offset)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			log.Printf("ERROR: Failed to read entry from file ID %d at offset %d: %v", id, offset, err)
			break
		}

		if len(entry.Value) == 0 {
			kv.keyDir.Delete(string(entry.Key))
		} else {
			kv.keyDir.Put(string(entry.Key), &IndexItem{
				FileId:    id,
				Offset:    offset,
				Size:      int32(bytesRead),
				Timestamp: entry.Timestamp,
			})
		}

		offset += int64(bytesRead)
	}

	return nil
}
