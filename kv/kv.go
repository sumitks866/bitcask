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
	maxFileSize int64
	maxFileID   int64
	activeFile  *DataFile
	syncOnPut   bool // TODO: sync options configuration
	keyDir      *KeyDir
	lock        sync.RWMutex
}

func NewKVStore() *KVStore {
	log.Printf("INFO: Initializing KVStore")

	activeFile, err := getActiveFile()
	if err != nil {
		log.Panicf("FATAL: Failed to initialize KVStore: %v", err)
	}
	log.Printf("INFO: KVStore initialized with active file ID %d", activeFile.ID)
	log.Printf("INFO: active file offset: %d", activeFile.offset)

	maxFileID, err := getMaxFileID()
	if err != nil {
		panic(fmt.Sprintf("Failed to get max file ID: %v", err))
	}

	keyDir := NewKeyDir()

	kv := &KVStore{
		activeFile:  activeFile,
		maxFileSize: 1024 * 1024, // 1MB for testing, can be configured as needed
		maxFileID:   maxFileID,
		keyDir:      keyDir,
	}

	err = kv.loadIndex()
	if err != nil {
		log.Panicf("FATAL: Failed to load index: %v", err)
	}

	return kv
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
		return nil, fmt.Errorf("Key not found")
	}
	// TODO: under high concurrent load, many file may stay open simultaneouly. Implement pooling or LRU cache for file handles
	file, err := openDataFile(item.FileId)
	if err != nil {
		return nil, fmt.Errorf("Failed to open data file ID %d: %w", item.FileId, err)
	}
	defer file.File.Close()

	entry, _, err := ReadEntry(file.File, item.Offset)
	if err != nil {
		return nil, fmt.Errorf("Failed to read entry from file ID %d at offset %d: %w", item.FileId, item.Offset, err)
	}

	return entry.Value, nil
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
	newActiveFile, err := createNewDataFile(newFileId)
	if err != nil {
		return fmt.Errorf("Failed to create new data file ID %d: %w", newFileId, err)
	}
	kv.activeFile = newActiveFile
	kv.maxFileID = newFileId
	return nil
}

// builds in-memory index by scanning all data files on startup
func (kv *KVStore) loadIndex() error {
	fileIds, err := getSortedDataFileIds()
	if err != nil {
		return fmt.Errorf("Failed to get data file IDs: %w", err)
	}

	for _, id := range fileIds {
		file, err := openDataFile(id)
		if err != nil {
			return fmt.Errorf("Failed to open data file ID %d: %w", id, err)
		}

		offset := int64(0)
		for {
			// TODO: optimize by implementing hint files to avoid scanning entire file on startup
			entry, bytesRead, err := ReadEntry(file.File, offset)
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

		file.File.Close()
	}

	return nil
}
