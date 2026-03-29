package kv

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

type DataFile struct {
	ID     int64
	Path   string
	File   *os.File
	offset int64
}

type KVStore struct {
	maxFileSize int64
	maxFileID   int64
	activeFile  *DataFile
	syncOnPut   bool // TODO: sync options configuration
	keyDir      *KeyDir
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
		maxFileSize: 1024 * 1024, // 1MB
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
	entry := &Entry{
		Key:       key,
		Value:     value,
		Timestamp: time.Now().Unix(),
	}

	// Capture offset before writing
	entryOffset := kv.activeFile.offset
	err := kv.storeEntry(entry)
	if err != nil {
		log.Printf("ERROR: KVStore.Put key=%q err=%v", string(key), err)
		return err
	}

	entrySize := int32(headerSize + len(key) + len(value))
	kv.keyDir.Put(string(key), &IndexItem{
		FileId:    kv.activeFile.ID,
		Offset:    entryOffset,
		Size:      entrySize,
		Timestamp: entry.Timestamp,
	})

	log.Printf("INFO: KVStore.Put key=%q file offset after write: %d", string(key), kv.activeFile.offset)
	return nil
}

func (kv *KVStore) Get(key []byte) ([]byte, error) {
	item, ok := kv.keyDir.Get(string(key))
	if !ok {
		return nil, fmt.Errorf("Key not found")
	}

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

// stores entry in active file and updates in-memory index
func (kv *KVStore) storeEntry(entry *Entry) error {
	log.Printf("INFO: KVStore.storeEntry key=%q", string(entry.Key))

	encodedEntry, err := EncodeEntry(entry)
	if err != nil {
		return fmt.Errorf("Failed to encode entry: %w", err)
	}

	// TODO: handle rotation if current file exceeds max size

	err = kv.activeFile.Append(encodedEntry, kv.syncOnPut)
	if err != nil {
		return fmt.Errorf("Failed to append entry: %w", err)
	}

	kv.maxFileID = kv.activeFile.ID
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

		defer file.File.Close()
	}

	return nil
}
