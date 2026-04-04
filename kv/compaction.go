package kv

import (
	"fmt"
	"log"
)

func (kv *KVStore) Compact() error {
	// create a snapshot of keyDir to avoid holding lock during entire compaction process
	keydirSnapshot := make(map[string]*IndexItem)

	kv.lock.RLock()
	activeFileID := kv.activeFile.ID
	for key, item := range kv.keyDir.index {
		if item.FileId >= activeFileID {
			continue // skip active file and any newer files
		}
		keydirSnapshot[key] = item
	}
	kv.lock.RUnlock()

	// Build eligibleFiles from disk — this captures files with 0 live keydir
	// entries (all entries overwritten) that would otherwise be missed.
	allFileIDs, err := getSortedDataFileIds()
	if err != nil {
		return fmt.Errorf("failed to list data files: %w", err)
	}
	eligibleFiles := make(map[int64]struct{})
	for _, id := range allFileIDs {
		if id < activeFileID {
			eligibleFiles[id] = struct{}{}
		}
	}

	if len(eligibleFiles) == 0 {
		log.Printf("No files eligible for compaction")
		return nil
	}

	kv.lock.Lock()
	mergedFileId := kv.maxFileID + 1
	mergedFile, err := createNewDataFile(mergedFileId)
	if err != nil {
		kv.lock.Unlock()
		return fmt.Errorf("Failed to create merged file ID %d: %w", mergedFileId, err)
	}
	kv.maxFileID = mergedFileId
	kv.lock.Unlock()

	hintFile, err := createNewHintFile(mergedFileId)
	if err != nil {
		return fmt.Errorf("Failed to create hint file for merged file ID %d: %w", mergedFileId, err)
	}

	// track files that are eligible for deletion after compaction:
	// a file is only safe to delete if every key that referenced it was successfully migrated
	filePendingKeys := make(map[int64]int) // fileId → count of keys yet to be migrated
	fileFailedKeys := make(map[int64]bool) // fileId → had at least one migration failure
	for _, item := range keydirSnapshot {
		if _, ok := eligibleFiles[item.FileId]; ok {
			filePendingKeys[item.FileId]++
		}
	}

	openedFiles := make(map[int64]*DataFile)     // cache of opened files to minimize open/close overhead
	keydirUpdates := make(map[string]*IndexItem) // batched keydir updates, applied under a single lock at the end

	for key, item := range keydirSnapshot {
		if _, ok := eligibleFiles[item.FileId]; !ok {
			continue // skip entries whose files are not eligible for compaction
		}

		// tombstone — skip (don't copy deleted keys into merged file)
		if item.Size == headerSize+int32(len(key)) {
			filePendingKeys[item.FileId]--
			continue
		}

		file, ok := openedFiles[item.FileId]
		if !ok {
			file, err = openDataFile(item.FileId)
			if err != nil {
				log.Printf("ERROR: Failed to open file ID %d for key '%s': %v", item.FileId, key, err)
				fileFailedKeys[item.FileId] = true
				continue
			}
			openedFiles[item.FileId] = file
		}

		// TODO, we can also return encoded entry directly from ReadEntry to avoid another encoding for compaction.
		entry, encodedEntry, _, err := ReadEntry(file.File, item.Offset)
		if err != nil {
			log.Printf("ERROR: Failed to read entry for key '%s' from file ID %d: %v", key, item.FileId, err)
			fileFailedKeys[item.FileId] = true
			continue
		}

		if mergedFile.offset+int64(len(encodedEntry)) > kv.maxFileSize {
			// fsync and close current merged file before switching to a new one
			err := mergedFile.File.Sync()
			if err != nil {
				log.Printf("ERROR: Failed to sync merged file ID %d: %v", mergedFile.ID, err)
			}
			err = mergedFile.File.Close()
			if err != nil {
				log.Printf("ERROR: Failed to close merged file ID %d: %v", mergedFile.ID, err)
			}

			// close current hint file and create a new one for the next merged file
			err = hintFile.Sync()
			if err != nil {
				log.Printf("ERROR: Failed to sync hint file for merged file ID %d: %v", mergedFile.ID, err)
			}
			err = hintFile.Close()
			if err != nil {
				log.Printf("ERROR: Failed to close hint file for merged file ID %d: %v", mergedFile.ID, err)
			}

			kv.lock.Lock()
			mergedFileId = kv.maxFileID + 1
			mergedFile, err = createNewDataFile(mergedFileId)
			if err != nil {
				kv.lock.Unlock()
				return fmt.Errorf("Failed to create merged file ID %d: %w", mergedFileId, err)
			}
			hintFile, err = createNewHintFile(mergedFileId)
			if err != nil {
				kv.lock.Unlock()
				return fmt.Errorf("Failed to create hint file for merged file ID %d: %w", mergedFileId, err)
			}
			kv.maxFileID = mergedFileId
			kv.lock.Unlock()
		}

		entryOffset := mergedFile.offset
		err = mergedFile.Append(encodedEntry, false)
		if err != nil {
			log.Printf("ERROR: Failed to append entry for key '%s' to merged file ID %d: %v", key, mergedFile.ID, err)
			fileFailedKeys[item.FileId] = true
			continue
		}

		// write to hint file
		hintItem := &HintItem{
			Timestamp: entry.Timestamp,
			KeySize:   int32(len(entry.Key)),
			ValueSize: int32(len(entry.Value)),
			Offset:    entryOffset,
			Key:       entry.Key,
		}
		err = kv.writeHintFileEntry(hintFile, hintItem)
		if err != nil {
			log.Printf("ERROR: Failed to write hint item for key '%s' to hint file for merged file ID %d: %v", key, mergedFile.ID, err)
			fileFailedKeys[item.FileId] = true
			continue
		}

		// stage keydir update — applied in batch after the loop
		keydirUpdates[key] = &IndexItem{
			FileId:    mergedFile.ID,
			Offset:    entryOffset,
			Size:      int32(len(encodedEntry)),
			Timestamp: entry.Timestamp,
		}
		filePendingKeys[item.FileId]--
	}

	// apply all keydir updates in a single write lock
	kv.lock.Lock()
	for key, newItem := range keydirUpdates {
		// only update if keyDir still points to the old file (a newer write may have superseded it)
		if cur := kv.keyDir.index[key]; cur != nil && cur.FileId == keydirSnapshot[key].FileId {
			kv.keyDir.Put(key, newItem)
		}
	}
	kv.lock.Unlock()

	// build oldFileIds: only include files where every key migrated successfully
	oldFileIds := make(map[int64]struct{})
	for fileId := range eligibleFiles {
		if !fileFailedKeys[fileId] && filePendingKeys[fileId] == 0 {
			oldFileIds[fileId] = struct{}{}
		} else {
			log.Printf("WARN: Skipping deletion of file ID %d (migration incomplete or had errors)", fileId)
		}
	}

	// close all opened files
	if err := mergedFile.File.Sync(); err != nil {
		log.Printf("ERROR: Failed to sync merged file ID %d: %v", mergedFile.ID, err)
	}
	if err := mergedFile.File.Close(); err != nil {
		log.Printf("ERROR: Failed to close merged file ID %d: %v", mergedFile.ID, err)
	}
	if err := hintFile.Sync(); err != nil {
		log.Printf("ERROR: Failed to sync hint file for merged file ID %d: %v", mergedFile.ID, err)
	}
	if err := hintFile.Close(); err != nil {
		log.Printf("ERROR: Failed to close hint file for merged file ID %d: %v", mergedFile.ID, err)
	}

	for fileId := range oldFileIds {
		if file, ok := openedFiles[fileId]; ok {
			if err := file.File.Close(); err != nil {
				log.Printf("ERROR: Failed to close file ID %d: %v", fileId, err)
			}
			delete(openedFiles, fileId)
		}
		err := deleteDataFile(fileId)
		if err != nil {
			log.Printf("ERROR: Failed to delete old data file ID %d: %v", fileId, err)
		}
	}

	log.Printf("Compaction completed. Merged file ID: %d, Reclaimed files: %d", mergedFile.ID, len(oldFileIds))

	return nil
}
