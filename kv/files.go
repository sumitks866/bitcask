package kv

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type DataFile struct {
	ID     int64
	Path   string
	File   *os.File
	offset int64
}

func (df *DataFile) Append(data []byte, shouldSync bool) error {
	_, err := df.File.Write(data)
	if err != nil {
		return err
	}
	df.offset += int64(len(data))

	if shouldSync {
		return df.File.Sync() // ensure data is flushed to disk
	}

	return nil
}

// getActiveFile returns the current active data file, creating a new one if necessary
func getActiveFile(dataDir string) (*DataFile, error) {
	_, err := os.ReadDir(dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(dataDir, os.ModePerm)
			if err != nil {
				return nil, fmt.Errorf("Failed to create data directory: %w", err)
			}
			return createNewDataFile(dataDir, 1)
		}
		return nil, fmt.Errorf("Failed to read data directory: %w", err)
	}

	maxFileId, err := getMaxFileID(dataDir)
	if err != nil {
		return nil, fmt.Errorf("Failed to get max file ID: %w", err)
	}
	// if no files exist, create a new one with ID 1
	if maxFileId == 0 {
		return createNewDataFile(dataDir, 1)
	}

	return openDataFile(dataDir, int64(maxFileId))
}

func createNewDataFile(dataDir string, id int64) (*DataFile, error) {
	fileName := fmt.Sprintf("%d%s", id, fileExt)
	filePath := filepath.Join(dataDir, fileName)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, filePerm)
	if err != nil {
		return nil, err
	}

	return &DataFile{ID: id, Path: filePath, File: file, offset: 0}, nil
}

func openDataFile(dataDir string, id int64) (*DataFile, error) {
	fileName := fmt.Sprintf("%d%s", id, fileExt)
	filePath := filepath.Join(dataDir, fileName)

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_APPEND, filePerm)
	if err != nil {
		return nil, err
	}

	// get file size to set offset
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	return &DataFile{
		ID:     id,
		Path:   filePath,
		File:   file,
		offset: stat.Size(),
	}, nil
}

func getMaxFileID(dataDir string) (int64, error) {
	fileIds, err := getSortedDataFileIds(dataDir)
	if err != nil {
		return 0, err
	}

	if len(fileIds) == 0 {
		return 0, nil // no files exist yet
	}

	maxFileId := fileIds[len(fileIds)-1]
	return maxFileId, nil
}

func getSortedDataFileIds(dataDir string) ([]int64, error) {
	files, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, err
	}

	fileIds := []int64{}

	for _, file := range files {
		name := file.Name()

		id, valid := isValidDataFileName(name)
		if !valid {
			continue
		}

		fileIds = append(fileIds, int64(id))
	}

	// sort file IDs in ascending order
	sort.Slice(fileIds, func(i, j int) bool {
		return fileIds[i] < fileIds[j]
	})
	return fileIds, nil
}

func deleteDataFile(dataDir string, id int64) error {
	fileName := fmt.Sprintf("%d%s", id, fileExt)
	filePath := filepath.Join(dataDir, fileName)

	return os.Remove(filePath)
}

// isValidDataFileName checks if the given file name matches the expected data file pattern and extracts the file ID
func isValidDataFileName(name string) (int64, bool) {
	if !strings.HasSuffix(name, fileExt) {
		return 0, false
	}

	idStr := strings.TrimSuffix(name, fileExt)
	id, err := strconv.Atoi(idStr)
	if err != nil {
		return 0, false
	}

	return int64(id), true
}

func createNewHintFile(dataDir string, id int64) (*os.File, error) {
	fileName := fmt.Sprintf("%d%s", id, hintFileExt)
	filePath := filepath.Join(dataDir, fileName)

	return os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, filePerm)
}

func openHintFile(dataDir string, id int64) (*os.File, error) {
	fileName := fmt.Sprintf("%d%s", id, hintFileExt)
	filePath := filepath.Join(dataDir, fileName)

	return os.Open(filePath)
}
