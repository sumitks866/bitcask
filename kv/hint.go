package kv

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type HintItem struct {
	Timestamp int64
	KeySize   int32
	ValueSize int32
	Offset    int64
	Key       []byte
}

const hintItemHeaderSize = 8 + 4 + 4 + 8 // Timestamp (8) + KeySize (4) + ValueSize (4) + Offset (8)

func EncodeHintItem(item *HintItem) ([]byte, error) {
	buf := make([]byte, hintItemHeaderSize+len(item.Key))

	// Timestamp
	binary.LittleEndian.PutUint64(buf[0:8], uint64(item.Timestamp))
	// Key Size
	binary.LittleEndian.PutUint32(buf[8:12], uint32(item.KeySize))
	// Value Size
	binary.LittleEndian.PutUint32(buf[12:16], uint32(item.ValueSize))
	// Offset
	binary.LittleEndian.PutUint64(buf[16:24], uint64(item.Offset))
	// Key
	copy(buf[hintItemHeaderSize:], item.Key)

	return buf, nil
}

func writeHintFileEntry(hintFile *os.File, item *HintItem) error {
	encodedItem, err := EncodeHintItem(item)
	if err != nil {
		return err
	}
	_, err = hintFile.Write(encodedItem)
	if err != nil {
		return fmt.Errorf("Failed to write hint item for key '%s' to hint file: %w", string(item.Key), err)
	}

	return nil
}

// readHintFile reads all hint entries from the hint file for the given file ID.
// Returns the entries and nil error, or nil and an error if the file cannot be read.
// Returns os.ErrNotExist if the hint file does not exist.
func readHintFile(dataDir string, fileId int64) ([]*HintItem, error) {
	f, err := openHintFile(dataDir, fileId)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var items []*HintItem
	header := make([]byte, hintItemHeaderSize)

	for {
		_, err := io.ReadFull(f, header)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading hint header for file %d: %w", fileId, err)
		}

		timestamp := int64(binary.LittleEndian.Uint64(header[0:8]))
		keySize := int32(binary.LittleEndian.Uint32(header[8:12]))
		valueSize := int32(binary.LittleEndian.Uint32(header[12:16]))
		offset := int64(binary.LittleEndian.Uint64(header[16:24]))

		key := make([]byte, keySize)
		_, err = io.ReadFull(f, key)
		if err != nil {
			return nil, fmt.Errorf("error reading hint key for file %d: %w", fileId, err)
		}

		items = append(items, &HintItem{
			Timestamp: timestamp,
			KeySize:   keySize,
			ValueSize: valueSize,
			Offset:    offset,
			Key:       key,
		})
	}

	return items, nil
}
