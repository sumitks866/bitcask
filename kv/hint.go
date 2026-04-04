package kv

import (
	"encoding/binary"
	"fmt"
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

func (kv *KVStore) writeHintFileEntry(hintFile *os.File, item *HintItem) error {
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
