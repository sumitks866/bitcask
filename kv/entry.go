package kv

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
)

type Entry struct {
	Key       []byte
	Value     []byte
	Timestamp int64
}

func EncodeEntry(entry *Entry) ([]byte, error) {
	keySize := int32(len(entry.Key))
	valueSize := int32(len(entry.Value)) // 0 for delete entries

	// total size = crc(4) + timestamp(8) + keySize(4) + valueSize(4) = (20) + key + value
	totalSize := headerSize + keySize + valueSize
	buf := make([]byte, totalSize)

	// Timestamp
	binary.LittleEndian.PutUint64(buf[4:12], uint64(entry.Timestamp))
	// Key Size
	binary.LittleEndian.PutUint32(buf[12:16], uint32(keySize))
	// Value Size
	binary.LittleEndian.PutUint32(buf[16:20], uint32(valueSize))

	// key and value
	copy(buf[headerSize:headerSize+keySize], entry.Key)
	copy(buf[headerSize+keySize:], entry.Value)

	// compute CRC32 checksum for the entire entry (excluding the first 4 bytes reserved for CRC)
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], crc)

	return buf, nil
}

// Returns the entry, total bytes read (header + key + value), and error if any
func ReadEntry(file *os.File, offset int64) (*Entry, int32, error) {
	// read header first (20 bytes)
	header := make([]byte, headerSize)
	n, err := file.ReadAt(header, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("Error reading entry header: %w", err)
	}
	if n < headerSize {
		return nil, 0, fmt.Errorf("Incomplete entry header")
	}

	// parse header
	crc := int32(binary.LittleEndian.Uint32(header[0:4]))
	timestamp := int64(binary.LittleEndian.Uint64(header[4:12]))
	keySize := int32(binary.LittleEndian.Uint32(header[12:16]))
	valueSize := int32(binary.LittleEndian.Uint32(header[16:20]))

	dataSize := keySize + valueSize
	data := make([]byte, dataSize)
	n, err = file.ReadAt(data, offset+headerSize)
	if err != nil {
		return nil, 0, fmt.Errorf("Error reading entry data: %w", err)
	}
	if int32(n) < dataSize {
		return nil, 0, fmt.Errorf("Incomplete entry data")
	}

	// verify CRC
	checkBuf := make([]byte, headerSize+dataSize-4) // exclude CRC field
	copy(checkBuf[0:16], header[4:20])              // copy header (timestamp + keysize + valuesize)
	copy(checkBuf[16:], data)                       // key + value

	calculatedCrc := crc32.ChecksumIEEE(checkBuf)
	if int32(calculatedCrc) != crc {
		return nil, 0, fmt.Errorf("CRC mismatch: expected %d, got %d", crc, int32(calculatedCrc))
	}

	entry := &Entry{
		Key:       data[0:keySize],
		Value:     data[keySize:],
		Timestamp: timestamp,
	}

	return entry, headerSize + dataSize, nil
}
