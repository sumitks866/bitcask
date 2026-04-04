package kv

import "errors"

const (
	defaultDataDir = "./data"
	fileExt        = ".data"
	hintFileExt    = ".hint"
	filePerm       = 0644
	headerSize     = 20 // crc(4) + timestamp(8) + keySize(4) + valueSize(4)
)

var (
	Tombstone      = []byte{} // empty value indicates deletion
	ErrKeyNotFound = errors.New("key not found")
)
