package kv

const (
	dataDir    = "./data"
	fileExt    = ".data"
	filePerm   = 0644
	headerSize = 20 // crc(4) + timestamp(8) + keySize(4) + valueSize(4)
)
