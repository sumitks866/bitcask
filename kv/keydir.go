package kv

type IndexItem struct {
	FileId    int64
	Offset    int64
	Size      int32 // total size of entry in the data file (including header and key/value)
	Timestamp int64
}

type KeyDir struct {
	index map[string]*IndexItem
}

func NewKeyDir() *KeyDir {
	return &KeyDir{
		index: make(map[string]*IndexItem),
	}
}

func (kd *KeyDir) Put(key string, item *IndexItem) {
	kd.index[key] = item
}

func (kd *KeyDir) Get(key string) (*IndexItem, bool) {
	item, ok := kd.index[key]
	return item, ok
}

func (kd *KeyDir) Delete(key string) {
	delete(kd.index, key)
}
