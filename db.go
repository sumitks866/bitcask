package bitcask

import (
	"github.com/sumitks866/bitcask/kv"
)

type DB interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
}

type db struct {
	store *kv.KVStore
}

func NewDB() DB {
	return &db{
		store: kv.NewKVStore(),
	}
}

func (d *db) Put(key []byte, value []byte) error {
	return d.store.Put(key, value)
}

func (d *db) Get(key []byte) ([]byte, error) {
	return d.store.Get(key)
}

func (d *db) Delete(key []byte) error {
	return d.store.Put(key, []byte{})
}
