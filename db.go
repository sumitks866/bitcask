package bitcask

import (
	"log"
	"time"

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
	start := time.Now()
	err := d.store.Put(key, value)
	duration := time.Since(start)

	if err != nil {
		log.Printf("ERROR: db.Put key=%q latency=%dµs err=%v", string(key), duration.Microseconds(), err)
		return err
	}

	log.Printf("INFO: db.Put key=%q latency=%dµs", string(key), duration.Microseconds())
	return nil
}

func (d *db) Get(key []byte) ([]byte, error) {
	start := time.Now()
	value, err := d.store.Get(key)
	duration := time.Since(start)

	if err != nil {
		log.Printf("ERROR: db.Get key=%q latency=%dµs err=%v", string(key), duration.Microseconds(), err)
		return nil, err
	}

	log.Printf("INFO: db.Get key=%q latency=%dµs", string(key), duration.Microseconds())
	return value, nil
}

func (d *db) Delete(key []byte) error {
	return nil
}
