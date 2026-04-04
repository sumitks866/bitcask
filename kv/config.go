package kv

import "time"

type KVStoreConfig struct {
	DataDir            *string
	MaxFileSize        *int64
	SyncOnPut          *bool
	CompactionInterval *time.Duration
}

func (cfg *KVStoreConfig) applyDefaults() {
	if cfg.DataDir == nil {
		d := defaultDataDir
		cfg.DataDir = &d
	}
	if cfg.MaxFileSize == nil {
		defaultSize := int64(1024 * 512) // 512 KB
		cfg.MaxFileSize = &defaultSize
	}
	if cfg.SyncOnPut == nil {
		defaultSync := false
		cfg.SyncOnPut = &defaultSync
	}
	if cfg.CompactionInterval == nil {
		defaultInterval := time.Minute * 30
		cfg.CompactionInterval = &defaultInterval
	}
}
