package kv

import (
	"fmt"
	"io"
	"log"
	"os"
	"testing"
	"time"
)

// ptr returns a pointer to v (convenience for KVStoreConfig fields).
func ptr[T any](v T) *T { return &v }

// newTestKV creates a KVStore in a fresh temp directory.
// Periodic compaction is disabled (24h interval).
// The returned cleanup function closes the store and restores the cwd.
func newTestKV(t testing.TB, maxFileSize int64) (*KVStore, func()) {
	t.Helper()
	log.SetOutput(io.Discard)

	tmpDir := t.TempDir()
	orig, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir into tmpDir: %v", err)
	}

	kv, err := NewKVStore(&KVStoreConfig{
		MaxFileSize:        ptr(maxFileSize),
		SyncOnPut:          ptr(false),
		CompactionInterval: ptr(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("NewKVStore: %v", err)
	}

	return kv, func() {
		kv.Close()
		_ = os.Chdir(orig)
	}
}

// mustPut calls kv.Put and fails the test on error.
func mustPut(t testing.TB, kv *KVStore, key, value string) {
	t.Helper()
	if err := kv.Put([]byte(key), []byte(value)); err != nil {
		t.Fatalf("Put(%q, %q): %v", key, value, err)
	}
}

// mustDelete calls kv.Delete and fails the test on error.
func mustDelete(t testing.TB, kv *KVStore, key string) {
	t.Helper()
	if err := kv.Delete([]byte(key)); err != nil {
		t.Fatalf("Delete(%q): %v", key, err)
	}
}

// mustGet asserts Get returns the expected value.
func mustGet(t testing.TB, kv *KVStore, key, wantValue string) {
	t.Helper()
	got, err := kv.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if string(got) != wantValue {
		t.Errorf("Get(%q) = %q, want %q", key, got, wantValue)
	}
}

// mustNotExist asserts that Get returns an error (key absent or deleted).
func mustNotExist(t testing.TB, kv *KVStore, key string) {
	t.Helper()
	_, err := kv.Get([]byte(key))
	if err == nil {
		t.Errorf("Get(%q) succeeded; expected key to be absent", key)
	}
}

// dataFileCount returns the number of .data files currently on disk.
func dataFileCount(t testing.TB) int {
	t.Helper()
	ids, err := getSortedDataFileIds(defaultDataDir)
	if err != nil {
		t.Fatalf("getSortedDataFileIds: %v", err)
	}
	return len(ids)
}

// fillAndRotate writes n unique large-value entries to trigger file rotation.
func fillAndRotate(t testing.TB, kv *KVStore, prefix string, n int) {
	t.Helper()
	value := make([]byte, 50)
	for i := 0; i < n; i++ {
		if err := kv.Put([]byte(fmt.Sprintf("%s-%d", prefix, i)), value); err != nil {
			t.Fatalf("Put(%s-%d): %v", prefix, i, err)
		}
	}
}

// ── Unit Tests ────────────────────────────────────────────────────────────────

// TestCompact_NoEligibleFiles: only the active file exists → no-op.
func TestCompact_NoEligibleFiles(t *testing.T) {
	kv, cleanup := newTestKV(t, 512*1024)
	defer cleanup()

	mustPut(t, kv, "k1", "v1")
	mustPut(t, kv, "k2", "v2")

	before := dataFileCount(t)

	if err := kv.Compact(); err != nil {
		t.Fatalf("Compact() error: %v", err)
	}

	after := dataFileCount(t)
	if after != before {
		t.Errorf("file count changed %d → %d; expected no change", before, after)
	}
	mustGet(t, kv, "k1", "v1")
	mustGet(t, kv, "k2", "v2")
}

// TestCompact_DataIntegrityAfterCompaction: all live keys must return the most
// recent value after compaction runs.
func TestCompact_DataIntegrityAfterCompaction(t *testing.T) {
	// ~80-byte entries, 300-byte file → ~3 entries per file → rotation every 3 writes.
	kv, cleanup := newTestKV(t, 300)
	defer cleanup()

	// Write initial values.
	for i := 0; i < 10; i++ {
		mustPut(t, kv, fmt.Sprintf("key-%d", i), "original")
	}
	// Overwrite half the keys — old entries are now stale.
	for i := 0; i < 5; i++ {
		mustPut(t, kv, fmt.Sprintf("key-%d", i), "updated")
	}
	// Push the overwritten entries out of the active file.
	fillAndRotate(t, kv, "extra", 10)

	if err := kv.Compact(); err != nil {
		t.Fatalf("Compact() error: %v", err)
	}

	for i := 0; i < 5; i++ {
		mustGet(t, kv, fmt.Sprintf("key-%d", i), "updated")
	}
	for i := 5; i < 10; i++ {
		mustGet(t, kv, fmt.Sprintf("key-%d", i), "original")
	}
	for i := 0; i < 10; i++ {
		if _, err := kv.Get([]byte(fmt.Sprintf("extra-%d", i))); err != nil {
			t.Errorf("Get(extra-%d): %v", i, err)
		}
	}
}

// TestCompact_StaleEntriesReclaimed: after compaction the number of data files
// on disk must decrease when all entries in old files have been migrated.
func TestCompact_StaleEntriesReclaimed(t *testing.T) {
	kv, cleanup := newTestKV(t, 300)
	defer cleanup()

	// Write 20 keys → several old files.
	for i := 0; i < 20; i++ {
		mustPut(t, kv, fmt.Sprintf("k-%d", i), "val-original")
	}
	// Overwrite all 20 → every entry in the old files is stale.
	for i := 0; i < 20; i++ {
		mustPut(t, kv, fmt.Sprintf("k-%d", i), "val-updated")
	}
	// Ensure the last batch is not the active file.
	fillAndRotate(t, kv, "pad", 5)

	before := dataFileCount(t)
	if before < 2 {
		t.Fatalf("wanted ≥2 data files before compaction, got %d", before)
	}

	if err := kv.Compact(); err != nil {
		t.Fatalf("Compact() error: %v", err)
	}

	after := dataFileCount(t)
	if after >= before {
		t.Errorf("expected fewer files after compaction: before=%d after=%d", before, after)
	}

	for i := 0; i < 20; i++ {
		mustGet(t, kv, fmt.Sprintf("k-%d", i), "val-updated")
	}
}

// TestCompact_DeletedKeysNotCopied: keys deleted before compaction must not
// appear in the merged file and must remain absent afterwards.
func TestCompact_DeletedKeysNotCopied(t *testing.T) {
	kv, cleanup := newTestKV(t, 300)
	defer cleanup()

	// Write 10 keys to be deleted and 5 to survive.
	for i := 0; i < 10; i++ {
		mustPut(t, kv, fmt.Sprintf("del-%d", i), "to-be-deleted")
	}
	for i := 0; i < 5; i++ {
		mustPut(t, kv, fmt.Sprintf("keep-%d", i), "survivor")
	}
	// Delete the first 10 keys.
	for i := 0; i < 10; i++ {
		mustDelete(t, kv, fmt.Sprintf("del-%d", i))
	}
	// Ensure everything is in non-active files.
	fillAndRotate(t, kv, "pad", 5)

	if err := kv.Compact(); err != nil {
		t.Fatalf("Compact() error: %v", err)
	}

	for i := 0; i < 10; i++ {
		mustNotExist(t, kv, fmt.Sprintf("del-%d", i))
	}
	for i := 0; i < 5; i++ {
		mustGet(t, kv, fmt.Sprintf("keep-%d", i), "survivor")
	}
}

// TestCompact_MultipleRounds: calling Compact() twice is safe and idempotent.
func TestCompact_MultipleRounds(t *testing.T) {
	kv, cleanup := newTestKV(t, 300)
	defer cleanup()

	for i := 0; i < 15; i++ {
		mustPut(t, kv, fmt.Sprintf("key-%d", i), "v1")
	}
	for i := 0; i < 15; i++ {
		mustPut(t, kv, fmt.Sprintf("key-%d", i), "v2")
	}
	fillAndRotate(t, kv, "pad", 5)

	if err := kv.Compact(); err != nil {
		t.Fatalf("Compact() round 1: %v", err)
	}
	if err := kv.Compact(); err != nil {
		t.Fatalf("Compact() round 2: %v", err)
	}

	for i := 0; i < 15; i++ {
		mustGet(t, kv, fmt.Sprintf("key-%d", i), "v2")
	}
}

// TestCompact_ActiveFileNotCompacted: when there is only one data file (the
// active file), Compact() must leave it untouched.
func TestCompact_ActiveFileNotCompacted(t *testing.T) {
	kv, cleanup := newTestKV(t, 512*1024) // large → no rotation
	defer cleanup()

	for i := 0; i < 20; i++ {
		mustPut(t, kv, fmt.Sprintf("active-%d", i), "live")
	}
	if dataFileCount(t) != 1 {
		t.Skip("unexpected rotation; test assumes a single data file")
	}

	if err := kv.Compact(); err != nil {
		t.Fatalf("Compact() error: %v", err)
	}

	for i := 0; i < 20; i++ {
		mustGet(t, kv, fmt.Sprintf("active-%d", i), "live")
	}
	if got := dataFileCount(t); got != 1 {
		t.Errorf("expected 1 data file after no-op compact, got %d", got)
	}
}

// TestCompact_ConcurrentReadsAreSafe: Get calls issued while Compact() runs
// in a goroutine must all succeed without error.
func TestCompact_ConcurrentReadsAreSafe(t *testing.T) {
	kv, cleanup := newTestKV(t, 300)
	defer cleanup()

	const n = 30
	for i := 0; i < n; i++ {
		mustPut(t, kv, fmt.Sprintf("c-%d", i), "value-new")
	}
	fillAndRotate(t, kv, "pad", 5)

	compactDone := make(chan struct{})
	go func() {
		defer close(compactDone)
		_ = kv.Compact()
	}()

	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			_, err := kv.Get([]byte(fmt.Sprintf("c-%d", idx)))
			errCh <- err
		}(i)
	}
	<-compactDone

	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Errorf("concurrent Get error: %v", err)
		}
	}
}

// ── Benchmarks ────────────────────────────────────────────────────────────────

// BenchmarkCompact times Compact() against a dataset with significant stale data
// spread across multiple old files.
func BenchmarkCompact(b *testing.B) {
	log.SetOutput(io.Discard)
	const (
		numKeys     = 200
		overwrites  = 3
		maxFileSize = 1024
	)

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		tmpDir, err := os.MkdirTemp("", "bitcask-bench-compact-*")
		if err != nil {
			b.Fatalf("MkdirTemp: %v", err)
		}
		orig, _ := os.Getwd()
		_ = os.Chdir(tmpDir)

		kv, err := NewKVStore(&KVStoreConfig{
			MaxFileSize:        ptr(int64(maxFileSize)),
			SyncOnPut:          ptr(false),
			CompactionInterval: ptr(24 * time.Hour),
		})
		if err != nil {
			b.Fatalf("NewKVStore: %v", err)
		}

		value := make([]byte, 50)
		for round := 0; round <= overwrites; round++ {
			for j := 0; j < numKeys; j++ {
				_ = kv.Put([]byte(fmt.Sprintf("key-%d", j)), value)
			}
		}
		// Push the last batch out of the active file.
		for j := 0; j < 20; j++ {
			_ = kv.Put([]byte(fmt.Sprintf("pad-%d", j)), value)
		}

		b.StartTimer()
		if err := kv.Compact(); err != nil {
			b.Fatalf("Compact: %v", err)
		}
		b.StopTimer()

		kv.Close()
		_ = os.Chdir(orig)
		_ = os.RemoveAll(tmpDir)
		b.StartTimer()
	}
}

// BenchmarkCompact_LargeDataset profiles compaction over a bigger key space.
func BenchmarkCompact_LargeDataset(b *testing.B) {
	log.SetOutput(io.Discard)
	const (
		numKeys     = 2000
		overwrites  = 2
		maxFileSize = 4096
	)

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		tmpDir, err := os.MkdirTemp("", "bitcask-bench-compact-large-*")
		if err != nil {
			b.Fatalf("MkdirTemp: %v", err)
		}
		orig, _ := os.Getwd()
		_ = os.Chdir(tmpDir)

		kv, err := NewKVStore(&KVStoreConfig{
			MaxFileSize:        ptr(int64(maxFileSize)),
			SyncOnPut:          ptr(false),
			CompactionInterval: ptr(24 * time.Hour),
		})
		if err != nil {
			b.Fatalf("NewKVStore: %v", err)
		}
		value := make([]byte, 50)
		for round := 0; round <= overwrites; round++ {
			for j := 0; j < numKeys; j++ {
				_ = kv.Put([]byte(fmt.Sprintf("key-%d", j)), value)
			}
		}
		for j := 0; j < 20; j++ {
			_ = kv.Put([]byte(fmt.Sprintf("pad-%d", j)), value)
		}

		b.StartTimer()
		if err := kv.Compact(); err != nil {
			b.Fatalf("Compact: %v", err)
		}
		b.StopTimer()

		kv.Close()
		_ = os.Chdir(orig)
		_ = os.RemoveAll(tmpDir)
		b.StartTimer()
	}
}
