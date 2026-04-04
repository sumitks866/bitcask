package bench

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync/atomic"
	"testing"

	bitcask "github.com/sumitks866/bitcask"
)

// setupDB creates a fresh DB for benchmarking, using a temp data dir.
func setupDB(b *testing.B) (bitcask.DB, func()) {
	b.Helper()
	log.SetOutput(io.Discard) // silence internal logs during benchmarks
	// point data dir to a temp location so benchmarks don't pollute ./data
	if err := os.MkdirAll("testdata", 0755); err != nil {
		b.Fatalf("failed to create testdata dir: %v", err)
	}
	if err := os.Chdir("testdata"); err != nil {
		b.Fatalf("failed to chdir: %v", err)
	}
	db, err := bitcask.NewDB()
	if err != nil {
		b.Fatalf("NewDB: %v", err)
	}
	cleanup := func() {
		// go back up and remove leftover data files
		_ = os.Chdir("..")
		_ = os.RemoveAll("testdata/data")
	}
	return db, cleanup
}

// BenchmarkPut measures raw write throughput and per-op latency.
func BenchmarkPut(b *testing.B) {
	db, cleanup := setupDB(b)
	defer cleanup()

	key := []byte("bench-key")
	value := []byte("bench-value-1234567890")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		if err := db.Put(k, value); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
		_ = key
	}
}

// BenchmarkGet measures read throughput and per-op latency for keys already written.
func BenchmarkGet(b *testing.B) {
	db, cleanup := setupDB(b)
	defer cleanup()

	// Pre-populate N keys
	const preload = 10_000
	value := []byte("bench-value-1234567890")
	for i := 0; i < preload; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		if err := db.Put(k, value); err != nil {
			b.Fatalf("Put (preload) failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k := []byte(fmt.Sprintf("key-%d", i%preload))
		if _, err := db.Get(k); err != nil {
			b.Fatalf("Get failed: %v", err)
		}
	}
}

// BenchmarkPutGet measures mixed read/write throughput (50/50).
func BenchmarkPutGet(b *testing.B) {
	db, cleanup := setupDB(b)
	defer cleanup()

	value := []byte("bench-value-1234567890")
	// pre-write some keys so Get has something to read at the start
	const preload = 1000
	for i := 0; i < preload; i++ {
		_ = db.Put([]byte(fmt.Sprintf("key-%d", i)), value)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		if i%2 == 0 {
			if err := db.Put(k, value); err != nil {
				b.Fatalf("Put failed: %v", err)
			}
		} else {
			if _, err := db.Get([]byte(fmt.Sprintf("key-%d", i%preload))); err != nil {
				b.Fatalf("Get failed: %v", err)
			}
		}
	}
}

// BenchmarkConcurrentReads measures read throughput under parallel readers.
func BenchmarkConcurrentReads(b *testing.B) {
	db, cleanup := setupDB(b)
	defer cleanup()

	// Pre-populate N keys
	const preload = 20_000
	value := []byte("bench-value-1234567890")
	for i := 0; i < preload; i++ {
		k := []byte(fmt.Sprintf("key-%d", i))
		if err := db.Put(k, value); err != nil {
			b.Fatalf("Put (preload) failed: %v", err)
		}
	}

	var counter uint64
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddUint64(&counter, 1)
			k := []byte(fmt.Sprintf("key-%d", i%preload))
			if _, err := db.Get(k); err != nil {
				b.Fatalf("Get failed: %v", err)
			}
		}
	})
}
