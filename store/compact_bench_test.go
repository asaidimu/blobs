package store_test

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/asaidimu/blobs/index/backend"
	"github.com/asaidimu/blobs/object"
	"github.com/asaidimu/blobs/store"
)

// setupCompactBenchStore creates a store with a small chunk size so a
// single moderately-sized blob is split into chunkCount chunks — standing
// in for a multi-gigabyte streamed blob, scaled down so the benchmark
// runs quickly. It writes the blob, then deletes it, so RefCount drops to
// zero and compactPhase1 has real work to do resolving and reclaiming
// every one of that blob's chunks.
func setupCompactBenchStore(b *testing.B, chunkCount int) (*store.Store, *store.NamespaceHandle) {
	const chunkSize = 1024
	be, err := backend.Open(backend.Options{Path: filepath.Join(b.TempDir(), "bench.db")})
	if err != nil {
		b.Fatalf("open bbolt backend: %v", err)
	}
	b.Cleanup(func() { be.Close() })

	s, err := store.Open(store.Config{
		DataDir:        b.TempDir(),
		Index:          be,
		ChunkSize:      chunkSize,
		MaxSegmentSize: chunkSize * int64(chunkCount) * 4, // one segment, never seals mid-blob
	})
	if err != nil {
		b.Fatalf("store.Open: %v", err)
	}
	b.Cleanup(func() { s.Close() })

	ctx := context.Background()
	if err := s.CreateNamespace(ctx, object.Namespace{ID: "bench-ns"}); err != nil {
		b.Fatalf("CreateNamespace: %v", err)
	}
	ns := s.Namespace("bench-ns")

	payload := make([]byte, chunkSize*int64(chunkCount))
	for i := range payload {
		payload[i] = byte(i)
	}
	if _, err := ns.Put(ctx, "big-blob", bytes.NewReader(payload), store.PutOptions{}); err != nil {
		b.Fatalf("Put: %v", err)
	}
	if err := ns.Delete(ctx, "big-blob"); err != nil {
		b.Fatalf("Delete: %v", err)
	}

	return s, ns
}

func benchmarkCompact(b *testing.B, chunkCount int) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s, ns := setupCompactBenchStore(b, chunkCount)
		b.StartTimer()

		if _, err := ns.Compact(context.Background()); err != nil {
			b.Fatalf("Compact: %v", err)
		}

		b.StopTimer()
		s.Close()
		b.StartTimer()
	}
}

func BenchmarkCompact_100chunks(b *testing.B) { benchmarkCompact(b, 100) }
func BenchmarkCompact_500chunks(b *testing.B) { benchmarkCompact(b, 500) }
