package store_test

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/asaidimu/blobs/index/backend"
	"github.com/asaidimu/blobs/object"
	"github.com/asaidimu/blobs/store"
)

// BenchmarkChunkDedup_AppendedBlob measures the cost of putting blob B =
// blob A + suffix after blob A already exists. Without dedup, B's Put
// re-writes every shared chunk's bytes and index record; with chunk-level
// dedup, the shared chunks are resolved against the index and their
// duplicate physical copies are reaped. The benchmark uses a small chunk
// size (as setupCompactBenchStore does) so a modest payload splits into
// many chunks.
func BenchmarkChunkDedup_AppendedBlob(b *testing.B) {
	const chunkSize = 1024
	be, err := backend.Open(backend.Options{Path: filepath.Join(b.TempDir(), "bench.db")})
	if err != nil {
		b.Fatalf("open bbolt backend: %v", err)
	}
	b.Cleanup(func() { be.Close() })

	s, err := store.Open(store.Config{
		DataDir:   b.TempDir(),
		Index:     be,
		ChunkSize: chunkSize,
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

	rng := rand.New(rand.NewSource(1))
	base := make([]byte, 256*chunkSize)
	if _, err := rng.Read(base); err != nil {
		b.Fatal(err)
	}
	if _, err := ns.Put(ctx, "a", bytes.NewReader(base), store.PutOptions{}); err != nil {
		b.Fatalf("Put(a): %v", err)
	}

	suffix := make([]byte, 64*chunkSize)
	if _, err := rng.Read(suffix); err != nil {
		b.Fatal(err)
	}
	full := append(append([]byte{}, base...), suffix...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ns.Put(ctx, "b", bytes.NewReader(full), store.PutOptions{}); err != nil {
			b.Fatalf("Put(b): %v", err)
		}
		rc, err := ns.Get(ctx, "b")
		if err != nil {
			b.Fatalf("Get(b): %v", err)
		}
		if _, err := io.ReadAll(rc); err != nil {
			rc.Close()
			b.Fatalf("ReadAll(b): %v", err)
		}
		rc.Close()
		if err := ns.Delete(ctx, "b"); err != nil {
			b.Fatalf("Delete(b): %v", err)
		}
	}
}
