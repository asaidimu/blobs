package index_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/asaidimu/blobs/index"
	"github.com/asaidimu/blobs/index/backend"
	"github.com/asaidimu/blobs/object"
)

// seedChunks writes n chunk entries for a synthetic blob and returns their
// IDs, for benchmarking chunk-location resolution at realistic counts (a
// multi-gigabyte blob chunked at a few MB per chunk easily reaches a few
// hundred chunks).
func seedChunks(b *testing.B, idx *index.Index, n int) []object.ChunkID {
	ctx := context.Background()
	ids := make([]object.ChunkID, n)
	for i := 0; i < n; i++ {
		id := object.ChunkID(fmt.Sprintf("chunk-%06d", i))
		ids[i] = id
		if err := idx.PutChunk(ctx, object.ChunkEntry{
			ChunkID:     id,
			BlobID:      "blob-0",
			NamespaceID: "bench",
			PageOffset:  int64(i) * 4096,
			PageCount:   1,
			Length:      4096,
			Seq:         i,
		}); err != nil {
			b.Fatalf("seed chunk %d: %v", i, err)
		}
	}
	return ids
}

// loopGetChunk resolves ids the OLD way: one idx.GetChunk call per id.
// This is exactly the pattern store.go's getChunkReader and Verify used
// before the batching fix.
func loopGetChunk(b *testing.B, idx *index.Index, ids []object.ChunkID) {
	ctx := context.Background()
	for _, id := range ids {
		if _, err := idx.GetChunk(ctx, id); err != nil {
			b.Fatalf("GetChunk(%s): %v", id, err)
		}
	}
}

func benchmarkChunkResolution(b *testing.B, idx *index.Index, chunkCount int) {
	ids := seedChunks(b, idx, chunkCount)
	ctx := context.Background()

	b.Run("Loop_GetChunk_per_chunk", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			loopGetChunk(b, idx, ids)
		}
	})

	b.Run("Batched_GetChunks", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := idx.GetChunks(ctx, ids); err != nil {
				b.Fatalf("GetChunks: %v", err)
			}
		}
	})
}

func BenchmarkChunkResolution_Memory_100chunks(b *testing.B) {
	idx := index.New(index.NewMemoryBackend())
	defer idx.Close()
	benchmarkChunkResolution(b, idx, 100)
}

func BenchmarkChunkResolution_Memory_500chunks(b *testing.B) {
	idx := index.New(index.NewMemoryBackend())
	defer idx.Close()
	benchmarkChunkResolution(b, idx, 500)
}

func BenchmarkChunkResolution_Bbolt_100chunks(b *testing.B) {
	be, err := backend.Open(backend.Options{Path: filepath.Join(b.TempDir(), "bench.db")})
	if err != nil {
		b.Fatalf("open bbolt backend: %v", err)
	}
	defer be.Close()
	idx := index.New(be)
	defer idx.Close()
	benchmarkChunkResolution(b, idx, 100)
}

func BenchmarkChunkResolution_Bbolt_500chunks(b *testing.B) {
	be, err := backend.Open(backend.Options{Path: filepath.Join(b.TempDir(), "bench.db")})
	if err != nil {
		b.Fatalf("open bbolt backend: %v", err)
	}
	defer be.Close()
	idx := index.New(be)
	defer idx.Close()
	benchmarkChunkResolution(b, idx, 500)
}
