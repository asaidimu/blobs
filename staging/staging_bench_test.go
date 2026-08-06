package staging

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"testing"
)

func benchManager(b *testing.B) *Manager {
	b.Helper()
	m, err := NewManager(b.TempDir())
	if err != nil {
		b.Fatalf("NewManager: %v", err)
	}
	return m
}

func benchPayload(b *testing.B, size int) []byte {
	b.Helper()
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*31 + (i >> 8))
	}
	return data
}

func hexSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// ── Begin ─────────────────────────────────────────────────────────────────────

func BenchmarkBegin(b *testing.B) {
	m := benchManager(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sess, err := m.Begin(ctx, "ns", "key", BeginOptions{})
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		m.Abort(sess.ID)
		b.StartTimer()
	}
}

func BenchmarkBegin_Aligned(b *testing.B) {
	m := benchManager(b)
	ctx := context.Background()
	const blockSize = 1 << 20
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sess, err := m.Begin(ctx, "ns", "key", BeginOptions{
			ExpectedSize: 64 * blockSize,
			BlockSize:    blockSize,
		})
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		m.Abort(sess.ID)
		b.StartTimer()
	}
}

// ── WriteChunk: block-aligned path ────────────────────────────────────────────

// BenchmarkWriteChunk_Aligned measures a single block-aligned chunk write of
// 1 MiB into a fresh session each iteration.
func BenchmarkWriteChunk_Aligned(b *testing.B) {
	m := benchManager(b)
	ctx := context.Background()
	const blockSize = 1 << 20
	data := benchPayload(b, blockSize)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		sess, err := m.Begin(ctx, "ns", "key", BeginOptions{
			ExpectedSize: 64 * blockSize,
			BlockSize:    blockSize,
		})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if _, err := m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(data), ""); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		m.Abort(sess.ID)
	}
}

// BenchmarkWriteChunk_Aligned_Checksum adds per-chunk SHA-256 verification on
// top of the aligned write path.
func BenchmarkWriteChunk_Aligned_Checksum(b *testing.B) {
	m := benchManager(b)
	ctx := context.Background()
	const blockSize = 1 << 20
	data := benchPayload(b, blockSize)
	sum := hexSHA256(data)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		sess, err := m.Begin(ctx, "ns", "key", BeginOptions{
			ExpectedSize: 64 * blockSize,
			BlockSize:    blockSize,
		})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if _, err := m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(data), sum); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		m.Abort(sess.ID)
	}
}

// BenchmarkWriteChunk_Aligned_PieceHashes verifies one piece hash per block.
func BenchmarkWriteChunk_Aligned_PieceHashes(b *testing.B) {
	m := benchManager(b)
	ctx := context.Background()
	const blockSize = 1 << 20
	data := benchPayload(b, blockSize)
	pieces := make([]string, 64)
	for i := range pieces {
		pieces[i] = hexSHA256(data)
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		sess, err := m.Begin(ctx, "ns", "key", BeginOptions{
			ExpectedSize: 64 * blockSize,
			BlockSize:    blockSize,
			PieceHashes:  pieces,
		})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if _, err := m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(data), ""); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		m.Abort(sess.ID)
	}
}

// BenchmarkWriteChunk_Aligned_ManyChunks writes 64 sequential 64 KiB chunks
// into one session — stresses per-chunk open/close of the data file.
func BenchmarkWriteChunk_Aligned_ManyChunks(b *testing.B) {
	m := benchManager(b)
	ctx := context.Background()
	const blockSize = 64 << 10
	const chunks = 64
	data := benchPayload(b, blockSize)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		sess, err := m.Begin(ctx, "ns", "key", BeginOptions{
			ExpectedSize: blockSize * chunks,
			BlockSize:    blockSize,
		})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		for c := 0; c < chunks; c++ {
			if _, err := m.WriteChunk(ctx, sess.ID, int64(c*blockSize), bytes.NewReader(data), ""); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		m.Abort(sess.ID)
	}
}

// ── WriteChunk: range-set path ────────────────────────────────────────────────

func BenchmarkWriteChunk_RangeSet(b *testing.B) {
	m := benchManager(b)
	ctx := context.Background()
	const chunkSize = 1 << 20
	data := benchPayload(b, chunkSize)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		sess, err := m.Begin(ctx, "ns", "key", BeginOptions{ExpectedSize: 64 * chunkSize})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if _, err := m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(data), ""); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		m.Abort(sess.ID)
	}
}

func BenchmarkWriteChunk_RangeSet_Checksum(b *testing.B) {
	m := benchManager(b)
	ctx := context.Background()
	const chunkSize = 1 << 20
	data := benchPayload(b, chunkSize)
	sum := hexSHA256(data)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		sess, err := m.Begin(ctx, "ns", "key", BeginOptions{ExpectedSize: 64 * chunkSize})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if _, err := m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(data), sum); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		m.Abort(sess.ID)
	}
}

// BenchmarkWriteChunk_RangeSet_ManyChunks writes 64 sequential 64 KiB chunks —
// each add re-sorts the range set (O(n log n) per insert).
func BenchmarkWriteChunk_RangeSet_ManyChunks(b *testing.B) {
	m := benchManager(b)
	ctx := context.Background()
	const chunkSize = 64 << 10
	const chunks = 64
	data := benchPayload(b, chunkSize)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		sess, err := m.Begin(ctx, "ns", "key", BeginOptions{ExpectedSize: chunkSize * chunks})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		for c := 0; c < chunks; c++ {
			if _, err := m.WriteChunk(ctx, sess.ID, int64(c*chunkSize), bytes.NewReader(data), ""); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		m.Abort(sess.ID)
	}
}

// ── Complete ──────────────────────────────────────────────────────────────────

func BenchmarkComplete_Aligned(b *testing.B) {
	m := benchManager(b)
	ctx := context.Background()
	const blockSize = 1 << 20
	data := benchPayload(b, blockSize)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		sess, err := m.Begin(ctx, "ns", "key", BeginOptions{
			ExpectedSize: blockSize,
			BlockSize:    blockSize,
		})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(data), ""); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		cu, err := m.Complete(ctx, sess.ID)
		if err != nil {
			b.Fatal(err)
		}
		cu.Close()
		b.StopTimer()
		m.Abort(sess.ID)
		b.StartTimer()
	}
}

func BenchmarkComplete_RangeSet(b *testing.B) {
	m := benchManager(b)
	ctx := context.Background()
	const chunkSize = 1 << 20
	data := benchPayload(b, chunkSize)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		sess, err := m.Begin(ctx, "ns", "key", BeginOptions{ExpectedSize: chunkSize})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(data), ""); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		cu, err := m.Complete(ctx, sess.ID)
		if err != nil {
			b.Fatal(err)
		}
		cu.Close()
		b.StopTimer()
		m.Abort(sess.ID)
		b.StartTimer()
	}
}

// BenchmarkComplete_RangeSet_WholeFileChecksum fsyncs + re-reads the whole file
// to verify the expected SHA-256.
func BenchmarkComplete_RangeSet_WholeFileChecksum(b *testing.B) {
	m := benchManager(b)
	ctx := context.Background()
	const chunkSize = 4 << 20
	data := benchPayload(b, chunkSize)
	sum := hexSHA256(data)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		sess, err := m.Begin(ctx, "ns", "key", BeginOptions{ExpectedSize: chunkSize, ExpectedSHA256: sum})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(data), ""); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		cu, err := m.Complete(ctx, sess.ID)
		if err != nil {
			b.Fatal(err)
		}
		cu.Close()
		b.StopTimer()
		m.Abort(sess.ID)
		b.StartTimer()
	}
}

// ── Concurrent aligned writes ─────────────────────────────────────────────────

// BenchmarkWriteChunk_Aligned_Concurrent hammers one session from 8 goroutines
// writing disjoint blocks, exercising the lock-free bitmap path.
func BenchmarkWriteChunk_Aligned_Concurrent(b *testing.B) {
	m := benchManager(b)
	ctx := context.Background()
	const blockSize = 1 << 20
	const workers = 8
	const blocksPerWorker = 8
	data := benchPayload(b, blockSize)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		sess, err := m.Begin(ctx, "ns", "key", BeginOptions{
			ExpectedSize: blockSize * workers * blocksPerWorker,
			BlockSize:    blockSize,
		})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		var wg sync.WaitGroup
		errs := make(chan error, workers)
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for c := 0; c < blocksPerWorker; c++ {
					off := int64((w*blocksPerWorker + c) * blockSize)
					if _, err := m.WriteChunk(ctx, sess.ID, off, bytes.NewReader(data), ""); err != nil {
						errs <- fmt.Errorf("worker %d chunk %d: %w", w, c, err)
						return
					}
				}
			}(w)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			b.Fatal(err)
		}
		b.StopTimer()
		m.Abort(sess.ID)
	}
}

// ── Large-file end-to-end (throughput) ────────────────────────────────────────

// BenchmarkWriteChunk_Aligned_Throughput streams a full 64 MiB file through
// the aligned path one 4 MiB chunk at a time, and measures MiB/s via
// b.SetBytes.
func BenchmarkWriteChunk_Aligned_Throughput(b *testing.B) {
	m := benchManager(b)
	ctx := context.Background()
	const blockSize = 4 << 20
	const totalBlocks = 16
	payload := benchPayload(b, blockSize)

	b.SetBytes(blockSize * totalBlocks)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		sess, err := m.Begin(ctx, "ns", "key", BeginOptions{
			ExpectedSize: blockSize * totalBlocks,
			BlockSize:    blockSize,
		})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		var total int64
		for c := 0; c < totalBlocks; c++ {
			n, err := m.WriteChunk(ctx, sess.ID, int64(c*blockSize), bytes.NewReader(payload), "")
			if err != nil {
				b.Fatal(err)
			}
			total = n
		}
		if total != blockSize*totalBlocks {
			b.Fatalf("total = %d, want %d", total, blockSize*totalBlocks)
		}

		cu, err := m.Complete(ctx, sess.ID)
		if err != nil {
			b.Fatal(err)
		}
		// Verify a read-back to make the benchmark honest.
		if _, err := io.Copy(io.Discard, cu); err != nil {
			b.Fatal(err)
		}
		cu.Close()
		b.StopTimer()
		m.Abort(sess.ID)
	}
}
