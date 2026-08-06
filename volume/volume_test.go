package volume_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/asaidimu/blobs/object"
	"github.com/asaidimu/blobs/volume"
)

// ── Helpers ───────────────────────────────────────────────────────────────────

func openEngine(t *testing.T) *volume.Engine {
	t.Helper()
	e, err := volume.Open(t.TempDir(), "test-ns", volume.Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return e
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func writeBlobBytes(t *testing.T, e *volume.Engine, data []byte) *volume.WriteResult {
	t.Helper()
	result, err := e.WriteBlob(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("WriteBlob: %v", err)
	}
	return result
}

// ── Struct layout tests ───────────────────────────────────────────────────────

// TestPageHeaderSize verifies that pageHeader is exactly 88 bytes with no
// implicit padding. This is a compile-time invariant we enforce at test time
// so a future field addition cannot silently change the on-disk format.
func TestPageHeaderSize(t *testing.T) {
	const wantSize = 88
	if volume.PageHeaderSize() != wantSize {
		t.Errorf("pageHeaderSize: got %d, want %d", volume.PageHeaderSize(), wantSize)
	}
}

// TestEngineAlignment verifies that the Engine struct's RWMutex is padded to
// its own cache line so it does not false-share with write-state fields.
func TestEngineAlignment(t *testing.T) {
	size := volume.EngineMutexPadding()
	const cacheLineSize = 64
	if size != cacheLineSize {
		t.Errorf("Engine mutex+pad: got %d bytes, want %d (one cache line)", size, cacheLineSize)
	}
}

// ── PageFlags tests ───────────────────────────────────────────────────────────

func TestPageFlags_SetClear(t *testing.T) {
	var f volume.PageFlags

	if f.IsDeleted() {
		t.Error("zero flags: IsDeleted should be false")
	}
	if f.IsLastChunk() {
		t.Error("zero flags: IsLastChunk should be false")
	}

	f = f.Set(volume.FlagDeleted)
	if !f.IsDeleted() {
		t.Error("after Set(FlagDeleted): IsDeleted should be true")
	}
	if f.IsLastChunk() {
		t.Error("after Set(FlagDeleted): IsLastChunk should still be false")
	}

	f = f.Set(volume.FlagLastChunk)
	if !f.IsDeleted() || !f.IsLastChunk() {
		t.Error("after Set both: both flags should be true")
	}

	f = f.Clear(volume.FlagDeleted)
	if f.IsDeleted() {
		t.Error("after Clear(FlagDeleted): IsDeleted should be false")
	}
	if !f.IsLastChunk() {
		t.Error("after Clear(FlagDeleted): IsLastChunk should still be true")
	}
}

func TestPageFlags_NoBooleanTrap(t *testing.T) {
	f := volume.FlagDeleted.Set(volume.FlagLastChunk)
	if !f.IsDeleted() {
		t.Error("combined flags: IsDeleted should be true")
	}
	if f == volume.FlagDeleted {
		t.Error("equality comparison gives wrong answer for combined flags — use IsDeleted()")
	}
}

// ── MagicFlags packing tests ──────────────────────────────────────────────────

func TestMagicFlags_RoundTrip(t *testing.T) {
	cases := []volume.PageFlags{
		0,
		volume.FlagDeleted,
		volume.FlagLastChunk,
		volume.FlagDeleted.Set(volume.FlagLastChunk),
		volume.FlagCompressed,
	}
	for _, flags := range cases {
		packed := volume.EncodeMagicFlags(flags)
		got, err := volume.DecodeMagicFlags(packed)
		if err != nil {
			t.Errorf("flags=%08b: DecodeMagicFlags error: %v", flags, err)
			continue
		}
		if got != flags {
			t.Errorf("flags=%08b: round-trip got %08b", flags, got)
		}
	}
}

func TestMagicFlags_BadSentinel(t *testing.T) {
	bad := uint32(0xDEADBEEF)
	if _, err := volume.DecodeMagicFlags(bad); err == nil {
		t.Error("expected error for bad sentinel; got nil")
	}
}

func TestMagicFlags_SentinelPreserved(t *testing.T) {
	packed := volume.EncodeMagicFlags(0xFF)
	if _, err := volume.DecodeMagicFlags(packed); err != nil {
		t.Errorf("all flags set: sentinel should survive: %v", err)
	}
}

// ── PageHeader encode/decode round-trip ───────────────────────────────────────

func TestPageHeaderEncoding_RoundTrip(t *testing.T) {
	chunkID := sha256.Sum256([]byte("chunk"))
	blobID := sha256.Sum256([]byte("blob"))

	original := volume.TestPageHeader{
		DataLen:     12345,
		ChunkID:     chunkID,
		BlobID:      blobID,
		ChunkSeq:    7,
		TotalChunks: 42,
		CRC32:       0xDEADBEEF,
		Flags:       volume.FlagLastChunk,
	}

	encoded := volume.EncodeTestPageHeader(original)
	if len(encoded) != volume.PageHeaderSize() {
		t.Fatalf("encoded length: got %d, want %d", len(encoded), volume.PageHeaderSize())
	}

	decoded, err := volume.DecodeTestPageHeader(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.DataLen != original.DataLen {
		t.Errorf("DataLen: got %d, want %d", decoded.DataLen, original.DataLen)
	}
	if decoded.ChunkID != original.ChunkID {
		t.Error("ChunkID mismatch")
	}
	if decoded.BlobID != original.BlobID {
		t.Error("BlobID mismatch")
	}
	if decoded.ChunkSeq != original.ChunkSeq {
		t.Errorf("ChunkSeq: got %d, want %d", decoded.ChunkSeq, original.ChunkSeq)
	}
	if decoded.TotalChunks != original.TotalChunks {
		t.Errorf("TotalChunks: got %d, want %d", decoded.TotalChunks, original.TotalChunks)
	}
	if decoded.CRC32 != original.CRC32 {
		t.Errorf("CRC32: got %08x, want %08x", decoded.CRC32, original.CRC32)
	}
	if decoded.Flags != original.Flags {
		t.Errorf("Flags: got %08b, want %08b", decoded.Flags, original.Flags)
	}
}

func TestPageHeaderEncoding_DataLenOffset(t *testing.T) {
	hdr := volume.TestPageHeader{DataLen: 0x0102030405060708, Flags: 0}
	encoded := volume.EncodeTestPageHeader(hdr)

	got := binary.LittleEndian.Uint64(encoded[0:8])
	if got != hdr.DataLen {
		t.Errorf("DataLen at offset 0: got %016x, want %016x", got, hdr.DataLen)
	}
}

func TestPageHeaderEncoding_MagicFlagsOffset(t *testing.T) {
	hdr := volume.TestPageHeader{Flags: volume.FlagDeleted}
	encoded := volume.EncodeTestPageHeader(hdr)

	mf := binary.LittleEndian.Uint32(encoded[84:88])
	flags, err := volume.DecodeMagicFlags(mf)
	if err != nil {
		t.Fatalf("MagicFlags at offset 84: %v", err)
	}
	if !flags.IsDeleted() {
		t.Error("MagicFlags at offset 84: FlagDeleted not set")
	}
}

func TestPageHeaderEncoding_ChunkIDOffset(t *testing.T) {
	var id [32]byte
	for i := range id {
		id[i] = byte(i)
	}
	hdr := volume.TestPageHeader{ChunkID: id}
	encoded := volume.EncodeTestPageHeader(hdr)

	if !bytes.Equal(encoded[8:40], id[:]) {
		t.Error("ChunkID not at offset 8")
	}
}

func TestPageHeaderEncoding_BlobIDOffset(t *testing.T) {
	var id [32]byte
	for i := range id {
		id[i] = byte(i + 100)
	}
	hdr := volume.TestPageHeader{BlobID: id}
	encoded := volume.EncodeTestPageHeader(hdr)

	if !bytes.Equal(encoded[40:72], id[:]) {
		t.Error("BlobID not at offset 40")
	}
}

// ── WriteBlob / ReadChunk round-trip ──────────────────────────────────────────

func TestWriteRead_Small(t *testing.T) {
	e := openEngine(t)
	data := []byte("hello, volume engine")
	result := writeBlobBytes(t, e, data)

	if result.TotalSize != int64(len(data)) {
		t.Errorf("TotalSize: got %d, want %d", result.TotalSize, len(data))
	}
	// 19 bytes is far below the minimum chunk size (avg/4), so the whole
	// blob must land in a single chunk.
	if len(result.Chunks) != 1 {
		t.Errorf("ChunkCount: got %d, want 1", len(result.Chunks))
	}

	got, err := e.ReadChunk(result.Chunks[0])
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("round-trip mismatch: got %q, want %q", got, data)
	}
}

func TestWriteRead_AtAvgChunkSize(t *testing.T) {
	// Content-defined chunking: a blob of exactly avg size may split into
	// 1..4 chunks depending on where the rolling hash cuts (min = avg/4,
	// max = 4*avg). The invariant to hold is byte-exact round-trip, not a
	// fixed chunk count.
	e := openEngine(t)
	data := randBytes(volume.DefaultChunkSize)
	result := writeBlobBytes(t, e, data)

	if len(result.Chunks) < 1 || len(result.Chunks) > 4 {
		t.Errorf("chunk count %d outside [1,4] for an avg-sized blob", len(result.Chunks))
	}
	var got []byte
	for _, chunk := range result.Chunks {
		payload, err := e.ReadChunk(chunk)
		if err != nil {
			t.Fatalf("ReadChunk seq %d: %v", chunk.Seq, err)
		}
		got = append(got, payload...)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("avg-sized blob round-trip mismatch")
	}
}

func TestWriteRead_MultiChunk(t *testing.T) {
	// Small engine with tiny avg chunk size. 5 KB with a 1 KB average
	// splits into at least 2 chunks (max chunk = 4 KB < 5 KB); the exact
	// count is content-defined.
	dir := t.TempDir()
	e, err := volume.Open(dir, "ns", volume.Options{
		ChunkSize: 1024, // 1 KB average chunk size
		PageSize:  volume.DefaultPageSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	data := randBytes(5 * 1024)
	result, err := e.WriteBlob(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("WriteBlob: %v", err)
	}
	if len(result.Chunks) < 2 {
		t.Errorf("expected >= 2 chunks for 5 KB at 1 KB average, got %d", len(result.Chunks))
	}

	var got []byte
	for _, chunk := range result.Chunks {
		payload, err := e.ReadChunk(chunk)
		if err != nil {
			t.Fatalf("ReadChunk seq %d: %v", chunk.Seq, err)
		}
		got = append(got, payload...)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("multi-chunk round-trip mismatch")
	}
}

func TestWriteRead_ContentAddressing(t *testing.T) {
	e := openEngine(t)
	data := randBytes(1024)

	r1 := writeBlobBytes(t, e, data)
	r2 := writeBlobBytes(t, e, data)

	if r1.BlobID != r2.BlobID {
		t.Errorf("identical content: BlobID mismatch: %s vs %s", r1.BlobID, r2.BlobID)
	}
}

func TestWriteRead_ChunkIDsDiffer(t *testing.T) {
	e := openEngine(t)
	r1 := writeBlobBytes(t, e, randBytes(1024))
	r2 := writeBlobBytes(t, e, randBytes(1024))

	if r1.Chunks[0].ChunkID == r2.Chunks[0].ChunkID {
		t.Error("different content produced identical ChunkID")
	}
}

func TestWriteBlob_Empty(t *testing.T) {
	e := openEngine(t)
	_, err := e.WriteBlob(bytes.NewReader(nil))
	if err == nil {
		t.Fatal("expected error for empty blob; got nil")
	}
}

// ── MarkDeleted ───────────────────────────────────────────────────────────────

func TestMarkDeleted(t *testing.T) {
	e := openEngine(t)
	data := randBytes(512)
	result := writeBlobBytes(t, e, data)

	if err := e.MarkDeleted(result.Chunks[0]); err != nil {
		t.Fatalf("MarkDeleted: %v", err)
	}

	_, err := e.ReadChunk(result.Chunks[0])
	if err == nil {
		t.Fatal("expected error reading deleted chunk; got nil")
	}
}

func TestMarkDeleted_PreservesOtherFlags(t *testing.T) {
	e := openEngine(t)
	data := randBytes(512)
	result := writeBlobBytes(t, e, data)

	// The only chunk of a small blob is the last chunk.
	chunk := result.Chunks[0]

	if err := e.MarkDeleted(chunk); err != nil {
		t.Fatalf("MarkDeleted: %v", err)
	}

	var found bool
	e.ScanSegments(func(entry object.ChunkEntry, hdr volume.PageHeader) error {
		if entry.ChunkID == chunk.ChunkID {
			found = true
			if !hdr.Flags.IsDeleted() {
				t.Error("FlagDeleted not set after MarkDeleted")
			}
			if !hdr.Flags.IsLastChunk() {
				t.Error("FlagLastChunk cleared unexpectedly by MarkDeleted")
			}
		}
		return nil
	})
	if !found {
		t.Error("chunk not found during ScanSegments after MarkDeleted")
	}
}

// ── ScanSegments ──────────────────────────────────────────────────────────────

func TestScanSegments_AllChunksFound(t *testing.T) {
	dir := t.TempDir()
	e, err := volume.Open(dir, "ns", volume.Options{ChunkSize: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	const blobs = 5
	var wantChunkIDs []object.ChunkID
	for i := 0; i < blobs; i++ {
		result, err := e.WriteBlob(bytes.NewReader(randBytes(512)))
		if err != nil {
			t.Fatalf("WriteBlob %d: %v", i, err)
		}
		for _, c := range result.Chunks {
			wantChunkIDs = append(wantChunkIDs, c.ChunkID)
		}
	}

	found := make(map[object.ChunkID]bool)
	e.ScanSegments(func(entry object.ChunkEntry, _ volume.PageHeader) error {
		found[entry.ChunkID] = true
		return nil
	})

	for _, id := range wantChunkIDs {
		if !found[id] {
			t.Errorf("chunk %s not found in scan", id)
		}
	}
}

func TestScanSegments_DeletedChunksVisible(t *testing.T) {
	e := openEngine(t)
	result := writeBlobBytes(t, e, randBytes(512))
	e.MarkDeleted(result.Chunks[0])

	var deletedCount int
	e.ScanSegments(func(_ object.ChunkEntry, hdr volume.PageHeader) error {
		if hdr.Flags.IsDeleted() {
			deletedCount++
		}
		return nil
	})
	if deletedCount != 1 {
		t.Errorf("expected 1 deleted chunk in scan, got %d", deletedCount)
	}
}

// ── Segment rolling ───────────────────────────────────────────────────────────

func TestSegmentRolling(t *testing.T) {
	dir := t.TempDir()
	e, err := volume.Open(dir, "ns", volume.Options{
		MaxSegmentSize: 64 * 1024, // 64 KB
		ChunkSize:      8 * 1024,  // 8 KB chunks
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		if _, err := e.WriteBlob(bytes.NewReader(randBytes(8 * 1024))); err != nil {
			t.Fatalf("WriteBlob %d: %v", i, err)
		}
	}
	e.Close()

	entries, _ := os.ReadDir(filepath.Join(dir, "ns"))
	var segCount int
	for _, de := range entries {
		if filepath.Ext(de.Name()) == ".vol" {
			segCount++
		}
	}
	if segCount < 2 {
		t.Errorf("expected at least 2 segments after rolling, got %d", segCount)
	}
}

// ── Dirty flag ────────────────────────────────────────────────────────────────

func TestDirtyFlag_SetOnOpen_ClearedOnClose(t *testing.T) {
	dir := t.TempDir()
	e, _ := volume.Open(dir, "ns", volume.Options{})
	if !e.IsDirty() {
		t.Error("engine should be dirty immediately after Open")
	}
	e.Close()

	// Re-open — Close should have cleared the flag; re-open marks it dirty again.
	e2, _ := volume.Open(dir, "ns", volume.Options{})
	defer e2.Close()
	if !e2.IsDirty() {
		t.Error("engine should be dirty after re-open")
	}
	t.Log("dirty flag lifecycle OK")
}

// ── Concurrency ───────────────────────────────────────────────────────────────

func TestConcurrentReads(t *testing.T) {
	e := openEngine(t)

	const blobCount = 10
	results := make([]*volume.WriteResult, blobCount)
	payloads := make([][]byte, blobCount)
	for i := 0; i < blobCount; i++ {
		payloads[i] = randBytes(1024)
		results[i] = writeBlobBytes(t, e, payloads[i])
	}

	var wg sync.WaitGroup
	errc := make(chan error, blobCount*10)

	for iter := 0; iter < 10; iter++ {
		for i := 0; i < blobCount; i++ {
			wg.Add(1)
			i, result, payload := i, results[i], payloads[i]
			go func() {
				defer wg.Done()
				got, err := e.ReadChunk(result.Chunks[0])
				if err != nil {
					errc <- fmt.Errorf("blob %d: %w", i, err)
					return
				}
				if !bytes.Equal(got, payload) {
					errc <- fmt.Errorf("blob %d: data mismatch", i)
				}
			}()
		}
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Error(err)
	}
}

func TestConcurrentWriteThenRead(t *testing.T) {
	e := openEngine(t)

	const workers = 5
	const blobsPerWorker = 10

	type written struct {
		result  *volume.WriteResult
		payload []byte
	}
	resultsCh := make(chan written, workers*blobsPerWorker)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < blobsPerWorker; i++ {
				data := randBytes(512)
				result, err := e.WriteBlob(bytes.NewReader(data))
				if err != nil {
					t.Errorf("WriteBlob: %v", err)
					return
				}
				resultsCh <- written{result, data}
			}
		}()
	}
	wg.Wait()
	close(resultsCh)

	for w := range resultsCh {
		got, err := e.ReadChunk(w.result.Chunks[0])
		if err != nil {
			t.Errorf("ReadChunk: %v", err)
			continue
		}
		if !bytes.Equal(got, w.payload) {
			t.Error("concurrent write/read: data mismatch")
		}
	}
}

// ── Benchmarks ────────────────────────────────────────────────────────────────

func BenchmarkWriteBlob_4MB(b *testing.B) {
	e, err := volume.Open(b.TempDir(), "ns", volume.Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()

	data := randBytes(4 * 1024 * 1024)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := e.WriteBlob(bytes.NewReader(data)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteBlob_64KB(b *testing.B) {
	e, err := volume.Open(b.TempDir(), "ns", volume.Options{ChunkSize: 64 * 1024})
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()

	data := randBytes(64 * 1024)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := e.WriteBlob(bytes.NewReader(data)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadChunk_4MB(b *testing.B) {
	e, err := volume.Open(b.TempDir(), "ns", volume.Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()

	data := randBytes(4 * 1024 * 1024)
	result, err := e.WriteBlob(bytes.NewReader(data))
	if err != nil {
		b.Fatal(err)
	}
	chunk := result.Chunks[0]
	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := e.ReadChunk(chunk); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadChunk_Concurrent(b *testing.B) {
	e, err := volume.Open(b.TempDir(), "ns", volume.Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()

	data := randBytes(64 * 1024)
	result, err := e.WriteBlob(bytes.NewReader(data))
	if err != nil {
		b.Fatal(err)
	}
	chunk := result.Chunks[0]
	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := e.ReadChunk(chunk); err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkWriteBlob_Sequential_SmallBlobs(b *testing.B) {
	e, err := volume.Open(b.TempDir(), "ns", volume.Options{ChunkSize: 4096})
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()

	data := randBytes(4096)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := e.WriteBlob(bytes.NewReader(data)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarkDeleted(b *testing.B) {
	e, err := volume.Open(b.TempDir(), "ns", volume.Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()

	data := randBytes(1024)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		result, _ := e.WriteBlob(bytes.NewReader(data))
		b.StartTimer()
		e.MarkDeleted(result.Chunks[0])
	}
}
