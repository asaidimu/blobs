package staging

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// ── RangeSet Tests ─────────────────────────────────────────────────────────────

func TestRangeSet_Add(t *testing.T) {
	var rs RangeSet
	rs.Add(0, 10)
	rs.Add(20, 30)
	if len(rs) != 2 {
		t.Fatalf("expected 2 ranges, got %d", len(rs))
	}
	if rs[0].Start != 0 || rs[0].End != 10 {
		t.Errorf("range[0]: got [%d,%d), want [0,10)", rs[0].Start, rs[0].End)
	}
	if rs[1].Start != 20 || rs[1].End != 30 {
		t.Errorf("range[1]: got [%d,%d), want [20,30)", rs[1].Start, rs[1].End)
	}
}

func TestRangeSet_Add_Merge(t *testing.T) {
	var rs RangeSet
	rs.Add(0, 10)
	rs.Add(5, 15)
	if len(rs) != 1 {
		t.Fatalf("expected 1 merged range, got %d", len(rs))
	}
	if rs[0].Start != 0 || rs[0].End != 15 {
		t.Errorf("merged range: got [%d,%d), want [0,15)", rs[0].Start, rs[0].End)
	}
}

func TestRangeSet_Add_Adjacent(t *testing.T) {
	var rs RangeSet
	rs.Add(0, 10)
	rs.Add(10, 20)
	if len(rs) != 1 {
		t.Fatalf("expected 1 merged range for adjacent, got %d", len(rs))
	}
	if rs[0].Start != 0 || rs[0].End != 20 {
		t.Errorf("adjacent range: got [%d,%d), want [0,20)", rs[0].Start, rs[0].End)
	}
}

func TestRangeSet_Add_OutOfOrder(t *testing.T) {
	var rs RangeSet
	rs.Add(20, 30)
	rs.Add(0, 10)
	rs.Add(10, 20)
	if len(rs) != 1 {
		t.Fatalf("expected 1 merged range, got %d", len(rs))
	}
	if rs[0].Start != 0 || rs[0].End != 30 {
		t.Errorf("out-of-order merge: got [%d,%d), want [0,30)", rs[0].Start, rs[0].End)
	}
}

func TestRangeSet_Add_NonOverlapping(t *testing.T) {
	var rs RangeSet
	rs.Add(0, 5)
	rs.Add(10, 15)
	rs.Add(20, 25)
	if len(rs) != 3 {
		t.Fatalf("expected 3 ranges, got %d", len(rs))
	}
}

func TestRangeSet_Add_Invalid(t *testing.T) {
	var rs RangeSet
	rs.Add(10, 10) // start == end
	rs.Add(20, 5)  // start > end
	if len(rs) != 0 {
		t.Errorf("expected 0 ranges for invalid inputs, got %d", len(rs))
	}
}

func TestRangeSet_Add_Subsumes(t *testing.T) {
	var rs RangeSet
	rs.Add(5, 15)
	rs.Add(0, 20) // subsumes existing
	if len(rs) != 1 {
		t.Fatalf("expected 1 range, got %d", len(rs))
	}
	if rs[0].Start != 0 || rs[0].End != 20 {
		t.Errorf("subsumed: got [%d,%d), want [0,20)", rs[0].Start, rs[0].End)
	}
}

func TestRangeSet_TotalBytes(t *testing.T) {
	var rs RangeSet
	rs.Add(0, 100)
	rs.Add(200, 300)
	if got := rs.TotalBytes(); got != 200 {
		t.Errorf("TotalBytes: got %d, want 200", got)
	}
}

func TestRangeSet_TotalBytes_Empty(t *testing.T) {
	var rs RangeSet
	if got := rs.TotalBytes(); got != 0 {
		t.Errorf("TotalBytes empty: got %d, want 0", got)
	}
}

func TestRangeSet_IsComplete(t *testing.T) {
	tests := []struct {
		name     string
		ranges   RangeSet
		size     int64
		expected bool
	}{
		{"empty set zero size", nil, 0, true},
		{"empty set nonzero size", nil, 100, false},
		{"single full range", RangeSet{{0, 100}}, 100, true},
		{"partial range", RangeSet{{0, 50}}, 100, false},
		{"two ranges full", RangeSet{{0, 50}, {50, 100}}, 100, false}, // must be single range
		{"extra data", RangeSet{{0, 150}}, 100, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ranges.IsComplete(tt.size); got != tt.expected {
				t.Errorf("IsComplete(%d): got %v, want %v", tt.size, got, tt.expected)
			}
		})
	}
}

func TestRangeSet_Contains(t *testing.T) {
	var rs RangeSet
	rs.Add(10, 50)
	tests := []struct {
		start, end int64
		expected   bool
	}{
		{10, 50, true},
		{10, 30, true},
		{20, 50, true},
		{10, 60, false},
		{0, 10, false},
		{50, 60, false},
	}
	for _, tt := range tests {
		if got := rs.Contains(tt.start, tt.end); got != tt.expected {
			t.Errorf("Contains(%d,%d): got %v, want %v", tt.start, tt.end, got, tt.expected)
		}
	}
}

// ── blockBitmap Tests ──────────────────────────────────────────────────────────

func TestBlockBitmap_MarkAndComplete(t *testing.T) {
	bb := newBlockBitmap(3)
	bb.mark(0)
	bb.mark(2)
	if bb.isComplete() {
		t.Error("should not be complete after marking 0 and 2 of 3")
	}
	bb.mark(1)
	if !bb.isComplete() {
		t.Error("should be complete after marking all 3")
	}
}

func TestBlockBitmap_MarkDuplicate(t *testing.T) {
	bb := newBlockBitmap(1)
	bb.mark(0)
	bb.mark(0) // idempotent
	if !bb.isComplete() {
		t.Error("should be complete after duplicate mark")
	}
}

func TestBlockBitmap_MarkOutOfRange(t *testing.T) {
	bb := newBlockBitmap(2)
	bb.mark(-1)
	bb.mark(2)
	if bb.isComplete() {
		t.Error("should not be complete with out-of-range marks")
	}
}

func TestBlockBitmap_Large(t *testing.T) {
	const n = 200
	bb := newBlockBitmap(n)
	for i := int64(0); i < n; i++ {
		bb.mark(i)
	}
	if !bb.isComplete() {
		t.Error("should be complete after marking all blocks")
	}
}

func TestBlockBitmap_Empty(t *testing.T) {
	bb := newBlockBitmap(0)
	if !bb.isComplete() {
		t.Error("empty bitmap should be complete")
	}
}

// ── Manager Tests ──────────────────────────────────────────────────────────────

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	dir := t.TempDir()
	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return m
}

func sha256hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func TestNewManager_EmptyDir(t *testing.T) {
	_, err := NewManager("")
	if err == nil {
		t.Fatal("expected error for empty dir")
	}
}

func TestNewManager_CreatesDir(t *testing.T) {
	dir := t.TempDir() + "/nested/staging"
	m, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if m == nil {
		t.Fatal("expected non-nil manager")
	}
}

func TestBegin_Validation(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()

	// Empty namespace
	_, err := m.Begin(ctx, "", "key", BeginOptions{})
	if err == nil {
		t.Fatal("expected error for empty namespace")
	}

	// Empty key
	_, err = m.Begin(ctx, "ns", "", BeginOptions{})
	if err == nil {
		t.Fatal("expected error for empty key")
	}

	// Invalid ExpectedSHA256 length
	_, err = m.Begin(ctx, "ns", "key", BeginOptions{ExpectedSHA256: "abc"})
	if err == nil {
		t.Fatal("expected error for short SHA256")
	}

	// Negative ExpectedSize
	_, err = m.Begin(ctx, "ns", "key", BeginOptions{ExpectedSize: -1})
	if err == nil {
		t.Fatal("expected error for negative size")
	}

	// Negative BlockSize
	_, err = m.Begin(ctx, "ns", "key", BeginOptions{BlockSize: -1})
	if err == nil {
		t.Fatal("expected error for negative BlockSize")
	}

	// PieceHashes without BlockSize
	_, err = m.Begin(ctx, "ns", "key", BeginOptions{PieceHashes: []string{"abc"}})
	if err == nil {
		t.Fatal("expected error for PieceHashes without BlockSize")
	}

	// BlockSize without ExpectedSize
	_, err = m.Begin(ctx, "ns", "key", BeginOptions{BlockSize: 1024})
	if err == nil {
		t.Fatal("expected error for BlockSize without ExpectedSize")
	}

	// ExpectedSize not multiple of BlockSize
	_, err = m.Begin(ctx, "ns", "key", BeginOptions{BlockSize: 1024, ExpectedSize: 1500})
	if err == nil {
		t.Fatal("expected error for ExpectedSize not multiple of BlockSize")
	}

	// Wrong piece count
	_, err = m.Begin(ctx, "ns", "key", BeginOptions{
		BlockSize:    1024,
		ExpectedSize: 2048,
		PieceHashes:  []string{"abc"},
	})
	if !errors.Is(err, ErrPieceCountMismatch) {
		t.Fatalf("expected ErrPieceCountMismatch, got %v", err)
	}
}

func TestBegin_InvalidPieceHashLength(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	_, err := m.Begin(ctx, "ns", "key", BeginOptions{
		BlockSize:    1024,
		ExpectedSize: 1024,
		PieceHashes:  []string{"tooshort"},
	})
	if err == nil {
		t.Fatal("expected error for invalid piece hash length")
	}
}

func TestBegin_Success(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	sess, err := m.Begin(ctx, "ns", "key", BeginOptions{})
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if sess.ID == "" {
		t.Fatal("expected non-empty session ID")
	}
	if sess.NamespaceID != "ns" || sess.Key != "key" {
		t.Errorf("unexpected namespace/key: %s/%s", sess.NamespaceID, sess.Key)
	}
}

func TestBegin_CancelledContext(t *testing.T) {
	m := newTestManager(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := m.Begin(ctx, "ns", "key", BeginOptions{})
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

// ── WriteChunk Range-Set Path ──────────────────────────────────────────────────

func TestWriteChunk_RangeSet_Basic(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	sess, err := m.Begin(ctx, "ns", "key", BeginOptions{ExpectedSize: 100})
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("hello world")
	total, err := m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(data), "")
	if err != nil {
		t.Fatal(err)
	}
	if total != int64(len(data)) {
		t.Errorf("total bytes: got %d, want %d", total, len(data))
	}
}

func TestWriteChunk_RangeSet_MultipleChunks(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	sess, err := m.Begin(ctx, "ns", "key", BeginOptions{ExpectedSize: 20})
	if err != nil {
		t.Fatal(err)
	}

	chunk1 := []byte("0123456789")
	chunk2 := []byte("abcdefghij")

	m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(chunk1), "")
	m.WriteChunk(ctx, sess.ID, 10, bytes.NewReader(chunk2), "")

	ranges, err := m.Ranges(sess.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !ranges.IsComplete(20) {
		t.Error("expected complete upload after two chunks")
	}
}

func TestWriteChunk_RangeSet_OutOfOrder(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	sess, err := m.Begin(ctx, "ns", "key", BeginOptions{ExpectedSize: 20})
	if err != nil {
		t.Fatal(err)
	}

	chunk1 := []byte("0123456789")
	chunk2 := []byte("abcdefghij")

	// Write second chunk first
	m.WriteChunk(ctx, sess.ID, 10, bytes.NewReader(chunk2), "")
	m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(chunk1), "")

	ranges, _ := m.Ranges(sess.ID)
	if !ranges.IsComplete(20) {
		t.Error("expected complete after out-of-order chunks")
	}
}

func TestWriteChunk_RangeSet_WithChecksum(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	sess, err := m.Begin(ctx, "ns", "key", BeginOptions{ExpectedSize: 10})
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("0123456789")
	checksum := sha256hex(data)

	// Wrong checksum
	_, err = m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(data), strings.Repeat("0", 64))
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}

	// Correct checksum
	_, err = m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(data), checksum)
	if err != nil {
		t.Fatalf("correct checksum should succeed: %v", err)
	}
}

func TestWriteChunk_RangeSet_NilReader(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{})
	_, err := m.WriteChunk(ctx, sess.ID, 0, nil, "")
	if err == nil {
		t.Fatal("expected error for nil reader")
	}
}

func TestWriteChunk_RangeSet_NegativeOffset(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{})
	_, err := m.WriteChunk(ctx, sess.ID, -1, bytes.NewReader([]byte("a")), "")
	if err == nil {
		t.Fatal("expected error for negative offset")
	}
}

func TestWriteChunk_RangeSet_ExceedsExpectedSize(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{ExpectedSize: 5})
	_, err := m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader([]byte("0123456789")), "")
	if err == nil {
		t.Fatal("expected error when chunk exceeds expected size")
	}
}

func TestWriteChunk_RangeSet_DuplicateChunk(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{ExpectedSize: 10})

	data := []byte("0123456789")
	total1, _ := m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(data), "")
	total2, _ := m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(data), "")

	if total1 != total2 {
		t.Error("duplicate chunk should not change total")
	}
}

// ── WriteChunk Block-Aligned Path ──────────────────────────────────────────────

func TestWriteChunk_Aligned_Basic(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	const blockSize = 64
	sess, err := m.Begin(ctx, "ns", "key", BeginOptions{
		ExpectedSize: blockSize * 2,
		BlockSize:    blockSize,
	})
	if err != nil {
		t.Fatal(err)
	}

	chunk := make([]byte, blockSize)
	for i := range chunk {
		chunk[i] = byte(i)
	}

	total, err := m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(chunk), "")
	if err != nil {
		t.Fatal(err)
	}
	if total != int64(blockSize) {
		t.Errorf("total: got %d, want %d", total, blockSize)
	}

	chunk2 := make([]byte, blockSize)
	for i := range chunk2 {
		chunk2[i] = byte(i + blockSize)
	}
	total, err = m.WriteChunk(ctx, sess.ID, blockSize, bytes.NewReader(chunk2), "")
	if err != nil {
		t.Fatal(err)
	}
	if total != int64(blockSize*2) {
		t.Errorf("total: got %d, want %d", total, blockSize*2)
	}
}

func TestWriteChunk_Aligned_MisalignedOffset(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	const blockSize = 64
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{
		ExpectedSize: blockSize * 2,
		BlockSize:    blockSize,
	})

	chunk := make([]byte, blockSize)
	_, err := m.WriteChunk(ctx, sess.ID, 1, bytes.NewReader(chunk), "")
	if !errors.Is(err, ErrBlockAlignment) {
		t.Fatalf("expected ErrBlockAlignment, got %v", err)
	}
}

func TestWriteChunk_Aligned_MisalignedSize(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	const blockSize = 64
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{
		ExpectedSize: blockSize * 2,
		BlockSize:    blockSize,
	})

	chunk := make([]byte, blockSize+1) // not block-aligned
	_, err := m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(chunk), "")
	if !errors.Is(err, ErrBlockAlignment) {
		t.Fatalf("expected ErrBlockAlignment for chunk size, got %v", err)
	}
}

func TestWriteChunk_Aligned_Checksum(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	const blockSize = 64
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{
		ExpectedSize: blockSize * 2,
		BlockSize:    blockSize,
	})

	chunk := make([]byte, blockSize)
	for i := range chunk {
		chunk[i] = 0xAA
	}
	checksum := sha256hex(chunk)

	// Wrong checksum
	_, err := m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(chunk), strings.Repeat("0", 64))
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}

	// Correct checksum
	_, err = m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(chunk), checksum)
	if err != nil {
		t.Fatalf("correct checksum: %v", err)
	}
}

func TestWriteChunk_Aligned_ExceedsExpectedSize(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	const blockSize = 64
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{
		ExpectedSize: blockSize,
		BlockSize:    blockSize,
	})

	chunk := make([]byte, blockSize)
	_, err := m.WriteChunk(ctx, sess.ID, blockSize, bytes.NewReader(chunk), "")
	if err == nil {
		t.Fatal("expected error when chunk exceeds expected size")
	}
}

func TestWriteChunk_Aligned_EmptyChunk(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	const blockSize = 64
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{
		ExpectedSize: blockSize,
		BlockSize:    blockSize,
	})

	total, err := m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(nil), "")
	if err != nil {
		t.Fatalf("empty chunk should succeed: %v", err)
	}
	if total != 0 {
		t.Errorf("total for empty chunk: got %d, want 0", total)
	}
}

func TestWriteChunk_Aligned_PieceHashMismatch(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	const blockSize = 64
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{
		ExpectedSize: blockSize * 2,
		BlockSize:    blockSize,
		PieceHashes:  []string{"0000000000000000000000000000000000000000000000000000000000000000", "0000000000000000000000000000000000000000000000000000000000000000"},
	})

	chunk := make([]byte, blockSize)
	_, err := m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(chunk), "")
	if !errors.Is(err, ErrPieceHashMismatch) {
		t.Fatalf("expected ErrPieceHashMismatch, got %v", err)
	}
}

// ── Complete Tests ─────────────────────────────────────────────────────────────

func TestComplete_Incomplete(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{ExpectedSize: 100})

	_, err := m.Complete(ctx, sess.ID)
	if !errors.Is(err, ErrIncompleteUpload) {
		t.Fatalf("expected ErrIncompleteUpload, got %v", err)
	}
}

func TestComplete_SizeMismatch(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{ExpectedSize: 50})

	// Write more than expected size would allow if the file is preallocated to 50
	// but write a full chunk so ranges cover [0,10). The file is preallocated to
	// 50 bytes, so sizes won't match.
	m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader([]byte("01234")), "")

	cu, err := m.Complete(ctx, sess.ID)
	if cu != nil {
		cu.Close()
	}
	// File is preallocated to 50, ranges cover [0,5), so IsComplete won't match
	// either. We just verify it fails.
	if err == nil {
		t.Fatal("expected error for incomplete upload")
	}
}

func TestComplete_ChecksumMismatch(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()

	data := []byte("0123456789")
	wrongChecksum := sha256hex([]byte("wrong data"))

	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{
		ExpectedSize:   int64(len(data)),
		ExpectedSHA256: wrongChecksum,
	})

	m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(data), "")

	cu, err := m.Complete(ctx, sess.ID)
	if cu != nil {
		cu.Close()
	}
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}
}

func TestComplete_Success(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()

	data := []byte("hello, staging!")
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{
		ExpectedSize:   int64(len(data)),
		ExpectedSHA256: sha256hex(data),
	})

	m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(data), "")

	cu, err := m.Complete(ctx, sess.ID)
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	defer cu.Close()

	if cu.NamespaceID != "ns" || cu.Key != "key" {
		t.Errorf("unexpected namespace/key: %s/%s", cu.NamespaceID, cu.Key)
	}
	if cu.Size != int64(len(data)) {
		t.Errorf("Size: got %d, want %d", cu.Size, len(data))
	}

	got, err := io.ReadAll(cu)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("content mismatch: got %q, want %q", got, data)
	}
}

func TestComplete_Success_NoExpectedSize(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()

	data := []byte("dynamic size")
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{})

	m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(data), "")

	cu, err := m.Complete(ctx, sess.ID)
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	defer cu.Close()

	if cu.Size != int64(len(data)) {
		t.Errorf("Size: got %d, want %d", cu.Size, len(data))
	}
}

func TestComplete_Aligned_Success(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	const blockSize = 64
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{
		ExpectedSize: blockSize * 2,
		BlockSize:    blockSize,
	})

	chunk1 := make([]byte, blockSize)
	for i := range chunk1 {
		chunk1[i] = byte(i)
	}
	chunk2 := make([]byte, blockSize)
	for i := range chunk2 {
		chunk2[i] = byte(i + blockSize)
	}

	m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(chunk1), "")
	m.WriteChunk(ctx, sess.ID, blockSize, bytes.NewReader(chunk2), "")

	cu, err := m.Complete(ctx, sess.ID)
	if err != nil {
		t.Fatalf("Complete aligned: %v", err)
	}
	defer cu.Close()

	if cu.Size != blockSize*2 {
		t.Errorf("Size: got %d, want %d", cu.Size, blockSize*2)
	}
}

func TestComplete_Aligned_Incomplete(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	const blockSize = 64
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{
		ExpectedSize: blockSize * 2,
		BlockSize:    blockSize,
	})

	chunk := make([]byte, blockSize)
	m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(chunk), "")

	_, err := m.Complete(ctx, sess.ID)
	if !errors.Is(err, ErrIncompleteUpload) {
		t.Fatalf("expected ErrIncompleteUpload, got %v", err)
	}
}

func TestComplete_Aligned_WithPieceHashes(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	const blockSize = 32

	chunk1 := make([]byte, blockSize)
	for i := range chunk1 {
		chunk1[i] = 0xAA
	}
	chunk2 := make([]byte, blockSize)
	for i := range chunk2 {
		chunk2[i] = 0xBB
	}

	hash1 := sha256hex(chunk1)
	hash2 := sha256hex(chunk2)

	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{
		ExpectedSize: blockSize * 2,
		BlockSize:    blockSize,
		PieceHashes:  []string{hash1, hash2},
	})

	m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(chunk1), "")
	m.WriteChunk(ctx, sess.ID, blockSize, bytes.NewReader(chunk2), "")

	cu, err := m.Complete(ctx, sess.ID)
	if err != nil {
		t.Fatalf("Complete with piece hashes: %v", err)
	}
	cu.Close()
}

func TestComplete_CancelledContext(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{})

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := m.Complete(cancelled, sess.ID)
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestComplete_InvalidSessionID(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	_, err := m.Complete(ctx, "not-a-valid-session-id")
	if !errors.Is(err, ErrInvalidSessionID) {
		t.Fatalf("expected ErrInvalidSessionID, got %v", err)
	}
}

// ── Abort Tests ────────────────────────────────────────────────────────────────

func TestAbort(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{})

	if err := m.Abort(sess.ID); err != nil {
		t.Fatalf("Abort: %v", err)
	}

	// Session should no longer be accessible
	_, err := m.Offset(sess.ID)
	if !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("expected ErrSessionNotFound after abort, got %v", err)
	}
}

func TestAbort_InvalidSessionID(t *testing.T) {
	m := newTestManager(t)
	err := m.Abort("bad-id")
	if !errors.Is(err, ErrInvalidSessionID) {
		t.Fatalf("expected ErrInvalidSessionID, got %v", err)
	}
}

func TestAbort_NonexistentSession(t *testing.T) {
	m := newTestManager(t)
	err := m.Abort("00000000000000000000000000000000")
	if !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("expected ErrSessionNotFound, got %v", err)
	}
}

// ── Finalize Tests ─────────────────────────────────────────────────────────────

func TestFinalize_CleansUp(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()

	data := []byte("finalize me")
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{
		ExpectedSize: int64(len(data)),
	})
	m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(data), "")

	cu, err := m.Complete(ctx, sess.ID)
	if err != nil {
		t.Fatal(err)
	}

	cu.Finalize()
	cu.Close()

	// After finalize + close, session should be gone
	_, err = m.Offset(sess.ID)
	if !errors.Is(err, ErrSessionNotFound) {
		t.Errorf("expected session removed after Finalize+Close, got %v", err)
	}
}

func TestClose_WithoutFinalize_KeepsSession(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()

	data := []byte("keep me")
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{
		ExpectedSize: int64(len(data)),
	})
	m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(data), "")

	cu, err := m.Complete(ctx, sess.ID)
	if err != nil {
		t.Fatal(err)
	}

	// Close without Finalize
	cu.Close()

	// Session should still be accessible
	_, err = m.Offset(sess.ID)
	if err != nil {
		t.Errorf("session should still exist after close without finalize: %v", err)
	}

	// Cleanup
	m.Abort(sess.ID)
}

// ── Offset & Ranges ───────────────────────────────────────────────────────────

func TestOffset_RangeSet(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{ExpectedSize: 100})

	m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader([]byte("0123456789")), "")

	offset, err := m.Offset(sess.ID)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 10 {
		t.Errorf("Offset: got %d, want 10", offset)
	}
}

func TestOffset_Aligned(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	const blockSize = 32
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{
		ExpectedSize: blockSize * 3,
		BlockSize:    blockSize,
	})

	chunk := make([]byte, blockSize)
	m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(chunk), "")

	offset, _ := m.Offset(sess.ID)
	if offset != int64(blockSize) {
		t.Errorf("Offset: got %d, want %d", offset, blockSize)
	}
}

func TestOffset_NotFound(t *testing.T) {
	m := newTestManager(t)
	_, err := m.Offset("00000000000000000000000000000000")
	if !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("expected ErrSessionNotFound, got %v", err)
	}
}

func TestRanges_NotFound(t *testing.T) {
	m := newTestManager(t)
	_, err := m.Ranges("00000000000000000000000000000000")
	if !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("expected ErrSessionNotFound, got %v", err)
	}
}

// ── Session Persistence ────────────────────────────────────────────────────────

func TestSessionPersistence_FromDisk(t *testing.T) {
	dir := t.TempDir()

	// Create session with first manager
	m1, _ := NewManager(dir)
	ctx := context.Background()
	data := []byte("persist me")
	sess, _ := m1.Begin(ctx, "ns", "key", BeginOptions{ExpectedSize: int64(len(data))})
	m1.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(data), "")

	// Load with second manager (simulates restart)
	m2, _ := NewManager(dir)
	offset, err := m2.Offset(sess.ID)
	if err != nil {
		t.Fatalf("Offset after reload: %v", err)
	}
	if offset != int64(len(data)) {
		t.Errorf("Offset after reload: got %d, want %d", offset, len(data))
	}
}

// ── Reap Tests ─────────────────────────────────────────────────────────────────

func TestReap_RemovesStaleSessions(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{})

	// Manually age the session by writing metadata with old timestamp
	state, _ := m.getOrLoadSession(sess.ID)
	state.mu.Lock()
	state.meta.LastActivity = time.Now().UTC().Add(-2 * time.Hour)
	state.mu.Unlock()
	m.saveMeta(sess.ID, state.meta)
	m.sessions.Delete(sess.ID) // force reload from disk

	reaped, err := m.Reap(1 * time.Hour)
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if reaped != 1 {
		t.Errorf("expected 1 reaped session, got %d", reaped)
	}

	_, err = m.Offset(sess.ID)
	if !errors.Is(err, ErrSessionNotFound) {
		t.Errorf("expected session removed after reap, got %v", err)
	}
}

func TestReap_KeepsRecentSessions(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	m.Begin(ctx, "ns", "key", BeginOptions{})

	reaped, _ := m.Reap(1 * time.Hour)
	if reaped != 0 {
		t.Errorf("expected 0 reaped, got %d", reaped)
	}
}

func TestStartReaper(t *testing.T) {
	m := newTestManager(t)
	stop := m.StartReaper(10*time.Millisecond, 1*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	stop()
}

// ── WriteChunk Validation ─────────────────────────────────────────────────────

func TestWriteChunk_InvalidSessionID(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	_, err := m.WriteChunk(ctx, "bad-id", 0, bytes.NewReader([]byte("a")), "")
	if !errors.Is(err, ErrInvalidSessionID) {
		t.Fatalf("expected ErrInvalidSessionID, got %v", err)
	}
}

func TestWriteChunk_CancelledContext(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{})

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := m.WriteChunk(cancelled, sess.ID, 0, bytes.NewReader([]byte("a")), "")
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestWriteChunk_ChecksumValidation_InvalidLength(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{})
	_, err := m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader([]byte("a")), "short")
	if err == nil {
		t.Fatal("expected error for invalid checksum length")
	}
}

// ── Concurrency Tests ─────────────────────────────────────────────────────────

func TestWriteChunk_Concurrent_RangeSet(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	const chunkSize = 64
	const numChunks = 10
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{
		ExpectedSize: chunkSize * numChunks,
	})

	var wg sync.WaitGroup
	errs := make(chan error, numChunks)

	for i := 0; i < numChunks; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data := make([]byte, chunkSize)
			for j := range data {
				data[j] = byte(i)
			}
			offset := int64(i * chunkSize)
			_, err := m.WriteChunk(ctx, sess.ID, offset, bytes.NewReader(data), "")
			if err != nil {
				errs <- fmt.Errorf("chunk %d: %w", i, err)
			}
		}(i)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	offset, _ := m.Offset(sess.ID)
	if offset != chunkSize*numChunks {
		t.Errorf("Offset after concurrent writes: got %d, want %d", offset, chunkSize*numChunks)
	}
}

func TestWriteChunk_Concurrent_Aligned(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	const blockSize = 32
	const numBlocks = 8
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{
		ExpectedSize: blockSize * numBlocks,
		BlockSize:    blockSize,
	})

	var wg sync.WaitGroup
	errs := make(chan error, numBlocks)

	for i := 0; i < numBlocks; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data := make([]byte, blockSize)
			for j := range data {
				data[j] = byte(i)
			}
			offset := int64(i * blockSize)
			_, err := m.WriteChunk(ctx, sess.ID, offset, bytes.NewReader(data), "")
			if err != nil {
				errs <- fmt.Errorf("block %d: %w", i, err)
			}
		}(i)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	offset, _ := m.Offset(sess.ID)
	if offset != blockSize*numBlocks {
		t.Errorf("Offset after concurrent aligned writes: got %d, want %d", offset, blockSize*numBlocks)
	}
}

// ── CompletedUpload Read ───────────────────────────────────────────────────────

func TestCompletedUpload_Read_SmallChunks(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()

	data := []byte("read in small pieces, one byte at a time")
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{
		ExpectedSize: int64(len(data)),
	})
	m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(data), "")

	cu, err := m.Complete(ctx, sess.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cu.Close()
	}()

	var got []byte
	buf := make([]byte, 1)
	for {
		n, err := cu.Read(buf)
		got = append(got, buf[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
	}
	if !bytes.Equal(got, data) {
		t.Errorf("mismatch: got %d bytes, want %d", len(got), len(data))
	}
}

// ── Metadata Flush ─────────────────────────────────────────────────────────────

func TestMaybeFlushMeta_Dirty(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{ExpectedSize: 100})

	state, _ := m.getOrLoadSession(sess.ID)
	state.dirty = true
	err := m.maybeFlushMeta(state)
	if err != nil {
		t.Fatalf("maybeFlushMeta: %v", err)
	}
	if state.dirty {
		t.Error("expected dirty to be false after flush")
	}
}

// ── Errors ─────────────────────────────────────────────────────────────────────

func TestOffsetMismatchError(t *testing.T) {
	err := &OffsetMismatchError{Expected: 10, Actual: 20}
	if !strings.Contains(err.Error(), "10") || !strings.Contains(err.Error(), "20") {
		t.Errorf("error message should contain both values: %s", err.Error())
	}
}

// ── writeChunkRangeSet Data File Missing ───────────────────────────────────────

func TestWriteChunk_RangeSet_SessionNotFound(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()

	// Manually create a metadata file but no data file
	id := "00000000000000000000000000000000"
	metaPath := m.metaPath(id)
	os.WriteFile(metaPath, []byte(`{"id":"`+id+`","namespace_id":"ns","key":"key"}`), 0o644)
	defer os.Remove(metaPath)

	_, err := m.WriteChunk(ctx, id, 0, bytes.NewReader([]byte("a")), "")
	if err == nil {
		t.Fatal("expected error when data file is missing")
	}
}

// ── Streaming fast path (no verification) ─────────────────────────────────────

// TestWriteChunk_Aligned_Streaming_ReadBack drives the streaming fast path
// (no chunk checksum, no piece hashes) and verifies the bytes round-trip
// through Complete.
func TestWriteChunk_Aligned_Streaming_ReadBack(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	const blockSize = 32
	const blocks = 3

	sess, err := m.Begin(ctx, "ns", "key", BeginOptions{
		ExpectedSize: blockSize * blocks,
		BlockSize:    blockSize,
	})
	if err != nil {
		t.Fatal(err)
	}

	var want []byte
	for i := 0; i < blocks; i++ {
		chunk := make([]byte, blockSize)
		for j := range chunk {
			chunk[j] = byte(i*blockSize + j)
		}
		want = append(want, chunk...)
		if _, err := m.WriteChunk(ctx, sess.ID, int64(i*blockSize), bytes.NewReader(chunk), ""); err != nil {
			t.Fatalf("WriteChunk block %d: %v", i, err)
		}
	}

	cu, err := m.Complete(ctx, sess.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer cu.Close()

	got, err := io.ReadAll(cu)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("content mismatch: got %d bytes, want %d", len(got), len(want))
	}
}

// TestWriteChunk_Aligned_Streaming_MisalignedSize leaves no partial marks: a
// streaming-path chunk whose length is not a multiple of the block size must
// return ErrBlockAlignment and must not mark any blocks received.
func TestWriteChunk_Aligned_Streaming_MisalignedSize(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	const blockSize = 64
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{
		ExpectedSize: blockSize * 2,
		BlockSize:    blockSize,
	})

	bad := bytes.NewReader(make([]byte, blockSize+1))
	_, err := m.WriteChunk(ctx, sess.ID, 0, bad, "")
	if !errors.Is(err, ErrBlockAlignment) {
		t.Fatalf("expected ErrBlockAlignment, got %v", err)
	}

	offset, _ := m.Offset(sess.ID)
	if offset != 0 {
		t.Errorf("misaligned streaming chunk reported progress %d, want 0", offset)
	}
	rs, _ := m.Ranges(sess.ID)
	if len(rs) != 0 {
		t.Errorf("misaligned streaming chunk left ranges %v, want empty", rs)
	}
}

// TestWriteChunk_Aligned_Streaming_Overrun detects a chunk larger than the
// remaining expected size after streaming it, and reports it as an error
// without marking the upload complete.
func TestWriteChunk_Aligned_Streaming_Overrun(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	const blockSize = 64
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{
		ExpectedSize: blockSize,
		BlockSize:    blockSize,
	})

	over := bytes.NewReader(make([]byte, blockSize*2))
	_, err := m.WriteChunk(ctx, sess.ID, 0, over, "")
	if err == nil {
		t.Fatal("expected error for oversized streaming chunk")
	}

	// The overrun bytes were streamed to the file before the error surfaced, so
	// Complete must reject the session — via size mismatch, or incomplete if the
	// write path had already rejected them. Either way the upload must not
	// succeed.
	if cu, err := m.Complete(ctx, sess.ID); err == nil {
		cu.Close()
		t.Fatal("expected Complete to fail after an oversized streaming chunk")
	}
}

// ── Cached data-file handle lifecycle ─────────────────────────────────────────

// TestDataFileHandle_CachedAcrossWrites confirms a session reuses one open
// data-file handle across WriteChunk calls, and that Abort closes it.
func TestDataFileHandle_CachedAcrossWrites(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	const blockSize = 32
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{
		ExpectedSize: blockSize * 2,
		BlockSize:    blockSize,
	})

	state, _ := m.getOrLoadSession(sess.ID)
	if state.dataFile != nil {
		t.Fatal("expected no data file before first write")
	}

	chunk := make([]byte, blockSize)
	if _, err := m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(chunk), ""); err != nil {
		t.Fatal(err)
	}
	first := state.dataFile
	if first == nil {
		t.Fatal("expected data file to be opened and cached after first write")
	}
	if _, err := m.WriteChunk(ctx, sess.ID, blockSize, bytes.NewReader(chunk), ""); err != nil {
		t.Fatal(err)
	}
	if state.dataFile != first {
		t.Fatal("expected the same data-file handle to be reused across writes")
	}

	if err := m.Abort(sess.ID); err != nil {
		t.Fatal(err)
	}
	if state.dataFile != nil {
		t.Fatal("expected cached data file to be closed and nilled after Abort")
	}
}

// TestDataFileHandle_ClosedOnFinalize confirms Complete + Finalize + Close
// (the normal success path) releases the cached write handle.
func TestDataFileHandle_ClosedOnFinalize(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	const blockSize = 32
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{
		ExpectedSize: blockSize,
		BlockSize:    blockSize,
	})

	chunk := make([]byte, blockSize)
	if _, err := m.WriteChunk(ctx, sess.ID, 0, bytes.NewReader(chunk), ""); err != nil {
		t.Fatal(err)
	}

	state, _ := m.getOrLoadSession(sess.ID)
	cu, err := m.Complete(ctx, sess.ID)
	if err != nil {
		t.Fatal(err)
	}
	cu.Finalize()
	cu.Close()

	if state.dataFile != nil {
		t.Fatal("expected cached data file to be closed and nilled after Finalize+Close")
	}
}

// ── Word-parallel bitmap edge cases ───────────────────────────────────────────

// TestBlockBitmap_IsComplete_PartialWord verifies isComplete handles a final
// word whose valid bits are fewer than 64 (blockCount not a multiple of 64).
func TestBlockBitmap_IsComplete_PartialWord(t *testing.T) {
	bb := newBlockBitmap(130) // 3 words: 64 + 64 + 2
	for i := int64(0); i < 128; i++ {
		bb.mark(i)
	}
	if bb.isComplete() {
		t.Fatal("expected incomplete after 128 of 130 blocks")
	}
	bb.mark(128)
	bb.mark(129)
	if !bb.isComplete() {
		t.Fatal("expected complete after marking all 130 blocks")
	}
}

// TestOffset_Aligned_PartialWord exercises progress() across a partial final
// bitmap word (blockCount % 64 != 0).
func TestOffset_Aligned_PartialWord(t *testing.T) {
	m := newTestManager(t)
	ctx := context.Background()
	const blockSize = 16
	const blocks = 130 // 3 bitmap words, last one partial
	sess, _ := m.Begin(ctx, "ns", "key", BeginOptions{
		ExpectedSize: blockSize * blocks,
		BlockSize:    blockSize,
	})

	chunk := make([]byte, blockSize)
	for i := 0; i < blocks; i++ {
		if _, err := m.WriteChunk(ctx, sess.ID, int64(i*blockSize), bytes.NewReader(chunk), ""); err != nil {
			t.Fatalf("WriteChunk block %d: %v", i, err)
		}
	}

	offset, _ := m.Offset(sess.ID)
	if offset != blockSize*blocks {
		t.Errorf("Offset after %d blocks: got %d, want %d", blocks, offset, blockSize*blocks)
	}
}
