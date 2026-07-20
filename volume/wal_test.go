package volume_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/asaidimu/blobs/volume"
)

func openWALTestEngine(t *testing.T) (*volume.Engine, string, string) {
	t.Helper()
	root := t.TempDir()
	nsID := "default"
	eng, err := volume.Open(root, nsID, volume.Options{})
	if err != nil {
		t.Fatalf("volume.Open: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	return eng, root, nsID
}

func walPathForTest(root, nsID string, segID uint64) string {
	return filepath.Join(root, nsID, "seg-"+padHex(segID)+".wal")
}

func padHex(v uint64) string {
	const hexDigits = "0123456789abcdef"
	buf := make([]byte, 16)
	for i := 15; i >= 0; i-- {
		buf[i] = hexDigits[v&0xF]
		v >>= 4
	}
	return string(buf)
}

func TestListSegmentIDs_ReturnsSortedIDs(t *testing.T) {
	eng, _, _ := openWALTestEngine(t)

	// A fresh engine has exactly one (active) segment as soon as it
	// writes something.
	if _, err := eng.WriteBlob(bytes.NewReader([]byte("first"))); err != nil {
		t.Fatalf("WriteBlob: %v", err)
	}

	ids, err := eng.ListSegmentIDs()
	if err != nil {
		t.Fatalf("ListSegmentIDs: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("len(ids) = %d, want 1", len(ids))
	}
	activeID, ok := eng.ActiveSegmentID()
	if !ok || ids[0] != activeID {
		t.Fatalf("ListSegmentIDs = %v, want [%v]", ids, activeID)
	}
}

func TestParseWAL_ReturnsCommittedEntriesInOrder(t *testing.T) {
	eng, _, _ := openWALTestEngine(t)

	r1, err := eng.WriteBlob(bytes.NewReader([]byte("first blob content")))
	if err != nil {
		t.Fatalf("WriteBlob(first): %v", err)
	}
	r2, err := eng.WriteBlob(bytes.NewReader([]byte("second blob, a bit longer than the first")))
	if err != nil {
		t.Fatalf("WriteBlob(second): %v", err)
	}

	activeID, ok := eng.ActiveSegmentID()
	if !ok {
		t.Fatal("expected an active segment")
	}

	entries, err := eng.ParseWAL(activeID)
	if err != nil {
		t.Fatalf("ParseWAL: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("len(entries) = %d, want 2", len(entries))
	}

	if entries[0].BlobID != r1.BlobID {
		t.Fatalf("entries[0].BlobID = %s, want %s", entries[0].BlobID, r1.BlobID)
	}
	if entries[1].BlobID != r2.BlobID {
		t.Fatalf("entries[1].BlobID = %s, want %s", entries[1].BlobID, r2.BlobID)
	}

	if len(entries[0].Chunks) != len(r1.Chunks) {
		t.Fatalf("len(entries[0].Chunks) = %d, want %d", len(entries[0].Chunks), len(r1.Chunks))
	}
	for i, want := range r1.Chunks {
		got := entries[0].Chunks[i]
		if got.ChunkID != want.ChunkID {
			t.Errorf("entries[0].Chunks[%d].ChunkID = %s, want %s", i, got.ChunkID, want.ChunkID)
		}
		if got.BlobID != want.BlobID {
			t.Errorf("entries[0].Chunks[%d].BlobID = %s, want %s", i, got.BlobID, want.BlobID)
		}
		if got.SegmentID != want.SegmentID {
			t.Errorf("entries[0].Chunks[%d].SegmentID = %s, want %s", i, got.SegmentID, want.SegmentID)
		}
		if got.PageOffset != want.PageOffset {
			t.Errorf("entries[0].Chunks[%d].PageOffset = %d, want %d", i, got.PageOffset, want.PageOffset)
		}
		if got.Length != want.Length {
			t.Errorf("entries[0].Chunks[%d].Length = %d, want %d", i, got.Length, want.Length)
		}
		if got.Seq != want.Seq {
			t.Errorf("entries[0].Chunks[%d].Seq = %d, want %d", i, got.Seq, want.Seq)
		}
		if got.PageCount != want.PageCount {
			t.Errorf("entries[0].Chunks[%d].PageCount = %d, want %d (recomputed via ceil-division must match what WriteBlob actually used)", i, got.PageCount, want.PageCount)
		}
	}
}

func TestParseWAL_MissingWALFileReturnsNilNoError(t *testing.T) {
	eng, _, _ := openWALTestEngine(t)
	// SegmentID 999 was never created — no .vol or .wal file exists for it.
	entries, err := eng.ParseWAL(999)
	if err != nil {
		t.Fatalf("ParseWAL for nonexistent segment: %v", err)
	}
	if entries != nil {
		t.Fatalf("ParseWAL for nonexistent segment = %v, want nil", entries)
	}
}

func TestParseWAL_StopsCleanlyAtTornTrailingEntry(t *testing.T) {
	eng, root, nsID := openWALTestEngine(t)

	if _, err := eng.WriteBlob(bytes.NewReader([]byte("complete entry, must survive"))); err != nil {
		t.Fatalf("WriteBlob: %v", err)
	}
	activeID, ok := eng.ActiveSegmentID()
	if !ok {
		t.Fatal("expected an active segment")
	}

	// Confirm one clean entry exists before we corrupt anything.
	before, err := eng.ParseWAL(activeID)
	if err != nil {
		t.Fatalf("ParseWAL (before corruption): %v", err)
	}
	if len(before) != 1 {
		t.Fatalf("len(before) = %d, want 1", len(before))
	}

	// Simulate a crash mid-write of a second WAL entry: append a few
	// arbitrary trailing bytes that start a plausible-looking record
	// (correct magic) but are truncated partway through.
	walPath := walPathForTest(root, nsID, uint64(activeID))
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open WAL for corruption: %v", err)
	}
	// walMagicVal (0xBA1FACE, little-endian: CE FA A1 0B) + a blobIDLen
	// claiming more bytes follow than actually do.
	torn := []byte{0xCE, 0xFA, 0xA1, 0x0B, 0xFF, 0xFF} // magic (LE) + a huge, unsatisfiable blobIDLen
	if _, err := f.Write(torn); err != nil {
		t.Fatalf("write torn bytes: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close WAL after corruption: %v", err)
	}

	after, err := eng.ParseWAL(activeID)
	if err != nil {
		t.Fatalf("ParseWAL (after simulated torn write) returned an error, want it to stop cleanly instead: %v", err)
	}
	if len(after) != 1 {
		t.Fatalf("len(after) = %d, want 1 (the torn trailing entry must be silently dropped, not counted)", len(after))
	}
	if after[0].BlobID != before[0].BlobID {
		t.Fatalf("after[0].BlobID = %s, want %s (the one complete entry must be unaffected)", after[0].BlobID, before[0].BlobID)
	}
}
