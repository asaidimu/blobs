package store_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/asaidimu/blobs/index"
	"github.com/asaidimu/blobs/store"
)

// openCompactTestStore opens a Store tuned the same way
// volume/rewrite_test.go's openRewriteTestEngine is: exactly two small
// blobs fill a segment, so a third write reliably rolls it and seals it,
// making it a candidate for Compact's phase 2. See that file's doc
// comment for the exact arithmetic (chunks consume a full page each,
// regardless of payload size).
func openCompactTestStore(t *testing.T) *store.Store {
	t.Helper()
	const pageSize = 4 * 1024
	const segHeaderSize = 128
	s, err := store.Open(store.Config{
		DataDir:          t.TempDir(),
		Index:            index.NewMemoryBackend(),
		DefaultNamespace: "default",
		PageSize:         pageSize,
		ChunkSize:        1024,
		MaxSegmentSize:   segHeaderSize + 2*pageSize, // exactly two chunks per segment
	})
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestCompact_Phase2ReclaimsSpaceAndPreservesLiveData is the core
// end-to-end scenario for production readiness item 5.2: put two blobs
// (filling and sealing a segment), delete one (making its blob
// GC-eligible), run Compact, and confirm phase 2 both reports space
// reclaimed AND that the surviving blob is still readable — the whole
// point of a rewrite is that it must never lose or corrupt what wasn't
// deleted.
func TestCompact_Phase2ReclaimsSpaceAndPreservesLiveData(t *testing.T) {
	s := openCompactTestStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	if _, err := ns.Put(ctx, "keep", bytes.NewReader([]byte("this blob must survive compaction")), store.PutOptions{}); err != nil {
		t.Fatalf("Put(keep): %v", err)
	}
	if _, err := ns.Put(ctx, "remove", bytes.NewReader([]byte("this blob will be deleted before compaction")), store.PutOptions{}); err != nil {
		t.Fatalf("Put(remove): %v", err)
	}
	// Seal the segment containing both blobs above.
	if _, err := ns.Put(ctx, "force-roll", bytes.NewReader([]byte("forces the segment to seal")), store.PutOptions{}); err != nil {
		t.Fatalf("Put(force-roll): %v", err)
	}

	if err := ns.Delete(ctx, "remove"); err != nil {
		t.Fatalf("Delete(remove): %v", err)
	}

	result, err := ns.Compact(ctx)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if result.BlobsRemoved != 1 {
		t.Errorf("result.BlobsRemoved = %d, want 1 (phase 1 should have removed the deleted blob's manifest)", result.BlobsRemoved)
	}
	if result.SegmentsCompacted == 0 {
		t.Error("result.SegmentsCompacted = 0, want > 0 — the sealed segment should have crossed the dead-byte threshold and been rewritten")
	}
	if result.BytesFreed <= 0 {
		t.Errorf("result.BytesFreed = %d, want > 0", result.BytesFreed)
	}

	// The surviving blob must still be readable, byte-for-byte, after its
	// segment was physically rewritten out from under it.
	rc, err := ns.Get(ctx, "keep")
	if err != nil {
		t.Fatalf("Get(keep) after Compact: %v", err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read keep after Compact: %v", err)
	}
	if string(data) != "this blob must survive compaction" {
		t.Fatalf("Get(keep) after Compact = %q, want %q", data, "this blob must survive compaction")
	}

	// The deleted blob must be gone, not just marked — Head should 404.
	if _, err := ns.Head(ctx, "remove"); err == nil {
		t.Fatal("Head(remove) after Compact returned nil error, want *errors.NotFoundError")
	}
}

// TestCompact_SegmentBelowThresholdIsNotRewritten confirms Compact
// doesn't do unnecessary work: a segment with no dead chunks at all
// should not be touched by phase 2.
func TestCompact_SegmentBelowThresholdIsNotRewritten(t *testing.T) {
	s := openCompactTestStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	if _, err := ns.Put(ctx, "a", bytes.NewReader([]byte("blob a, never deleted")), store.PutOptions{}); err != nil {
		t.Fatalf("Put(a): %v", err)
	}
	if _, err := ns.Put(ctx, "b", bytes.NewReader([]byte("blob b, never deleted")), store.PutOptions{}); err != nil {
		t.Fatalf("Put(b): %v", err)
	}
	if _, err := ns.Put(ctx, "force-roll", bytes.NewReader([]byte("forces the segment to seal")), store.PutOptions{}); err != nil {
		t.Fatalf("Put(force-roll): %v", err)
	}

	result, err := ns.Compact(ctx)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if result.SegmentsCompacted != 0 {
		t.Errorf("result.SegmentsCompacted = %d, want 0 — nothing was deleted, so no segment should cross the rewrite threshold", result.SegmentsCompacted)
	}

	// Both blobs must still be trivially readable.
	for key, want := range map[string]string{"a": "blob a, never deleted", "b": "blob b, never deleted"} {
		rc, err := ns.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("read %q: %v", key, err)
		}
		if string(data) != want {
			t.Fatalf("Get(%q) = %q, want %q", key, data, want)
		}
	}
}

// TestCompact_CustomThresholdIsRespected exercises CompactWithOptions:
// a threshold set above the actual dead ratio should suppress the
// rewrite even though phase 1 still runs.
func TestCompact_CustomThresholdIsRespected(t *testing.T) {
	s := openCompactTestStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	if _, err := ns.Put(ctx, "keep", bytes.NewReader([]byte("survives")), store.PutOptions{}); err != nil {
		t.Fatalf("Put(keep): %v", err)
	}
	if _, err := ns.Put(ctx, "remove", bytes.NewReader([]byte("deleted")), store.PutOptions{}); err != nil {
		t.Fatalf("Put(remove): %v", err)
	}
	if _, err := ns.Put(ctx, "force-roll", bytes.NewReader([]byte("seals it")), store.PutOptions{}); err != nil {
		t.Fatalf("Put(force-roll): %v", err)
	}
	if err := ns.Delete(ctx, "remove"); err != nil {
		t.Fatalf("Delete(remove): %v", err)
	}

	// One out of two chunks is dead = 50%. A 99% threshold must suppress
	// the rewrite even though phase 1 (index cleanup) still happens.
	result, err := ns.CompactWithOptions(ctx, store.CompactOptions{RewriteThreshold: 0.99})
	if err != nil {
		t.Fatalf("CompactWithOptions: %v", err)
	}
	if result.BlobsRemoved != 1 {
		t.Errorf("result.BlobsRemoved = %d, want 1 (phase 1 always runs)", result.BlobsRemoved)
	}
	if result.SegmentsCompacted != 0 {
		t.Errorf("result.SegmentsCompacted = %d, want 0 (50%% dead is below the 99%% threshold)", result.SegmentsCompacted)
	}
}

// TestCompact_RunsCleanlyOnEmptyNamespace confirms Compact is safe to call
// on a namespace with no data at all — no segments, nothing to do.
func TestCompact_RunsCleanlyOnEmptyNamespace(t *testing.T) {
	s := openCompactTestStore(t)
	ns := s.Namespace("default")

	result, err := ns.Compact(context.Background())
	if err != nil {
		t.Fatalf("Compact on empty namespace: %v", err)
	}
	if result.BlobsRemoved != 0 || result.SegmentsCompacted != 0 || result.BytesFreed != 0 {
		t.Fatalf("Compact on empty namespace = %+v, want all-zero result", result)
	}
}
