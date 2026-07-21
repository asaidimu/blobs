package store_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/asaidimu/blobs/index"
	"github.com/asaidimu/blobs/object"
	"github.com/asaidimu/blobs/store"
	"github.com/asaidimu/blobs/volume"
)

// segmentDeadPageTotals opens a raw volume.Engine directly against
// dataDir/nsID (bypassing package store and its index entirely) and
// returns the total dead-page count summed across every segment. This is
// a direct, conclusive check of what Compact's MarkDeleted actually did
// on disk — unlike checking store-level Get/Head, which can't
// distinguish "the data survived" from "the data was deleted and
// something else papered over it."
func segmentDeadPageTotals(t *testing.T, dataDir, nsID string) int {
	t.Helper()
	eng, err := volume.Open(dataDir, nsID, volume.Options{})
	if err != nil {
		t.Fatalf("volume.Open (inspection): %v", err)
	}
	defer eng.Close()

	stats, err := eng.SegmentStats()
	if err != nil {
		t.Fatalf("SegmentStats: %v", err)
	}
	total := 0
	for _, s := range stats {
		total += s.DeadPages
	}
	return total
}

// TestRebuildIndex_SurvivesSubsequentCompact is the core disaster-recovery
// scenario: the index (bbolt file, or whatever backend) is lost or
// corrupted entirely, but the segment files on disk are intact.
// RebuildIndex must restore each chunk's owning BlobEntry with a RefCount
// that does NOT make it immediately reapable — otherwise the very next
// Compact call would see every recovered chunk as garbage (whether via
// "no BlobEntry at all" or "BlobEntry with RefCount==0" makes no
// practical difference) and delete everything RebuildIndex just restored.
func TestRebuildIndex_SurvivesSubsequentCompact(t *testing.T) {
	dataDir := t.TempDir()
	ctx := context.Background()

	// Phase 1: write real data with a real index.
	s1, err := store.Open(store.Config{
		DataDir: dataDir,
		Index:   index.NewMemoryBackend(),
	})
	if err != nil {
		t.Fatalf("store.Open (phase 1): %v", err)
	}
	if err := s1.CreateNamespace(ctx, object.Namespace{ID: "default"}); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	ns1 := s1.Namespace("default")

	blobs := map[string]string{
		"a.txt": "content of blob a",
		"b.txt": "content of blob b, a little longer",
		"c.txt": "content of blob c",
	}
	for key, content := range blobs {
		if _, err := ns1.Put(ctx, key, bytes.NewReader([]byte(content)), store.PutOptions{}); err != nil {
			t.Fatalf("Put(%q): %v", key, err)
		}
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close (phase 1): %v", err)
	}

	// Sanity: before any rebuild, nothing is marked dead. This just
	// confirms phase 1 wrote successfully; it is not the scenario under
	// test.
	if got := segmentDeadPageTotals(t, dataDir, "default"); got != 0 {
		t.Fatalf("dead pages = %d before rebuild even started, want 0 (test setup problem)", got)
	}

	// Phase 2: simulate total index loss. Same DataDir (segment files
	// untouched on disk), but a brand-new, empty index — as if the bbolt
	// file were deleted or corrupted and replaced.
	s2, err := store.Open(store.Config{
		DataDir: dataDir,
		Index:   index.NewMemoryBackend(), // fresh, empty — simulates index loss
	})
	if err != nil {
		t.Fatalf("store.Open (phase 2, fresh index): %v", err)
	}
	if err := s2.CreateNamespace(ctx, object.Namespace{ID: "default"}); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	ns2 := s2.Namespace("default")

	// Confirm the data is indeed gone from the (fresh) index's
	// perspective before rebuilding — otherwise this test wouldn't be
	// exercising anything.
	if _, err := ns2.Head(ctx, "a.txt"); err == nil {
		t.Fatal("Head(a.txt) succeeded against a fresh index before RebuildIndex — test setup is not simulating index loss")
	}

	if err := ns2.RebuildIndex(ctx); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}

	// The critical assertion: run Compact immediately after RebuildIndex,
	// then check the physical segment data directly. Before the fix
	// (either "no BlobEntry at all," or an earlier draft of this fix that
	// set RefCount: 0 instead of 1), this would mark every recovered
	// chunk's page deleted right here.
	if _, err := ns2.Compact(ctx); err != nil {
		t.Fatalf("Compact (immediately after RebuildIndex): %v", err)
	}
	if err := s2.Close(); err != nil {
		t.Fatalf("Close (phase 2): %v", err)
	}

	if got := segmentDeadPageTotals(t, dataDir, "default"); got != 0 {
		t.Fatalf("dead pages = %d after RebuildIndex+Compact, want 0 — Compact reaped data RebuildIndex had just recovered", got)
	}
}

// TestRebuildIndex_DoesNotDisturbAlreadyDeletedPages confirms RebuildIndex
// is a no-op with respect to pages that were already flagged deleted
// before the index was lost (e.g. by a prior Compact) — it must not
// resurrect them, and it must not error out or miscount when it
// encounters them while grouping chunks by BlobID.
func TestRebuildIndex_DoesNotDisturbAlreadyDeletedPages(t *testing.T) {
	dataDir := t.TempDir()
	ctx := context.Background()

	s1, err := store.Open(store.Config{
		DataDir: dataDir,
		Index:   index.NewMemoryBackend(),
	})
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	if err := s1.CreateNamespace(ctx, object.Namespace{ID: "default"}); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	ns1 := s1.Namespace("default")

	if _, err := ns1.Put(ctx, "keep", bytes.NewReader([]byte("survives")), store.PutOptions{}); err != nil {
		t.Fatalf("Put(keep): %v", err)
	}
	if _, err := ns1.Put(ctx, "remove", bytes.NewReader([]byte("deleted before rebuild")), store.PutOptions{}); err != nil {
		t.Fatalf("Put(remove): %v", err)
	}
	if err := ns1.Delete(ctx, "remove"); err != nil {
		t.Fatalf("Delete(remove): %v", err)
	}
	// Phase 1 of Compact marks "remove"'s pages deleted on disk.
	if _, err := ns1.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	deadBefore := segmentDeadPageTotals(t, dataDir, "default")
	if deadBefore == 0 {
		t.Fatal("expected at least one dead page after deleting+compacting \"remove\" (test setup problem)")
	}

	s2, err := store.Open(store.Config{
		DataDir: dataDir,
		Index:   index.NewMemoryBackend(),
	})
	if err != nil {
		t.Fatalf("store.Open (fresh index): %v", err)
	}
	if err := s2.CreateNamespace(ctx, object.Namespace{ID: "default"}); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	ns2 := s2.Namespace("default")

	if err := ns2.RebuildIndex(ctx); err != nil {
		t.Fatalf("RebuildIndex: %v", err)
	}
	if err := s2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// RebuildIndex only writes index records; it never touches segment
	// bytes. The dead-page count must be bit-for-bit identical to before.
	deadAfter := segmentDeadPageTotals(t, dataDir, "default")
	if deadAfter != deadBefore {
		t.Fatalf("dead pages = %d after RebuildIndex, want unchanged from %d — RebuildIndex must not alter segment data", deadAfter, deadBefore)
	}
}
