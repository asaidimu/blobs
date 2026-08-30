package store_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/asaidimu/blobs/index"
	"github.com/asaidimu/blobs/object"
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
		DataDir:        t.TempDir(),
		Index:          index.NewMemoryBackend(),
		PageSize:       pageSize,
		ChunkSize:      1024,
		MaxSegmentSize: segHeaderSize + 2*pageSize, // exactly two chunks per segment
	})
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	if err := s.CreateNamespace(ctx, object.Namespace{ID: "default"}); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
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

// TestCompact_StatsFullyReconcileAfterDeleteAndCompact is a direct check
// that NamespaceStats.DeadBytes / DeadChunks actually return to zero once
// every dead chunk has been reaped, and that BlobCount / BytesStored /
// ChunkCount reflect only the surviving blob. Before compaction the dead
// counters must be positive (data really is pending reclamation);
// afterward they must be exactly zero — a nonzero residual here would
// mean either compaction silently failed to reap something it owned, or
// double-counted a reap that a race had already backed out (see
// ReapPhysicalPageIfDead's recordDeleted-vs-safeToReap distinction).
func TestCompact_StatsFullyReconcileAfterDeleteAndCompact(t *testing.T) {
	s := openCompactTestStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	if _, err := ns.Put(ctx, "keep", bytes.NewReader([]byte("this blob must survive compaction")), store.PutOptions{}); err != nil {
		t.Fatalf("Put(keep): %v", err)
	}
	if _, err := ns.Put(ctx, "remove", bytes.NewReader([]byte("this blob will be deleted before compaction")), store.PutOptions{}); err != nil {
		t.Fatalf("Put(remove): %v", err)
	}
	if _, err := ns.Put(ctx, "force-roll", bytes.NewReader([]byte("forces the segment to seal")), store.PutOptions{}); err != nil {
		t.Fatalf("Put(force-roll): %v", err)
	}
	if err := ns.Delete(ctx, "remove"); err != nil {
		t.Fatalf("Delete(remove): %v", err)
	}

	before, err := ns.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats before compact: %v", err)
	}
	if before.DeadBytes <= 0 {
		t.Errorf("before compact: DeadBytes = %d, want > 0 (the deleted blob's chunk is pending reclamation)", before.DeadBytes)
	}
	if before.DeadChunks <= 0 {
		t.Errorf("before compact: DeadChunks = %d, want > 0", before.DeadChunks)
	}
	if before.BlobCount != 2 {
		t.Errorf("before compact: BlobCount = %d, want 2 (keep + force-roll; remove was deleted but its manifest isn't purged yet)", before.BlobCount)
	}

	if _, err := ns.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	after, err := ns.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats after compact: %v", err)
	}
	if after.DeadBytes != 0 {
		t.Errorf("after compact: DeadBytes = %d, want 0 — every dead chunk should have been fully reclaimed", after.DeadBytes)
	}
	if after.DeadChunks != 0 {
		t.Errorf("after compact: DeadChunks = %d, want 0", after.DeadChunks)
	}
	if after.BlobCount != 2 {
		t.Errorf("after compact: BlobCount = %d, want 2 (keep + force-roll survive; remove's manifest is purged)", after.BlobCount)
	}
	wantBytes := int64(len("this blob must survive compaction") + len("forces the segment to seal"))
	if after.BytesStored != wantBytes {
		t.Errorf("after compact: BytesStored = %d, want %d", after.BytesStored, wantBytes)
	}

	// A second Compact on an already-clean namespace must be a no-op and
	// must not perturb the now-reconciled stats.
	if _, err := ns.Compact(ctx); err != nil {
		t.Fatalf("second Compact: %v", err)
	}
	again, err := ns.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats after second compact: %v", err)
	}
	if again.DeadBytes != 0 || again.DeadChunks != 0 || again.BlobCount != 2 {
		t.Errorf("after second (no-op) compact: stats = %+v, want unchanged from %+v", again, after)
	}
}

// TestCompact_ConcurrentChurnDoesNotCorruptOrMisreport is the primary
// regression test for a class of TOCTOU races in compaction: a chunk's
// content hash can be created, referenced, deleted, and recreated at a
// brand-new physical location multiple times while a single Compact()
// call's scan is still in flight, since content-addressed IDs are
// deterministic and a tight delete/re-Put loop reuses the same hash every
// time. compactPhase1 must never let a stale liveness snapshot from one
// generation authorize deleting or physically reaping a page that
// belongs to a different, later generation of the same ChunkID — doing
// so is what silently destroyed live data and eventually deleted whole
// segment files that were still referenced by the index (surfacing as
// "segment file missing" reads).
//
// This drives many goroutines doing Put/Delete/Compact concurrently
// against content that intentionally collides on chunk hashes across
// goroutines, then — after quiescing — verifies both that every
// surviving key still reads back byte-for-byte correct AND that the
// namespace's stats have fully reconciled (no leaked DeadBytes/
// DeadChunks, no negative counters).
func TestCompact_ConcurrentChurnDoesNotCorruptOrMisreport(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy concurrency test in -short mode")
	}

	const pageSize = 4 * 1024
	const segHeaderSize = 128
	s, err := store.Open(store.Config{
		DataDir:        t.TempDir(),
		Index:          index.NewMemoryBackend(),
		PageSize:       pageSize,
		ChunkSize:      pageSize,
		MaxSegmentSize: segHeaderSize + 4*pageSize, // several pages per segment
	})
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	if err := s.CreateNamespace(ctx, object.Namespace{ID: "default"}); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	ns := s.Namespace("default")

	shared := bytes.Repeat([]byte("CHURN-SHARED-CONTENT-"), 100)
	victimData := append(append([]byte{}, shared...), []byte("-victim-tail")...)

	const junkWorkers = 6
	const victimIters = 400

	var stop int32
	var wg sync.WaitGroup

	// Junk churners: constant unrelated create/delete traffic so
	// compactPhase1's scan always has real work in flight while the
	// victim's chunk lifecycle races against it.
	for w := 0; w < junkWorkers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; atomic.LoadInt32(&stop) == 0; i++ {
				key := fmt.Sprintf("junk-%d-%d", w, i)
				data := []byte(fmt.Sprintf("junk-%d-%d-%s", w, i, bytes.Repeat([]byte{byte(i)}, 200)))
				if _, err := ns.Put(ctx, key, bytes.NewReader(data), store.PutOptions{}); err != nil {
					continue
				}
				_ = ns.Delete(ctx, key)
			}
		}()
	}

	// Compactor: hammer Compact() continuously while the churn runs.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for atomic.LoadInt32(&stop) == 0 {
			_, _ = ns.Compact(ctx)
		}
	}()

	// Victim: repeatedly create-verify-delete-recreate-verify-delete the
	// exact same content hash under fresh keys, racing the compactor. Any
	// read error or byte mismatch here is the corruption this test guards
	// against.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer atomic.StoreInt32(&stop, 1)
		for i := 0; i < victimIters; i++ {
			key := fmt.Sprintf("victim-%d", i)
			if _, err := ns.Put(ctx, key, bytes.NewReader(victimData), store.PutOptions{}); err != nil {
				t.Errorf("victim Put iter %d: %v", i, err)
				return
			}
			if got := readAllOrFail(t, ns, key); !bytes.Equal(got, victimData) {
				t.Errorf("victim Get iter %d: got %d bytes, want %d bytes (content mismatch)", i, len(got), len(victimData))
				return
			}
			if err := ns.Delete(ctx, key); err != nil {
				t.Errorf("victim Delete iter %d: %v", i, err)
				return
			}

			key2 := key + "-b"
			if _, err := ns.Put(ctx, key2, bytes.NewReader(victimData), store.PutOptions{}); err != nil {
				t.Errorf("victim resurrect Put iter %d: %v", i, err)
				return
			}
			if got := readAllOrFail(t, ns, key2); !bytes.Equal(got, victimData) {
				t.Errorf("victim resurrect Get iter %d: got %d bytes, want %d bytes (content mismatch)", i, len(got), len(victimData))
				return
			}
			if err := ns.Delete(ctx, key2); err != nil {
				t.Errorf("victim resurrect Delete iter %d: %v", i, err)
				return
			}
		}
	}()

	wg.Wait()
	if t.Failed() {
		return
	}

	// Quiesce: run Compact repeatedly until it reports nothing left to do,
	// so every dead generation created during the churn — including any
	// still mid-reclamation when the goroutines above stopped — gets
	// fully swept up. Check every field a call can report progress
	// through, not just BlobsRemoved/SegmentsCompacted: a call can still
	// be reaping dead chunks (ChunksRemoved/BytesFreed > 0) on a pass
	// that removed no blob manifests and rewrote no segments.
	for i := 0; i < 50; i++ {
		result, err := ns.Compact(ctx)
		if err != nil {
			t.Fatalf("quiesce Compact %d: %v", i, err)
		}
		if result.BlobsRemoved == 0 && result.SegmentsCompacted == 0 && result.ChunksRemoved == 0 && result.BytesFreed == 0 {
			break
		}
	}

	stats, err := ns.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats after churn: %v", err)
	}
	if stats.DeadBytes != 0 {
		t.Errorf("after churn+quiesce: DeadBytes = %d, want 0", stats.DeadBytes)
	}
	if stats.DeadChunks != 0 {
		t.Errorf("after churn+quiesce: DeadChunks = %d, want 0", stats.DeadChunks)
	}
	if stats.BlobCount != 0 {
		t.Errorf("after churn+quiesce: BlobCount = %d, want 0 (every key created was also deleted)", stats.BlobCount)
	}
	if stats.BytesStored != 0 {
		t.Errorf("after churn+quiesce: BytesStored = %d, want 0", stats.BytesStored)
	}
	if stats.ChunkCount < 0 || stats.DeadBytes < 0 || stats.DeadChunks < 0 || stats.BlobCount < 0 || stats.BytesStored < 0 {
		t.Errorf("after churn+quiesce: stats went negative: %+v", stats)
	}
}

func readAllOrFail(t *testing.T, ns *store.NamespaceHandle, key string) []byte {
	t.Helper()
	rc, err := ns.Get(context.Background(), key)
	if err != nil {
		t.Errorf("Get(%q): %v", key, err)
		return nil
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Errorf("read %q: %v", key, err)
		return nil
	}
	return data
}
