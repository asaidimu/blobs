package volume_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/asaidimu/blobs/object"
	"github.com/asaidimu/blobs/volume"
)

// rewriteTestFixture bundles everything a rewrite test needs: the engine
// itself, plus the root/nsID used to open it, since Engine does not
// expose its internal directory and a couple of tests need to verify a
// segment file's presence/absence on disk independently of what the
// Engine's own methods report.
type rewriteTestFixture struct {
	eng  *volume.Engine
	root string
	nsID string
}

// openRewriteTestEngine opens an Engine tuned so exactly two small blobs
// fill a segment and a third forces a roll. This exploits how rollIfNeeded
// actually works: every chunk consumes one full page (4096 bytes,
// regardless of payload size, since even a tiny payload still rounds up
// to a whole page) plus a 128-byte segment header, and the roll check
// runs BEFORE each append based on the segment's CURRENT size. So with
// MaxSegmentSize set to exactly segHeaderSize(128) + 2*PageSize(4096) =
// 8320: writing chunk 1 brings size to 4224 (< 8320, no roll), chunk 2
// brings it to exactly 8320 (< 8320 is now false, so the *next* write
// rolls before it's appended) — giving deterministic "2 chunks per
// sealed segment" test fixtures without depending on exact payload sizes.
func openRewriteTestEngine(t *testing.T) *rewriteTestFixture {
	t.Helper()
	root := t.TempDir()
	nsID := "default"
	const pageSize = 4 * 1024
	const segHeaderSize = 128
	eng, err := volume.Open(root, nsID, volume.Options{
		PageSize:       pageSize,
		ChunkSize:      1024,
		MaxSegmentSize: segHeaderSize + 2*pageSize, // exactly two chunks per segment
	})
	if err != nil {
		t.Fatalf("volume.Open: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	return &rewriteTestFixture{eng: eng, root: root, nsID: nsID}
}

// segFilePath reconstructs a segment's on-disk path the same way package
// volume does internally (seg-%016x.vol under filepath.Join(root, nsID)).
func (f *rewriteTestFixture) segFilePath(segID object.SegmentID) string {
	return filepath.Join(f.root, f.nsID, fmt.Sprintf("seg-%016x.vol", uint64(segID)))
}

func writeBlob(t *testing.T, eng *volume.Engine, payload string) *volume.WriteResult {
	t.Helper()
	res, err := eng.WriteBlob(bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatalf("WriteBlob(%q): %v", payload, err)
	}
	return res
}

// sealedSegments returns every SegmentStat except the currently active
// segment (SegmentStats already returns them sorted by SegmentID).
func sealedSegments(t *testing.T, eng *volume.Engine) []volume.SegmentStat {
	t.Helper()
	stats, err := eng.SegmentStats()
	if err != nil {
		t.Fatalf("SegmentStats: %v", err)
	}
	activeID, hasActive := eng.ActiveSegmentID()
	var out []volume.SegmentStat
	for _, s := range stats {
		if hasActive && s.SegmentID == activeID {
			continue
		}
		out = append(out, s)
	}
	return out
}

func TestSegmentStats_TalliesLiveAndDeadCorrectly(t *testing.T) {
	f := openRewriteTestEngine(t)

	writeBlob(t, f.eng, "first blob payload")
	dying := writeBlob(t, f.eng, "second blob payload, will be deleted")
	// A third write now reliably rolls: the segment is exactly full after
	// the two writes above (see openRewriteTestEngine's doc comment).
	writeBlob(t, f.eng, "third blob forces the roll")

	sealed := sealedSegments(t, f.eng)
	if len(sealed) == 0 {
		t.Fatal("expected at least one sealed segment after forcing a roll")
	}
	for _, s := range sealed {
		if s.DeadPages != 0 {
			t.Fatalf("segment %s has DeadPages=%d before anything was deleted", s.SegmentID, s.DeadPages)
		}
	}

	if err := f.eng.MarkDeleted(dying.Chunks[0]); err != nil {
		t.Fatalf("MarkDeleted: %v", err)
	}

	after, err := f.eng.SegmentStats()
	if err != nil {
		t.Fatalf("SegmentStats (after): %v", err)
	}
	var found bool
	for _, s := range after {
		if s.SegmentID != dying.Chunks[0].SegmentID {
			continue
		}
		found = true
		if s.DeadPages == 0 {
			t.Fatalf("segment %s DeadPages=0 after MarkDeleted, want > 0", s.SegmentID)
		}
		if s.DeadRatio() <= 0 {
			t.Fatalf("segment %s DeadRatio()=%v after MarkDeleted, want > 0", s.SegmentID, s.DeadRatio())
		}
	}
	if !found {
		t.Fatalf("SegmentStats did not report segment %s at all", dying.Chunks[0].SegmentID)
	}
}

func TestRewriteSegment_KeepsLiveDropsDeadAndPreservesBytes(t *testing.T) {
	f := openRewriteTestEngine(t)

	const livePayload = "this chunk survives the rewrite"
	const deadPayload = "this chunk is orphaned and should be dropped"

	live := writeBlob(t, f.eng, livePayload)
	dead := writeBlob(t, f.eng, deadPayload)
	// A third write now reliably rolls: the segment is exactly full after
	// the two writes above (see openRewriteTestEngine's doc comment).
	writeBlob(t, f.eng, "third blob forces the roll")

	targetSegID := live.Chunks[0].SegmentID
	if dead.Chunks[0].SegmentID != targetSegID {
		t.Fatalf("test setup: live and dead chunks landed in different segments (%s vs %s) — adjust MaxSegmentSize",
			live.Chunks[0].SegmentID, dead.Chunks[0].SegmentID)
	}

	if err := f.eng.MarkDeleted(dead.Chunks[0]); err != nil {
		t.Fatalf("MarkDeleted: %v", err)
	}

	result, relocated, err := f.eng.RewriteSegment(targetSegID)
	if err != nil {
		t.Fatalf("RewriteSegment: %v", err)
	}
	if result.OldSegmentID != targetSegID {
		t.Fatalf("result.OldSegmentID = %s, want %s", result.OldSegmentID, targetSegID)
	}
	if result.NewSegmentID == targetSegID {
		t.Fatal("result.NewSegmentID equals OldSegmentID — rewrite must create a distinct segment")
	}
	if result.ChunksKept != 1 {
		t.Fatalf("result.ChunksKept = %d, want 1 (only the live chunk)", result.ChunksKept)
	}
	if result.BytesFreed != int64(len(deadPayload)) {
		t.Fatalf("result.BytesFreed = %d, want %d (the dead chunk's payload length)", result.BytesFreed, len(deadPayload))
	}
	if len(relocated) != 1 {
		t.Fatalf("len(relocated) = %d, want 1", len(relocated))
	}

	newEntry := relocated[0]
	if newEntry.ChunkID != live.Chunks[0].ChunkID {
		t.Fatalf("relocated ChunkID = %s, want %s (must be unchanged — only location moves)", newEntry.ChunkID, live.Chunks[0].ChunkID)
	}
	if newEntry.SegmentID != result.NewSegmentID {
		t.Fatalf("relocated.SegmentID = %s, want %s", newEntry.SegmentID, result.NewSegmentID)
	}

	// The live chunk must be readable, byte-for-byte, at its NEW location.
	data, err := f.eng.ReadChunk(newEntry)
	if err != nil {
		t.Fatalf("ReadChunk(relocated live chunk): %v", err)
	}
	if string(data) != livePayload {
		t.Fatalf("ReadChunk(relocated) = %q, want %q", data, livePayload)
	}

	// Old segment file must still exist — RewriteSegment does not delete
	// it. Deletion is the caller's (store.Compact's) job, and only after
	// the relocated entries above are durably committed to the index.
	oldPath := f.segFilePath(targetSegID)
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("old segment file %s missing after RewriteSegment (it must not be deleted yet): %v", oldPath, err)
	}

	// New segment file must exist too.
	newPath := f.segFilePath(result.NewSegmentID)
	if _, err := os.Stat(newPath); err != nil {
		t.Fatalf("new segment file %s missing after RewriteSegment: %v", newPath, err)
	}
}

func TestRewriteSegment_RefusesActiveSegment(t *testing.T) {
	f := openRewriteTestEngine(t)
	writeBlob(t, f.eng, "only blob, stays in the active segment")

	activeID, ok := f.eng.ActiveSegmentID()
	if !ok {
		t.Fatal("expected an active segment after a write")
	}

	if _, _, err := f.eng.RewriteSegment(activeID); err == nil {
		t.Fatal("RewriteSegment on the active segment returned nil error, want an error")
	}
}

func TestDeleteSegmentFile_RefusesActiveSegment(t *testing.T) {
	f := openRewriteTestEngine(t)
	writeBlob(t, f.eng, "only blob, stays in the active segment")

	activeID, ok := f.eng.ActiveSegmentID()
	if !ok {
		t.Fatal("expected an active segment after a write")
	}

	if err := f.eng.DeleteSegmentFile(activeID); err == nil {
		t.Fatal("DeleteSegmentFile on the active segment returned nil error, want an error")
	}
}

func TestDeleteSegmentFile_RemovesDataAndWAL(t *testing.T) {
	f := openRewriteTestEngine(t)
	writeBlob(t, f.eng, "first")
	writeBlob(t, f.eng, "second") // segment now exactly full (see openRewriteTestEngine)
	writeBlob(t, f.eng, "third blob forces the roll")

	sealed := sealedSegments(t, f.eng)
	if len(sealed) == 0 {
		t.Fatal("expected a sealed segment")
	}
	segID := sealed[0].SegmentID
	path := f.segFilePath(segID)
	walPath := path[:len(path)-len(filepath.Ext(path))] + ".wal"

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("segment file should exist before delete: %v", err)
	}

	if err := f.eng.DeleteSegmentFile(segID); err != nil {
		t.Fatalf("DeleteSegmentFile: %v", err)
	}

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("segment file still exists after DeleteSegmentFile (err=%v)", err)
	}
	if _, err := os.Stat(walPath); err != nil && !os.IsNotExist(err) {
		t.Fatalf("unexpected error checking WAL file %s: %v", walPath, err)
	}

	// Deleting an already-gone segment must not error (idempotent, matches
	// os.Remove's IsNotExist handling documented on DeleteSegmentFile).
	if err := f.eng.DeleteSegmentFile(segID); err != nil {
		t.Fatalf("second DeleteSegmentFile on an already-removed segment = %v, want nil", err)
	}
}
