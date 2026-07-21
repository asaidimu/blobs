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

// TestWALReplay_RegistersInterruptedWriteAsNonReapable is the core
// scenario production readiness item 5.3 describes: WriteBlob completed
// and fsynced (so its WAL entry is durable), but the process crashed
// before CommitPut (ref + blob manifest) ever ran. A raw volume.Engine,
// used directly and bypassing package store entirely, is how this test
// simulates that crash deterministically — there is no CommitPut call
// anywhere in this test for the blob under test.
//
// Before WAL replay existed, store.Open would bring this namespace's
// engine online with no idea this blob exists in the index at all, and
// the first Compact call would find its chunks with no owning BlobEntry
// and reap them as orphans. With replay, the blob gets a BlobEntry
// (RefCount: 1, i.e. presumed referenced) before the store is handed back
// to any caller, so Compact leaves it alone.
func TestWALReplay_RegistersInterruptedWriteAsNonReapable(t *testing.T) {
	dataDir := t.TempDir()
	const nsID = "default"

	// Step 1: simulate the crash. Write directly through package volume,
	// with no store, no index, and — critically — no CommitPut. This is
	// exactly "WriteBlob succeeded and fsynced; the process died before
	// the caller's CommitPut ran."
	rawEng, err := volume.Open(dataDir, nsID, volume.Options{})
	if err != nil {
		t.Fatalf("volume.Open (simulating pre-crash write): %v", err)
	}
	const payload = "this blob's WriteBlob succeeded but CommitPut never ran"
	if _, err := rawEng.WriteBlob(bytes.NewReader([]byte(payload))); err != nil {
		t.Fatalf("WriteBlob: %v", err)
	}
	if err := rawEng.Close(); err != nil {
		t.Fatalf("close raw engine: %v", err)
	}

	// Step 2: "restart" — open a real Store pointed at the same DataDir,
	// with a fresh, empty index (nothing has ever been committed to any
	// index for this data — that's the whole point). CreateNamespace opens
	// the engine and replays the WAL as part of coming online.
	s, err := store.Open(store.Config{
		DataDir: dataDir,
		Index:   index.NewMemoryBackend(),
	})
	if err != nil {
		t.Fatalf("store.Open (post-crash restart): %v", err)
	}
	defer s.Close()
	ctx := context.Background()
	if err := s.CreateNamespace(ctx, object.Namespace{ID: nsID}); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	ns := s.Namespace(nsID)

	// The blob was never linked to any key — that mapping only ever
	// lived in a CommitPut call that never happened, and nothing durable
	// records it. So Get-by-key correctly still 404s; replay does not
	// and cannot restore that.
	if _, err := ns.Head(ctx, "never-had-a-key"); err == nil {
		t.Fatal("Head on an unrelated key unexpectedly succeeded")
	}

	// The real assertion: Compact must NOT reap the replayed blob's
	// chunks. Checked directly against on-disk page flags, the same way
	// the RebuildIndex tests do, so this can't be fooled by anything at
	// the index layer.
	if _, err := ns.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := segmentDeadPageTotals(t, dataDir, nsID); got != 0 {
		t.Fatalf("dead pages = %d after replay+Compact, want 0 — Compact reaped a blob WAL replay should have protected", got)
	}
}

// TestWALReplay_SkipsAlreadyCommittedBlobs confirms replay is a no-op —
// and, in particular, does not error or double-count — for the ordinary
// case: a blob whose CommitPut already completed before the store was
// closed. This is what should happen on every normal restart.
func TestWALReplay_SkipsAlreadyCommittedBlobs(t *testing.T) {
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

	if _, err := ns1.Put(ctx, "normal", bytes.NewReader([]byte("a completely normal, fully committed write")), store.PutOptions{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen with a NEW index (same simulate-index-loss setup as the
	// RebuildIndex tests) specifically so this test can tell the
	// difference between "replay skipped it because it's already
	// committed" and "the key still resolves because the index survived
	// from before" — with a fresh index, Head must 404 (ref layer isn't
	// replay's job), while replay itself must not treat this already-
	// complete write as something to touch, and must not error out.
	s2, err := store.Open(store.Config{
		DataDir: dataDir,
		Index:   index.NewMemoryBackend(),
	})
	if err != nil {
		t.Fatalf("store.Open (fresh index): %v", err)
	}
	defer s2.Close()
	if err := s2.CreateNamespace(ctx, object.Namespace{ID: "default"}); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	ns2 := s2.Namespace("default")

	if _, err := ns2.Head(ctx, "normal"); err == nil {
		t.Fatal("Head succeeded against a fresh index — replay does not restore RefEntry/key mappings, only chunk/blob records")
	}

	if _, err := ns2.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := s2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// This is the actual point of the test: a normally-committed write,
	// replayed against a fresh index, must ALSO survive Compact — WAL
	// replay's RefCount:1 registration applies identically whether the
	// original Put fully completed or was interrupted, since (with a
	// fresh index either way) neither case has any RefEntry.
	if got := segmentDeadPageTotals(t, dataDir, "default"); got != 0 {
		t.Fatalf("dead pages = %d, want 0", got)
	}
}

// TestWALReplay_EmptyNamespaceIsFastAndHarmless confirms opening a
// namespace with no segments at all (the common case: a brand new
// namespace, or a store that was always empty) doesn't error and doesn't
// try to read WAL files that don't exist.
func TestWALReplay_EmptyNamespaceIsFastAndHarmless(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(store.Config{
		DataDir: t.TempDir(),
		Index:   index.NewMemoryBackend(),
	})
	if err != nil {
		t.Fatalf("store.Open on a brand new DataDir: %v", err)
	}
	defer s.Close()
	if err := s.CreateNamespace(ctx, object.Namespace{ID: "default"}); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
}
