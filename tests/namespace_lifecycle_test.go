package store_test

// Tests for namespace identity and the delete → cleanup lifecycle.
//
// Every namespace incarnation gets an immutable, opaque UID at creation. The
// UID — never the human-chosen name — keys all of the namespace's index
// records and names its directory on disk. The name is only a pointer to the
// current UID. DeleteNamespace atomically unlinks that pointer (so the name is
// free immediately) and then erases everything owned by the old UID; if the
// erase cannot finish, a tombstone remains and
// Store.CleanupDeletedNamespaces (also run by Open) completes it.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	bserrors "github.com/asaidimu/blobs/errors"
	"github.com/asaidimu/blobs/index"
	"github.com/asaidimu/blobs/index/backend"
	"github.com/asaidimu/blobs/object"
	"github.com/asaidimu/blobs/store"
)

func nsUID(t *testing.T, s *store.Store, name string) string {
	t.Helper()
	ns, err := s.GetNamespace(context.Background(), name)
	if err != nil {
		t.Fatalf("GetNamespace(%q): %v", name, err)
	}
	if ns.UID == "" {
		t.Fatalf("namespace %q has no UID", name)
	}
	return ns.UID
}

func dirExists(path string) bool {
	st, err := os.Stat(path)
	return err == nil && st.IsDir()
}

func isNotFound(err error) bool {
	var nf *bserrors.NotFoundError
	return errors.As(err, &nf)
}

// ── identity ─────────────────────────────────────────────────────────────────

func TestNamespaceUID_UniquePerIncarnationAndDirectoryFollowsUID(t *testing.T) {
	s, dir := openStoreIn(t)
	ctx := context.Background()

	mustCreate(t, s, "tenant")
	first := nsUID(t, s, "tenant")
	if first == "tenant" {
		t.Fatal("UID must be opaque, not the namespace name")
	}
	if !dirExists(filepath.Join(dir, first)) {
		t.Fatalf("namespace directory should be named after its UID %q", first)
	}
	if dirExists(filepath.Join(dir, "tenant")) {
		t.Fatal("namespace directory must not be named after the human-chosen name")
	}

	if err := s.DeleteNamespace(ctx, "tenant"); err != nil {
		t.Fatalf("DeleteNamespace: %v", err)
	}
	if dirExists(filepath.Join(dir, first)) {
		t.Fatal("old incarnation's directory survived DeleteNamespace")
	}

	mustCreate(t, s, "tenant")
	second := nsUID(t, s, "tenant")
	if second == first {
		t.Fatal("re-created namespace reused its predecessor's UID")
	}
	if !dirExists(filepath.Join(dir, second)) {
		t.Fatal("re-created namespace has no directory of its own")
	}
}

func TestCreateNamespace_IgnoresCallerSuppliedUID(t *testing.T) {
	s, dir := openStoreIn(t)
	ctx := context.Background()

	mustCreate(t, s, "victim")
	victimUID := nsUID(t, s, "victim")

	for _, forged := range []string{victimUID, "../escape", "victim", ""} {
		name := fmt.Sprintf("attacker-%d", len(forged))
		err := s.CreateNamespace(ctx, object.Namespace{ID: name, UID: forged})
		if err != nil {
			t.Fatalf("CreateNamespace with UID %q: %v", forged, err)
		}
		got := nsUID(t, s, name)
		if got == forged || got == victimUID {
			t.Fatalf("store accepted caller-supplied UID %q", forged)
		}
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(dir), "escape")); err == nil {
		t.Fatal("a caller-supplied UID created a directory outside DataDir")
	}
}

func TestNamespacesWithSameContent_HaveSeparateDirectories(t *testing.T) {
	s, dir := openStoreIn(t)
	a := mustCreate(t, s, "tenant-a")
	b := mustCreate(t, s, "tenant-b")
	marker := []byte("ONLY-IN-TENANT-A-MARKER-0003")
	putBytes(t, a, "x", secretBlob(string(marker), 2048))
	putBytes(t, b, "y", randBytes(2048))

	bDir := filepath.Join(dir, nsUID(t, s, "tenant-b"))
	if dataDirContains(t, bDir, marker) {
		t.Fatal("tenant-a's bytes are present in tenant-b's directory")
	}
	aDir := filepath.Join(dir, nsUID(t, s, "tenant-a"))
	if !dataDirContains(t, aDir, marker) {
		t.Fatal("tenant-a's bytes are missing from tenant-a's directory")
	}
}

// ── operations on a deleted namespace ────────────────────────────────────────

func TestOperationsOnDeletedNamespace_ReturnNotFound(t *testing.T) {
	s, _ := openStoreIn(t)
	ctx := context.Background()

	ns := mustCreate(t, s, "gone")
	putBytes(t, ns, "k", randBytes(256))
	if err := s.DeleteNamespace(ctx, "gone"); err != nil {
		t.Fatalf("DeleteNamespace: %v", err)
	}

	checks := map[string]func() error{
		"Put": func() error {
			_, err := ns.Put(ctx, "k2", bytes.NewReader([]byte("x")), store.PutOptions{})
			return err
		},
		"Get":   func() error { _, err := ns.Get(ctx, "k"); return err },
		"Head":  func() error { _, err := ns.Head(ctx, "k"); return err },
		"List":  func() error { _, err := ns.List(ctx, store.ListOptions{}); return err },
		"Stats": func() error { _, err := ns.Stats(ctx); return err },
		"Verify": func() error {
			return ns.Verify(ctx)
		},
		"Compact":    func() error { _, err := ns.Compact(ctx); return err },
		"GetNS":      func() error { _, err := s.GetNamespace(ctx, "gone"); return err },
		"DeleteNS":   func() error { return s.DeleteNamespace(ctx, "gone") },
		"Rename":     func() error { return ns.Rename(ctx, "k", "k3") },
		"UpdateMeta": func() error { return ns.Update(ctx, "k", map[string]any{"a": 1}) },
	}
	for name, fn := range checks {
		if err := fn(); !isNotFound(err) {
			t.Errorf("%s on a deleted namespace: got %v, want *errors.NotFoundError", name, err)
		}
	}
}

// ── deletion with a reader still draining ────────────────────────────────────

// If the caller's context expires while DeleteNamespace waits for an in-flight
// reader, the delete has still logically happened: the name is released and
// the namespace is gone to new operations, but the reader keeps working and
// the physical erase is left pending.
func TestDeleteNamespace_ContextExpiryLeavesCleanupPending(t *testing.T) {
	s, dir := openStoreIn(t)
	ctx := context.Background()

	ns := mustCreate(t, s, "tenant")
	marker := []byte("DRAINING-READER-MARKER-0004")
	payload := secretBlob(string(marker), 16<<10)
	putBytes(t, ns, "doc", payload)
	oldDir := filepath.Join(dir, nsUID(t, s, "tenant"))

	rc, err := ns.Get(ctx, "doc")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	t.Cleanup(func() { rc.Close() })

	dctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	err = s.DeleteNamespace(dctx, "tenant")
	var pending *bserrors.CleanupPendingError
	if !errors.As(err, &pending) {
		t.Fatalf("DeleteNamespace with an expiring context: got %v, want *errors.CleanupPendingError", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("CleanupPendingError should wrap the context error, got %v", err)
	}

	// Logically deleted: name released, namespace unreachable by name.
	if _, err := ns.Get(ctx, "doc"); !isNotFound(err) {
		t.Errorf("Get after logical delete: got %v, want NotFound", err)
	}
	mustCreate(t, s, "tenant")
	if st, _ := s.Namespace("tenant").Stats(ctx); st == nil || st.BlobCount != 0 {
		t.Error("recreated namespace is not empty")
	}

	// The in-flight reader still sees correct bytes; erase has not happened.
	got, err := io.ReadAll(rc)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("in-flight reader broke during pending delete: err=%v equal=%v", err, bytes.Equal(got, payload))
	}
	rc.Close()
	if !dataDirContains(t, oldDir, marker) {
		t.Fatal("bytes were erased while a reader still held them")
	}

	n, err := s.CleanupDeletedNamespaces(ctx)
	if err != nil {
		t.Fatalf("CleanupDeletedNamespaces: %v", err)
	}
	if n != 1 {
		t.Errorf("CleanupDeletedNamespaces purged %d namespaces, want 1", n)
	}
	if dirExists(oldDir) || dataDirContains(t, dir, marker) {
		t.Fatal("bytes survived CleanupDeletedNamespaces")
	}
	if n, err := s.CleanupDeletedNamespaces(ctx); err != nil || n != 0 {
		t.Errorf("second CleanupDeletedNamespaces = (%d, %v), want (0, nil)", n, err)
	}
}

// ── failure part-way through erase ───────────────────────────────────────────

func TestCleanupDeletedNamespaces_CompletesAfterTransientFailure(t *testing.T) {
	fb := &flakyBackend{Backend: index.NewMemoryBackend()}
	dir := t.TempDir()
	s, err := store.Open(store.Config{DataDir: dir, Index: fb})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	ctx := context.Background()

	ns := mustCreate(t, s, "tenant")
	marker := []byte("TRANSIENT-FAILURE-MARKER-0005")
	putBytes(t, ns, "a", secretBlob(string(marker), 2048))
	putBytes(t, ns, "b", randBytes(2048))
	oldUID := nsUID(t, s, "tenant")

	fb.arm(1) // the atomic unlink succeeds; erase then fails
	err = s.DeleteNamespace(ctx, "tenant")
	var pending *bserrors.CleanupPendingError
	if !errors.As(err, &pending) {
		t.Fatalf("DeleteNamespace with failing erase: got %v, want *errors.CleanupPendingError", err)
	}
	if pending.NamespaceID != "tenant" {
		t.Errorf("CleanupPendingError.NamespaceID = %q, want %q", pending.NamespaceID, "tenant")
	}
	fb.disarm()

	// The name is free right away, even though the old data is still there.
	fresh := mustCreate(t, s, "tenant")
	putBytes(t, fresh, "new", randBytes(512))
	if nsUID(t, s, "tenant") == oldUID {
		t.Fatal("recreated namespace reused the UID of the namespace being cleaned up")
	}

	n, err := s.CleanupDeletedNamespaces(ctx)
	if err != nil {
		t.Fatalf("CleanupDeletedNamespaces: %v", err)
	}
	if n != 1 {
		t.Errorf("purged %d, want 1", n)
	}
	if dataDirContains(t, dir, marker) {
		t.Fatal("old namespace's bytes survived cleanup")
	}
	// The live namespace was not touched by the cleanup of its predecessor.
	if got := getBytes(t, s.Namespace("tenant"), "new"); len(got) != 512 {
		t.Fatalf("live namespace damaged by cleanup of its predecessor: %d bytes", len(got))
	}
}

// A store reopened with a tombstone outstanding finishes the erase on Open.
func TestOpen_CompletesPendingCleanup(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(t.TempDir(), "index.bbolt")
	ctx := context.Background()

	open := func(extra func(index.Backend) index.Backend) *store.Store {
		bb, err := backend.Open(backend.Options{Path: idxPath})
		if err != nil {
			t.Fatalf("backend.Open: %v", err)
		}
		var b index.Backend = bb
		if extra != nil {
			b = extra(bb)
		}
		s, err := store.Open(store.Config{DataDir: dir, Index: b})
		if err != nil {
			t.Fatalf("store.Open: %v", err)
		}
		return s
	}

	var fb *flakyBackend
	s := open(func(b index.Backend) index.Backend {
		fb = &flakyBackend{Backend: b}
		return fb
	})
	ns := mustCreate(t, s, "tenant")
	marker := []byte("SURVIVES-RESTART-MARKER-0006")
	putBytes(t, ns, "a", secretBlob(string(marker), 2048))

	fb.arm(1)
	var pending *bserrors.CleanupPendingError
	if err := s.DeleteNamespace(ctx, "tenant"); !errors.As(err, &pending) {
		t.Fatalf("DeleteNamespace: got %v, want CleanupPendingError", err)
	}
	fb.disarm()
	if !dataDirContains(t, dir, marker) {
		t.Fatal("test setup: erase should have failed and left the bytes in place")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2 := open(nil)
	defer s2.Close()
	if dataDirContains(t, dir, marker) {
		t.Fatal("Open did not finish erasing a previously deleted namespace")
	}
	if n, err := s2.CleanupDeletedNamespaces(ctx); err != nil || n != 0 {
		t.Errorf("CleanupDeletedNamespaces after Open = (%d, %v), want (0, nil)", n, err)
	}
	if _, err := s2.GetNamespace(ctx, "tenant"); !isNotFound(err) {
		t.Errorf("deleted namespace reappeared after restart: %v", err)
	}
}

// ── stress: delete/recreate churn beside live traffic ────────────────────────

func TestNamespaceChurn_DoesNotDisturbNeighbours(t *testing.T) {
	s, dir := openStoreIn(t)
	ctx := context.Background()

	stable := mustCreate(t, s, "stable")
	want := randBytes(8 << 10)
	putBytes(t, stable, "doc", want)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	errs := make(chan error, 16)

	// Readers/writers on the stable tenant.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			n := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				rc, err := stable.Get(ctx, "doc")
				if err != nil {
					errs <- fmt.Errorf("stable Get: %w", err)
					return
				}
				got, err := io.ReadAll(rc)
				rc.Close()
				if err != nil || !bytes.Equal(got, want) {
					errs <- fmt.Errorf("stable read wrong: err=%v", err)
					return
				}
				n++
				if _, err := stable.Put(ctx, fmt.Sprintf("w-%d-%d", i, n%5), bytes.NewReader(randBytes(1024)), store.PutOptions{ContentType: "application/octet-stream"}); err != nil {
					errs <- fmt.Errorf("stable Put: %w", err)
					return
				}
			}
		}(i)
	}

	// Churn: create, fill, delete, repeat, under a reused name.
	for round := 0; round < 25; round++ {
		if err := s.CreateNamespace(ctx, object.Namespace{ID: "churn"}); err != nil {
			t.Fatalf("round %d CreateNamespace: %v", round, err)
		}
		c := s.Namespace("churn")
		for k := 0; k < 3; k++ {
			if _, err := c.Put(ctx, fmt.Sprintf("k%d", k), bytes.NewReader(randBytes(2048)), store.PutOptions{ContentType: "application/octet-stream"}); err != nil {
				t.Fatalf("round %d Put: %v", round, err)
			}
		}
		if err := s.DeleteNamespace(ctx, "churn"); err != nil {
			t.Fatalf("round %d DeleteNamespace: %v", round, err)
		}
	}
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// Only the stable namespace's directory should remain.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != nsUID(t, s, "stable") {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("data dir has leftovers after churn: %v", names)
	}
}
