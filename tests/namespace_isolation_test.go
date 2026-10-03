package store_test

// Namespace lifecycle and tenant-isolation tests.
//
// These pin down the behaviour a multi-tenant deployment depends on:
//
//   - Deleting a namespace erases its bytes and index state; it is not merely
//     "hidden until Compact".
//   - Re-creating a namespace under a previously used name yields a pristine,
//     empty namespace that shares nothing with its predecessor — not its
//     stats, not its segment files, not its encryption key material.
//   - Deleting one tenant never stalls or corrupts another tenant.
//   - Identical content stored by two tenants stays physically and logically
//     separate.
//
// They use only the public store API, so they exercise exactly what an
// application can observe.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asaidimu/blobs/index"
	"github.com/asaidimu/blobs/object"
	"github.com/asaidimu/blobs/store"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// secretBlob returns a payload that contains a unique, easily searchable
// marker followed by random filler, so a test can ask "do these bytes still
// exist anywhere under the data directory?".
func secretBlob(marker string, size int) []byte {
	b := make([]byte, 0, size)
	b = append(b, marker...)
	b = append(b, randBytes(size-len(marker))...)
	return b
}

// dataDirContains reports whether any regular file under root contains needle.
func dataDirContains(t *testing.T, root string, needle []byte) bool {
	t.Helper()
	found := false
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(data, needle) {
			found = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return found
}

// openStoreIn is openStore with a caller-visible data directory and no
// pre-created namespaces.
func openStoreIn(t *testing.T) (*store.Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := store.Open(store.Config{DataDir: dir, Index: index.NewMemoryBackend()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return s, dir
}

func mustCreate(t *testing.T, s *store.Store, id string) *store.NamespaceHandle {
	t.Helper()
	if err := s.CreateNamespace(context.Background(), object.Namespace{ID: id}); err != nil {
		t.Fatalf("CreateNamespace(%q): %v", id, err)
	}
	return s.Namespace(id)
}

// ── deletion erases data ──────────────────────────────────────────────────────

func TestDeleteNamespace_ErasesBytesFromDisk(t *testing.T) {
	s, dir := openStoreIn(t)
	ctx := context.Background()

	ns := mustCreate(t, s, "tenant-a")
	marker := []byte("TENANT-A-CONFIDENTIAL-MARKER-0001")
	putBytes(t, ns, "secret", secretBlob(string(marker), 2048))

	if !dataDirContains(t, dir, marker) {
		t.Fatal("test setup: marker should be on disk after Put")
	}

	if err := s.DeleteNamespace(ctx, "tenant-a"); err != nil {
		t.Fatalf("DeleteNamespace: %v", err)
	}

	if dataDirContains(t, dir, marker) {
		t.Fatal("deleted namespace's bytes are still on disk: DeleteNamespace only unlinks refs and leaves segments behind")
	}
}

func TestRecreatedNamespace_StartsPristine(t *testing.T) {
	s, dir := openStoreIn(t)
	ctx := context.Background()

	old := mustCreate(t, s, "tenant")
	marker := []byte("PREVIOUS-TENANT-MARKER-0002")
	putBytes(t, old, "a", secretBlob(string(marker), 4096))
	putBytes(t, old, "b", randBytes(4096))
	if err := s.DeleteNamespace(ctx, "tenant"); err != nil {
		t.Fatalf("DeleteNamespace: %v", err)
	}

	// Same name, before anyone ran Compact (there is nothing left to run
	// it against).
	fresh := mustCreate(t, s, "tenant")

	st, err := fresh.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.BlobCount != 0 || st.BytesStored != 0 || st.BytesPhysical != 0 ||
		st.ChunkCount != 0 || st.DeadBytes != 0 || st.DeadChunks != 0 || st.SegmentCount != 0 {
		t.Errorf("recreated namespace inherited its predecessor's accounting: %+v", *st)
	}

	items, err := fresh.List(ctx, store.ListOptions{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(items) != 0 {
		t.Errorf("recreated namespace lists %d keys, want 0", len(items))
	}
	for _, key := range []string{"a", "b"} {
		if _, err := fresh.Get(ctx, key); err == nil {
			t.Errorf("Get(%q) on a recreated namespace succeeded", key)
		}
	}

	if dataDirContains(t, dir, marker) {
		t.Error("previous tenant's bytes are still on disk in the recreated namespace's directory")
	}

	// And it must be fully functional.
	payload := randBytes(1024)
	putBytes(t, fresh, "new", payload)
	if got := getBytes(t, fresh, "new"); !bytes.Equal(got, payload) {
		t.Error("recreated namespace round-trip mismatch")
	}
}

// With encryption on, a recreated namespace has a brand-new data key. If the
// old chunk record (with the old key's nonce/tag) is revived by a dedup hit,
// the new tenant's own data becomes undecryptable.
func TestRecreatedNamespace_EncryptedDedupDoesNotReuseOldKeyMaterial(t *testing.T) {
	dir := t.TempDir()
	s, err := store.Open(store.Config{
		DataDir:     dir,
		Index:       index.NewMemoryBackend(),
		KeyProvider: newStaticKeyProvider(t),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	ctx := context.Background()

	create := func() *store.NamespaceHandle {
		if err := s.CreateNamespace(ctx, object.Namespace{ID: "vault"}, store.WithEncryption()); err != nil {
			t.Fatalf("CreateNamespace: %v", err)
		}
		return s.Namespace("vault")
	}

	payload := randBytes(8192)

	first := create()
	putBytes(t, first, "doc", payload)
	if err := s.DeleteNamespace(ctx, "vault"); err != nil {
		t.Fatalf("DeleteNamespace: %v", err)
	}

	second := create()
	putBytes(t, second, "doc", payload) // identical content, new data key

	rc, err := second.Get(ctx, "doc")
	if err != nil {
		t.Fatalf("Get after recreate: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll after recreate: %v (chunk index record carries the previous namespace's nonce/tag)", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("recreated encrypted namespace returned wrong bytes")
	}
}

// ── deletion never stalls or damages other tenants ───────────────────────────

// An open reader on tenant A must not make DeleteNamespace(B) wait: the two
// tenants share nothing.
func TestDeleteNamespace_DoesNotStallOtherNamespaces(t *testing.T) {
	s, _ := openStoreIn(t)
	ctx := context.Background()

	a := mustCreate(t, s, "tenant-a")
	mustCreate(t, s, "tenant-b")
	putBytes(t, a, "doc", randBytes(1024))

	rc, err := a.Get(ctx, "doc")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	// Registered after openStoreIn's Close cleanup, so it runs first (LIFO);
	// otherwise a stalled delete would also wedge Close.
	t.Cleanup(func() { rc.Close() })

	done := make(chan error, 1)
	go func() { done <- s.DeleteNamespace(ctx, "tenant-b") }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("DeleteNamespace(tenant-b): %v", err)
		}
	case <-time.After(5 * time.Second):
		rc.Close()
		<-done
		t.Fatal("DeleteNamespace(tenant-b) blocked behind an open reader on tenant-a: deletion takes a store-wide lock")
	}

	// Tenant A is untouched and its reader is still good.
	if _, err := io.ReadAll(rc); err != nil {
		t.Fatalf("tenant-a reader broke after deleting tenant-b: %v", err)
	}
}

// A namespace's own in-flight reader must be allowed to finish: deleting the
// namespace may not pull the bytes out from under it.
func TestDeleteNamespace_LetsInFlightReaderFinish(t *testing.T) {
	s, _ := openStoreIn(t)
	ctx := context.Background()

	ns := mustCreate(t, s, "tenant")
	payload := randBytes(32 << 10)
	putBytes(t, ns, "doc", payload)

	rc, err := ns.Get(ctx, "doc")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	t.Cleanup(func() { rc.Close() })

	done := make(chan error, 1)
	go func() { done <- s.DeleteNamespace(ctx, "tenant") }()

	select {
	case err := <-done:
		t.Fatalf("DeleteNamespace returned (%v) while a reader on that namespace was still open", err)
	case <-time.After(200 * time.Millisecond):
	}

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("in-flight read failed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("in-flight read returned wrong bytes")
	}
	rc.Close()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("DeleteNamespace: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("DeleteNamespace never completed after the reader closed")
	}
}

// flakyBackend wraps a Backend and, once armed, lets failAfter more
// transactions/deletes succeed and then fails every further one until
// disarmed.
type flakyBackend struct {
	index.Backend
	armed     atomic.Bool
	remaining atomic.Int64
}

var errInjected = errors.New("injected backend failure")

func (f *flakyBackend) arm(failAfter int64) {
	f.remaining.Store(failAfter)
	f.armed.Store(true)
}
func (f *flakyBackend) disarm() { f.armed.Store(false) }

func (f *flakyBackend) shouldFail() bool {
	if !f.armed.Load() {
		return false
	}
	return f.remaining.Add(-1) < 0
}

func (f *flakyBackend) Tx(ctx context.Context, fn func(index.Tx) error) error {
	if f.shouldFail() {
		return errInjected
	}
	return f.Backend.Tx(ctx, fn)
}

func (f *flakyBackend) Delete(ctx context.Context, key []byte) error {
	if f.shouldFail() {
		return errInjected
	}
	return f.Backend.Delete(ctx, key)
}

// If DeleteNamespace fails part-way, the namespace name must not be left
// wedged: afterwards it must either still be fully operational (nothing
// happened) or be free to create again (the delete took effect).
func TestDeleteNamespace_FailurePartWayDoesNotWedgeTheName(t *testing.T) {
	fb := &flakyBackend{Backend: index.NewMemoryBackend()}
	dir := t.TempDir()
	s, err := store.Open(store.Config{DataDir: dir, Index: fb})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	ctx := context.Background()

	ns := mustCreate(t, s, "tenant")
	for _, k := range []string{"k1", "k2", "k3"} {
		putBytes(t, ns, k, randBytes(512))
	}

	fb.arm(1) // first index write succeeds, everything after fails
	if err := s.DeleteNamespace(ctx, "tenant"); err == nil {
		t.Log("DeleteNamespace reported success despite injected failures")
	}
	fb.disarm()

	operational := func() bool {
		payload := randBytes(256)
		if _, err := s.Namespace("tenant").Put(ctx, "probe", bytes.NewReader(payload), store.PutOptions{ContentType: "application/octet-stream"}); err != nil {
			return false
		}
		rc, err := s.Namespace("tenant").Get(ctx, "probe")
		if err != nil {
			return false
		}
		defer rc.Close()
		got, err := io.ReadAll(rc)
		return err == nil && bytes.Equal(got, payload)
	}

	if operational() {
		return
	}
	if err := s.CreateNamespace(ctx, object.Namespace{ID: "tenant"}); err != nil {
		t.Fatalf("namespace name is wedged after a failed delete: not operational and CreateNamespace says: %v", err)
	}
	if !operational() {
		t.Fatal("re-created namespace is not operational")
	}
}

// ── cross-namespace content isolation ────────────────────────────────────────

// Two tenants store identical bytes. Chunk records are content-addressed, but a
// chunk's location is inside one namespace's own segment files. Tenant B's
// activity must never redirect or remove tenant A's chunk records.
func TestIdenticalContentInTwoNamespaces_IsIsolated(t *testing.T) {
	s, _ := openStoreIn(t)
	ctx := context.Background()

	a := mustCreate(t, s, "tenant-a")
	b := mustCreate(t, s, "tenant-b")

	shared := randBytes(64 << 10)

	// Tenant A writes something else first so that the shared content sits at
	// a different physical offset in A than in B.
	putBytes(t, a, "filler", randBytes(40<<10))
	putBytes(t, a, "shared", shared)
	putBytes(t, b, "shared", shared)

	if got := getBytes(t, a, "shared"); !bytes.Equal(got, shared) {
		t.Fatal("tenant-a read wrong bytes after tenant-b stored identical content")
	}
	if got := getBytes(t, b, "shared"); !bytes.Equal(got, shared) {
		t.Fatal("tenant-b read wrong bytes")
	}

	// Tenant B deletes and compacts. That must not touch tenant A's data.
	if err := b.Delete(ctx, "shared"); err != nil {
		t.Fatalf("tenant-b Delete: %v", err)
	}
	if _, err := b.Compact(ctx); err != nil {
		t.Fatalf("tenant-b Compact: %v", err)
	}

	if got := getBytes(t, a, "shared"); !bytes.Equal(got, shared) {
		t.Fatal("tenant-a lost or corrupted data after tenant-b deleted and compacted identical content")
	}
	if err := a.Verify(ctx); err != nil {
		t.Fatalf("tenant-a Verify after tenant-b compaction: %v", err)
	}
}
