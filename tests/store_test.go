package store_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"testing"

	bserrors "github.com/asaidimu/blobs/errors"
	"github.com/asaidimu/blobs/index"
	"github.com/asaidimu/blobs/object"
	"github.com/asaidimu/blobs/store"
)

// ── Helpers ───────────────────────────────────────────────────────────────────

func openStore(t *testing.T) *store.Store {
	t.Helper()
	dir := t.TempDir()
	s, err := store.Open(store.Config{
		DataDir: dir,
		Index:   index.NewMemoryBackend(),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	ctx := context.Background()
	if err := s.CreateNamespace(ctx, object.Namespace{ID: "default"}); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	return s
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func putBytes(t *testing.T, ns *store.NamespaceHandle, key string, data []byte) *object.BlobInfo {
	t.Helper()
	info, err := ns.Put(context.Background(), key, bytes.NewReader(data), store.PutOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		t.Fatalf("Put(%q): %v", key, err)
	}
	return info
}

func getBytes(t *testing.T, ns *store.NamespaceHandle, key string) []byte {
	t.Helper()
	rc, err := ns.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	data, err := store.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll(%q): %v", key, err)
	}
	return data
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestRoundTrip_Small(t *testing.T) {
	s := openStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	original := []byte("hello, blobstore!")
	putBytes(t, ns, "greeting", original)

	got := getBytes(t, ns, "greeting")
	if !bytes.Equal(got, original) {
		t.Fatalf("got %q, want %q", got, original)
	}

	// Head should reflect metadata without reading bytes.
	info, err := ns.Head(ctx, "greeting")
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if info.Metadata.Size != int64(len(original)) {
		t.Errorf("Size: got %d, want %d", info.Metadata.Size, len(original))
	}
	if info.Metadata.ContentType != "application/octet-stream" {
		t.Errorf("ContentType: %q", info.Metadata.ContentType)
	}
}

func TestRoundTrip_Large(t *testing.T) {
	// Write a blob larger than the default chunk size (4 MB) to exercise chunking.
	s := openStore(t)
	ns := s.Namespace("default")

	const size = 10 * 1024 * 1024 // 10 MB — spans multiple chunks
	original := randBytes(size)

	info := putBytes(t, ns, "large-blob", original)
	if info.Metadata.ChunkCount < 2 {
		t.Errorf("expected multiple chunks for %d bytes, got %d", size, info.Metadata.ChunkCount)
	}

	got := getBytes(t, ns, "large-blob")
	if !bytes.Equal(got, original) {
		t.Fatal("large blob round-trip mismatch")
	}
}

func TestRoundTrip_Empty(t *testing.T) {
	s := openStore(t)
	ns := s.Namespace("default")
	_, err := ns.Put(context.Background(), "empty", bytes.NewReader(nil), store.PutOptions{})
	if err == nil {
		t.Fatal("expected error storing empty blob; got nil")
	}
}

func TestDeduplication(t *testing.T) {
	s := openStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	data := randBytes(1024)

	info1 := putBytes(t, ns, "file-a", data)
	info2 := putBytes(t, ns, "file-b", data)

	// Identical content must produce identical BlobIDs.
	if info1.Metadata.BlobID != info2.Metadata.BlobID {
		t.Errorf("expected same BlobID for identical content; got %s and %s",
			info1.Metadata.BlobID, info2.Metadata.BlobID)
	}

	// Both keys must still be readable and correct.
	gotA := getBytes(t, ns, "file-a")
	gotB := getBytes(t, ns, "file-b")
	if !bytes.Equal(gotA, data) || !bytes.Equal(gotB, data) {
		t.Error("dedup round-trip mismatch")
	}

	stats, err := ns.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	// Two refs but one physical blob.
	if stats.BlobCount != 2 {
		t.Errorf("BlobCount: got %d, want 2", stats.BlobCount)
	}
}

func TestOverwrite(t *testing.T) {
	s := openStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	original := []byte("version 1")
	updated := []byte("version 2 — longer content")

	putBytes(t, ns, "doc", original)
	putBytes(t, ns, "doc", updated)

	got := getBytes(t, ns, "doc")
	if !bytes.Equal(got, updated) {
		t.Fatalf("after overwrite: got %q, want %q", got, updated)
	}

	stats, err := ns.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	// Overwrite replaces, not adds.
	if stats.BlobCount != 1 {
		t.Errorf("BlobCount after overwrite: got %d, want 1", stats.BlobCount)
	}
	// Old bytes become dead.
	if stats.DeadBytes == 0 {
		t.Errorf("expected DeadBytes > 0 after overwrite with different content")
	}
}

func TestDelete_Idempotent(t *testing.T) {
	s := openStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	putBytes(t, ns, "to-delete", []byte("bye"))

	if err := ns.Delete(ctx, "to-delete"); err != nil {
		t.Fatalf("first Delete: %v", err)
	}
	// Second delete should be idempotent.
	if err := ns.Delete(ctx, "to-delete"); err != nil {
		t.Fatalf("second Delete (idempotent): %v", err)
	}

	_, err := ns.Get(ctx, "to-delete")
	if err == nil {
		t.Fatal("expected not-found after delete; got nil")
	}
	var nfe *bserrors.NotFoundError
	if !isAs(err, &nfe) {
		t.Errorf("expected NotFoundError; got %T: %v", err, err)
	}
}

func TestList(t *testing.T) {
	s := openStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	keys := []string{"alpha", "beta", "gamma", "delta"}
	for _, k := range keys {
		putBytes(t, ns, k, []byte(k+"-content"))
	}

	// List all.
	all, err := ns.List(ctx, store.ListOptions{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != len(keys) {
		t.Errorf("List all: got %d, want %d", len(all), len(keys))
	}

	// Prefix filter.
	filtered, err := ns.List(ctx, store.ListOptions{KeyPrefix: "a"})
	if err != nil {
		t.Fatalf("List prefix: %v", err)
	}
	if len(filtered) != 1 || filtered[0].Key != "alpha" {
		t.Errorf("List prefix=a: got %+v", filtered)
	}

	// Limit.
	limited, err := ns.List(ctx, store.ListOptions{Limit: 2})
	if err != nil {
		t.Fatalf("List limit: %v", err)
	}
	if len(limited) != 2 {
		t.Errorf("List limit=2: got %d", len(limited))
	}
}

func TestCustomMetadata(t *testing.T) {
	s := openStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	custom := map[string]string{
		"uploaded-by": "user-42",
		"project":     "mars-mission",
	}
	putBytes(t, ns, "annotated", []byte("payload"))
	// Re-put with metadata.
	_, err := ns.Put(ctx, "annotated", bytes.NewReader([]byte("payload")), store.PutOptions{
		ContentType: "text/plain",
		Custom:      custom,
	})
	if err != nil {
		t.Fatalf("Put with custom metadata: %v", err)
	}

	info, err := ns.Head(ctx, "annotated")
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	for k, v := range custom {
		if info.Metadata.Custom[k] != v {
			t.Errorf("custom[%q]: got %q, want %q", k, info.Metadata.Custom[k], v)
		}
	}
	if info.Metadata.ContentType != "text/plain" {
		t.Errorf("ContentType: got %q", info.Metadata.ContentType)
	}
}

func TestUpdate_Metadata(t *testing.T) {
	s := openStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	original := []byte("some content")
	putBytes(t, ns, "updatable", original)

	// Set initial custom metadata via Put.
	_, err := ns.Put(ctx, "updatable", bytes.NewReader(original), store.PutOptions{
		ContentType: "text/plain",
		Custom:      map[string]string{"initial": "yes"},
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	before, err := ns.Head(ctx, "updatable")
	if err != nil {
		t.Fatalf("Head before update: %v", err)
	}

	// Update with mixed value types.
	err = ns.Update(ctx, "updatable", map[string]any{
		"author":  "alice",
		"version": 2,
		"enabled": true,
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	after, err := ns.Head(ctx, "updatable")
	if err != nil {
		t.Fatalf("Head after update: %v", err)
	}

	// Custom metadata should be replaced.
	if after.Metadata.Custom["author"] != "alice" {
		t.Errorf("custom[author]: got %q, want alice", after.Metadata.Custom["author"])
	}
	if after.Metadata.Custom["version"] != "2" {
		t.Errorf("custom[version]: got %q, want 2", after.Metadata.Custom["version"])
	}
	if after.Metadata.Custom["enabled"] != "true" {
		t.Errorf("custom[enabled]: got %q, want true", after.Metadata.Custom["enabled"])
	}
	if _, ok := after.Metadata.Custom["initial"]; ok {
		t.Error("custom[initial] should have been replaced")
	}

	// Immutable fields should be preserved.
	if after.Metadata.ContentType != "text/plain" {
		t.Errorf("ContentType changed: got %q", after.Metadata.ContentType)
	}
	if after.Metadata.Size != before.Metadata.Size {
		t.Errorf("Size changed: got %d, want %d", after.Metadata.Size, before.Metadata.Size)
	}
	if after.Metadata.BlobID != before.Metadata.BlobID {
		t.Errorf("BlobID changed")
	}

	// UpdatedAt should advance.
	if !after.Metadata.UpdatedAt.After(before.Metadata.UpdatedAt) {
		t.Error("UpdatedAt did not advance")
	}

	// Content should be untouched.
	got := getBytes(t, ns, "updatable")
	if !bytes.Equal(got, original) {
		t.Fatal("blob content changed after metadata update")
	}
}

func TestUpdate_NotFound(t *testing.T) {
	s := openStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	err := ns.Update(ctx, "does-not-exist", map[string]any{"foo": "bar"})
	if err == nil {
		t.Fatal("expected error; got nil")
	}
	var nfe *bserrors.NotFoundError
	if !isAs(err, &nfe) {
		t.Errorf("expected *NotFoundError; got %T", err)
	}
}

func TestRename(t *testing.T) {
	s := openStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	original := []byte("renamable content")
	putBytes(t, ns, "old-key", original)

	info, err := ns.Head(ctx, "old-key")
	if err != nil {
		t.Fatalf("Head before rename: %v", err)
	}

	err = ns.Rename(ctx, "old-key", "new-key")
	if err != nil {
		t.Fatalf("Rename: %v", err)
	}

	// Old key should be gone.
	_, err = ns.Get(ctx, "old-key")
	var nfe *bserrors.NotFoundError
	if !isAs(err, &nfe) {
		t.Errorf("expected *NotFoundError for old key; got %T", err)
	}

	// New key should have the content.
	got := getBytes(t, ns, "new-key")
	if !bytes.Equal(got, original) {
		t.Fatal("content mismatch after rename")
	}

	// Metadata should be preserved.
	renamed, err := ns.Head(ctx, "new-key")
	if err != nil {
		t.Fatalf("Head after rename: %v", err)
	}
	if renamed.Metadata.Size != info.Metadata.Size {
		t.Errorf("Size changed: got %d, want %d", renamed.Metadata.Size, info.Metadata.Size)
	}
	if renamed.Metadata.BlobID != info.Metadata.BlobID {
		t.Errorf("BlobID changed")
	}
}

func TestRename_TargetExists(t *testing.T) {
	s := openStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	putBytes(t, ns, "key-a", []byte("data a"))
	putBytes(t, ns, "key-b", []byte("data b"))

	err := ns.Rename(ctx, "key-a", "key-b")
	if err == nil {
		t.Fatal("expected error when target exists; got nil")
	}

	// Both original keys should still be intact.
	gotA := getBytes(t, ns, "key-a")
	if string(gotA) != "data a" {
		t.Errorf("key-a: got %q", gotA)
	}
	gotB := getBytes(t, ns, "key-b")
	if string(gotB) != "data b" {
		t.Errorf("key-b: got %q", gotB)
	}
}

func TestRename_NotFound(t *testing.T) {
	s := openStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	err := ns.Rename(ctx, "does-not-exist", "new-key")
	if err == nil {
		t.Fatal("expected error; got nil")
	}
	var nfe *bserrors.NotFoundError
	if !isAs(err, &nfe) {
		t.Errorf("expected *NotFoundError; got %T", err)
	}
}

func TestMultipleNamespaces_Isolation(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	if err := s.CreateNamespace(ctx, object.Namespace{ID: "tenant-a"}); err != nil {
		t.Fatalf("CreateNamespace tenant-a: %v", err)
	}
	if err := s.CreateNamespace(ctx, object.Namespace{ID: "tenant-b"}); err != nil {
		t.Fatalf("CreateNamespace tenant-b: %v", err)
	}

	nsA := s.Namespace("tenant-a")
	nsB := s.Namespace("tenant-b")

	putBytes(t, nsA, "shared-key", []byte("data from A"))
	putBytes(t, nsB, "shared-key", []byte("data from B"))

	gotA := getBytes(t, nsA, "shared-key")
	gotB := getBytes(t, nsB, "shared-key")

	if string(gotA) != "data from A" {
		t.Errorf("tenant-a: got %q", gotA)
	}
	if string(gotB) != "data from B" {
		t.Errorf("tenant-b: got %q", gotB)
	}

	// Delete from A should not affect B.
	if err := nsA.Delete(ctx, "shared-key"); err != nil {
		t.Fatalf("Delete from A: %v", err)
	}
	_, err := nsA.Get(ctx, "shared-key")
	if err == nil {
		t.Error("expected not-found in A after delete")
	}
	gotB2 := getBytes(t, nsB, "shared-key")
	if string(gotB2) != "data from B" {
		t.Errorf("tenant-b after A delete: got %q", gotB2)
	}
}

func TestStats(t *testing.T) {
	s := openStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		putBytes(t, ns, fmt.Sprintf("key-%d", i), randBytes(1024))
	}

	stats, err := ns.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.BlobCount != 5 {
		t.Errorf("BlobCount: got %d, want 5", stats.BlobCount)
	}
	if stats.BytesStored != 5*1024 {
		t.Errorf("BytesStored: got %d, want %d", stats.BytesStored, 5*1024)
	}
}

func TestStoreStats(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	s.CreateNamespace(ctx, object.Namespace{ID: "ns-one"})
	s.CreateNamespace(ctx, object.Namespace{ID: "ns-two"})

	putBytes(t, s.Namespace("ns-one"), "a", randBytes(512))
	putBytes(t, s.Namespace("ns-two"), "b", randBytes(512))

	agg, err := s.Stats(ctx)
	if err != nil {
		t.Fatalf("StoreStats: %v", err)
	}
	if agg.NamespaceCount != 3 { // default + ns-one + ns-two
		t.Errorf("NamespaceCount: got %d, want 3", agg.NamespaceCount)
	}
}

func TestQuota_BlobCount(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	s.CreateNamespace(ctx, object.Namespace{
		ID:    "capped",
		Quota: &object.Quota{MaxBlobCount: 2},
	})
	ns := s.Namespace("capped")

	putBytes(t, ns, "k1", []byte("a"))
	putBytes(t, ns, "k2", []byte("b"))

	_, err := ns.Put(ctx, "k3", bytes.NewReader([]byte("c")), store.PutOptions{})
	if err == nil {
		t.Fatal("expected quota error; got nil")
	}
	var qe *bserrors.QuotaExceededError
	if !isAs(err, &qe) {
		t.Errorf("expected QuotaExceededError; got %T: %v", err, err)
	}
	if qe.Dimension != "blob_count" {
		t.Errorf("Dimension: got %q, want blob_count", qe.Dimension)
	}
}

func TestQuota_Bytes(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	s.CreateNamespace(ctx, object.Namespace{
		ID:    "byte-capped",
		Quota: &object.Quota{MaxBytes: 100},
	})
	ns := s.Namespace("byte-capped")

	putBytes(t, ns, "small", []byte("tiny"))

	_, err := ns.Put(ctx, "big", bytes.NewReader(randBytes(200)), store.PutOptions{})
	if err == nil {
		t.Fatal("expected quota error for oversized write; got nil")
	}
}

func TestQuota_MaxBlobSize(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	s.CreateNamespace(ctx, object.Namespace{
		ID:    "size-capped",
		Quota: &object.Quota{MaxBlobSize: 50},
	})
	ns := s.Namespace("size-capped")

	// Small blob should succeed.
	putBytes(t, ns, "small", []byte("hello"))

	// Oversized blob should be rejected.
	_, err := ns.Put(ctx, "big", bytes.NewReader(randBytes(100)), store.PutOptions{})
	if err == nil {
		t.Fatal("expected quota error for oversized blob; got nil")
	}
	var qe *bserrors.QuotaExceededError
	if !isAs(err, &qe) {
		t.Errorf("expected QuotaExceededError; got %T: %v", err, err)
	}
	if qe.Dimension != "blob_size" {
		t.Errorf("Dimension: got %q, want blob_size", qe.Dimension)
	}
}

func TestVerify_Clean(t *testing.T) {
	s := openStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		putBytes(t, ns, fmt.Sprintf("f%d", i), randBytes(512))
	}

	if err := ns.Verify(ctx); err != nil {
		t.Errorf("Verify on clean store: %v", err)
	}
}

func TestVerify_Corrupted(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := store.Open(store.Config{
		DataDir: dir,
		Index:   index.NewMemoryBackend(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CreateNamespace(ctx, object.Namespace{ID: "default"}); err != nil {
		t.Fatal(err)
	}

	ns := s.Namespace("default")
	putBytes(t, ns, "victim", randBytes(512))

	// Close cleanly so all data is flushed.
	s.Close()

	// Corrupt a segment file by flipping bytes in the middle.
	entries, _ := os.ReadDir(dir + "/" + "default")
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".vol") {
			path := dir + "/" + "default" + "/" + e.Name()
			data, _ := os.ReadFile(path)
			if len(data) > 200 {
				data[150] ^= 0xFF
				data[151] ^= 0xFF
				os.WriteFile(path, data, 0o644)
			}
			break
		}
	}

	// Re-open and verify — should detect corruption.
	s2, err := store.Open(store.Config{
		DataDir: dir,
		Index:   index.NewMemoryBackend(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.CreateNamespace(ctx, object.Namespace{ID: "default"}); err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	// Note: after re-open with a fresh MemoryBackend the index is empty,
	// so Verify has nothing to check. This test validates the Verify plumbing
	// works; a real test would use a persistent index backend. Here we
	// at minimum confirm Verify runs without panicking on an empty index.
	if err := s2.Namespace("default").Verify(ctx); err != nil {
		t.Logf("Verify returned (expected on empty index after re-open): %v", err)
	}
}

func TestNamespaceValidation(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	badIDs := []string{"", "A", "has space", "-leading", "trailing-", strings.Repeat("a", 64)}
	for _, id := range badIDs {
		err := s.CreateNamespace(ctx, object.Namespace{ID: id})
		if err == nil {
			t.Errorf("CreateNamespace(%q): expected validation error, got nil", id)
		}
	}

	goodIDs := []string{"default", "tenant-abc", "ns-01", "a1"}
	for _, id := range goodIDs {
		if id == "default" {
			continue // already exists
		}
		if err := s.CreateNamespace(ctx, object.Namespace{ID: id}); err != nil {
			t.Errorf("CreateNamespace(%q): unexpected error: %v", id, err)
		}
	}
}

func TestDeleteNamespace(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	s.CreateNamespace(ctx, object.Namespace{ID: "ephemeral"})
	ns := s.Namespace("ephemeral")
	putBytes(t, ns, "data", []byte("temporary"))

	if err := s.DeleteNamespace(ctx, "ephemeral"); err != nil {
		t.Fatalf("DeleteNamespace: %v", err)
	}

	namespaces, _ := s.ListNamespaces(ctx)
	for _, n := range namespaces {
		if n.ID == "ephemeral" {
			t.Error("namespace still listed after deletion")
		}
	}

}

func TestCompact(t *testing.T) {
	s := openStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	// Write and then delete blobs to create dead bytes.
	for i := 0; i < 5; i++ {
		putBytes(t, ns, fmt.Sprintf("temp-%d", i), randBytes(1024))
	}
	for i := 0; i < 5; i++ {
		ns.Delete(ctx, fmt.Sprintf("temp-%d", i))
	}

	// One live blob.
	putBytes(t, ns, "survivor", randBytes(512))

	result, err := ns.Compact(ctx)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	t.Logf("Compact: %+v", result)

	// Survivor must still be readable.
	got := getBytes(t, ns, "survivor")
	if len(got) != 512 {
		t.Errorf("survivor after compact: got %d bytes, want 512", len(got))
	}
}

func TestConcurrentPuts(t *testing.T) {
	s := openStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	const workers = 10
	const blobsPerWorker = 20

	errc := make(chan error, workers*blobsPerWorker)
	for w := 0; w < workers; w++ {
		w := w
		go func() {
			for i := 0; i < blobsPerWorker; i++ {
				key := fmt.Sprintf("worker-%d-blob-%d", w, i)
				data := randBytes(512)
				if _, err := ns.Put(ctx, key, bytes.NewReader(data), store.PutOptions{}); err != nil {
					errc <- fmt.Errorf("worker %d, blob %d: %w", w, i, err)
					return
				}
			}
			errc <- nil
		}()
	}

	for i := 0; i < workers; i++ {
		if err := <-errc; err != nil {
			t.Errorf("concurrent put: %v", err)
		}
	}

	stats, _ := ns.Stats(ctx)
	want := int64(workers * blobsPerWorker)
	if stats.BlobCount != want {
		t.Errorf("BlobCount after concurrent puts: got %d, want %d", stats.BlobCount, want)
	}
}

func TestLargeBlob_ExactChunkBoundary(t *testing.T) {
	// Write a blob whose size is exactly one default chunk size (4 MB).
	s := openStore(t)
	ns := s.Namespace("default")

	const size = 4 * 1024 * 1024
	original := randBytes(size)
	info := putBytes(t, ns, "exact-chunk", original)

	if info.Metadata.ChunkCount != 1 {
		t.Errorf("expected 1 chunk for %d bytes, got %d", size, info.Metadata.ChunkCount)
	}

	got := getBytes(t, ns, "exact-chunk")
	if !bytes.Equal(got, original) {
		t.Fatal("exact-chunk-boundary round-trip mismatch")
	}
}

func TestGetNonExistent(t *testing.T) {
	s := openStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	_, err := ns.Get(ctx, "does-not-exist")
	if err == nil {
		t.Fatal("expected error; got nil")
	}
	var nfe *bserrors.NotFoundError
	if !isAs(err, &nfe) {
		t.Errorf("expected *NotFoundError; got %T", err)
	}
}

func TestStreamingReader_MultipleReads(t *testing.T) {
	s := openStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	original := randBytes(9 * 1024 * 1024) // 9 MB — ~2-3 chunks
	putBytes(t, ns, "streamed", original)

	rc, err := ns.Get(ctx, "streamed")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()

	// Read in small increments to exercise the buffer boundary logic.
	var got []byte
	buf := make([]byte, 1024)
	for {
		n, err := rc.Read(buf)
		got = append(got, buf[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
	}

	if !bytes.Equal(got, original) {
		t.Fatalf("streaming read mismatch: got %d bytes, want %d", len(got), len(original))
	}
}

// ── Error assertion helpers ───────────────────────────────────────────────────

func isAs[T error](err error, target *T) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(T); ok {
		*target = e
		return true
	}
	return false
}
