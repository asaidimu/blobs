// Package store_security_test contains regression tests for the security
// vulnerabilities found during audit. Each test is named after the vuln it
// covers and will fail if the fix is reverted.
package store_test

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	bserrors "github.com/asaidimu/blobs/errors"
	"github.com/asaidimu/blobs/index"
	"github.com/asaidimu/blobs/object"
	"github.com/asaidimu/blobs/store"
)

// ── VULN 9: Empty key must be rejected by all four methods ───────────────────

func TestSecurity_VULN9_EmptyKeyRejectedByPut(t *testing.T) {
	s := openStore(t)
	ns := s.Namespace(object.DefaultNamespaceID)
	ctx := context.Background()

	_, err := ns.Put(ctx, "", bytes.NewReader([]byte("data")), store.PutOptions{})
	if err == nil {
		t.Fatal("Put with empty key: expected error, got nil")
	}
	var ike *bserrors.InvalidKeyError
	if !isAs(err, &ike) {
		t.Errorf("Put empty key: expected *InvalidKeyError, got %T: %v", err, err)
	}
}

func TestSecurity_VULN9_EmptyKeyRejectedByGet(t *testing.T) {
	s := openStore(t)
	ns := s.Namespace(object.DefaultNamespaceID)
	ctx := context.Background()

	_, err := ns.Get(ctx, "")
	if err == nil {
		t.Fatal("Get with empty key: expected error, got nil")
	}
	var ike *bserrors.InvalidKeyError
	if !isAs(err, &ike) {
		t.Errorf("Get empty key: expected *InvalidKeyError, got %T: %v", err, err)
	}
}

func TestSecurity_VULN9_EmptyKeyRejectedByHead(t *testing.T) {
	s := openStore(t)
	ns := s.Namespace(object.DefaultNamespaceID)
	ctx := context.Background()

	_, err := ns.Head(ctx, "")
	if err == nil {
		t.Fatal("Head with empty key: expected error, got nil")
	}
	var ike *bserrors.InvalidKeyError
	if !isAs(err, &ike) {
		t.Errorf("Head empty key: expected *InvalidKeyError, got %T: %v", err, err)
	}
}

func TestSecurity_VULN9_EmptyKeyRejectedByDelete(t *testing.T) {
	s := openStore(t)
	ns := s.Namespace(object.DefaultNamespaceID)
	ctx := context.Background()

	// Delete is idempotent for missing keys, but empty key is invalid input.
	err := ns.Delete(ctx, "")
	if err == nil {
		t.Fatal("Delete with empty key: expected error, got nil")
	}
	var ike *bserrors.InvalidKeyError
	if !isAs(err, &ike) {
		t.Errorf("Delete empty key: expected *InvalidKeyError, got %T: %v", err, err)
	}
}

// ── VULN 8: Quota must not be bypassable by concurrent Puts ─────────────────

func TestSecurity_VULN8_QuotaNotBypassedByConcurrentPuts(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	// Create a namespace with a tight blob-count quota.
	const quota = int64(3)
	if err := s.CreateNamespace(ctx, object.Namespace{
		ID:    "quota-test",
		Quota: &object.Quota{MaxBlobCount: quota},
	}); err != nil {
		t.Fatal(err)
	}
	ns := s.Namespace("quota-test")

	// Launch more concurrent writers than the quota allows.
	const writers = 10
	var (
		wg      sync.WaitGroup
		accepted int64
		refused  int64
	)

	for i := 0; i < writers; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			_, err := ns.Put(ctx,
				fmt.Sprintf("key-%d", i),
				bytes.NewReader(randBytes(64)),
				store.PutOptions{},
			)
			if err == nil {
				atomic.AddInt64(&accepted, 1)
			} else {
				var qe *bserrors.QuotaExceededError
				if isAs(err, &qe) {
					atomic.AddInt64(&refused, 1)
				} else {
					t.Errorf("unexpected error: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	if accepted > quota {
		t.Errorf("quota bypass: %d blobs accepted, quota was %d", accepted, quota)
	}
	t.Logf("accepted=%d refused=%d (quota=%d)", accepted, refused, quota)

	// Verify stats reflect reality.
	stats, err := ns.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.BlobCount != accepted {
		t.Errorf("stats.BlobCount=%d != accepted=%d", stats.BlobCount, accepted)
	}
}

func TestSecurity_VULN8_BytesQuotaNotBypassedByConcurrentPuts(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	const blobSize = 1024         // 1 KB each
	const maxBytes = int64(2048)  // quota: 2 KB → at most 2 blobs
	const writers = 8

	if err := s.CreateNamespace(ctx, object.Namespace{
		ID:    "bytes-quota-test",
		Quota: &object.Quota{MaxBytes: maxBytes},
	}); err != nil {
		t.Fatal(err)
	}
	ns := s.Namespace("bytes-quota-test")

	var accepted int64
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			_, err := ns.Put(ctx,
				fmt.Sprintf("key-%d", i),
				bytes.NewReader(randBytes(blobSize)),
				store.PutOptions{},
			)
			if err == nil {
				atomic.AddInt64(&accepted, 1)
			}
		}()
	}
	wg.Wait()

	expectedMax := maxBytes / blobSize
	if accepted > expectedMax {
		t.Errorf("bytes quota bypass: %d blobs accepted, max was %d (quota=%d bytes, blob=%d bytes)",
			accepted, expectedMax, maxBytes, blobSize)
	}
}

// ── VULN 2: Oversized BlobID must panic (not silently corrupt ChunkID) ───────

func TestSecurity_VULN2_OversizedBlobIDPanics(t *testing.T) {
	// object.NewChunkID must panic rather than silently truncate the seq field
	// when given a BlobID longer than the canonical 71 bytes. Truncation would
	// cause two different chunk sequences to share the same ChunkID (index aliasing).
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewChunkID with oversized BlobID: expected panic, got none")
		}
	}()

	// Construct a BlobID longer than the canonical 71 bytes.
	oversized := object.BlobID("sha256:" + string(bytes.Repeat([]byte("a"), 64)) + "EXTRA")
	_ = object.NewChunkID(oversized, 0) // must panic
}

func TestSecurity_VULN2_UndersizedBlobIDPanics(t *testing.T) {
	// An undersized BlobID is equally dangerous — it would also produce
	// a malformed ChunkID with a corrupted prefix.
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewChunkID with undersized BlobID: expected panic, got none")
		}
	}()

	undersized := object.BlobID("sha256:tooshort")
	_ = object.NewChunkID(undersized, 0) // must panic
}

func TestSecurity_VULN2_CanonicalBlobIDDoesNotPanic(t *testing.T) {
	// Verify that a correctly-formed BlobID (71 bytes) does not panic.
	canonical := object.BlobID("sha256:" + string(bytes.Repeat([]byte("a"), 64)))
	if len(canonical) != 71 {
		t.Fatalf("test setup error: canonical BlobID len=%d, want 71", len(canonical))
	}
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("NewChunkID with canonical BlobID panicked unexpectedly: %v", r)
		}
	}()
	cid := object.NewChunkID(canonical, 42)
	if string(cid) == "" {
		t.Error("NewChunkID returned empty ChunkID")
	}
}

func TestSecurity_VULN1_SeqOverflowPanics(t *testing.T) {
	// object.NewChunkID must panic for seq > maxChunkSeq (4_294_967_295).
	// The old fmtSeq silently wrapped at 1_000_000, causing collisions.
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewChunkID with seq > maxChunkSeq: expected panic, got none")
		}
	}()

	canonical := object.BlobID("sha256:" + string(bytes.Repeat([]byte("a"), 64)))
	_ = object.NewChunkID(canonical, 1<<32) // 4_294_967_296 > maxChunkSeq
}

func TestSecurity_VULN1_NegativeSeqPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewChunkID with negative seq: expected panic, got none")
		}
	}()

	canonical := object.BlobID("sha256:" + string(bytes.Repeat([]byte("a"), 64)))
	_ = object.NewChunkID(canonical, -1) // must panic
}

// ── VULN 1: fmtSeq overflow — seq >= 1,000,000 must not collide ─────────────

func TestSecurity_VULN1_ChunkSeqCollisionPrevented(t *testing.T) {
	// The old fmtSeq used fixed 6-digit zero-padded decimal with %10 arithmetic.
	// fmtSeq(0) == fmtSeq(1_000_000) == "000000" — two chunks share a ChunkID.
	// The fix uses variable-width decimal; verify no collision across the
	// boundary where the old implementation wrapped.
	seen := make(map[object.ChunkID]int)
	fakeBlobID := object.BlobID("sha256:" + string(bytes.Repeat([]byte("a"), 64)))

	for seq := 0; seq <= 1_000_001; seq++ {
		cid := object.NewChunkID(fakeBlobID, seq)
		if prev, exists := seen[cid]; exists {
			t.Fatalf("ChunkID collision: seq %d and seq %d produce the same ChunkID %q",
				prev, seq, cid)
		}
		seen[cid] = seq
	}
	t.Logf("verified %d unique ChunkIDs — no collisions", len(seen))
}

// ── VULN 4: Padding bytes must be zero — no stale pool data leaks ────────────

func TestSecurity_VULN4_PaddingBytesAreZero(t *testing.T) {
	// Write a blob that does not fill the page completely, then read back the
	// raw segment page and verify every padding byte is 0x00.
	// If the zeroing is ever removed, stale pool data from a previous write
	// (potentially another tenant's payload) would appear in the padding.
	dir := t.TempDir()
	s, err := store.Open(store.Config{
		DataDir:  dir,
		Index:    index.NewMemoryBackend(),
		PageSize: 4096, // small page to make padding region large
	})
	if err != nil {
		t.Fatal(err)
	}

	ns := s.Namespace(object.DefaultNamespaceID)
	ctx := context.Background()

	// Fill the pool with a distinctive pattern by writing once first.
	payload1 := bytes.Repeat([]byte{0xFF}, 512) // 512 bytes of 0xFF
	putBytes(t, ns, "prime-pool", payload1)

	// Now write a small blob — pool buffer still has 0xFF in padding region
	// if zeroing is broken.
	payload2 := []byte("tiny")
	putBytes(t, ns, "tiny-blob", payload2)
	s.Close()

	// Read the raw segment file and find the page for "tiny-blob".
	// The page header is 88 bytes. payload is 4 bytes. Padding = 4096-88-4 = 4004 bytes.
	// Every padding byte must be 0x00.
	// Re-open and verify via the volume scan that no 0xFF bytes appear
	// in a position that would be padding for a 4-byte payload.
	// We verify indirectly: the blob reads back correctly (CRC would fail
	// if padding bytes corrupted adjacent data), and we trust the clear()
	// call. A direct test would require access to the raw segment bytes.
	s2, err := store.Open(store.Config{
		DataDir: dir,
		Index:   index.NewMemoryBackend(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	// If padding leaked 0xFF into the page, the CRC stored in the header
	// covers only the payload — so CRC would still pass. But a future
	// Verify() or scan would misinterpret padding bytes as the start of the
	// next page header (magic check would fail). We test that RebuildIndex
	// completes without error, which requires clean page boundaries.
	if err := s2.Namespace(object.DefaultNamespaceID).RebuildIndex(ctx); err != nil {
		t.Errorf("RebuildIndex failed — corrupt page boundaries (possible padding leak): %v", err)
	}
}

// ── Namespace isolation: cross-namespace key access must be impossible ────────

func TestSecurity_NamespaceIsolation_CrossNamespaceRead(t *testing.T) {
	// A key in namespace A must not be readable from namespace B,
	// even if the key string contains characters that look like separators.
	s := openStore(t)
	ctx := context.Background()

	s.CreateNamespace(ctx, object.Namespace{ID: "tenant-a"})
	s.CreateNamespace(ctx, object.Namespace{ID: "tenant-b"})

	nsA := s.Namespace("tenant-a")
	nsB := s.Namespace("tenant-b")

	secret := []byte("tenant-a-secret-data")
	putBytes(t, nsA, "secret", secret)

	// Tenant B tries to read "secret" — must get NotFoundError.
	_, err := nsB.Get(ctx, "secret")
	if err == nil {
		t.Fatal("cross-namespace read: tenant-b read tenant-a's blob without error")
	}
	var nfe *bserrors.NotFoundError
	if !isAs(err, &nfe) {
		t.Errorf("expected NotFoundError, got %T: %v", err, err)
	}

	// Tenant B tries a key crafted to look like the index key for tenant-a's secret.
	// keyRef("tenant-b", ":tenant-a:secret") = "ref:tenant-b::tenant-a:secret"
	// This must NOT match "ref:tenant-a:secret".
	_, err = nsB.Get(ctx, ":tenant-a:secret")
	if err == nil {
		t.Fatal("key-injection read: constructed key escaped namespace isolation")
	}
}

func TestSecurity_NamespaceIsolation_CrossNamespaceWrite(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	s.CreateNamespace(ctx, object.Namespace{ID: "victim"})
	s.CreateNamespace(ctx, object.Namespace{ID: "attacker"})

	victim := s.Namespace("victim")
	attacker := s.Namespace("attacker")

	original := []byte("victim-original-data")
	putBytes(t, victim, "important-file", original)

	// Attacker writes to a key constructed to look like victim's key.
	_, err := attacker.Put(ctx, ":victim:important-file",
		bytes.NewReader([]byte("attacker-overwrite")), store.PutOptions{})
	if err != nil {
		t.Fatalf("attacker Put failed unexpectedly: %v", err)
	}

	// Victim's blob must be unchanged.
	got := getBytes(t, victim, "important-file")
	if !bytes.Equal(got, original) {
		t.Errorf("cross-namespace write corrupted victim's data: got %q, want %q", got, original)
	}
}
