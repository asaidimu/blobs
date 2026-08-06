package store_test

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"testing"

	"github.com/asaidimu/blobs/index"
	"github.com/asaidimu/blobs/object"
	"github.com/asaidimu/blobs/store"
)

// deterministicData returns n bytes of deterministic pseudorandom data so
// chunk boundaries (which are content-defined) are stable across runs.
func deterministicData(t *testing.T, n int) []byte {
	t.Helper()
	rng := rand.New(rand.NewSource(42))
	b := make([]byte, n)
	if _, err := rng.Read(b); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	return b
}

func readBlob(t *testing.T, ns *store.NamespaceHandle, key string) []byte {
	t.Helper()
	rc, err := ns.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll(%q): %v", key, err)
	}
	return got
}

// TestChunkDedup_AppendSharesChunks is the store-level cross-blob dedup
// scenario: blob B is blob A with a suffix appended. Because chunks are
// content-addressed, every one of A's chunks that survives into B is
// physically stored exactly once — B's Put reuses them and reaps its own
// duplicate copies. It asserts (a) both blobs round-trip byte-exact,
// (b) physical bytes stayed well below logical bytes, and (c) deleting A
// and compacting does not drop any chunk B still shares with it.
func TestChunkDedup_AppendSharesChunks(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(store.Config{
		DataDir: t.TempDir(),
		Index:   index.NewMemoryBackend(),
	})
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()
	if err := s.CreateNamespace(ctx, object.Namespace{ID: "default"}); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	ns := s.Namespace("default")

	base := deterministicData(t, 16<<20)                   // 16 MiB
	appended := deterministicData(t, 4<<20)                // 4 MiB of new bytes
	full := append(append([]byte{}, base...), appended...) // 20 MiB

	if _, err := ns.Put(ctx, "a", bytes.NewReader(base), store.PutOptions{}); err != nil {
		t.Fatalf("Put(a): %v", err)
	}
	if _, err := ns.Put(ctx, "b", bytes.NewReader(full), store.PutOptions{}); err != nil {
		t.Fatalf("Put(b): %v", err)
	}

	if got := readBlob(t, ns, "a"); !bytes.Equal(got, base) {
		t.Fatal("blob a did not round-trip byte-exact")
	}
	if got := readBlob(t, ns, "b"); !bytes.Equal(got, full) {
		t.Fatal("blob b did not round-trip byte-exact")
	}

	// b shares virtually all of a's chunks, so physical usage must be
	// well under the 36 MiB of logical bytes (16 + 20).
	stats, err := s.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.TotalBytesStored != 36<<20 {
		t.Errorf("TotalBytesStored = %d, want %d", stats.TotalBytesStored, 36<<20)
	}
	if stats.TotalBytesPhysical >= stats.TotalBytesStored {
		t.Fatalf("TotalBytesPhysical (%d) not below TotalBytesStored (%d) — no dedup happened",
			stats.TotalBytesPhysical, stats.TotalBytesStored)
	}
	if stats.DeduplicationRatio <= 1.0 {
		t.Errorf("DeduplicationRatio = %v, want > 1.0", stats.DeduplicationRatio)
	}

	// Delete a. Its chunks are still referenced by b, so they must not be
	// reaped even after a full compact — the shared data has to survive.
	if err := ns.Delete(ctx, "a"); err != nil {
		t.Fatalf("Delete(a): %v", err)
	}
	if _, err := ns.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if got := readBlob(t, ns, "b"); !bytes.Equal(got, full) {
		t.Fatalf("blob b corrupted after deleting a + compact: got %d bytes, want %d",
			len(got), len(full))
	}
}

// TestChunkDedup_CrossNamespaceStoresOwnCopies documents the current
// dedup scope: chunk dedup happens within a namespace only. A chunk's
// physical location is a (segment, page) inside its own namespace's
// segment files and readers can only open their own namespace's engine,
// so a namespace storing bytes another namespace already has keeps its own
// copies (cross-namespace physical sharing would need a shared chunk store
// — out of scope). The assertion is that both namespaces still store and
// read back identical content correctly.
func TestChunkDedup_CrossNamespaceStoresOwnCopies(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(store.Config{
		DataDir: t.TempDir(),
		Index:   index.NewMemoryBackend(),
	})
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer s.Close()

	for _, id := range []string{"one", "two"} {
		if err := s.CreateNamespace(ctx, object.Namespace{ID: id}); err != nil {
			t.Fatalf("CreateNamespace(%q): %v", id, err)
		}
	}

	data := deterministicData(t, 8<<20)
	for _, id := range []string{"one", "two"} {
		if _, err := s.Namespace(id).Put(ctx, "blob", bytes.NewReader(data), store.PutOptions{}); err != nil {
			t.Fatalf("Put(namespace %s): %v", id, err)
		}
	}

	for _, id := range []string{"one", "two"} {
		if got := readBlob(t, s.Namespace(id), "blob"); !bytes.Equal(got, data) {
			t.Fatalf("namespace %s's blob did not round-trip byte-exact", id)
		}
	}
}
