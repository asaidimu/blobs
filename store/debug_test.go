package store_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/asaidimu/blobs/index"
	"github.com/asaidimu/blobs/object"
	"github.com/asaidimu/blobs/store"
	"github.com/asaidimu/blobs/volume"
)

func TestDebugSameNS(t *testing.T) {
	ctx := context.Background()
	backend := index.NewMemoryBackend()
	dataDir := t.TempDir()
	s, _ := store.Open(store.Config{DataDir: dataDir, Index: backend})
	defer s.Close()
	s.CreateNamespace(ctx, object.Namespace{ID: "default"})
	ns := s.Namespace("default")
	idx := index.New(backend)

	rng := rand.New(rand.NewSource(42))
	base := make([]byte, 16<<20)
	rng.Read(base)
	app := make([]byte, 4<<20)
	rng.Read(app)
	full := append(append([]byte{}, base...), app...)

	if _, err := ns.Put(ctx, "a", bytes.NewReader(base), store.PutOptions{}); err != nil {
		t.Fatalf("Put a: %v", err)
	}
	if _, err := ns.Put(ctx, "b", bytes.NewReader(full), store.PutOptions{}); err != nil {
		t.Fatalf("Put b: %v", err)
	}

	dumpPages := func(label string) {
		eng, err := volume.Open(dataDir, "default", volume.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer eng.Close()
		fmt.Println("---", label, "---")
		eng.ScanSegments(func(entry object.ChunkEntry, hdr volume.PageHeader) error {
			fmt.Printf("  page %s@%-8d flags=%d\n", entry.SegmentID, entry.PageOffset, int(hdr.Flags))
			return nil
		})
	}

	blobB, err := idx.GetBlob(ctx, blobIDOf(t, ns, "b"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("blob b has %d chunks\n", len(blobB.ChunkIDs))
	for _, cid := range blobB.ChunkIDs {
		ce, err := idx.GetChunk(ctx, cid)
		if err != nil {
			fmt.Printf("  chunk %s: MISSING\n", cid)
			continue
		}
		fmt.Printf("  chunk %.16s ref=%d seg=%s off=%d pc=%d len=%d\n", cid, ce.RefCount, ce.SegmentID, ce.PageOffset, ce.PageCount, ce.Length)
	}

	dumpPages("pages right after Put(a)+Put(b)")

	if err := ns.Delete(ctx, "a"); err != nil {
		t.Fatalf("Delete a: %v", err)
	}
	if _, err := ns.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	dumpPages("pages after Delete(a)+Compact")
	rc, err := ns.Get(ctx, "b")
	if err != nil {
		t.Fatalf("Get(b): %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll(b): %v", err)
	}
	fmt.Printf("read b: %d bytes (want %d)\n", len(got), len(full))
}

func blobIDOf(t *testing.T, ns *store.NamespaceHandle, key string) object.BlobID {
	t.Helper()
	info, err := ns.Head(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	return info.Metadata.BlobID
}
