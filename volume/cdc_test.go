package volume_test

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/asaidimu/blobs/volume"
)

// cdcEngine opens a volume engine with a small average chunk size so content-
// defined chunking yields multiple chunks without needing multi-GB inputs.
func cdcEngine(t *testing.T) *volume.Engine {
	t.Helper()
	e, err := volume.Open(t.TempDir(), "cdc-ns", volume.Options{
		ChunkSize: 256 * 1024,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return e
}

func cdcBytes(seed int64, size int) []byte {
	b := make([]byte, size)
	rand.New(rand.NewSource(seed)).Read(b)
	return b
}

func cdcWriteBlob(t *testing.T, e *volume.Engine, data []byte) *volume.WriteResult {
	t.Helper()
	r, err := e.WriteBlob(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("WriteBlob: %v", err)
	}
	return r
}

func cdcReadBlob(t *testing.T, e *volume.Engine, r *volume.WriteResult) []byte {
	t.Helper()
	var out []byte
	for _, c := range r.Chunks {
		payload, err := e.ReadChunk(c)
		if err != nil {
			t.Fatalf("ReadChunk %s: %v", c.ChunkID, err)
		}
		out = append(out, payload...)
	}
	return out
}

// TestWriteBlob_RoundTrip verifies that a CDC-chunked blob reassembles
// byte-for-byte from its chunks, and that identical inputs produce identical
// chunk IDs and blob IDs (determinism).
func TestWriteBlob_RoundTrip(t *testing.T) {
	e := cdcEngine(t)
	data := cdcBytes(1, 8*1024*1024)

	a := cdcWriteBlob(t, e, data)
	if got := cdcReadBlob(t, e, a); !bytes.Equal(got, data) {
		t.Fatal("round-trip mismatch")
	}

	b := cdcWriteBlob(t, e, data)
	if len(a.Chunks) != len(b.Chunks) {
		t.Fatalf("chunk count differs for identical input: %d vs %d", len(a.Chunks), len(b.Chunks))
	}
	for i := range a.Chunks {
		if a.Chunks[i].ChunkID != b.Chunks[i].ChunkID {
			t.Fatalf("chunk %d ID differs for identical input: %s vs %s", i, a.Chunks[i].ChunkID, b.Chunks[i].ChunkID)
		}
	}
	if a.BlobID != b.BlobID {
		t.Fatalf("BlobID differs for identical input: %s vs %s", a.BlobID, b.BlobID)
	}
}

// TestWriteBlob_ContentAddressedChunkIDs verifies the chunk IDs are pure
// content hashes — not derived from the parent blob.
func TestWriteBlob_ContentAddressedChunkIDs(t *testing.T) {
	e := cdcEngine(t)
	data := cdcBytes(2, 1024*1024) // fits in a handful of chunks

	// Same content in two different "blobs" (here two separate writes) must
	// produce identical chunk IDs. Content-addressing means the chunk ID
	// carries the chunk's own hash, so it cannot embed a parent-blob-specific
	// sequence like the old "<blobID>#<seq>" format did.
	first := cdcWriteBlob(t, e, data)
	second := cdcWriteBlob(t, e, data)

	if len(first.Chunks) != len(second.Chunks) {
		t.Fatalf("chunk count differs: %d vs %d", len(first.Chunks), len(second.Chunks))
	}
	for i := range first.Chunks {
		if first.Chunks[i].ChunkID != second.Chunks[i].ChunkID {
			t.Fatalf("content-address mismatch: %s vs %s", first.Chunks[i].ChunkID, second.Chunks[i].ChunkID)
		}
	}
	if len(first.Chunks) == 0 {
		t.Fatal("no chunks produced")
	}
}

// TestWriteBlob_PrefixInsertionSharesChunks is the dedup property at the
// engine level: a blob and "blob plus a different prefix" must share the
// chunks that lie deep inside the unchanged bytes. This is exactly the case
// fixed-size chunking fails, and it is the property the store's cross-blob
// dedup layer will exploit.
func TestWriteBlob_PrefixInsertionSharesChunks(t *testing.T) {
	e := cdcEngine(t)

	const runSize = 16 * 1024 * 1024
	const prefixSize = 4 * 1024 * 1024
	run := cdcBytes(3, runSize)

	plain := cdcWriteBlob(t, e, run)
	prefixed := cdcWriteBlob(t, e, append(cdcBytes(4, prefixSize), run...))

	plainIDs := make(map[string]bool, len(plain.Chunks))
	for _, c := range plain.Chunks {
		plainIDs[string(c.ChunkID)] = true
	}

	// Every chunk ID of the plain blob must reappear in the prefixed blob,
	// except for the handful straddling the splice point.
	missing := 0
	for id := range plainIDs {
		found := false
		for _, c := range prefixed.Chunks {
			if string(c.ChunkID) == id {
				found = true
				break
			}
		}
		if !found {
			missing++
		}
	}
	total := len(plain.Chunks)
	if missing > total/4 {
		t.Fatalf("prefix insertion lost %d of %d chunks (%d%%); expected only the splice-adjacent few",
			missing, total, missing*100/total)
	}
	if missing == total {
		t.Fatal("prefix insertion shared no chunks at all — content-defined chunking is not working")
	}
}
