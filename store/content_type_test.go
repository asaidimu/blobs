package store_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/asaidimu/blobs/index"
	"github.com/asaidimu/blobs/store"
)

func openContentTypeTestStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(store.Config{
		DataDir:          t.TempDir(),
		Index:            index.NewMemoryBackend(),
		DefaultNamespace: "default",
	})
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestPut_AutoDetectsContentType confirms Put sniffs the actual content
// when the caller leaves ContentType empty, across several distinct
// signatures — not just falling back to application/octet-stream, which
// is what the pre-detection default did unconditionally.
func TestPut_AutoDetectsContentType(t *testing.T) {
	s := openContentTypeTestStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	pngMagic := []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0, 0, 0, 0, 0, 0, 0, 0}

	cases := []struct {
		name       string
		content    []byte
		wantPrefix string // checked as a prefix so "text/plain; charset=utf-8" matches "text/plain"
	}{
		{"PNG", pngMagic, "image/png"},
		{"PlainText", []byte("hello, this is plain text content, definitely not binary"), "text/plain"},
		{"JSON", []byte(`{"a":1,"b":"two"}`), "application/json"},
		{"PDF", []byte("%PDF-1.4\n%stuff after the magic bytes"), "application/pdf"},
	}

	for _, tc := range cases {
		key := "auto-" + tc.name
		info, err := ns.Put(ctx, key, bytes.NewReader(tc.content), store.PutOptions{})
		if err != nil {
			t.Fatalf("Put(%s): %v", tc.name, err)
		}
		if !strings.HasPrefix(info.Metadata.ContentType, tc.wantPrefix) {
			t.Errorf("Put(%s).Metadata.ContentType = %q, want prefix %q", tc.name, info.Metadata.ContentType, tc.wantPrefix)
		}

		// Confirm Head reports the same detected type, and — the actual
		// point of this whole feature — that the blob's bytes survived
		// detection completely intact (detection must not consume or
		// corrupt the stream WriteBlob receives).
		head, err := ns.Head(ctx, key)
		if err != nil {
			t.Fatalf("Head(%s): %v", tc.name, err)
		}
		if head.Metadata.ContentType != info.Metadata.ContentType {
			t.Errorf("Head(%s).ContentType = %q, want %q (same as Put's result)", tc.name, head.Metadata.ContentType, info.Metadata.ContentType)
		}

		rc, err := ns.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get(%s): %v", tc.name, err)
		}
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("read %s: %v", tc.name, err)
		}
		if !bytes.Equal(got, tc.content) {
			t.Fatalf("Get(%s) = %d bytes, want %d bytes matching the original content exactly (detection must not alter the stream)", tc.name, len(got), len(tc.content))
		}
	}
}

// TestPut_ExplicitContentTypeSkipsDetection confirms an explicitly
// supplied ContentType is used as-is, with no sniffing at all — even when
// it deliberately contradicts what the content would actually sniff as.
func TestPut_ExplicitContentTypeSkipsDetection(t *testing.T) {
	s := openContentTypeTestStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	// This is valid JSON, which would auto-detect as application/json —
	// but an explicit ContentType must override that entirely.
	content := []byte(`{"a":1}`)
	info, err := ns.Put(ctx, "explicit", bytes.NewReader(content), store.PutOptions{
		ContentType: "application/x-custom-override",
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if info.Metadata.ContentType != "application/x-custom-override" {
		t.Fatalf("Metadata.ContentType = %q, want the explicit override, unchanged", info.Metadata.ContentType)
	}
}

// TestPut_EmptyBlobContentTypeDetection confirms detection itself does not
// introduce a new failure mode for a zero-byte blob (io.ReadFull on an
// empty reader returns io.EOF immediately, which detectContentType must
// treat as "nothing to sniff," not an error worth failing Put over).
// WriteBlob separately refuses empty blobs entirely — that is pre-existing
// behavior this feature does not change — so the assertion here is that
// the error, if any, is that same pre-existing rejection and not some
// different failure coming from content-type detection.
func TestPut_EmptyBlobContentTypeDetection(t *testing.T) {
	s := openContentTypeTestStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	_, err := ns.Put(ctx, "empty", bytes.NewReader(nil), store.PutOptions{})
	if err == nil {
		t.Fatal("Put with an empty blob returned nil error — expected WriteBlob's pre-existing empty-blob rejection")
	}
	if !strings.Contains(err.Error(), "empty blob") {
		t.Fatalf("Put with an empty blob returned %v, want an error about the empty blob (not a content-type detection failure)", err)
	}
}

// TestPut_ContentLargerThanSniffWindowDetectsAndRoundTrips confirms
// detection and the reader-reconstruction it relies on both work
// correctly for a blob much larger than the sniff window itself,
// including within a single chunk and across a multi-chunk blob.
func TestPut_ContentLargerThanSniffWindowDetectsAndRoundTrips(t *testing.T) {
	s := openContentTypeTestStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	// PNG magic bytes followed by several times the sniff window's worth
	// of filler — larger than mimeSniffLimit (3072 bytes) so the
	// reconstructed reader must correctly splice the peeked prefix with
	// everything WriteBlob still needs to read afterward.
	content := make([]byte, 10*1024)
	copy(content, []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A})
	for i := 8; i < len(content); i++ {
		content[i] = byte(i % 251) // arbitrary, deterministic, non-repeating-in-a-short-cycle filler
	}

	info, err := ns.Put(ctx, "large-png", bytes.NewReader(content), store.PutOptions{})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !strings.HasPrefix(info.Metadata.ContentType, "image/png") {
		t.Fatalf("Metadata.ContentType = %q, want image/png", info.Metadata.ContentType)
	}
	if info.Metadata.Size != int64(len(content)) {
		t.Fatalf("Metadata.Size = %d, want %d", info.Metadata.Size, len(content))
	}

	rc, err := ns.Get(ctx, "large-png")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("Get after Put with auto-detection on a large blob did not round-trip byte-for-byte")
	}
}
