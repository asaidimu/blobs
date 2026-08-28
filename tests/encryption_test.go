package store_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/asaidimu/blobs/encryption"
	"github.com/asaidimu/blobs/index"
	"github.com/asaidimu/blobs/object"
	"github.com/asaidimu/blobs/store"
)

// staticKeyProvider is a fixed-key encryption.KeyProvider for tests. A
// real deployment would back this with an env var, file, or KMS — see
// package encryption's doc comment.
type staticKeyProvider struct {
	version string
	key     []byte
}

func newStaticKeyProvider(t *testing.T) *staticKeyProvider {
	t.Helper()
	key, err := encryption.GenerateKey()
	if err != nil {
		t.Fatalf("generate master key: %v", err)
	}
	return &staticKeyProvider{version: "v1", key: key}
}

func (p *staticKeyProvider) CurrentKey() ([]byte, string, error) {
	return p.key, p.version, nil
}

func (p *staticKeyProvider) Key(version string) ([]byte, error) {
	if version != p.version {
		return nil, fmt.Errorf("staticKeyProvider: unknown key version %q", version)
	}
	return p.key, nil
}

// openEncryptedStore is openStore's counterpart for encryption tests: a
// store with a KeyProvider configured, and one namespace ("secure")
// created with WithEncryption().
func openEncryptedStore(t *testing.T) (*store.Store, string) {
	t.Helper()
	dir := t.TempDir()
	kp := newStaticKeyProvider(t)
	s, err := store.Open(store.Config{
		DataDir:     dir,
		Index:       index.NewMemoryBackend(),
		KeyProvider: kp,
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
	if err := s.CreateNamespace(ctx, object.Namespace{ID: "secure"}, store.WithEncryption()); err != nil {
		t.Fatalf("CreateNamespace with encryption: %v", err)
	}
	return s, dir
}

// segmentBytes concatenates every seg-*.vol file's raw bytes under a
// namespace's directory, for tests that need to inspect what's actually
// on disk rather than what the store API returns.
func segmentBytes(t *testing.T, dataDir, nsID string) []byte {
	t.Helper()
	nsDir := filepath.Join(dataDir, nsID)
	entries, err := os.ReadDir(nsDir)
	if err != nil {
		t.Fatalf("read namespace dir: %v", err)
	}
	var all []byte
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".vol" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(nsDir, e.Name()))
		if err != nil {
			t.Fatalf("read segment file %s: %v", e.Name(), err)
		}
		all = append(all, b...)
	}
	return all
}

func TestEncryptedNamespace_PutGetRoundTrip(t *testing.T) {
	s, _ := openEncryptedStore(t)
	ns := s.Namespace("secure")
	ctx := context.Background()

	want := randBytes(3 * 1024 * 1024) // spans multiple chunks at default 4MB avg is fine too, but exercise a size that isn't tiny
	if _, err := ns.Put(ctx, "photo.jpg", bytes.NewReader(want), store.PutOptions{}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	rc, err := ns.Get(ctx, "photo.jpg")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()
	got, err := store.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("round-tripped bytes do not match: got %d bytes, want %d bytes", len(got), len(want))
	}
}

func TestEncryptedNamespace_SeekableGetRoundTrip(t *testing.T) {
	// Exercises the same chunk-granular decrypt path streaming reads
	// (e.g. HTTP Range requests for video) depend on.
	s, _ := openEncryptedStore(t)
	ns := s.Namespace("secure")
	ctx := context.Background()

	want := randBytes(2 * 1024 * 1024)
	if _, err := ns.Put(ctx, "video.mp4", bytes.NewReader(want), store.PutOptions{}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	rsc, err := ns.GetSeekable(ctx, "video.mp4")
	if err != nil {
		t.Fatalf("GetSeekable: %v", err)
	}
	defer rsc.Close()

	// Seek into the middle and read a range, like a Range: bytes=N-
	// request would.
	const offset = 1_500_000
	if _, err := rsc.Seek(offset, 0); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	got, err := store.ReadAll(rsc)
	if err != nil {
		t.Fatalf("ReadAll after seek: %v", err)
	}
	if !bytes.Equal(got, want[offset:]) {
		t.Fatalf("seeked read mismatch: got %d bytes, want %d bytes", len(got), len(want)-offset)
	}
}

func TestEncryptedNamespace_CiphertextOnDisk(t *testing.T) {
	s, dataDir := openEncryptedStore(t)
	ns := s.Namespace("secure")
	ctx := context.Background()

	secret := bytes.Repeat([]byte("THIS IS PLAINTEXT MARKER "), 200) // > 4KB, well above any header noise
	if _, err := ns.Put(ctx, "doc.txt", bytes.NewReader(secret), store.PutOptions{}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	onDisk := segmentBytes(t, dataDir, "secure")
	if bytes.Contains(onDisk, secret) {
		t.Fatal("plaintext marker found verbatim in encrypted namespace's segment file(s) on disk")
	}
	if bytes.Contains(onDisk, []byte("THIS IS PLAINTEXT MARKER")) {
		t.Fatal("plaintext marker substring found in encrypted namespace's segment file(s) on disk")
	}
}

func TestUnencryptedNamespace_PlaintextOnDisk(t *testing.T) {
	// Control case: confirms segmentBytes/the test approach actually
	// detects plaintext when it's present, so
	// TestEncryptedNamespace_CiphertextOnDisk passing means something.
	ctx := context.Background()
	dir := t.TempDir()
	s, err := store.Open(store.Config{DataDir: dir, Index: index.NewMemoryBackend()}) // no KeyProvider — plain store
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	if err := s.CreateNamespace(ctx, object.Namespace{ID: "plain"}); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	ns := s.Namespace("plain")

	secret := bytes.Repeat([]byte("THIS IS PLAINTEXT MARKER "), 200)
	if _, err := ns.Put(ctx, "doc.txt", bytes.NewReader(secret), store.PutOptions{}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	onDisk := segmentBytes(t, dir, "plain")
	if !bytes.Contains(onDisk, []byte("THIS IS PLAINTEXT MARKER")) {
		t.Fatal("expected plaintext marker to appear verbatim in unencrypted namespace's segment file(s)")
	}
}

func TestEncryptedNamespace_SurvivesCompaction(t *testing.T) {
	s, _ := openEncryptedStore(t)
	ns := s.Namespace("secure")
	ctx := context.Background()

	keep := randBytes(64 * 1024)
	drop := randBytes(64 * 1024)
	if _, err := ns.Put(ctx, "keep.bin", bytes.NewReader(keep), store.PutOptions{}); err != nil {
		t.Fatalf("Put keep: %v", err)
	}
	if _, err := ns.Put(ctx, "drop.bin", bytes.NewReader(drop), store.PutOptions{}); err != nil {
		t.Fatalf("Put drop: %v", err)
	}
	if err := ns.Delete(ctx, "drop.bin"); err != nil {
		t.Fatalf("Delete drop: %v", err)
	}

	// Force phase 2 (segment rewrite) regardless of dead-byte ratio, to
	// exercise RewriteSegment + the RefCount/Nonce/Tag-preserving merge
	// in compactPhase2.
	if _, err := ns.CompactWithOptions(ctx, store.CompactOptions{RewriteThreshold: 0}); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	rc, err := ns.Get(ctx, "keep.bin")
	if err != nil {
		t.Fatalf("Get keep.bin after compaction: %v", err)
	}
	defer rc.Close()
	got, err := store.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll after compaction: %v", err)
	}
	if !bytes.Equal(got, keep) {
		t.Fatal("surviving blob's bytes changed after compaction — Nonce/Tag were not preserved across segment rewrite")
	}

	if _, err := ns.Get(ctx, "drop.bin"); err == nil {
		t.Fatal("expected drop.bin to be gone after delete+compact")
	}
}

func TestCreateNamespace_WithEncryptionRequiresKeyProvider(t *testing.T) {
	dir := t.TempDir()
	s, err := store.Open(store.Config{DataDir: dir, Index: index.NewMemoryBackend()}) // no KeyProvider
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	err = s.CreateNamespace(context.Background(), object.Namespace{ID: "secure"}, store.WithEncryption())
	if err == nil {
		t.Fatal("expected error creating an encrypted namespace without a KeyProvider")
	}
}

func TestEncryptedNamespace_WrongMasterKeyFailsToOpen(t *testing.T) {
	dir := t.TempDir()
	kp := newStaticKeyProvider(t)
	idx := index.NewMemoryBackend()

	s, err := store.Open(store.Config{DataDir: dir, Index: idx, KeyProvider: kp})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.CreateNamespace(context.Background(), object.Namespace{ID: "secure"}, store.WithEncryption()); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen the same on-disk store, but with a KeyProvider that hands
	// back a different key under the same version — simulating a lost
	// or wrong master key. Note: this test reuses the same *index*
	// backend instance (index.NewMemoryBackend() is in-memory, so a
	// fresh instance would be empty) to simulate "same store, different
	// key," matching how a real deployment persists its index
	// separately from its master key.
	wrongKP := &staticKeyProvider{version: kp.version, key: randBytes(encryption.KeySize)}
	_, err = store.Open(store.Config{DataDir: dir, Index: idx, KeyProvider: wrongKP})
	if err == nil {
		t.Fatal("expected Open to fail when the master key cannot decrypt the namespace's wrapped DEK")
	}
}
