package backend_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	blobserrors "github.com/asaidimu/blobs/errors"
	"github.com/asaidimu/blobs/index"
	backends "github.com/asaidimu/blobs/index/backend"
	"github.com/asaidimu/blobs/index/indextest"
)

func openTestBackend(t *testing.T) *backends.BboltBackend {
	t.Helper()
	path := filepath.Join(t.TempDir(), "index.db")
	b, err := backends.Open(backends.Options{Path: path})
	if err != nil {
		t.Fatalf("backends.Open: %v", err)
	}
	return b
}

// TestBboltBackendCompliance runs the exact same contract suite that
// index.MemoryBackend is held to (see index/memory_test.go). Any behavior
// that differs between the two backends — ordering, atomicity, copy
// semantics, not-found handling — surfaces here, not in production.
func TestBboltBackendCompliance(t *testing.T) {
	indextest.Run(t, func(t *testing.T) index.Backend {
		return openTestBackend(t)
	})
}

// TestPersistsAcrossRestart is the property MemoryBackend fundamentally
// cannot have, and the entire reason this adapter exists: data written
// before a process restart (or crash-free shutdown) must still be there
// after the database file is reopened.
func TestPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db")
	ctx := context.Background()

	b1, err := backends.Open(backends.Options{Path: path})
	if err != nil {
		t.Fatalf("Open (first): %v", err)
	}
	if err := b1.Put(ctx, []byte("ns:default"), []byte("namespace-record")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := b1.Tx(ctx, func(tx index.Tx) error {
		return tx.Put([]byte("ref:default:my-key"), []byte("ref-record"))
	}); err != nil {
		t.Fatalf("Tx: %v", err)
	}
	if err := b1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	b2, err := backends.Open(backends.Options{Path: path})
	if err != nil {
		t.Fatalf("Open (second, same path): %v", err)
	}
	defer b2.Close()

	got, err := b2.Get(ctx, []byte("ns:default"))
	if err != nil {
		t.Fatalf("Get(ns:default) after reopen: %v", err)
	}
	if string(got) != "namespace-record" {
		t.Fatalf("Get(ns:default) after reopen = %q, want %q", got, "namespace-record")
	}

	got, err = b2.Get(ctx, []byte("ref:default:my-key"))
	if err != nil {
		t.Fatalf("Get(ref:default:my-key) after reopen: %v", err)
	}
	if string(got) != "ref-record" {
		t.Fatalf("Get(ref:default:my-key) after reopen = %q, want %q", got, "ref-record")
	}
}

// TestOpenRequiresPath checks the one piece of config validation this
// adapter owns: an empty path is a caller bug, not something to silently
// default.
func TestOpenRequiresPath(t *testing.T) {
	_, err := backends.Open(backends.Options{})
	if err == nil {
		t.Fatal("Open with empty Path: got nil error, want non-nil")
	}
}

// TestOpenTimesOutOnLockedFile verifies a second Open against a
// still-held database file fails within roughly the configured timeout
// instead of hanging indefinitely — a bad Config here (or its absence)
// would turn a second process starting against the same store into a
// silent hang.
func TestOpenTimesOutOnLockedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db")

	holder, err := backends.Open(backends.Options{Path: path})
	if err != nil {
		t.Fatalf("Open (holder): %v", err)
	}
	defer holder.Close()

	start := time.Now()
	_, err = backends.Open(backends.Options{Path: path, Timeout: 200 * time.Millisecond})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("second Open against a locked file: got nil error, want a timeout error")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("second Open took %v to fail, want it bounded by the configured timeout", elapsed)
	}
}

// TestNotFoundErrorIsAssertableAfterWrap makes explicit the property the
// package doc comment promises and index.IsNotFound depends on: even
// though Get's error path is produced inside a bolt.DB.View closure and
// returned back out through this package's error handling, it must still
// satisfy a plain type assertion to *errors.NotFoundError, not just
// errors.As.
func TestNotFoundErrorIsAssertableAfterWrap(t *testing.T) {
	b := openTestBackend(t)
	defer b.Close()

	_, err := b.Get(context.Background(), []byte("missing"))
	if _, ok := err.(*blobserrors.NotFoundError); !ok {
		t.Fatalf("Get error type = %T, want *errors.NotFoundError to satisfy a plain type assertion (index.IsNotFound uses one, not errors.As)", err)
	}

	err = b.Tx(context.Background(), func(tx index.Tx) error {
		_, err := tx.Get([]byte("missing"))
		return err
	})
	if _, ok := err.(*blobserrors.NotFoundError); !ok {
		t.Fatalf("Tx error type = %T, want *errors.NotFoundError to satisfy a plain type assertion", err)
	}
	if !errors.As(err, new(*blobserrors.NotFoundError)) {
		t.Fatalf("errors.As also failed for %v", err)
	}
}
