package store_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	bserrors "github.com/asaidimu/blobs/errors"
	"github.com/asaidimu/blobs/index"
	"github.com/asaidimu/blobs/store"
)

func openTestStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(store.Config{
		DataDir: t.TempDir(),
		Index:   index.NewMemoryBackend(),
	})
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	return s
}

func isClosedErr(err error) bool {
	var ce *bserrors.ClosedError
	return errors.As(err, &ce)
}

// waitOrFatal waits for done to be closed, failing the test if it takes
// longer than d. Used for the "eventually completes" side of every
// assertion below, so a real bug hangs the test with a clear failure
// instead of hanging the whole test binary forever.
func waitOrFatal(t *testing.T, done <-chan struct{}, d time.Duration, msg string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatal(msg)
	}
}

// assertStillBlocked checks that done has NOT fired within d, i.e. the
// operation it guards is still in flight. A short, generous d is used
// deliberately: this only needs to observe "hasn't happened yet", not
// prove a negative for all time.
func assertStillBlocked(t *testing.T, done <-chan struct{}, d time.Duration, msg string) {
	t.Helper()
	select {
	case <-done:
		t.Fatal(msg)
	case <-time.After(d):
	}
}

// TestClose_WaitsForInFlightPut is the core scenario from production
// readiness report item 5.7: Close must not tear down an engine while a
// Put is still writing to it. An io.Pipe reader lets us hold WriteBlob's
// read call open for as long as we choose, deterministically, without
// sleeps standing in for synchronization.
func TestClose_WaitsForInFlightPut(t *testing.T) {
	s := openTestStore(t)
	ns := s.Namespace("default")

	pr, pw := io.Pipe()
	putDone := make(chan struct{})
	var putErr error
	go func() {
		defer close(putDone)
		_, putErr = ns.Put(context.Background(), "slow-key", pr, store.PutOptions{})
	}()

	// Give the Put goroutine a moment to actually start — beginNSOp is
	// acquired synchronously as Put's very first action, so in practice
	// this happens almost immediately; the sleep is just scheduling
	// slack, not something correctness depends on.
	time.Sleep(20 * time.Millisecond)

	closeDone := make(chan struct{})
	var closeErr error
	go func() {
		defer close(closeDone)
		closeErr = s.Close()
	}()

	assertStillBlocked(t, closeDone, 150*time.Millisecond,
		"Store.Close() returned while a Put was still writing — it must wait for in-flight writes (report item 5.7)")

	// Let the blocked WriteBlob read finish.
	if _, err := pw.Write([]byte("hello world, this is the blob body")); err != nil {
		t.Fatalf("pw.Write: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("pw.Close: %v", err)
	}

	waitOrFatal(t, putDone, 2*time.Second, "Put never finished after the pipe was closed")
	if putErr != nil {
		t.Fatalf("Put returned an error: %v", putErr)
	}

	waitOrFatal(t, closeDone, 2*time.Second, "Store.Close() never completed after the in-flight Put finished")
	if closeErr != nil {
		t.Fatalf("Close returned an error: %v", closeErr)
	}
}

// TestClose_WaitsForOpenGetReader covers the subtler half of the same
// fix: NamespaceHandle.Get returns before its io.ReadCloser has been
// read. The store's guard must be held for the reader's whole lifetime,
// not just for the duration of the Get call itself.
func TestClose_WaitsForOpenGetReader(t *testing.T) {
	s := openTestStore(t)
	ns := s.Namespace("default")

	if _, err := ns.Put(context.Background(), "k", bytes.NewReader([]byte("payload")), store.PutOptions{}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	rc, err := ns.Get(context.Background(), "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	closeDone := make(chan struct{})
	var closeErr error
	go func() {
		defer close(closeDone)
		closeErr = s.Close()
	}()

	assertStillBlocked(t, closeDone, 150*time.Millisecond,
		"Store.Close() returned while a Get reader was still open — the guard must outlive the Get call itself")

	if err := rc.Close(); err != nil {
		t.Fatalf("reader Close: %v", err)
	}

	waitOrFatal(t, closeDone, 2*time.Second, "Store.Close() never completed after the open Get reader was closed")
	if closeErr != nil {
		t.Fatalf("Close returned an error: %v", closeErr)
	}
}

// TestClose_IdempotentAndRejectsDoubleClose confirms the existing
// ClosedError contract on Close itself still holds.
func TestClose_IdempotentAndRejectsDoubleClose(t *testing.T) {
	s := openTestStore(t)
	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	err := s.Close()
	if !isClosedErr(err) {
		t.Fatalf("second Close() = %v, want *errors.ClosedError", err)
	}
}

// TestOperationsRejectedAfterClose exercises every NamespaceHandle and
// top-level Store method that touches the index or an engine, confirming
// each now fails fast with ClosedError instead of racing a closed engine
// or a closed index backend.
func TestOperationsRejectedAfterClose(t *testing.T) {
	s := openTestStore(t)
	ns := s.Namespace("default")
	ctx := context.Background()

	// Seed one blob before closing, so Get/Head/Delete have a key to
	// (attempt to) act on — irrelevant to the assertion since every one
	// of these must fail with ClosedError before it ever reaches the
	// index or engine.
	if _, err := ns.Put(ctx, "seed", bytes.NewReader([]byte("x")), store.PutOptions{}); err != nil {
		t.Fatalf("seed Put: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	checks := []struct {
		name string
		call func() error
	}{
		{"NamespaceHandle.Put", func() error {
			_, err := ns.Put(ctx, "k", bytes.NewReader([]byte("x")), store.PutOptions{})
			return err
		}},
		{"NamespaceHandle.Get", func() error {
			_, err := ns.Get(ctx, "seed")
			return err
		}},
		{"NamespaceHandle.Head", func() error {
			_, err := ns.Head(ctx, "seed")
			return err
		}},
		{"NamespaceHandle.Delete", func() error {
			return ns.Delete(ctx, "seed")
		}},
		{"NamespaceHandle.List", func() error {
			_, err := ns.List(ctx, store.ListOptions{})
			return err
		}},
		{"NamespaceHandle.Stats", func() error {
			_, err := ns.Stats(ctx)
			return err
		}},
		{"NamespaceHandle.Verify", func() error {
			return ns.Verify(ctx)
		}},
		{"NamespaceHandle.RebuildIndex", func() error {
			return ns.RebuildIndex(ctx)
		}},
		{"NamespaceHandle.Compact", func() error {
			_, err := ns.Compact(ctx)
			return err
		}},
		{"Store.GetNamespace", func() error {
			_, err := s.GetNamespace(ctx, "default")
			return err
		}},
		{"Store.ListNamespaces", func() error {
			_, err := s.ListNamespaces(ctx)
			return err
		}},
		{"Store.Stats", func() error {
			_, err := s.Stats(ctx)
			return err
		}},
	}

	for _, c := range checks {
		t.Run(c.name, func(t *testing.T) {
			err := c.call()
			if !isClosedErr(err) {
				t.Fatalf("%s after Close() = %v (%T), want *errors.ClosedError", c.name, err, err)
			}
		})
	}
}

// TestConfigValidation_RejectsNonsensicalPageSize is the store-level half
// of production readiness item 5.8: store.Open must fail immediately —
// before touching the index or filesystem — on the exact PageSize=50
// example the report names, rather than deferring to a panic later at
// write time.
func TestConfigValidation_RejectsNonsensicalPageSize(t *testing.T) {
	_, err := store.Open(store.Config{
		DataDir:  t.TempDir(),
		Index:    index.NewMemoryBackend(),
		PageSize: 50, // < pageHeaderSize (88)
	})
	if err == nil {
		t.Fatal("store.Open with PageSize=50 = nil error, want a validation error")
	}
}

func TestConfigValidation_AcceptsZeroValues(t *testing.T) {
	s, err := store.Open(store.Config{
		DataDir: t.TempDir(),
		Index:   index.NewMemoryBackend(),
		// PageSize, ChunkSize, MaxSegmentSize all left at zero.
	})
	if err != nil {
		t.Fatalf("store.Open with zero-value volume tuning = %v, want nil (zero means use defaults)", err)
	}
	defer s.Close()
}

func TestConfigValidation_RejectsInconsistentChunkAndSegmentSize(t *testing.T) {
	_, err := store.Open(store.Config{
		DataDir:        t.TempDir(),
		Index:          index.NewMemoryBackend(),
		ChunkSize:      10 * 1024 * 1024,
		MaxSegmentSize: 1024 * 1024, // smaller than ChunkSize
	})
	if err == nil {
		t.Fatal("store.Open with ChunkSize > MaxSegmentSize = nil error, want a validation error")
	}
}
