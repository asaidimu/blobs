// Package indextest provides a backend-agnostic compliance test suite for
// index.Backend implementations. Every Backend — index.MemoryBackend,
// backends.BboltBackend, and any future adapter (BadgerDB, SQLite, etc.) —
// is expected to behave identically from the caller's point of view. Rather
// than duplicating that expectation across N separate test files (and
// inevitably letting them drift), Run exercises one Backend against the
// full contract described in index.Backend's doc comments, and every
// backend's own _test.go file just calls Run with a constructor for itself.
package indextest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	blobserrors "github.com/asaidimu/blobs/errors"
	"github.com/asaidimu/blobs/index"
)

// Run exercises newBackend() — which must return a fresh, empty Backend —
// against the full index.Backend contract. newBackend is called once per
// subtest so state from one case never leaks into another; each subtest
// closes the backend it creates.
func Run(t *testing.T, newBackend func(t *testing.T) index.Backend) {
	t.Helper()

	t.Run("PutGetRoundTrip", func(t *testing.T) { testPutGetRoundTrip(t, newBackend) })
	t.Run("GetMissingReturnsNotFound", func(t *testing.T) { testGetMissingReturnsNotFound(t, newBackend) })
	t.Run("DeleteThenGetReturnsNotFound", func(t *testing.T) { testDeleteThenGetReturnsNotFound(t, newBackend) })
	t.Run("DeleteMissingIsNoop", func(t *testing.T) { testDeleteMissingIsNoop(t, newBackend) })
	t.Run("ScanOrdersByKeyAndRespectsPrefix", func(t *testing.T) { testScanOrdersByKeyAndRespectsPrefix(t, newBackend) })
	t.Run("ScanNoMatchesCallsFnZeroTimes", func(t *testing.T) { testScanNoMatchesCallsFnZeroTimes(t, newBackend) })
	t.Run("ScanPropagatesCallbackError", func(t *testing.T) { testScanPropagatesCallbackError(t, newBackend) })
	t.Run("TxCommitsAllWritesAtomically", func(t *testing.T) { testTxCommitsAllWritesAtomically(t, newBackend) })
	t.Run("TxRollsBackAllWritesOnError", func(t *testing.T) { testTxRollsBackAllWritesOnError(t, newBackend) })
	t.Run("TxSeesItsOwnUncommittedWrites", func(t *testing.T) { testTxSeesItsOwnUncommittedWrites(t, newBackend) })
	t.Run("TxSeesPriorCommittedWrites", func(t *testing.T) { testTxSeesPriorCommittedWrites(t, newBackend) })
	t.Run("GetReturnsACopyNotAnAliasOfCallerBytes", func(t *testing.T) { testGetReturnsACopyNotAnAliasOfCallerBytes(t, newBackend) })
	t.Run("PutDoesNotAliasCallerBytes", func(t *testing.T) { testPutDoesNotAliasCallerBytes(t, newBackend) })
	t.Run("ConcurrentPutGetIsSafe", func(t *testing.T) { testConcurrentPutGetIsSafe(t, newBackend) })
	t.Run("CloseThenOperationErrors", func(t *testing.T) { testCloseThenOperationErrors(t, newBackend) })
}

func testPutGetRoundTrip(t *testing.T, newBackend func(t *testing.T) index.Backend) {
	b := newBackend(t)
	defer b.Close()
	ctx := context.Background()

	if err := b.Put(ctx, []byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := b.Get(ctx, []byte("k1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("Get = %q, want %q", got, "v1")
	}

	// Overwrite.
	if err := b.Put(ctx, []byte("k1"), []byte("v2")); err != nil {
		t.Fatalf("Put (overwrite): %v", err)
	}
	got, err = b.Get(ctx, []byte("k1"))
	if err != nil {
		t.Fatalf("Get (after overwrite): %v", err)
	}
	if !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("Get (after overwrite) = %q, want %q", got, "v2")
	}
}

func testGetMissingReturnsNotFound(t *testing.T, newBackend func(t *testing.T) index.Backend) {
	b := newBackend(t)
	defer b.Close()
	ctx := context.Background()

	_, err := b.Get(ctx, []byte("does-not-exist"))
	if err == nil {
		t.Fatal("Get on missing key: got nil error, want *errors.NotFoundError")
	}
	var nfe *blobserrors.NotFoundError
	if !errors.As(err, &nfe) {
		t.Fatalf("Get on missing key: err = %v (%T), want *errors.NotFoundError", err, err)
	}
}

func testDeleteThenGetReturnsNotFound(t *testing.T, newBackend func(t *testing.T) index.Backend) {
	b := newBackend(t)
	defer b.Close()
	ctx := context.Background()

	if err := b.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := b.Delete(ctx, []byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err := b.Get(ctx, []byte("k"))
	var nfe *blobserrors.NotFoundError
	if !errors.As(err, &nfe) {
		t.Fatalf("Get after Delete: err = %v (%T), want *errors.NotFoundError", err, err)
	}
}

func testDeleteMissingIsNoop(t *testing.T, newBackend func(t *testing.T) index.Backend) {
	b := newBackend(t)
	defer b.Close()
	ctx := context.Background()

	if err := b.Delete(ctx, []byte("never-existed")); err != nil {
		t.Fatalf("Delete on missing key should be a no-op, got: %v", err)
	}
}

func testScanOrdersByKeyAndRespectsPrefix(t *testing.T, newBackend func(t *testing.T) index.Backend) {
	b := newBackend(t)
	defer b.Close()
	ctx := context.Background()

	entries := map[string]string{
		"ref:ns1:banana": "1",
		"ref:ns1:apple":  "2",
		"ref:ns1:cherry": "3",
		"ref:ns2:apple":  "4", // different namespace, must not appear
		"blob:sha256:x":  "5", // different key type, must not appear
	}
	for k, v := range entries {
		if err := b.Put(ctx, []byte(k), []byte(v)); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}

	var gotKeys []string
	err := b.Scan(ctx, []byte("ref:ns1:"), func(key, value []byte) error {
		gotKeys = append(gotKeys, string(key))
		return nil
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	want := []string{"ref:ns1:apple", "ref:ns1:banana", "ref:ns1:cherry"}
	if len(gotKeys) != len(want) {
		t.Fatalf("Scan returned %d keys, want %d: got %v", len(gotKeys), len(want), gotKeys)
	}
	for i := range want {
		if gotKeys[i] != want[i] {
			t.Fatalf("Scan order[%d] = %q, want %q (full: %v)", i, gotKeys[i], want[i], gotKeys)
		}
	}
}

func testScanNoMatchesCallsFnZeroTimes(t *testing.T, newBackend func(t *testing.T) index.Backend) {
	b := newBackend(t)
	defer b.Close()
	ctx := context.Background()

	if err := b.Put(ctx, []byte("ns:default"), []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	calls := 0
	err := b.Scan(ctx, []byte("ref:nothing-here:"), func(key, value []byte) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if calls != 0 {
		t.Fatalf("Scan called fn %d times for a prefix with no matches, want 0", calls)
	}
}

func testScanPropagatesCallbackError(t *testing.T, newBackend func(t *testing.T) index.Backend) {
	b := newBackend(t)
	defer b.Close()
	ctx := context.Background()

	if err := b.Put(ctx, []byte("ref:ns1:a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := b.Put(ctx, []byte("ref:ns1:b"), []byte("2")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	sentinel := fmt.Errorf("boom")
	calls := 0
	err := b.Scan(ctx, []byte("ref:ns1:"), func(key, value []byte) error {
		calls++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Scan callback error not propagated: got %v, want wrapping %v", err, sentinel)
	}
	if calls != 1 {
		t.Fatalf("Scan should stop at the first callback error, got %d calls", calls)
	}
}

func testTxCommitsAllWritesAtomically(t *testing.T, newBackend func(t *testing.T) index.Backend) {
	b := newBackend(t)
	defer b.Close()
	ctx := context.Background()

	err := b.Tx(ctx, func(tx index.Tx) error {
		if err := tx.Put([]byte("a"), []byte("1")); err != nil {
			return err
		}
		if err := tx.Put([]byte("b"), []byte("2")); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Tx: %v", err)
	}

	for k, want := range map[string]string{"a": "1", "b": "2"} {
		got, err := b.Get(ctx, []byte(k))
		if err != nil {
			t.Fatalf("Get(%q) after committed Tx: %v", k, err)
		}
		if string(got) != want {
			t.Fatalf("Get(%q) after committed Tx = %q, want %q", k, got, want)
		}
	}
}

func testTxRollsBackAllWritesOnError(t *testing.T, newBackend func(t *testing.T) index.Backend) {
	b := newBackend(t)
	defer b.Close()
	ctx := context.Background()

	// Pre-existing value that the failed Tx will try (and fail) to overwrite.
	if err := b.Put(ctx, []byte("existing"), []byte("original")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	sentinel := fmt.Errorf("rollback me")
	err := b.Tx(ctx, func(tx index.Tx) error {
		if err := tx.Put([]byte("existing"), []byte("clobbered")); err != nil {
			return err
		}
		if err := tx.Put([]byte("new-key"), []byte("should-not-exist")); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Tx error = %v, want wrapping %v", err, sentinel)
	}

	got, err := b.Get(ctx, []byte("existing"))
	if err != nil {
		t.Fatalf("Get(existing) after rolled-back Tx: %v", err)
	}
	if string(got) != "original" {
		t.Fatalf("Get(existing) after rolled-back Tx = %q, want %q (rollback did not revert the overwrite)", got, "original")
	}

	_, err = b.Get(ctx, []byte("new-key"))
	var nfe *blobserrors.NotFoundError
	if !errors.As(err, &nfe) {
		t.Fatalf("Get(new-key) after rolled-back Tx: err = %v, want *errors.NotFoundError (write should not have persisted)", err)
	}
}

func testTxSeesItsOwnUncommittedWrites(t *testing.T, newBackend func(t *testing.T) index.Backend) {
	b := newBackend(t)
	defer b.Close()
	ctx := context.Background()

	err := b.Tx(ctx, func(tx index.Tx) error {
		if err := tx.Put([]byte("k"), []byte("v1")); err != nil {
			return err
		}
		got, err := tx.Get([]byte("k"))
		if err != nil {
			return fmt.Errorf("tx.Get of its own uncommitted write: %w", err)
		}
		if string(got) != "v1" {
			return fmt.Errorf("tx.Get of its own uncommitted write = %q, want %q", got, "v1")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Tx: %v", err)
	}
}

func testTxSeesPriorCommittedWrites(t *testing.T, newBackend func(t *testing.T) index.Backend) {
	b := newBackend(t)
	defer b.Close()
	ctx := context.Background()

	if err := b.Put(ctx, []byte("k"), []byte("committed-value")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	err := b.Tx(ctx, func(tx index.Tx) error {
		got, err := tx.Get([]byte("k"))
		if err != nil {
			return err
		}
		if string(got) != "committed-value" {
			return fmt.Errorf("tx.Get of prior committed write = %q, want %q", got, "committed-value")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Tx: %v", err)
	}
}

func testGetReturnsACopyNotAnAliasOfCallerBytes(t *testing.T, newBackend func(t *testing.T) index.Backend) {
	b := newBackend(t)
	defer b.Close()
	ctx := context.Background()

	original := []byte("original-value")
	if err := b.Put(ctx, []byte("k"), original); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got1, err := b.Get(ctx, []byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	// Mutate the slice we got back. If the backend handed us an internal
	// pointer instead of a copy, this corrupts its stored value — exactly
	// the class of stale/aliased-buffer bug VULN 4 was about.
	for i := range got1 {
		got1[i] = 'X'
	}

	got2, err := b.Get(ctx, []byte("k"))
	if err != nil {
		t.Fatalf("Get (second read): %v", err)
	}
	if !bytes.Equal(got2, []byte("original-value")) {
		t.Fatalf("mutating a Get result corrupted the stored value: got %q, want %q (Get is not returning a copy)", got2, "original-value")
	}
}

func testPutDoesNotAliasCallerBytes(t *testing.T, newBackend func(t *testing.T) index.Backend) {
	b := newBackend(t)
	defer b.Close()
	ctx := context.Background()

	buf := []byte("initial")
	if err := b.Put(ctx, []byte("k"), buf); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Mutate the caller's buffer after Put returns. A conforming backend
	// must have already copied it internally.
	for i := range buf {
		buf[i] = 'Y'
	}

	got, err := b.Get(ctx, []byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("initial")) {
		t.Fatalf("mutating the caller's buffer after Put corrupted the stored value: got %q, want %q (Put is not copying its input)", got, "initial")
	}
}

func testConcurrentPutGetIsSafe(t *testing.T, newBackend func(t *testing.T) index.Backend) {
	b := newBackend(t)
	defer b.Close()
	ctx := context.Background()

	const goroutines = 16
	const opsPerGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := []byte(fmt.Sprintf("concurrent:%d:%d", g, i))
				val := []byte(fmt.Sprintf("value-%d-%d", g, i))
				if err := b.Put(ctx, key, val); err != nil {
					t.Errorf("Put(%s): %v", key, err)
					return
				}
				got, err := b.Get(ctx, key)
				if err != nil {
					t.Errorf("Get(%s): %v", key, err)
					return
				}
				if !bytes.Equal(got, val) {
					t.Errorf("Get(%s) = %q, want %q", key, got, val)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

func testCloseThenOperationErrors(t *testing.T, newBackend func(t *testing.T) index.Backend) {
	b := newBackend(t)
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ctx := context.Background()
	if err := b.Put(ctx, []byte("k"), []byte("v")); err == nil {
		t.Fatal("Put after Close: got nil error, want non-nil")
	}
}
