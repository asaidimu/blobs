// Package backend provides concrete implementations of the index.Backend
// interface. This file implements a persistent backend on top of bbolt
// (go.etcd.io/bbolt), a single-file, embedded B+-tree with real ACID
// transactions.
//
// # Design
//
// The entire key schema defined in package index (ns:, ref:, blob:, chunk:,
// seg:, stats:) is a flat, lexicographically-ordered string space. bbolt
// stores keys sorted within a bucket, so that schema maps directly onto a
// single bucket: no need to split index record types across buckets, and
// prefix scans (index.Index.ListNamespaces, ListRefs, ListSegments) work
// exactly as they do against index.MemoryBackend, just backed by a cursor
// over a B+-tree instead of a sorted slice of map keys.
//
// index.Backend.Tx maps directly onto bolt.DB.Update: bbolt's transactions
// are single-writer, serializable, and crash-safe, and a non-nil error
// returned from the update function automatically rolls back every write
// made inside it. That gives CommitPut/CommitDelete real atomicity instead
// of the manual staged-write/rollback bookkeeping MemoryBackend needs.
//
// Every value returned by Get, Scan, or a Tx's Get is copied before it is
// handed to the caller. bbolt only guarantees a []byte returned from a
// bucket is valid for the lifetime of the transaction that produced it —
// holding onto it afterward, or across a transaction boundary, is exactly
// the class of stale-buffer bug the security audit already flagged once
// (VULN 4, sync.Pool page padding). Backend.Get and Backend.Scan callers
// are entitled to assume the bytes they receive are theirs to keep.
//
// Durability matches the rest of the store: fsync is never disabled here
// (bolt.Options.NoSync is left at its default, false), so every committed
// transaction is durable on return, mirroring the WAL fsync fix (VULN 5).
package backend

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	"go.etcd.io/bbolt"

	"github.com/asaidimu/blobs/errors"
	"github.com/asaidimu/blobs/index"
)

// DefaultBucket is the bucket name used when Options.BucketName is empty.
const DefaultBucket = "index"

// DefaultTimeout is the file-lock acquisition timeout used when
// Options.Timeout is zero. bbolt takes an exclusive flock on the database
// file; without a timeout, opening a file that's already held by another
// process (or another instance in the same process) blocks forever.
const DefaultTimeout = 5 * time.Second

// Options configures a bbolt-backed index.Backend.
type Options struct {
	// Path is the filesystem path to the bbolt database file. It is
	// created if it does not exist. Required.
	Path string

	// BucketName is the single bucket all index records are stored in.
	// Defaults to DefaultBucket.
	BucketName string

	// Timeout bounds how long Open waits to acquire the database file's
	// lock before giving up. Defaults to DefaultTimeout. A negative
	// value disables the timeout and waits indefinitely, matching
	// bbolt's own zero-value behavior.
	Timeout time.Duration

	// FileMode is the permission mode used if the database file must be
	// created. Defaults to 0600 (owner read/write only) — index data is
	// not meant to be world-readable.
	FileMode os.FileMode

	// ReadOnly opens the database without acquiring the write lock, for
	// tooling (e.g. inspection, backups) that must never mutate the
	// store. Backend.Put, Delete, and Tx all fail against a read-only
	// backend.
	ReadOnly bool
}

func (o Options) withDefaults() Options {
	if o.BucketName == "" {
		o.BucketName = DefaultBucket
	}
	if o.Timeout == 0 {
		o.Timeout = DefaultTimeout
	}
	if o.FileMode == 0 {
		o.FileMode = 0o600
	}
	return o
}

func (o Options) validate() error {
	if o.Path == "" {
		return fmt.Errorf("backends: bbolt: Options.Path must not be empty")
	}
	return nil
}

// BboltBackend is a persistent, single-file index.Backend implementation.
// Safe for concurrent use — bbolt serializes writers internally and allows
// concurrent readers without blocking them.
type BboltBackend struct {
	db     *bbolt.DB
	bucket []byte
}

// Open opens (creating if necessary) a bbolt database at opts.Path and
// returns a ready-to-use Backend. The caller must call Close when done.
func Open(opts Options) (*BboltBackend, error) {
	opts = opts.withDefaults()
	if err := opts.validate(); err != nil {
		return nil, err
	}

	boltOpts := &bbolt.Options{
		ReadOnly: opts.ReadOnly,
	}
	if opts.Timeout >= 0 {
		boltOpts.Timeout = opts.Timeout
	}

	db, err := bbolt.Open(opts.Path, opts.FileMode, boltOpts)
	if err != nil {
		return nil, fmt.Errorf("backends: bbolt: open %q: %w", opts.Path, err)
	}

	bucket := []byte(opts.BucketName)

	if !opts.ReadOnly {
		if err := db.Update(func(tx *bbolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists(bucket)
			return err
		}); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("backends: bbolt: create bucket %q: %w", opts.BucketName, err)
		}
	}

	return &BboltBackend{db: db, bucket: bucket}, nil
}

// Path returns the filesystem path of the underlying database file.
func (b *BboltBackend) Path() string { return b.db.Path() }

func (b *BboltBackend) getBucket(tx *bbolt.Tx) (*bbolt.Bucket, error) {
	bkt := tx.Bucket(b.bucket)
	if bkt == nil {
		return nil, fmt.Errorf("backends: bbolt: bucket %q missing (database not initialized via Open)", b.bucket)
	}
	return bkt, nil
}

// Put implements index.Backend.
func (b *BboltBackend) Put(ctx context.Context, key, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := b.db.Update(func(tx *bbolt.Tx) error {
		bkt, err := b.getBucket(tx)
		if err != nil {
			return err
		}
		return bkt.Put(key, value)
	})
	if err != nil {
		return fmt.Errorf("backends: bbolt: put: %w", err)
	}
	return nil
}

// Get implements index.Backend. Returns *errors.NotFoundError if key is
// absent, matching the contract documented on index.Backend.
func (b *BboltBackend) Get(ctx context.Context, key []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var out []byte
	err := b.db.View(func(tx *bbolt.Tx) error {
		bkt, err := b.getBucket(tx)
		if err != nil {
			return err
		}
		v := bkt.Get(key)
		if v == nil {
			return &errors.NotFoundError{}
		}
		out = append([]byte(nil), v...)
		return nil
	})
	if err != nil {
		if _, ok := err.(*errors.NotFoundError); ok {
			return nil, err
		}
		return nil, fmt.Errorf("backends: bbolt: get: %w", err)
	}
	return out, nil
}

// GetMulti implements index.BatchGetter: it resolves every key within a
// single bbolt read transaction instead of calling Get (and opening a
// fresh db.View transaction) once per key. For a caller resolving many
// chunk locations at once — e.g. every chunk of a multi-gigabyte streamed
// or verified blob, which can easily be a few hundred entries — this
// turns a few hundred transaction acquisitions into exactly one.
//
// A missing key's slot in the result is nil, matching index.BatchGetter's
// contract exactly; unlike Get, this never returns *errors.NotFoundError
// itself — callers that need "found vs missing" per key check for a nil
// slot instead.
func (b *BboltBackend) GetMulti(ctx context.Context, keys [][]byte) ([][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([][]byte, len(keys))
	err := b.db.View(func(tx *bbolt.Tx) error {
		bkt, err := b.getBucket(tx)
		if err != nil {
			return err
		}
		for i, k := range keys {
			v := bkt.Get(k)
			if v == nil {
				continue // leave out[i] nil, per BatchGetter's contract
			}
			out[i] = append([]byte(nil), v...)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("backends: bbolt: get multi: %w", err)
	}
	return out, nil
}

// Delete implements index.Backend. Deleting an absent key is not an error,
// matching index.MemoryBackend's behavior.
func (b *BboltBackend) Delete(ctx context.Context, key []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := b.db.Update(func(tx *bbolt.Tx) error {
		bkt, err := b.getBucket(tx)
		if err != nil {
			return err
		}
		return bkt.Delete(key)
	})
	if err != nil {
		return fmt.Errorf("backends: bbolt: delete: %w", err)
	}
	return nil
}

// Scan implements index.Backend. Keys are visited in ascending
// lexicographic order, which bbolt's B+-tree provides natively. fn is
// called within a read transaction; a long-running fn will hold that
// transaction (and therefore the writer that's waiting behind it) open for
// its duration, exactly as it would against any other transactional store.
func (b *BboltBackend) Scan(ctx context.Context, prefix []byte, fn func(key, value []byte) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := b.db.View(func(tx *bbolt.Tx) error {
		bkt, err := b.getBucket(tx)
		if err != nil {
			return err
		}
		c := bkt.Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			kc := append([]byte(nil), k...)
			vc := append([]byte(nil), v...)
			if err := fn(kc, vc); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("backends: bbolt: scan: %w", err)
	}
	return nil
}

// Tx implements index.Backend. fn runs inside a single bolt.DB.Update call:
// every write made through the supplied index.Tx is committed atomically
// if fn returns nil, and none of them are visible if fn returns an error —
// bbolt rolls the whole transaction back automatically.
func (b *BboltBackend) Tx(ctx context.Context, fn func(index.Tx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// bolt.DB.Update returns fn's error verbatim when it rolls back (see
	// bbolt's db.go), so whatever fn returned — including a sentinel like
	// *errors.NotFoundError that index.CommitDelete relies on being
	// type-assertable — comes straight back out here, untouched and
	// unwrapped.
	return b.db.Update(func(tx *bbolt.Tx) error {
		bkt, err := b.getBucket(tx)
		if err != nil {
			return err
		}
		return fn(&boltTx{bucket: bkt})
	})
}

// Close implements index.Backend.
func (b *BboltBackend) Close() error {
	if err := b.db.Close(); err != nil {
		return fmt.Errorf("backends: bbolt: close: %w", err)
	}
	return nil
}

// boltTx adapts a single bbolt bucket, scoped to one in-flight
// bolt.DB.Update call, to the index.Tx interface.
type boltTx struct {
	bucket *bbolt.Bucket
}

func (t *boltTx) Put(key, value []byte) error {
	return t.bucket.Put(key, value)
}

func (t *boltTx) Get(key []byte) ([]byte, error) {
	v := t.bucket.Get(key)
	if v == nil {
		return nil, &errors.NotFoundError{}
	}
	return append([]byte(nil), v...), nil
}

func (t *boltTx) Delete(key []byte) error {
	return t.bucket.Delete(key)
}
