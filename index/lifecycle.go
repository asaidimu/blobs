package index

import (
	"context"
	"errors"
	"fmt"
	"time"

	bserrors "github.com/asaidimu/blobs/errors"
	"github.com/asaidimu/blobs/object"
)

// FormatCurrent is the value of the meta:format record in an index laid out
// as described in the package documentation.
const FormatCurrent = "2"

// purgeBatch bounds how many keys one purge transaction deletes, so erasing a
// huge namespace never holds a single enormous transaction open.
const purgeBatch = 512

// errStopScan aborts a Scan early once a batch has been collected.
var errStopScan = errors.New("index: stop scan")

// ── format marker ─────────────────────────────────────────────────────────────

// Format returns the index format marker, or "" if none has been written
// (a fresh index, or one written by store v1.3.x and earlier).
func (idx *Index) Format(ctx context.Context) (string, error) {
	v, err := idx.b.Get(ctx, keyFormat())
	if err != nil {
		if IsNotFound(err) {
			return "", nil
		}
		return "", err
	}
	return string(v), nil
}

// SetFormat records the index format marker.
func (idx *Index) SetFormat(ctx context.Context, format string) error {
	return idx.b.Put(ctx, keyFormat(), []byte(format))
}

// ── namespace unlink / tombstones ─────────────────────────────────────────────

// UnlinkNamespace atomically releases a namespace's name and leaves a
// tombstone for its UID, in one transaction: afterwards the name is free to be
// created again and the old incarnation is unreachable by name, whatever
// happens to the erase that follows.
//
// It fails with NotFoundError if name is not currently bound to uid. The
// namespace record — including any wrapped encryption key — is deleted here, so
// for an encrypted namespace the data is cryptographically unrecoverable from
// this point on.
func (idx *Index) UnlinkNamespace(ctx context.Context, name, uid string, at time.Time) error {
	return idx.b.Tx(ctx, func(tx Tx) error {
		raw, err := tx.Get(keyNS(name))
		if err != nil {
			if IsNotFound(err) {
				return &bserrors.NotFoundError{NamespaceID: name}
			}
			return err
		}
		var ns object.Namespace
		if err := unmarshal(raw, &ns); err != nil {
			return err
		}
		cur := ns.UID
		if cur == "" {
			cur = ns.ID
		}
		if cur != uid {
			return &bserrors.NotFoundError{NamespaceID: name}
		}
		v, err := marshal(object.DeletedNamespace{UID: uid, Name: name, DeletedAt: at})
		if err != nil {
			return err
		}
		if err := tx.Put(keyTombstone(uid), v); err != nil {
			return err
		}
		return tx.Delete(keyNS(name))
	})
}

// ListTombstones returns every namespace that has been deleted but whose data
// has not been fully erased yet.
func (idx *Index) ListTombstones(ctx context.Context) ([]object.DeletedNamespace, error) {
	var out []object.DeletedNamespace
	err := idx.b.Scan(ctx, prefixTombstone(), func(_, v []byte) error {
		var d object.DeletedNamespace
		if err := unmarshal(v, &d); err != nil {
			return err
		}
		out = append(out, d)
		return nil
	})
	return out, err
}

// DeleteTombstone removes the tombstone for uid. Call it only after the
// namespace's data has been erased.
func (idx *Index) DeleteTombstone(ctx context.Context, uid string) error {
	return idx.b.Delete(ctx, keyTombstone(uid))
}

// ── owner purge ───────────────────────────────────────────────────────────────

// PurgeOwner deletes every index record owned by uid: refs, blob manifests,
// chunk records, segment records and stats. It is idempotent and resumable —
// if it fails part-way, calling it again finishes the job — and works in
// bounded batches. It does not touch the tombstone.
func (idx *Index) PurgeOwner(ctx context.Context, uid string) error {
	prefixes := [][]byte{
		prefixRef(uid), prefixBlob(uid), prefixChunk(uid), prefixSeg(uid),
	}
	for _, p := range prefixes {
		if err := idx.deletePrefix(ctx, p); err != nil {
			return fmt.Errorf("index: purge %q: %w", p, err)
		}
	}
	if err := idx.b.Delete(ctx, keyStats(uid)); err != nil && !IsNotFound(err) {
		return fmt.Errorf("index: purge stats: %w", err)
	}
	return nil
}

// deletePrefix removes every key with the given prefix, purgeBatch at a time.
// Keys are collected first and deleted in a separate transaction because a
// backend's Scan may hold a read transaction open.
func (idx *Index) deletePrefix(ctx context.Context, prefix []byte) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		keys := make([][]byte, 0, purgeBatch)
		err := idx.b.Scan(ctx, prefix, func(k, _ []byte) error {
			keys = append(keys, append([]byte(nil), k...))
			if len(keys) >= purgeBatch {
				return errStopScan
			}
			return nil
		})
		if err != nil && !errors.Is(err, errStopScan) {
			return err
		}
		if len(keys) == 0 {
			return nil
		}
		if err := idx.b.Tx(ctx, func(tx Tx) error {
			for _, k := range keys {
				if err := tx.Delete(k); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
}

// PutRecords writes blob manifests and chunk records for owner in one
// transaction. It exists for the format-1 → format-2 upgrade, which would
// otherwise pay one durable commit per record.
func (idx *Index) PutRecords(ctx context.Context, owner string, blobs []object.BlobEntry, chunks []object.ChunkEntry) error {
	return idx.b.Tx(ctx, func(tx Tx) error {
		for _, b := range blobs {
			v, err := marshal(b)
			if err != nil {
				return err
			}
			if err := tx.Put(keyBlob(owner, b.BlobID), v); err != nil {
				return err
			}
		}
		for _, c := range chunks {
			c.NamespaceID = owner
			v, err := marshal(c)
			if err != nil {
				return err
			}
			if err := tx.Put(keyChunk(owner, c.ChunkID), v); err != nil {
				return err
			}
		}
		return nil
	})
}
