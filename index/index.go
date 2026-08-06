// Package index defines the pluggable index backend interface and owns all
// serialisation of index records. The backend is deliberately minimal —
// it speaks only in raw bytes. All structure lives here, not in the backend.
//
// Key schema (shown as strings; stored as []byte):
//
//	ns:<nsID>                        → Namespace
//	ref:<nsID>:<key>                 → RefEntry
//	blob:<blobID>                    → BlobEntry
//	chunk:<chunkID>                  → ChunkEntry
//	seg:<nsID>:<segID16hex>          → SegmentEntry
//	stats:<nsID>                     → NamespaceStats
package index

import (
	"maps"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/asaidimu/blobs/errors"
	"github.com/asaidimu/blobs/object"
)

// ── Backend interface ─────────────────────────────────────────────────────────

// Backend is the only contract the blobstore places on an external KV store.
// Implementations must be safe for concurrent use.
// Get must return *errors.NotFoundError when a key is absent.
type Backend interface {
	Put(ctx context.Context, key, value []byte) error
	Get(ctx context.Context, key []byte) ([]byte, error)
	Delete(ctx context.Context, key []byte) error
	Scan(ctx context.Context, prefix []byte, fn func(key, value []byte) error) error
	Tx(ctx context.Context, fn func(Tx) error) error
	Close() error
}

// Tx is a handle to an in-progress atomic transaction.
type Tx interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
}

// BatchGetter is an optional capability a Backend may implement for more
// efficient multi-key reads — resolving many keys within a single
// read-only transaction instead of issuing one Get (and one transaction
// acquisition) per key. A Backend that doesn't implement this still
// works correctly everywhere it's used: callers fall back to individual
// Get calls, just without the batching benefit.
type BatchGetter interface {
	// GetMulti returns one value per key, in the same order as keys. A
	// missing key's slot must be nil — GetMulti reports absence
	// positionally rather than failing the whole batch, since a batch of
	// otherwise-valid lookups shouldn't error out entirely just because
	// one entry happens to already be gone.
	GetMulti(ctx context.Context, keys [][]byte) ([][]byte, error)
}

// ── Key constructors ──────────────────────────────────────────────────────────

func keyNS(nsID string) []byte          { return []byte("ns:" + nsID) }
func prefixNS() []byte                  { return []byte("ns:") }
func keyRef(nsID, key string) []byte    { return []byte("ref:" + nsID + ":" + key) }
func prefixRef(nsID string) []byte      { return []byte("ref:" + nsID + ":") }
func keyBlob(id object.BlobID) []byte   { return []byte("blob:" + string(id)) }
func prefixBlob() []byte                { return []byte("blob:") }
func keyChunk(id object.ChunkID) []byte { return []byte("chunk:" + string(id)) }
func keySeg(nsID string, id object.SegmentID) []byte {
	return fmt.Appendf(nil, "seg:%s:%s", nsID, id.String())
}
func prefixSeg(nsID string) []byte { return []byte("seg:" + nsID + ":") }
func keyStats(nsID string) []byte  { return []byte("stats:" + nsID) }

// ── Index ─────────────────────────────────────────────────────────────────────

// Index wraps a Backend with typed, schema-aware operations.
type Index struct{ b Backend }

// New wraps b in an Index.
func New(b Backend) *Index { return &Index{b: b} }

// Close closes the underlying backend.
func (idx *Index) Close() error { return idx.b.Close() }

// IsNotFound reports whether err is a not-found sentinel.
func IsNotFound(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*errors.NotFoundError)
	return ok
}

func marshal(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("index: marshal %T: %w", v, err)
	}
	return b, nil
}

func unmarshal(data []byte, v any) error {
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("index: unmarshal %T: %w", v, err)
	}
	return nil
}

// ── Namespace ─────────────────────────────────────────────────────────────────

func (idx *Index) PutNamespace(ctx context.Context, ns object.Namespace) error {
	v, err := marshal(ns)
	if err != nil {
		return err
	}
	return idx.b.Put(ctx, keyNS(ns.ID), v)
}

func (idx *Index) GetNamespace(ctx context.Context, nsID string) (*object.Namespace, error) {
	v, err := idx.b.Get(ctx, keyNS(nsID))
	if err != nil {
		return nil, err
	}
	var ns object.Namespace
	return &ns, unmarshal(v, &ns)
}

func (idx *Index) DeleteNamespace(ctx context.Context, nsID string) error {
	return idx.b.Delete(ctx, keyNS(nsID))
}

func (idx *Index) ListNamespaces(ctx context.Context) ([]object.Namespace, error) {
	var out []object.Namespace
	err := idx.b.Scan(ctx, prefixNS(), func(_, v []byte) error {
		var ns object.Namespace
		if err := unmarshal(v, &ns); err != nil {
			return err
		}
		out = append(out, ns)
		return nil
	})
	return out, err
}

// ── Refs ──────────────────────────────────────────────────────────────────────

func (idx *Index) PutRef(ctx context.Context, ref object.RefEntry) error {
	v, err := marshal(ref)
	if err != nil {
		return err
	}
	return idx.b.Put(ctx, keyRef(ref.NamespaceID, ref.Key), v)
}

func (idx *Index) GetRef(ctx context.Context, nsID, key string) (*object.RefEntry, error) {
	v, err := idx.b.Get(ctx, keyRef(nsID, key))
	if err != nil {
		return nil, err
	}
	var ref object.RefEntry
	return &ref, unmarshal(v, &ref)
}

func (idx *Index) DeleteRef(ctx context.Context, nsID, key string) error {
	return idx.b.Delete(ctx, keyRef(nsID, key))
}

func (idx *Index) ListRefs(ctx context.Context, nsID, keyPrefix string) ([]object.RefEntry, error) {
	prefix := prefixRef(nsID)
	if keyPrefix != "" {
		prefix = append(prefix, []byte(keyPrefix)...)
	}
	var out []object.RefEntry
	err := idx.b.Scan(ctx, prefix, func(_, v []byte) error {
		var ref object.RefEntry
		if err := unmarshal(v, &ref); err != nil {
			return err
		}
		out = append(out, ref)
		return nil
	})
	return out, err
}

// ── Blobs ─────────────────────────────────────────────────────────────────────

func (idx *Index) PutBlob(ctx context.Context, blob object.BlobEntry) error {
	v, err := marshal(blob)
	if err != nil {
		return err
	}
	return idx.b.Put(ctx, keyBlob(blob.BlobID), v)
}

func (idx *Index) GetBlob(ctx context.Context, id object.BlobID) (*object.BlobEntry, error) {
	v, err := idx.b.Get(ctx, keyBlob(id))
	if err != nil {
		return nil, err
	}
	var blob object.BlobEntry
	return &blob, unmarshal(v, &blob)
}

func (idx *Index) DeleteBlob(ctx context.Context, id object.BlobID) error {
	return idx.b.Delete(ctx, keyBlob(id))
}

// PurgeDeadBlobs removes every blob manifest whose RefCount has reached
// zero. Blob records are keyed by content and hold no owned data — chunk
// records are independent and carry their own refcounts — so removing a
// dead manifest is always safe and never drops live bytes. Returns the
// number of manifests removed.
func (idx *Index) PurgeDeadBlobs(ctx context.Context) (int, error) {
	var removed int
	err := idx.b.Scan(ctx, prefixBlob(), func(k, v []byte) error {
		var blob object.BlobEntry
		if err := unmarshal(v, &blob); err != nil {
			return err
		}
		if blob.RefCount == 0 {
			if err := idx.b.Delete(ctx, k); err != nil {
				return err
			}
			removed++
		}
		return nil
	})
	return removed, err
}

// ── Chunks ────────────────────────────────────────────────────────────────────

func (idx *Index) PutChunk(ctx context.Context, chunk object.ChunkEntry) error {
	v, err := marshal(chunk)
	if err != nil {
		return err
	}
	return idx.b.Put(ctx, keyChunk(chunk.ChunkID), v)
}

func (idx *Index) GetChunk(ctx context.Context, id object.ChunkID) (*object.ChunkEntry, error) {
	v, err := idx.b.Get(ctx, keyChunk(id))
	if err != nil {
		return nil, err
	}
	var chunk object.ChunkEntry
	return &chunk, unmarshal(v, &chunk)
}

// GetChunks resolves multiple chunk IDs at once, returning entries in the
// same order as ids. If the backend implements BatchGetter, this issues a
// single batched read instead of len(ids) separate Get calls — for a
// backend like the bbolt one, that collapses len(ids) separate read
// transactions into one, which matters a lot for a blob with hundreds of
// chunks (e.g. resolving every chunk location needed to stream or verify
// a multi-gigabyte blob). Backends without BatchGetter fall back to
// calling GetChunk in a loop, with identical behavior to before this
// method existed.
//
// Returns *errors.NotFoundError, identifying the specific missing chunk
// ID in its error message, if any id in ids has no index entry.
func (idx *Index) GetChunks(ctx context.Context, ids []object.ChunkID) ([]object.ChunkEntry, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	bg, ok := idx.b.(BatchGetter)
	if !ok {
		chunks := make([]object.ChunkEntry, len(ids))
		for i, id := range ids {
			chunk, err := idx.GetChunk(ctx, id)
			if err != nil {
				return nil, err
			}
			chunks[i] = *chunk
		}
		return chunks, nil
	}

	keys := make([][]byte, len(ids))
	for i, id := range ids {
		keys[i] = keyChunk(id)
	}
	raws, err := bg.GetMulti(ctx, keys)
	if err != nil {
		return nil, err
	}
	chunks := make([]object.ChunkEntry, len(ids))
	for i, raw := range raws {
		if raw == nil {
			return nil, fmt.Errorf("index: get chunk %s: %w", ids[i], &errors.NotFoundError{})
		}
		if err := unmarshal(raw, &chunks[i]); err != nil {
			return nil, err
		}
	}
	return chunks, nil
}

func (idx *Index) DeleteChunk(ctx context.Context, id object.ChunkID) error {
	return idx.b.Delete(ctx, keyChunk(id))
}

// ── Segments ──────────────────────────────────────────────────────────────────

func (idx *Index) PutSegment(ctx context.Context, seg object.SegmentEntry) error {
	v, err := marshal(seg)
	if err != nil {
		return err
	}
	return idx.b.Put(ctx, keySeg(seg.NamespaceID, seg.SegmentID), v)
}

func (idx *Index) GetSegment(ctx context.Context, nsID string, segID object.SegmentID) (*object.SegmentEntry, error) {
	v, err := idx.b.Get(ctx, keySeg(nsID, segID))
	if err != nil {
		return nil, err
	}
	var seg object.SegmentEntry
	return &seg, unmarshal(v, &seg)
}

func (idx *Index) ListSegments(ctx context.Context, nsID string) ([]object.SegmentEntry, error) {
	var out []object.SegmentEntry
	err := idx.b.Scan(ctx, prefixSeg(nsID), func(_, v []byte) error {
		var seg object.SegmentEntry
		if err := unmarshal(v, &seg); err != nil {
			return err
		}
		out = append(out, seg)
		return nil
	})
	return out, err
}

// ── Stats ─────────────────────────────────────────────────────────────────────

func (idx *Index) GetStats(ctx context.Context, nsID string) (*object.NamespaceStats, error) {
	v, err := idx.b.Get(ctx, keyStats(nsID))
	if err != nil {
		if IsNotFound(err) {
			return &object.NamespaceStats{NamespaceID: nsID}, nil
		}
		return nil, err
	}
	var stats object.NamespaceStats
	return &stats, unmarshal(v, &stats)
}

func getStatsTx(tx Tx, nsID string) (object.NamespaceStats, error) {
	v, err := tx.Get(keyStats(nsID))
	if err != nil {
		if _, ok := err.(*errors.NotFoundError); ok {
			return object.NamespaceStats{NamespaceID: nsID}, nil
		}
		return object.NamespaceStats{}, err
	}
	var stats object.NamespaceStats
	return stats, unmarshal(v, &stats)
}

func putStatsTx(tx Tx, stats object.NamespaceStats) error {
	v, err := marshal(stats)
	if err != nil {
		return err
	}
	return tx.Put(keyStats(stats.NamespaceID), v)
}

// ── Atomic write paths ────────────────────────────────────────────────────────

// CommitPut atomically writes a new blob to the index and updates stats.
// Handles both new keys and overwrites. Must be called after the volume
// engine has durably written all chunks to disk.
// CommitPut atomically writes a new blob to the index and updates stats.
// Handles both new keys and overwrites. Must be called after the volume
// engine has durably written all chunks to disk.
//
// Chunks are content-addressed, so CommitPut deduplicates at chunk
// granularity: if a chunk with the same content hash is already indexed,
// its existing physical location is reused (RefCount incremented) and the
// newly written duplicate is returned in the result for the caller to mark
// deleted. The returned entries are exactly the physical copies that are
// now unreferenced and safe to reap.
func (idx *Index) CommitPut(
	ctx context.Context,
	ref object.RefEntry,
	blob object.BlobEntry,
	chunks []object.ChunkEntry,
) (reused []object.ChunkEntry, err error) {
	err = idx.b.Tx(ctx, func(tx Tx) error {
		// 1. Check for existing ref (overwrite detection).
		var oldBlobID object.BlobID
		var oldBlobSize int64
		var oldChunkCount int64

		oldRefRaw, err := tx.Get(keyRef(ref.NamespaceID, ref.Key))
		if err != nil && !IsNotFound(err) {
			return err
		}
		isOverwrite := err == nil
		if isOverwrite {
			var oldRef object.RefEntry
			if err := unmarshal(oldRefRaw, &oldRef); err != nil {
				return err
			}
			oldBlobID = oldRef.BlobID
			oldBlobSize = oldRef.Metadata.Size
			oldChunkCount = int64(oldRef.Metadata.ChunkCount)
		}
		sameContent := isOverwrite && oldBlobID == ref.BlobID

		// 2. Write the ref.
		refVal, err := marshal(ref)
		if err != nil {
			return err
		}
		if err := tx.Put(keyRef(ref.NamespaceID, ref.Key), refVal); err != nil {
			return err
		}

		// 3. Upsert the blob entry.
		//    If this blobID already exists (dedup hit), just increment RefCount.
		//    Otherwise write a fresh entry with RefCount=1.
		existingBlobRaw, err := tx.Get(keyBlob(blob.BlobID))
		if err != nil && !IsNotFound(err) {
			return err
		}
		if err == nil {
			// Blob already indexed — bump RefCount.
			var existing object.BlobEntry
			if err := unmarshal(existingBlobRaw, &existing); err != nil {
				return err
			}
			existing.RefCount++
			v, err := marshal(existing)
			if err != nil {
				return err
			}
			if err := tx.Put(keyBlob(blob.BlobID), v); err != nil {
				return err
			}
		} else {
			// New blob.
			blob.RefCount = 1
			v, err := marshal(blob)
			if err != nil {
				return err
			}
			if err := tx.Put(keyBlob(blob.BlobID), v); err != nil {
				return err
			}
		}

		// 4. Decrement the old blob's refcount and chunk refcounts if an
		//    overwrite replaced it with different content.
		var deadBytes, deadChunks int64
		if isOverwrite && !sameContent {
			raw, err := tx.Get(keyBlob(oldBlobID))
			if err != nil && !IsNotFound(err) {
				return err
			}
			if err == nil {
				var oldBlob object.BlobEntry
				if err := unmarshal(raw, &oldBlob); err != nil {
					return err
				}
				oldBlob.RefCount--
				v, err := marshal(oldBlob)
				if err != nil {
					return err
				}
				if err := tx.Put(keyBlob(oldBlobID), v); err != nil {
					return err
				}

				// Old chunks lose one reference; those reaching zero are dead.
				for _, cid := range oldBlob.ChunkIDs {
					deadLen, err := decrementChunkRefTx(tx, cid)
					if err != nil {
						return err
					}
					if deadLen > 0 {
						deadBytes += deadLen
						deadChunks++
					}
				}
			}
		}

		// 5. Upsert chunk entries with refcount-aware dedup. Chunks whose
		//    content hash is already indexed are reused: we keep the existing
		//    location, bump its RefCount, and report the freshly written
		//    duplicate back to the caller for reaping.
		//
		//    Reuse is limited to chunks already indexed in THIS namespace:
		//    a chunk's location is a (segment, page) inside its namespace's
		//    own segment files, and a reader can only open its own
		//    namespace's engine. Pointing a blob at a foreign namespace's
		//    location would make reads fail, so an existing record owned by
		//    another namespace is treated as absent — we register our own
		//    copy instead (the pre-dedup behavior).
		var newPhysicalBytes int64
		for _, chunk := range chunks {
			existingRaw, err := tx.Get(keyChunk(chunk.ChunkID))
			if err == nil {
				var existing object.ChunkEntry
				if err := unmarshal(existingRaw, &existing); err != nil {
					return err
				}
				if existing.NamespaceID == ref.NamespaceID {
					existing.RefCount++
					v, err := marshal(existing)
					if err != nil {
						return err
					}
					if err := tx.Put(keyChunk(chunk.ChunkID), v); err != nil {
						return err
					}
					reused = append(reused, chunk)
					continue
				}
				// Fall through: foreign namespace's record — keep our copy.
			} else if !IsNotFound(err) {
				return err
			}
			chunk.RefCount = 1
			v, err := marshal(chunk)
			if err != nil {
				return err
			}
			if err := tx.Put(keyChunk(chunk.ChunkID), v); err != nil {
				return err
			}
			newPhysicalBytes += chunk.Length
		}

		// 6. Update stats.
		stats, err := getStatsTx(tx, ref.NamespaceID)
		if err != nil {
			return err
		}
		if !isOverwrite {
			stats.BlobCount++
			stats.BytesStored += blob.TotalSize
			stats.ChunkCount += int64(len(chunks))
		} else if !sameContent {
			// Different content replaces old — adjust logical accounting;
			// old bytes that fully lost their references were already
			// counted as dead in step 4.
			stats.BytesStored += blob.TotalSize - oldBlobSize
			stats.ChunkCount += int64(len(chunks)) - oldChunkCount
		}
		stats.BytesPhysical += newPhysicalBytes
		stats.DeadBytes += deadBytes
		stats.DeadChunks += deadChunks
		return putStatsTx(tx, stats)
	})
	if err != nil {
		return nil, err
	}
	return reused, nil
}

// decrementChunkRefTx removes one reference from a chunk's index record.
// A chunk whose RefCount reaches zero is left in place with RefCount 0 —
// Compact's phase 1 sweep reaps it. Requires a live transaction. Returns
// the chunk's payload length when the reference count drops to zero
// (report it as newly dead bytes), and 0 otherwise.
func decrementChunkRefTx(tx Tx, cid object.ChunkID) (int64, error) {
	raw, err := tx.Get(keyChunk(cid))
	if err != nil && !IsNotFound(err) {
		return 0, err
	}
	if err != nil {
		return 0, nil // already gone; nothing to do
	}
	var chunk object.ChunkEntry
	if err := unmarshal(raw, &chunk); err != nil {
		return 0, err
	}
	deadLen := int64(0)
	if chunk.RefCount > 0 {
		chunk.RefCount--
		if chunk.RefCount == 0 {
			deadLen = chunk.Length
		}
	}
	v, err := marshal(chunk)
	if err != nil {
		return 0, err
	}
	return deadLen, tx.Put(keyChunk(cid), v)
}

// CommitDelete atomically removes a ref and decrements the blob's RefCount.
// Each referenced chunk also loses one reference; chunks reaching zero are
// reported as dead bytes in the stats. Physical bytes are not reclaimed
// until Compact runs.
func (idx *Index) CommitDelete(ctx context.Context, nsID, key string) error {
	return idx.b.Tx(ctx, func(tx Tx) error {
		raw, err := tx.Get(keyRef(nsID, key))
		if err != nil {
			return err
		}
		var ref object.RefEntry
		if err := unmarshal(raw, &ref); err != nil {
			return err
		}

		if err := tx.Delete(keyRef(nsID, key)); err != nil {
			return err
		}

		// Decrement blob RefCount.
		var blob *object.BlobEntry
		blobRaw, err := tx.Get(keyBlob(ref.BlobID))
		if err != nil && !IsNotFound(err) {
			return err
		}
		if err == nil {
			var b object.BlobEntry
			if err := unmarshal(blobRaw, &b); err != nil {
				return err
			}
			b.RefCount--
			v, err := marshal(b)
			if err != nil {
				return err
			}
			if err := tx.Put(keyBlob(ref.BlobID), v); err != nil {
				return err
			}
			blob = &b
		}

		// Update stats.
		stats, err := getStatsTx(tx, nsID)
		if err != nil {
			return err
		}
		stats.BlobCount--
		stats.BytesStored -= ref.Metadata.Size
		stats.ChunkCount -= int64(ref.Metadata.ChunkCount)
		if blob != nil {
			// Decrement each chunk's RefCount; those hitting zero are dead.
			for _, cid := range blob.ChunkIDs {
				deadLen, err := decrementChunkRefTx(tx, cid)
				if err != nil {
					return err
				}
				if deadLen > 0 {
					stats.DeadBytes += deadLen
					stats.DeadChunks++
				}
			}
		}
		return putStatsTx(tx, stats)
	})
}

// CommitRenameRef atomically renames a ref from oldKey to newKey.
// Returns NotFoundError if oldKey does not exist.
// Returns an error if newKey already exists.
func (idx *Index) CommitRenameRef(ctx context.Context, nsID, oldKey, newKey string) error {
	return idx.b.Tx(ctx, func(tx Tx) error {
		oldRaw, err := tx.Get(keyRef(nsID, oldKey))
		if err != nil {
			return err
		}
		var ref object.RefEntry
		if err := unmarshal(oldRaw, &ref); err != nil {
			return err
		}

		_, err = tx.Get(keyRef(nsID, newKey))
		if err == nil {
			return fmt.Errorf("index: key %q already exists in namespace %q", newKey, nsID)
		}
		if !IsNotFound(err) {
			return err
		}

		ref.Key = newKey
		v, err := marshal(ref)
		if err != nil {
			return err
		}
		if err := tx.Put(keyRef(nsID, newKey), v); err != nil {
			return err
		}
		return tx.Delete(keyRef(nsID, oldKey))
	})
}

// CommitUpdateRefMetadata atomically updates the custom metadata and
// updated-at timestamp of an existing ref entry. Returns NotFoundError
// if the key does not exist.
func (idx *Index) CommitUpdateRefMetadata(ctx context.Context, nsID, key string, custom map[string]string, updatedAt time.Time) error {
	return idx.b.Tx(ctx, func(tx Tx) error {
		raw, err := tx.Get(keyRef(nsID, key))
		if err != nil {
			return err
		}
		var ref object.RefEntry
		if err := unmarshal(raw, &ref); err != nil {
			return err
		}
		ref.Metadata.Custom = custom
		ref.Metadata.UpdatedAt = updatedAt
		v, err := marshal(ref)
		if err != nil {
			return err
		}
		return tx.Put(keyRef(nsID, key), v)
	})
}

// ── In-memory backend ─────────────────────────────────────────────────────────

// MemoryBackend is an in-memory Backend implementation for testing.
// Safe for concurrent use. Data is lost when the process exits.
type MemoryBackend struct {
	mu     sync.RWMutex
	data   map[string][]byte
	closed bool
}

// NewMemoryBackend returns a ready-to-use in-memory backend.
func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{data: make(map[string][]byte)}
}

func (m *MemoryBackend) Put(_ context.Context, key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return &errors.ClosedError{}
	}
	dst := make([]byte, len(value))
	copy(dst, value)
	m.data[string(key)] = dst
	return nil
}

func (m *MemoryBackend) Get(_ context.Context, key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, &errors.ClosedError{}
	}
	v, ok := m.data[string(key)]
	if !ok {
		return nil, &errors.NotFoundError{}
	}
	dst := make([]byte, len(v))
	copy(dst, v)
	return dst, nil
}

func (m *MemoryBackend) Delete(_ context.Context, key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return &errors.ClosedError{}
	}
	delete(m.data, string(key))
	return nil
}

// GetMulti implements BatchGetter: a single RLock covers every key
// instead of one RLock per key, matching the guarantee MemoryBackend's
// individual Get already makes (a defensive copy per returned value).
func (m *MemoryBackend) GetMulti(_ context.Context, keys [][]byte) ([][]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, &errors.ClosedError{}
	}
	out := make([][]byte, len(keys))
	for i, k := range keys {
		v, ok := m.data[string(k)]
		if !ok {
			continue // leave out[i] nil, per BatchGetter's contract
		}
		dst := make([]byte, len(v))
		copy(dst, v)
		out[i] = dst
	}
	return out, nil
}

func (m *MemoryBackend) Scan(_ context.Context, prefix []byte, fn func(k, v []byte) error) error {
	m.mu.RLock()
	var keys []string
	for k := range m.data {
		if bytes.HasPrefix([]byte(k), prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	type pair struct{ k, v []byte }
	pairs := make([]pair, 0, len(keys))
	for _, k := range keys {
		v := m.data[k]
		kc := []byte(k)
		vc := make([]byte, len(v))
		copy(vc, v)
		pairs = append(pairs, pair{kc, vc})
	}
	m.mu.RUnlock()

	for _, p := range pairs {
		if err := fn(p.k, p.v); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemoryBackend) Tx(_ context.Context, fn func(Tx) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return &errors.ClosedError{}
	}
	tx := &memTx{
		parent:  m.data,
		writes:  make(map[string][]byte),
		deletes: make(map[string]struct{}),
	}
	if err := fn(tx); err != nil {
		return err
	}
	maps.Copy(m.data, tx.writes)
	for k := range tx.deletes {
		delete(m.data, k)
	}
	return nil
}

func (m *MemoryBackend) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.data = nil
	return nil
}

type memTx struct {
	parent  map[string][]byte
	writes  map[string][]byte
	deletes map[string]struct{}
}

func (t *memTx) Put(key, value []byte) error {
	k := string(key)
	dst := make([]byte, len(value))
	copy(dst, value)
	t.writes[k] = dst
	delete(t.deletes, k)
	return nil
}

func (t *memTx) Get(key []byte) ([]byte, error) {
	k := string(key)
	if _, deleted := t.deletes[k]; deleted {
		return nil, &errors.NotFoundError{}
	}
	if v, ok := t.writes[k]; ok {
		dst := make([]byte, len(v))
		copy(dst, v)
		return dst, nil
	}
	v, ok := t.parent[k]
	if !ok {
		return nil, &errors.NotFoundError{}
	}
	dst := make([]byte, len(v))
	copy(dst, v)
	return dst, nil
}

func (t *memTx) Delete(key []byte) error {
	k := string(key)
	delete(t.writes, k)
	t.deletes[k] = struct{}{}
	return nil
}
