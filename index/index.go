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

// ── Key constructors ──────────────────────────────────────────────────────────

func keyNS(nsID string) []byte          { return []byte("ns:" + nsID) }
func prefixNS() []byte                  { return []byte("ns:") }
func keyRef(nsID, key string) []byte    { return []byte("ref:" + nsID + ":" + key) }
func prefixRef(nsID string) []byte      { return []byte("ref:" + nsID + ":") }
func keyBlob(id object.BlobID) []byte   { return []byte("blob:" + string(id)) }
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
func (idx *Index) CommitPut(
	ctx context.Context,
	ref object.RefEntry,
	blob object.BlobEntry,
	chunks []object.ChunkEntry,
) error {
	return idx.b.Tx(ctx, func(tx Tx) error {
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

		// 4. Decrement old blob RefCount if overwriting with different content.
		if isOverwrite && oldBlobID != ref.BlobID {
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
			}
		}

		// 5. Write chunk entries (idempotent — dedup means chunk may pre-exist).
		for _, chunk := range chunks {
			chunkVal, err := marshal(chunk)
			if err != nil {
				return err
			}
			if err := tx.Put(keyChunk(chunk.ChunkID), chunkVal); err != nil {
				return err
			}
		}

		// 6. Update stats.
		stats, err := getStatsTx(tx, ref.NamespaceID)
		if err != nil {
			return err
		}
		if !isOverwrite {
			stats.BlobCount++
		} else if oldBlobID != ref.BlobID {
			// Different content replaces old — old bytes become dead.
			stats.DeadBytes += oldBlobSize
			stats.DeadChunks += oldChunkCount
		}
		stats.BytesStored += blob.TotalSize
		stats.ChunkCount += int64(len(chunks))
		return putStatsTx(tx, stats)
	})
}

// CommitDelete atomically removes a ref and decrements the blob's RefCount.
// Physical bytes are not reclaimed until Compact runs.
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
		blobRaw, err := tx.Get(keyBlob(ref.BlobID))
		if err != nil && !IsNotFound(err) {
			return err
		}
		if err == nil {
			var blob object.BlobEntry
			if err := unmarshal(blobRaw, &blob); err != nil {
				return err
			}
			blob.RefCount--
			v, err := marshal(blob)
			if err != nil {
				return err
			}
			if err := tx.Put(keyBlob(ref.BlobID), v); err != nil {
				return err
			}
		}

		// Update stats.
		stats, err := getStatsTx(tx, nsID)
		if err != nil {
			return err
		}
		stats.BlobCount--
		stats.DeadBytes += ref.Metadata.Size
		stats.DeadChunks += int64(ref.Metadata.ChunkCount)
		return putStatsTx(tx, stats)
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
