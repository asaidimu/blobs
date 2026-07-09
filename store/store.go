// Package store is the public API surface of the blobstore.
// It orchestrates the volume engine and index to implement the full lifecycle
// of blobs: write, read, delete, list, stats, verify, and index rebuild.
//
// Callers interact with two types:
//
//   - Store: opened once, manages namespace lifecycle and aggregate stats.
//   - NamespaceHandle: scoped handle obtained from Store.Namespace(); all CRUD
//     lives here. Cheap to create — no I/O on construction.
//
// Typical usage:
//
//	s, err := store.Open(store.Config{
//	    DataDir: "/var/lib/blobstore",
//	    Index:   index.NewMemoryBackend(), // swap for bbolt, badger, etc.
//	})
//
//	ns := s.Namespace("default")
//	info, err := ns.Put(ctx, "my-file.jpg", r, store.PutOptions{ContentType: "image/jpeg"})
//	rc, err := ns.Get(ctx, "my-file.jpg")
//	err = ns.Delete(ctx, "my-file.jpg")
package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	bserrors "github.com/asaidimu/blobs/errors"
	"github.com/asaidimu/blobs/index"
	"github.com/asaidimu/blobs/object"
	"github.com/asaidimu/blobs/volume"
)

// ── Config ────────────────────────────────────────────────────────────────────

// Config is the top-level configuration for opening a Store.
type Config struct {
	// DataDir is the root directory for all segment and WAL files.
	// The store creates one subdirectory per namespace beneath this root.
	// This is the only filesystem assumption the store makes.
	DataDir string

	// Index is the pluggable index backend. The caller opens and owns it;
	// Store.Close will call Index.Close().
	// Use index.NewMemoryBackend() for tests.
	Index index.Backend

	// Volume tuning — zero values use package defaults (16 KB page, 4 MB
	// chunk, 512 MB segment).
	PageSize       int
	ChunkSize      int64
	MaxSegmentSize int64
}

// ── Options for individual operations ────────────────────────────────────────

// PutOptions carries optional parameters for a Put call.
type PutOptions struct {
	// ContentType is the MIME type. Defaults to "application/octet-stream".
	ContentType string

	// Custom is an arbitrary map of caller-defined metadata.
	// Keys must not start with "_bs_" (reserved for internal use).
	Custom map[string]string
}

// ListOptions controls the result set of a List call.
type ListOptions struct {
	// KeyPrefix restricts results to keys with this prefix.
	KeyPrefix string

	// After is an exclusive lower bound for pagination.
	// Results start after this key (lexicographic order).
	After string

	// Limit caps the number of results. 0 means no limit.
	Limit int
}

// ── Store ─────────────────────────────────────────────────────────────────────

// Store is the top-level handle. Safe for concurrent use.
type Store struct {
	mu      sync.RWMutex
	cfg     Config
	idx     *index.Index
	engines map[string]*volume.Engine // keyed by namespace ID
	closed  bool

	// quotaMu provides per-namespace mutual exclusion for the quota-check +
	// CommitPut window (VULN 8 fix). Without this, N concurrent Puts in the
	// same namespace could all read stats=0, all pass the quota check, all
	// write to disk, then all commit — exceeding the quota by factor N.
	// sync.Map stores *sync.Mutex keyed by namespace ID.
	// Using a per-namespace mutex (not the global store mutex) avoids
	// blocking Puts across different namespaces.
	quotaMu sync.Map
}

// Open opens an existing store or creates a new one.
// A "default" namespace is created automatically if none exist.
func Open(cfg Config) (*Store, error) {
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("store: Config.DataDir must not be empty")
	}
	if cfg.Index == nil {
		return nil, fmt.Errorf("store: Config.Index must be provided")
	}

	s := &Store{
		cfg:     cfg,
		idx:     index.New(cfg.Index),
		engines: make(map[string]*volume.Engine),
	}

	ctx := context.Background()

	// Ensure the default namespace exists.
	if _, err := s.idx.GetNamespace(ctx, object.DefaultNamespaceID); err != nil {
		if !index.IsNotFound(err) {
			return nil, fmt.Errorf("store: check default namespace: %w", err)
		}
		if err := s.idx.PutNamespace(ctx, object.Namespace{
			ID:          object.DefaultNamespaceID,
			DisplayName: "Default",
			CreatedAt:   time.Now().UTC(),
		}); err != nil {
			return nil, fmt.Errorf("store: create default namespace: %w", err)
		}
	}

	// Open volume engines for all known namespaces.
	namespaces, err := s.idx.ListNamespaces(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: list namespaces on open: %w", err)
	}
	for _, ns := range namespaces {
		if err := s.openEngine(ns.ID); err != nil {
			return nil, fmt.Errorf("store: open engine for %q: %w", ns.ID, err)
		}
	}

	return s, nil
}

// Close flushes and closes all engines and the index backend.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return &bserrors.ClosedError{}
	}
	s.closed = true

	var errs []error
	for id, eng := range s.engines {
		if err := eng.Close(); err != nil {
			errs = append(errs, fmt.Errorf("engine %q: %w", id, err))
		}
	}
	if err := s.idx.Close(); err != nil {
		errs = append(errs, fmt.Errorf("index: %w", err))
	}
	if len(errs) > 0 {
		return fmt.Errorf("store: close errors: %v", errs)
	}
	return nil
}

// ── Namespace management ──────────────────────────────────────────────────────

// CreateNamespace creates a new isolated partition within the store.
// Returns *errors.AlreadyExistsError if nsID is already in use.
func (s *Store) CreateNamespace(ctx context.Context, ns object.Namespace) error {
	if err := object.ValidateNamespaceID(ns.ID); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.idx.GetNamespace(ctx, ns.ID); err == nil {
		return &bserrors.AlreadyExistsError{NamespaceID: ns.ID}
	} else if !index.IsNotFound(err) {
		return fmt.Errorf("store: check namespace: %w", err)
	}

	ns.CreatedAt = time.Now().UTC()
	if err := s.idx.PutNamespace(ctx, ns); err != nil {
		return fmt.Errorf("store: persist namespace: %w", err)
	}
	return s.openEngine(ns.ID)
}

// GetNamespace returns the metadata for a namespace.
func (s *Store) GetNamespace(ctx context.Context, nsID string) (*object.Namespace, error) {
	return s.idx.GetNamespace(ctx, nsID)
}

// ListNamespaces returns all namespaces in the store.
func (s *Store) ListNamespaces(ctx context.Context) ([]object.Namespace, error) {
	return s.idx.ListNamespaces(ctx)
}

// DeleteNamespace removes a namespace and all its refs.
// The default namespace cannot be deleted.
// Physical bytes are not reclaimed until GC/compaction runs on the engine.
func (s *Store) DeleteNamespace(ctx context.Context, nsID string) error {
	if nsID == object.DefaultNamespaceID {
		return fmt.Errorf("store: cannot delete the default namespace")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	eng, ok := s.engines[nsID]
	if !ok {
		return &bserrors.NotFoundError{NamespaceID: nsID}
	}

	// Close and discard the engine.
	if err := eng.Close(); err != nil {
		return fmt.Errorf("store: close engine for %q: %w", nsID, err)
	}
	delete(s.engines, nsID)

	// Remove all refs from the index.
	refs, err := s.idx.ListRefs(ctx, nsID, "")
	if err != nil {
		return fmt.Errorf("store: list refs for deletion: %w", err)
	}
	for _, ref := range refs {
		if err := s.idx.CommitDelete(ctx, nsID, ref.Key); err != nil && !index.IsNotFound(err) {
			return fmt.Errorf("store: delete ref %q: %w", ref.Key, err)
		}
	}

	return s.idx.DeleteNamespace(ctx, nsID)
}

// Namespace returns a scoped handle for operations within nsID.
// This is a cheap call — it performs no I/O.
func (s *Store) Namespace(nsID string) *NamespaceHandle {
	return &NamespaceHandle{store: s, nsID: nsID}
}

// Stats returns aggregate metrics across all namespaces.
func (s *Store) Stats(ctx context.Context) (*object.StoreStats, error) {
	namespaces, err := s.idx.ListNamespaces(ctx)
	if err != nil {
		return nil, err
	}

	out := &object.StoreStats{
		NamespaceCount: int64(len(namespaces)),
		ComputedAt:     time.Now().UTC(),
	}
	for _, ns := range namespaces {
		nsStats, err := s.idx.GetStats(ctx, ns.ID)
		if err != nil {
			return nil, err
		}
		out.PerNamespace = append(out.PerNamespace, *nsStats)
		out.TotalBlobCount += nsStats.BlobCount
		out.TotalBytesStored += nsStats.BytesStored
		out.TotalBytesPhysical += nsStats.BytesPhysical
		out.TotalDeadBytes += nsStats.DeadBytes
		out.SegmentCount += nsStats.SegmentCount
	}
	if out.TotalBytesPhysical > 0 {
		out.DeduplicationRatio = float64(out.TotalBytesStored) / float64(out.TotalBytesPhysical)
	}
	return out, nil
}

// ── Internal helpers ──────────────────────────────────────────────────────────

func (s *Store) openEngine(nsID string) error {
	eng, err := volume.Open(s.cfg.DataDir, nsID, volume.Options{
		PageSize:       s.cfg.PageSize,
		ChunkSize:      s.cfg.ChunkSize,
		MaxSegmentSize: s.cfg.MaxSegmentSize,
	})
	if err != nil {
		return err
	}
	s.engines[nsID] = eng
	return nil
}

// nsMutex returns the per-namespace mutex used to serialise quota checks.
// Creates one on first use. The mutex is never deleted — namespace deletion
// is rare and a leaked empty mutex costs 8 bytes.
func (s *Store) nsMutex(nsID string) *sync.Mutex {
	v, _ := s.quotaMu.LoadOrStore(nsID, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func (s *Store) engine(nsID string) (*volume.Engine, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, &bserrors.ClosedError{}
	}
	eng, ok := s.engines[nsID]
	if !ok {
		return nil, &bserrors.NotFoundError{NamespaceID: nsID}
	}
	return eng, nil
}

// ── NamespaceHandle ───────────────────────────────────────────────────────────

// NamespaceHandle is the primary interaction surface for callers.
// All CRUD operations are scoped to a single namespace.
// Safe for concurrent use.
type NamespaceHandle struct {
	store *Store
	nsID  string
}

// Put writes the content of r under key.
// If key already exists, its ref is updated to point at the new blob.
// The old blob's ref count is decremented and it becomes GC-eligible.
//
// Durability guarantee: when Put returns without error, the blob is fsync'd
// to disk and the index is atomically updated. A crash after Put returns
// will not lose the write.
func (h *NamespaceHandle) Put(ctx context.Context, key string, r io.Reader, opts PutOptions) (*object.BlobInfo, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	if opts.ContentType == "" {
		opts.ContentType = "application/octet-stream"
	}

	eng, err := h.store.engine(h.nsID)
	if err != nil {
		return nil, err
	}

	// VULN 8 fix: acquire the per-namespace quota mutex before reading stats
	// and hold it through CommitPut. This serialises the check-then-commit
	// window within a namespace so N concurrent Puts cannot all read stats=0,
	// all pass the quota check, and all commit — exceeding quota by factor N.
	//
	// The disk write (WriteBlob) happens outside this mutex to avoid holding
	// it during potentially slow I/O. If WriteBlob succeeds but the quota
	// check fails, the orphaned chunks are reclaimed by the next Compact.
	result, err := eng.WriteBlob(r)
	if err != nil {
		return nil, fmt.Errorf("store: write blob: %w", err)
	}

	mu := h.store.nsMutex(h.nsID)
	mu.Lock()
	defer mu.Unlock()

	if err := h.checkQuota(ctx, result.TotalSize); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	meta := object.Metadata{
		ContentType: opts.ContentType,
		Size:        result.TotalSize,
		BlobID:      result.BlobID,
		ChunkCount:  len(result.Chunks),
		CreatedAt:   now,
		UpdatedAt:   now,
		Custom:      opts.Custom,
	}

	ref := object.RefEntry{
		NamespaceID: h.nsID,
		Key:         key,
		BlobID:      result.BlobID,
		Metadata:    meta,
	}

	blob := object.BlobEntry{
		BlobID:    result.BlobID,
		TotalSize: result.TotalSize,
		ChunkIDs:  make([]object.ChunkID, len(result.Chunks)),
		CreatedAt: now,
	}
	for i, c := range result.Chunks {
		blob.ChunkIDs[i] = c.ChunkID
	}

	// Phase 2 — atomically commit ref + blob + chunks to index.
	if err := h.store.idx.CommitPut(ctx, ref, blob, result.Chunks); err != nil {
		return nil, fmt.Errorf("store: commit to index: %w", err)
	}

	return &object.BlobInfo{Key: key, NamespaceID: h.nsID, Metadata: meta}, nil
}

// Get returns a streaming reader for the blob stored under key.
// The caller must close the returned ReadCloser when done.
// Returns *errors.NotFoundError if key does not exist.
func (h *NamespaceHandle) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	ref, err := h.store.idx.GetRef(ctx, h.nsID, key)
	if err != nil {
		return nil, err
	}

	blob, err := h.store.idx.GetBlob(ctx, ref.BlobID)
	if err != nil {
		return nil, fmt.Errorf("store: get blob manifest: %w", err)
	}

	eng, err := h.store.engine(h.nsID)
	if err != nil {
		return nil, err
	}

	// Resolve chunk locations eagerly so the reader is self-contained.
	locations := make([]object.ChunkEntry, len(blob.ChunkIDs))
	for i, cid := range blob.ChunkIDs {
		loc, err := h.store.idx.GetChunk(ctx, cid)
		if err != nil {
			return nil, fmt.Errorf("store: get chunk location for %s: %w", cid, err)
		}
		locations[i] = *loc
	}

	return newChunkReader(eng, locations), nil
}

// Head returns metadata for key without reading any blob data.
// Returns *errors.NotFoundError if key does not exist.
func (h *NamespaceHandle) Head(ctx context.Context, key string) (*object.BlobInfo, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	ref, err := h.store.idx.GetRef(ctx, h.nsID, key)
	if err != nil {
		return nil, err
	}
	return &object.BlobInfo{
		Key:         ref.Key,
		NamespaceID: ref.NamespaceID,
		Metadata:    ref.Metadata,
	}, nil
}

// Delete removes the ref for key, making its blob GC-eligible.
// Physical bytes are not reclaimed until Compact is called.
// Returns nil if the key does not exist (idempotent).
func (h *NamespaceHandle) Delete(ctx context.Context, key string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	err := h.store.idx.CommitDelete(ctx, h.nsID, key)
	if err != nil && index.IsNotFound(err) {
		return nil // idempotent
	}
	return err
}

// List returns BlobInfo for blobs in this namespace matching opts.
// Results are in lexicographic key order.
func (h *NamespaceHandle) List(ctx context.Context, opts ListOptions) ([]object.BlobInfo, error) {
	refs, err := h.store.idx.ListRefs(ctx, h.nsID, opts.KeyPrefix)
	if err != nil {
		return nil, err
	}

	var out []object.BlobInfo
	pastAfter := opts.After == ""
	for _, ref := range refs {
		if !pastAfter {
			if ref.Key == opts.After {
				pastAfter = true
			}
			continue
		}
		out = append(out, object.BlobInfo{
			Key:         ref.Key,
			NamespaceID: ref.NamespaceID,
			Metadata:    ref.Metadata,
		})
		if opts.Limit > 0 && len(out) >= opts.Limit {
			break
		}
	}
	return out, nil
}

// Stats returns live resource usage for this namespace.
func (h *NamespaceHandle) Stats(ctx context.Context) (*object.NamespaceStats, error) {
	return h.store.idx.GetStats(ctx, h.nsID)
}

// Verify reads and CRC-checks every chunk for every blob in this namespace.
// Returns the first integrity error encountered, or nil.
// This is an expensive operation — run it offline or on a schedule.
func (h *NamespaceHandle) Verify(ctx context.Context) error {
	eng, err := h.store.engine(h.nsID)
	if err != nil {
		return err
	}

	refs, err := h.store.idx.ListRefs(ctx, h.nsID, "")
	if err != nil {
		return err
	}

	for _, ref := range refs {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		blob, err := h.store.idx.GetBlob(ctx, ref.BlobID)
		if err != nil {
			return fmt.Errorf("store: verify: missing manifest for blob %s (key %q): %w",
				ref.BlobID, ref.Key, err)
		}

		for _, cid := range blob.ChunkIDs {
			loc, err := h.store.idx.GetChunk(ctx, cid)
			if err != nil {
				return fmt.Errorf("store: verify: missing location for chunk %s: %w", cid, err)
			}
			// ReadChunk performs CRC verification internally.
			if _, err := eng.ReadChunk(*loc); err != nil {
				return err
			}
		}
	}
	return nil
}

// RebuildIndex reconstructs chunk entries in the index by scanning all segment
// files on disk. Should be called after Open when IsDirty() is true.
// It does not reconstruct refs — those can only come from the WAL or a backup.
func (h *NamespaceHandle) RebuildIndex(ctx context.Context) error {
	eng, err := h.store.engine(h.nsID)
	if err != nil {
		return err
	}
	return eng.ScanSegments(func(entry object.ChunkEntry, _ volume.PageHeader) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return h.store.idx.PutChunk(ctx, entry)
	})
}

// Compact marks orphaned chunks deleted and returns stats on reclaimed space.
// "Orphaned" means the chunk's blob has RefCount == 0.
// This is a multi-phase operation:
//  1. Walk all blob entries; collect BlobIDs with RefCount == 0.
//  2. For each such blob, mark all its chunks deleted in the segment.
//  3. Remove the blob and chunk index entries.
//  4. Update stats.
//
// Physical segment rewriting (to reclaim actual disk space) is not yet
// implemented — that is Phase 2 of compaction (segment merge/rewrite).
func (h *NamespaceHandle) Compact(ctx context.Context) (CompactResult, error) {
	eng, err := h.store.engine(h.nsID)
	if err != nil {
		return CompactResult{}, err
	}

	// Collect all chunk entries that belong to blobs with RefCount == 0.
	// We scan segment files (the ground truth) and cross-reference the index.
	var (
		deadChunks []object.ChunkEntry
		deadBlobs  []object.BlobID
		bytesFreed int64
		chunksFreed int64
	)

	if err := eng.ScanSegments(func(entry object.ChunkEntry, hdr volume.PageHeader) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if hdr.Flags&0x01 != 0 { // already deleted
			return nil
		}

		// Look up the blob for this chunk.
		blob, err := h.store.idx.GetBlob(ctx, entry.BlobID)
		if err != nil {
			// Blob not in index — orphaned chunk. Mark for deletion.
			deadChunks = append(deadChunks, entry)
			bytesFreed += entry.Length
			chunksFreed++
			return nil
		}
		if blob.RefCount == 0 {
			deadChunks = append(deadChunks, entry)
			bytesFreed += entry.Length
			chunksFreed++
			if len(deadBlobs) == 0 || deadBlobs[len(deadBlobs)-1] != blob.BlobID {
				deadBlobs = append(deadBlobs, blob.BlobID)
			}
		}
		return nil
	}); err != nil {
		return CompactResult{}, fmt.Errorf("store: compact scan: %w", err)
	}

	// Mark deleted in the volume.
	for _, chunk := range deadChunks {
		if err := eng.MarkDeleted(chunk); err != nil {
			return CompactResult{}, fmt.Errorf("store: mark deleted: %w", err)
		}
	}

	// Remove from index.
	for _, blobID := range deadBlobs {
		blob, err := h.store.idx.GetBlob(ctx, blobID)
		if err != nil {
			continue
		}
		for _, cid := range blob.ChunkIDs {
			_ = h.store.idx.DeleteChunk(ctx, cid)
		}
		_ = h.store.idx.DeleteBlob(ctx, blobID)
	}

	return CompactResult{
		BlobsRemoved:  int64(len(deadBlobs)),
		ChunksRemoved: chunksFreed,
		BytesFreed:    bytesFreed,
	}, nil
}

// CompactResult summarises what Compact reclaimed.
type CompactResult struct {
	BlobsRemoved  int64
	ChunksRemoved int64
	BytesFreed    int64
}

// ── Quota check ───────────────────────────────────────────────────────────────

// validateKey returns an error if key is not a valid blob key.
// Called by all four read/write methods (Put, Get, Head, Delete) so the
// validation surface is uniform. Previously only Put validated the key,
// leaving Get/Head/Delete able to construct "ref:<nsID>:" (empty suffix)
// which is a valid index key that could alias unexpected entries.
func validateKey(key string) error {
	if key == "" {
		return &bserrors.InvalidKeyError{Key: key, Reason: "key must not be empty"}
	}
	return nil
}

func (h *NamespaceHandle) checkQuota(ctx context.Context, incomingSize int64) error {
	ns, err := h.store.idx.GetNamespace(ctx, h.nsID)
	if err != nil {
		return err
	}
	if ns.Quota == nil {
		return nil
	}

	stats, err := h.store.idx.GetStats(ctx, h.nsID)
	if err != nil {
		return err
	}

	q := ns.Quota
	if q.MaxBlobCount > 0 && stats.BlobCount >= q.MaxBlobCount {
		return &bserrors.QuotaExceededError{
			NamespaceID: h.nsID,
			Dimension:   "blob_count",
			Limit:       q.MaxBlobCount,
			Requested:   stats.BlobCount + 1,
		}
	}
	if q.MaxBytes > 0 && stats.BytesStored+incomingSize > q.MaxBytes {
		return &bserrors.QuotaExceededError{
			NamespaceID: h.nsID,
			Dimension:   "bytes",
			Limit:       q.MaxBytes,
			Requested:   stats.BytesStored + incomingSize,
		}
	}
	return nil
}

// ── chunkReader — streaming reassembly ───────────────────────────────────────

// chunkReader implements io.ReadCloser by reading chunks from the volume engine
// in order, buffering one chunk at a time.
type chunkReader struct {
	engine    *volume.Engine
	locations []object.ChunkEntry
	idx       int    // next chunk to load
	buf       []byte // current chunk payload
	pos       int    // read position within buf
}

func newChunkReader(eng *volume.Engine, locations []object.ChunkEntry) *chunkReader {
	return &chunkReader{engine: eng, locations: locations}
}

func (r *chunkReader) Read(p []byte) (int, error) {
	for {
		// Serve from buffer if available.
		if r.pos < len(r.buf) {
			n := copy(p, r.buf[r.pos:])
			r.pos += n
			return n, nil
		}

		// Load next chunk.
		if r.idx >= len(r.locations) {
			return 0, io.EOF
		}
		data, err := r.engine.ReadChunk(r.locations[r.idx])
		if err != nil {
			return 0, err
		}
		r.buf = data
		r.pos = 0
		r.idx++
	}
}

func (r *chunkReader) Close() error {
	r.buf = nil
	r.idx = len(r.locations)
	return nil
}

// ── Convenience helpers ───────────────────────────────────────────────────────

// ReadAll reads an entire ReadCloser into a byte slice.
// Intended for tests and small blobs. Do not use for large blobs.
func ReadAll(rc io.ReadCloser) ([]byte, error) {
	defer rc.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, rc); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
