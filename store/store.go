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

	"github.com/gabriel-vasile/mimetype"

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
	volOpts := volume.Options{
		PageSize:       cfg.PageSize,
		ChunkSize:      cfg.ChunkSize,
		MaxSegmentSize: cfg.MaxSegmentSize,
	}
	if err := volOpts.Validate(); err != nil {
		return nil, fmt.Errorf("store: %w", err)
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
//
// Close waits for every in-flight operation to finish before tearing
// anything down: it does this by taking Store's write lock, and every
// operation that touches the index or a volume engine holds the
// corresponding read lock for its own full duration (see beginOp /
// beginNSOp), not just for the moment it looks up an engine pointer. Since
// a pending Lock() call blocks new RLock() callers and waits for existing
// ones to release, this makes Close a true drain point: no operation that
// was already running when Close was called can still be touching an
// engine or the index by the time Close proceeds to shut them down, and no
// new operation can start once Close has begun.
//
// One case Close cannot protect against: NamespaceHandle.Get returns an
// io.ReadCloser whose actual disk reads happen after Get itself has
// returned, so the read guard it acquired is held until the caller closes
// that reader — not until Get returns. A caller that obtains a reader and
// never closes it will cause Close to block indefinitely, exactly as
// leaking any other closable handle (an unclosed *os.File, an unclosed
// http.Response.Body) would jam an equivalent drain/shutdown sequence
// elsewhere. Always close readers returned by Get.
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
	if err := s.beginOp(); err != nil {
		return nil, err
	}
	defer s.endOp()
	return s.idx.GetNamespace(ctx, nsID)
}

// ListNamespaces returns all namespaces in the store.
func (s *Store) ListNamespaces(ctx context.Context) ([]object.Namespace, error) {
	if err := s.beginOp(); err != nil {
		return nil, err
	}
	defer s.endOp()
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
	if err := s.beginOp(); err != nil {
		return nil, err
	}
	defer s.endOp()

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

// beginOp acquires a read guard on the store for an operation that
// touches the index but not a specific volume engine. It returns
// *errors.ClosedError if the store is already closed. On success, the
// caller MUST call endOp exactly once when the operation is entirely
// done — for an operation that hands back a long-lived handle (only
// NamespaceHandle.Get does today), that means when the handle itself is
// closed, not when the method that created it returns. See Close's doc
// comment for why this is the store's drain mechanism.
func (s *Store) beginOp() error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return &bserrors.ClosedError{}
	}
	return nil
}

// endOp releases the guard acquired by beginOp or beginNSOp.
func (s *Store) endOp() {
	s.mu.RUnlock()
}

// beginNSOp is beginOp plus a namespace engine lookup, for operations that
// touch a volume engine directly (Put, Get, Verify, RebuildIndex,
// Compact). On success the read guard is held and the caller must call
// endOp when done; on error no guard is held and endOp must not be
// called.
func (s *Store) beginNSOp(nsID string) (*volume.Engine, error) {
	if err := s.beginOp(); err != nil {
		return nil, err
	}
	eng, ok := s.engines[nsID]
	if !ok {
		s.endOp()
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

	eng, err := h.store.beginNSOp(h.nsID)
	if err != nil {
		return nil, err
	}
	defer h.store.endOp()

	if opts.ContentType == "" {
		head, err := peekForMIME(r)
		if err != nil {
			return nil, fmt.Errorf("store: detect mime type: %w", err)
		}
		opts.ContentType = mimetype.Detect(head.Bytes()).String()
		r = io.MultiReader(head, r)
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

	eng, err := h.store.beginNSOp(h.nsID)
	if err != nil {
		return nil, err
	}
	// Ownership of the store's read guard transfers to the returned
	// chunkReader on success: Get itself returns quickly, but the actual
	// chunk reads happen later, via chunkReader.Read, well after this call
	// has returned. The guard must stay held for that whole lifetime so
	// Store.Close cannot close this engine out from under an in-flight
	// read — so it is released here only on the error paths below; on the
	// success path, chunkReader.Close releases it exactly once instead.
	guardTransferred := false
	defer func() {
		if !guardTransferred {
			h.store.endOp()
		}
	}()

	ref, err := h.store.idx.GetRef(ctx, h.nsID, key)
	if err != nil {
		return nil, err
	}

	blob, err := h.store.idx.GetBlob(ctx, ref.BlobID)
	if err != nil {
		return nil, fmt.Errorf("store: get blob manifest: %w", err)
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

	guardTransferred = true
	return newChunkReader(eng, locations, h.store.endOp), nil
}

// Head returns metadata for key without reading any blob data.
// Returns *errors.NotFoundError if key does not exist.
func (h *NamespaceHandle) Head(ctx context.Context, key string) (*object.BlobInfo, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	if err := h.store.beginOp(); err != nil {
		return nil, err
	}
	defer h.store.endOp()

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
	if err := h.store.beginOp(); err != nil {
		return err
	}
	defer h.store.endOp()

	err := h.store.idx.CommitDelete(ctx, h.nsID, key)
	if err != nil && index.IsNotFound(err) {
		return nil // idempotent
	}
	return err
}

// UpdateMetadata updates the ContentType and/or Custom metadata for an
// existing blob without rewriting its content. Size, BlobID, and CreatedAt
// are preserved from the stored ref; UpdatedAt is set to the current time.
// Returns *errors.NotFoundError if key does not exist.
func (h *NamespaceHandle) UpdateMetadata(ctx context.Context, key, contentType string, custom map[string]string) (*object.BlobInfo, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	if err := h.store.beginOp(); err != nil {
		return nil, err
	}
	defer h.store.endOp()

	ref, err := h.store.idx.GetRef(ctx, h.nsID, key)
	if err != nil {
		return nil, err
	}

	ref.Metadata.ContentType = contentType
	ref.Metadata.Custom = custom
	ref.Metadata.UpdatedAt = time.Now().UTC()

	if err := h.store.idx.PutRef(ctx, *ref); err != nil {
		return nil, err
	}

	return &object.BlobInfo{
		Key:         key,
		NamespaceID: h.nsID,
		Metadata:    ref.Metadata,
	}, nil
}

// List returns BlobInfo for blobs in this namespace matching opts.
// Results are in lexicographic key order.
func (h *NamespaceHandle) List(ctx context.Context, opts ListOptions) ([]object.BlobInfo, error) {
	if err := h.store.beginOp(); err != nil {
		return nil, err
	}
	defer h.store.endOp()

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
	if err := h.store.beginOp(); err != nil {
		return nil, err
	}
	defer h.store.endOp()
	return h.store.idx.GetStats(ctx, h.nsID)
}

// Verify reads and CRC-checks every chunk for every blob in this namespace.
// Returns the first integrity error encountered, or nil.
// This is an expensive operation — run it offline or on a schedule.
func (h *NamespaceHandle) Verify(ctx context.Context) error {
	eng, err := h.store.beginNSOp(h.nsID)
	if err != nil {
		return err
	}
	defer h.store.endOp()

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
	eng, err := h.store.beginNSOp(h.nsID)
	if err != nil {
		return err
	}
	defer h.store.endOp()

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
	eng, err := h.store.beginNSOp(h.nsID)
	if err != nil {
		return CompactResult{}, err
	}
	defer h.store.endOp()

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

// peekForMIME reads up to 3072 bytes from r for MIME type detection
// and returns them in a buffer, preserving r for subsequent reading.
func peekForMIME(r io.Reader) (*bytes.Buffer, error) {
	var head bytes.Buffer
	_, err := io.CopyN(&head, r, 3072)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return &head, nil
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
//
// It also carries ownership of the Store's read guard that NamespaceHandle.Get
// acquired: release is called exactly once, when Close is called, so the
// guard stays held for as long as the caller might still call Read — not
// just for the duration of the Get call that created this reader. See
// Store.Close's doc comment.
type chunkReader struct {
	engine    *volume.Engine
	locations []object.ChunkEntry
	idx       int    // next chunk to load
	buf       []byte // current chunk payload
	pos       int    // read position within buf
	release   func()
	closeOnce sync.Once
}

func newChunkReader(eng *volume.Engine, locations []object.ChunkEntry, release func()) *chunkReader {
	return &chunkReader{engine: eng, locations: locations, release: release}
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
	if r.release != nil {
		r.closeOnce.Do(r.release)
	}
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
