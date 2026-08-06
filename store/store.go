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
//	s.CreateNamespace(ctx, object.Namespace{ID: "my-app"})
//	ns := s.Namespace("my-app")
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
	// ContentType is the MIME type. If left empty, it is detected
	// automatically by sniffing the blob's content (via
	// github.com/gabriel-vasile/mimetype) rather than defaulting to
	// "application/octet-stream" outright — that fallback is now only
	// what a genuinely undetectable stream (or a zero-byte blob) resolves
	// to. Set this explicitly to skip detection and force a specific type.
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

	// nsRW provides per-namespace read-write mutual exclusion between
	// ordinary volume-engine operations (Put, Get, Verify, RebuildIndex,
	// Compact phase 1 — all taken as shared read guards) and Compact
	// phase 2's segment rewrite (taken as an exclusive write guard) for
	// that same namespace. sync.Map stores *sync.RWMutex keyed by
	// namespace ID. This is what lets a segment rewrite on namespace A
	// block only namespace A's own operations instead of the whole
	// store — see beginNSOp and beginExclusiveNSOp.
	nsRW sync.Map
}

// Open opens an existing store or creates a new one.
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

	// Open volume engines for all known namespaces.
	namespaces, err := s.idx.ListNamespaces(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: list namespaces on open: %w", err)
	}
	for _, ns := range namespaces {
		if err := s.openEngine(ctx, ns.ID); err != nil {
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
	return s.openEngine(ctx, ns.ID)
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
// Physical bytes are not reclaimed until GC/compaction runs on the engine.
func (s *Store) DeleteNamespace(ctx context.Context, nsID string) error {
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

// openEngine opens nsID's volume engine and, before making it available to
// any caller, replays its WAL: for every durably-committed WriteBlob group
// found in a WAL file whose blob has no BlobEntry yet in the index — i.e.
// WriteBlob completed and fsynced, but the process crashed before
// CommitPut (ref + blob manifest) finished — its BlobEntry and
// ChunkEntry records are written now.
//
// This mirrors RebuildIndex's approach and inherits the same reasoning
// for RefCount: 1 rather than 0 (see RebuildIndex's doc comment) — a
// replayed blob has no way to carry forward its real reference count
// (that mapping lives only in RefEntry, which nothing durable recorded
// for an interrupted Put), so it is registered as presumed-referenced
// rather than immediately reapable. An operator (or the original,
// presumably-retried caller) re-establishes real refs by Put-ing the
// same content again — safe and idempotent thanks to content addressing.
//
// This runs on every openEngine call, not just after a real crash: for a
// namespace with no unreplayed WAL entries (the overwhelmingly common
// case — a clean prior shutdown, or a brand new namespace with no
// segments at all), it costs one cheap directory listing
// (ListSegmentIDs) and reads each segment's small WAL file, not a full
// segment scan.
func (s *Store) openEngine(ctx context.Context, nsID string) error {
	eng, err := volume.Open(s.cfg.DataDir, nsID, volume.Options{
		PageSize:       s.cfg.PageSize,
		ChunkSize:      s.cfg.ChunkSize,
		MaxSegmentSize: s.cfg.MaxSegmentSize,
	})
	if err != nil {
		return err
	}
	if err := s.replayWAL(ctx, nsID, eng); err != nil {
		_ = eng.Close()
		return fmt.Errorf("replay WAL: %w", err)
	}
	s.engines[nsID] = eng
	return nil
}

// replayWAL parses every segment's WAL file for eng and registers any
// not-yet-committed blob it finds. See openEngine's doc comment for the
// full rationale.
func (s *Store) replayWAL(ctx context.Context, nsID string, eng *volume.Engine) error {
	segIDs, err := eng.ListSegmentIDs()
	if err != nil {
		return fmt.Errorf("list segments for %q: %w", nsID, err)
	}

	for _, segID := range segIDs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		entries, err := eng.ParseWAL(segID)
		if err != nil {
			return fmt.Errorf("parse WAL for segment %s: %w", segID, err)
		}

		for _, entry := range entries {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			if _, err := s.idx.GetBlob(ctx, entry.BlobID); err == nil {
				continue // already committed via CommitPut — nothing to replay
			} else if !index.IsNotFound(err) {
				return fmt.Errorf("check blob %s: %w", entry.BlobID, err)
			}

			var totalSize int64
			chunkIDs := make([]object.ChunkID, len(entry.Chunks))
			for _, c := range entry.Chunks {
				// Presumed referenced, mirroring the blob's RefCount: 1 —
				// see openEngine's doc comment. A chunk whose WAL entry
				// survived a crash is referenced by the blob that wrote it.
				c.RefCount = 1
				if err := s.idx.PutChunk(ctx, c); err != nil {
					return fmt.Errorf("replay chunk %s: %w", c.ChunkID, err)
				}
				totalSize += c.Length
				chunkIDs[c.Seq] = c.ChunkID
			}

			if err := s.idx.PutBlob(ctx, object.BlobEntry{
				BlobID:    entry.BlobID,
				ChunkIDs:  chunkIDs,
				TotalSize: totalSize,
				RefCount:  1, // presumed referenced — see openEngine's doc comment
			}); err != nil {
				return fmt.Errorf("replay blob %s: %w", entry.BlobID, err)
			}
		}
	}
	return nil
}

// nsMutex returns the per-namespace mutex used to serialise quota checks.
// Creates one on first use. The mutex is never deleted — namespace deletion
// is rare and a leaked empty mutex costs 8 bytes.
func (s *Store) nsMutex(nsID string) *sync.Mutex {
	v, _ := s.quotaMu.LoadOrStore(nsID, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// nsRWMutex returns the per-namespace read-write mutex used to scope
// Compact phase 2's exclusivity to a single namespace. Creates one on
// first use. Never deleted, for the same reason nsMutex isn't.
func (s *Store) nsRWMutex(nsID string) *sync.RWMutex {
	v, _ := s.nsRW.LoadOrStore(nsID, &sync.RWMutex{})
	return v.(*sync.RWMutex)
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
// touch a volume engine directly (Put, Get, Verify, RebuildIndex, and
// Compact's phase 1). On success it holds TWO guards: the store's shared
// read guard (from beginOp) and this namespace's shared read guard. The
// second is what scopes Compact phase 2's exclusivity (see
// beginExclusiveNSOp) to a single namespace: a rewrite running for
// namespace A takes namespace A's write guard, which blocks new
// beginNSOp calls for namespace A, but has no effect whatsoever on
// namespace B's RWMutex — so namespace B's Puts, Gets, and everything
// else proceed exactly as if nothing were happening.
//
// On success the caller MUST call endNSOp(nsID) exactly once when the
// operation is entirely done — for Get, which hands back a long-lived
// reader, that means when the reader itself is Closed, not when Get
// returns (same rule beginOp/endOp already follow; see Close's doc
// comment). On error, no guard is held and endNSOp must not be called.
func (s *Store) beginNSOp(nsID string) (*volume.Engine, error) {
	if err := s.beginOp(); err != nil {
		return nil, err
	}
	eng, ok := s.engines[nsID]
	if !ok {
		s.endOp()
		return nil, &bserrors.NotFoundError{NamespaceID: nsID}
	}
	s.nsRWMutex(nsID).RLock()
	return eng, nil
}

// endNSOp releases both guards a successful beginNSOp call for nsID
// acquired.
func (s *Store) endNSOp(nsID string) {
	s.nsRWMutex(nsID).RUnlock()
	s.endOp()
}

// beginExclusiveNSOp acquires the store's shared read guard — the same
// one beginNSOp takes, so this still counts as "an operation in flight"
// for Close's drain to wait on — plus this namespace's WRITE guard,
// giving exclusive access to nsID's volume engine without affecting any
// other namespace at all.
//
// This is what lets Compact's phase 2 physically relocate live chunks'
// bytes safely: it blocks new beginNSOp calls for the SAME namespace (so
// no Get elsewhere can resolve to a location this rewrite is about to
// invalidate), while every other namespace's Puts, Gets, and everything
// else proceeds untouched for the full duration of the rewrite, since
// they contend on a completely different namespace's RWMutex. Prior to
// this, exclusivity was store-wide (via the store's own write lock),
// meaning a segment rewrite on one namespace froze every namespace —
// that store-wide throughput cost is what this narrows away.
//
// It returns *errors.ClosedError if the store is already closed, or
// *errors.NotFoundError if nsID does not exist. On success the caller
// MUST call endExclusiveNSOp(nsID) exactly once when done.
func (s *Store) beginExclusiveNSOp(nsID string) (*volume.Engine, error) {
	if err := s.beginOp(); err != nil {
		return nil, err
	}
	eng, ok := s.engines[nsID]
	if !ok {
		s.endOp()
		return nil, &bserrors.NotFoundError{NamespaceID: nsID}
	}
	s.nsRWMutex(nsID).Lock()
	return eng, nil
}

// endExclusiveNSOp releases both guards a successful beginExclusiveNSOp
// call for nsID acquired.
func (s *Store) endExclusiveNSOp(nsID string) {
	s.nsRWMutex(nsID).Unlock()
	s.endOp()
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
	defer h.store.endNSOp(h.nsID)

	// Content-type detection reads from r before WriteBlob does, and that
	// read can block on slow caller I/O exactly like WriteBlob's own reads
	// can — so it must happen after the guard above, not before it. If it
	// ran before beginNSOp, a Put stalled mid-sniff would not yet be
	// "in flight" as far as Store.Close is concerned, and Close could
	// complete out from under it (see Close's doc comment: every
	// operation must hold the guard for its full duration, not just the
	// part after its first blocking read).
	if opts.ContentType == "" {
		r, opts.ContentType, err = detectContentType(r)
		if err != nil {
			return nil, fmt.Errorf("store: detect content type: %w", err)
		}
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
	// Chunks whose content was already indexed are reused; their just-written
	// physical copies come back in `reused` and are reaped immediately.
	reused, err := h.store.idx.CommitPut(ctx, ref, blob, result.Chunks)
	if err != nil {
		return nil, fmt.Errorf("store: commit to index: %w", err)
	}
	for _, dup := range reused {
		if err := eng.MarkDeleted(dup); err != nil {
			return nil, fmt.Errorf("store: reap duplicate chunk %s: %w", dup.ChunkID, err)
		}
	}

	return &object.BlobInfo{Key: key, NamespaceID: h.nsID, Metadata: meta}, nil
}

// Get returns a streaming reader for the blob stored under key.
// The caller must close the returned ReadCloser when done.
// Returns *errors.NotFoundError if key does not exist.
func (h *NamespaceHandle) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return h.getChunkReader(ctx, key)
}

// GetSeekable is Get, but returns a reader that additionally implements
// io.Seeker — suitable for handing directly to http.ServeContent (see the
// streaming package), which already implements the full HTTP Range,
// conditional-GET, and multi-range-request protocol against any
// io.ReadSeeker. This package doesn't need to know anything about the
// Range header to serve blobs efficiently; the standard library already
// does, correctly, given a seekable reader.
//
// Seeking to any offset is efficient regardless of target: chunk
// boundaries and lengths are already known from the blob's manifest, so
// no chunk before the seek target is ever read from disk (see
// chunkReader.Seek).
//
// The caller must close the returned ReadSeekCloser when done. Returns
// *errors.NotFoundError if key does not exist.
func (h *NamespaceHandle) GetSeekable(ctx context.Context, key string) (io.ReadSeekCloser, error) {
	return h.getChunkReader(ctx, key)
}

// getChunkReader is the shared implementation behind Get and
// GetSeekable — both hand back the exact same underlying *chunkReader,
// which always implements io.Seeker regardless of which method returns
// it. The two public methods differ only in which interface they expose
// it as: Get's narrower io.ReadCloser signals "you probably just want to
// stream this end to end," GetSeekable's wider io.ReadSeekCloser signals
// "this supports random access."
func (h *NamespaceHandle) getChunkReader(ctx context.Context, key string) (*chunkReader, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}

	eng, err := h.store.beginNSOp(h.nsID)
	if err != nil {
		return nil, err
	}
	// Ownership of the store's read guard transfers to the returned
	// chunkReader on success: this method returns quickly, but the actual
	// chunk reads happen later, via chunkReader.Read/Seek, well after this
	// call has returned. The guard must stay held for that whole lifetime so
	// Store.Close cannot close this engine out from under an in-flight
	// read — so it is released here only on the error paths below; on the
	// success path, chunkReader.Close releases it exactly once instead.
	guardTransferred := false
	defer func() {
		if !guardTransferred {
			h.store.endNSOp(h.nsID)
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
	// GetChunks batches this into a single index round trip when the
	// backend supports it, instead of one Get per chunk — for a
	// multi-gigabyte blob with hundreds of chunks (exactly the case a
	// streamed Get/GetSeekable needs to resolve before any byte can be
	// served), that's the difference between one transaction and
	// hundreds.
	locations, err := h.store.idx.GetChunks(ctx, blob.ChunkIDs)
	if err != nil {
		return nil, fmt.Errorf("store: get chunk locations: %w", err)
	}

	guardTransferred = true
	return newChunkReader(eng, locations, func() { h.store.endNSOp(h.nsID) }), nil
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

// Update replaces the custom metadata on a blob. It does not affect the
// blob's content, content type, or any other metadata field.
// Returns NotFoundError if the key does not exist.
func (h *NamespaceHandle) Update(ctx context.Context, key string, metadata map[string]any) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if err := h.store.beginOp(); err != nil {
		return err
	}
	defer h.store.endOp()

	custom := make(map[string]string, len(metadata))
	for k, v := range metadata {
		custom[k] = fmt.Sprint(v)
	}

	return h.store.idx.CommitUpdateRefMetadata(ctx, h.nsID, key, custom, time.Now())
}

// Rename atomically renames a blob from oldKey to newKey.
// Returns NotFoundError if oldKey does not exist.
// Returns an error if newKey already exists.
func (h *NamespaceHandle) Rename(ctx context.Context, oldKey, newKey string) error {
	if err := validateKey(oldKey); err != nil {
		return err
	}
	if err := validateKey(newKey); err != nil {
		return err
	}
	if err := h.store.beginOp(); err != nil {
		return err
	}
	defer h.store.endOp()

	return h.store.idx.CommitRenameRef(ctx, h.nsID, oldKey, newKey)
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
	defer h.store.endNSOp(h.nsID)

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

		locations, err := h.store.idx.GetChunks(ctx, blob.ChunkIDs)
		if err != nil {
			return fmt.Errorf("store: verify: missing location for a chunk of blob %s (key %q): %w",
				ref.BlobID, ref.Key, err)
		}
		for _, loc := range locations {
			// ReadChunk performs CRC verification internally.
			if _, err := eng.ReadChunk(loc); err != nil {
				return err
			}
		}
	}
	return nil
}

// RebuildIndex reconstructs chunk entries in the index by scanning all segment
// files on disk. Should be called after Open when IsDirty() is true.
// It does not reconstruct refs — those can only come from the WAL or a backup.
// RebuildIndex reconstructs this namespace's index records by scanning
// every segment file directly — the disaster-recovery path for when the
// index itself has been lost or corrupted but the segment files are
// intact. It rebuilds two things, in this order:
//
//  1. Every chunk's ChunkEntry (location on disk), skipping any page
//     already flagged deleted.
//  2. Every blob's BlobEntry (ordered ChunkIDs, TotalSize), reconstructed
//     from the BlobID and TotalChunks that are already stored in every
//     page's header — grouping by BlobID and checking each group's chunk
//     count against TotalChunks tells us whether a blob's chunks are all
//     present (a mid-write crash could leave a blob's later chunks
//     missing from disk entirely, not just uncommitted).
//
// Step 2 matters more than it looks: without it, a rebuilt index would
// contain ChunkEntry records with no owning BlobEntry for ANY blob,
// including every blob that was fully, correctly committed before the
// index was lost. Compact's orphan sweep treats "no BlobEntry" as
// garbage — so skipping this step would mean the very next Compact call
// deletes everything RebuildIndex just recovered.
//
// Rebuilt blobs get RefCount: 1, not RefCount: 0. RebuildIndex, scanning
// only physical segment files, has no way to know how many keys actually
// point at a given blob — that mapping lives in RefEntry, which this
// method does not and cannot reconstruct (a key's mapping existing only
// in a lost index is unrecoverable from disk alone). RefCount: 0 would
// look like a conservative default, but Compact's phase 1 treats
// RefCount == 0 as immediately reapable — so it would produce the exact
// same data loss as skipping step 2 entirely, just via a different code
// path. RefCount: 1 means a rebuilt blob is not touched by Compact until
// an operator re-establishes its real refs (e.g. by re-Put-ing known
// keys — safe and idempotent thanks to content addressing) or explicitly
// intervenes; that trades a disk-space leak for recovered blobs nobody
// re-links against the much worse alternative of silently deleting data
// an operator is still in the middle of recovering.
func (h *NamespaceHandle) RebuildIndex(ctx context.Context) error {
	eng, err := h.store.beginNSOp(h.nsID)
	if err != nil {
		return err
	}
	defer h.store.endNSOp(h.nsID)

	type blobAccum struct {
		chunkIDs    map[int]object.ChunkID // by Seq, so out-of-order page scans still assemble correctly
		totalSize   int64
		totalChunks uint32
	}
	blobs := make(map[object.BlobID]*blobAccum)

	if err := eng.ScanSegments(func(entry object.ChunkEntry, hdr volume.PageHeader) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if hdr.Flags.IsDeleted() {
			return nil // dead page — do not resurrect it into the rebuilt index
		}
		// Presumed referenced: the location recorded here IS the canonical
		// copy on disk, and Compact's phase 1 reaps only chunks whose
		// index record has RefCount <= 0 (or whose location doesn't match
		// the physical page). See the RefCount: 1 rationale below.
		entry.RefCount = 1
		if err := h.store.idx.PutChunk(ctx, entry); err != nil {
			return fmt.Errorf("rebuild chunk %s: %w", entry.ChunkID, err)
		}

		acc, ok := blobs[entry.BlobID]
		if !ok {
			acc = &blobAccum{chunkIDs: make(map[int]object.ChunkID)}
			blobs[entry.BlobID] = acc
		}
		acc.chunkIDs[entry.Seq] = entry.ChunkID
		acc.totalSize += entry.Length
		if hdr.TotalChunks > acc.totalChunks {
			acc.totalChunks = hdr.TotalChunks
		}
		return nil
	}); err != nil {
		return fmt.Errorf("store: rebuild index scan: %w", err)
	}

	for blobID, acc := range blobs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if uint32(len(acc.chunkIDs)) != acc.totalChunks {
			// A crash mid-write can leave a blob's later chunks never
			// written to disk at all (not merely uncommitted — physically
			// absent). Such a blob can never be reassembled correctly, so
			// it must not get a BlobEntry: that would make it look
			// complete and retrievable when reading it back would fail or
			// return truncated data partway through. Its chunks were
			// already registered as ChunkEntry above; without a BlobEntry
			// they're orphaned and Compact will reclaim them normally.
			continue
		}
		orderedChunkIDs := make([]object.ChunkID, acc.totalChunks)
		for seq, cid := range acc.chunkIDs {
			orderedChunkIDs[seq] = cid
		}
		if err := h.store.idx.PutBlob(ctx, object.BlobEntry{
			BlobID:    blobID,
			ChunkIDs:  orderedChunkIDs,
			TotalSize: acc.totalSize,
			// RefCount is set to 1, not 0, and this is deliberate, not a
			// placeholder. RebuildIndex has no way to know a blob's real
			// reference count — that mapping lives entirely in RefEntry
			// records, which were lost along with the index and cannot be
			// reconstructed from segment files. Compact's phase 1 treats
			// RefCount == 0 as "safe to reap right now." If RebuildIndex
			// set RefCount to 0 here, the very next Compact call would
			// delete every blob it just restored — via a different code
			// path than the pre-fix bug, but with the identical
			// data-loss outcome. Given the choice between "a truly
			// unreferenced blob leaks disk space until an operator
			// explicitly reaps it" and "a blob an operator is mid-recovery
			// on gets silently deleted the moment they run routine
			// maintenance," this errs toward the former. An operator doing
			// disaster recovery is expected to re-establish real refs
			// (e.g. by re-Put-ing known keys, which is idempotent thanks
			// to content addressing) before relying on Compact again.
			RefCount: 1,
			// CreatedAt is left zero-valued: page headers do not carry a
			// timestamp, so when a blob's original CreatedAt has to be
			// reconstructed from segment scans alone, there is no source
			// for it. Reporting a fabricated "now" would be more
			// misleading than an honest zero value.
		}); err != nil {
			return fmt.Errorf("store: rebuild blob %s: %w", blobID, err)
		}
	}

	return nil
}

// Compact reclaims space in two phases:
//
//  1. Mark-and-sweep (always runs): walk all blob entries, collect
//     BlobIDs with RefCount == 0, mark their chunks deleted in the
//     segment, and remove their blob/chunk index records. This runs
//     under the shared store guard (see beginNSOp) — it only flips a flag
//     on chunks nothing references anymore, so it's safe alongside other
//     namespaces', and this namespace's, concurrent Puts and Gets.
//
//  2. Segment rewrite (this namespace's sealed segments only): any sealed
//     segment whose dead-byte ratio is >= the configured threshold
//     (CompactOptions.RewriteThreshold, default
//     volume.DefaultSegmentRewriteThreshold) is physically rewritten —
//     its live chunks copied into a fresh segment, the index updated to
//     point at the new locations, and the old segment's files removed.
//     This DOES relocate live chunks' physical bytes, so it runs under
//     this namespace's exclusive guard (see beginExclusiveNSOp): it will
//     not start while a Get elsewhere has an open reader that might still
//     resolve to the segment being rewritten, and nothing new can start
//     on THIS namespace until it finishes. That exclusivity is scoped to
//     this namespace's own RWMutex, not the whole store — Puts, Gets, and
//     everything else on every other namespace proceed untouched for the
//     full duration of the rewrite. Compact is still a deliberate,
//     occasional maintenance operation rather than a hot path, but it no
//     longer needs to be scheduled around unrelated namespaces' traffic.
//
// If phase 1 fails, phase 2 does not run and the phase 1 error is
// returned. If phase 1 succeeds but phase 2 fails, the phase 1 results
// are still returned alongside the phase 2 error — phase 1's work is
// already durably committed at that point and does not need retrying.
func (h *NamespaceHandle) Compact(ctx context.Context) (CompactResult, error) {
	return h.CompactWithOptions(ctx, CompactOptions{})
}

// CompactWithOptions is Compact with a tunable rewrite threshold. See
// Compact's doc comment for what each phase does.
func (h *NamespaceHandle) CompactWithOptions(ctx context.Context, opts CompactOptions) (CompactResult, error) {
	opts = opts.withDefaults()

	result, err := h.compactPhase1(ctx)
	if err != nil {
		return result, err
	}

	bytesFreed, segmentsCompacted, err := h.compactPhase2(ctx, opts.RewriteThreshold)
	if err != nil {
		return result, fmt.Errorf("store: compact phase 2 (segment rewrite): %w", err)
	}
	result.BytesFreed += bytesFreed
	result.SegmentsCompacted = segmentsCompacted

	return result, nil
}

// compactPhase1 marks orphaned chunks deleted and removes their index
// records. See Compact's doc comment.
//
// A chunk is live if and only if its index record still has RefCount > 0.
// ScanSegments yields one entry per page while the index record is per
// chunk (which can span many pages), so a page's liveness cannot be judged
// by comparing offsets — only the record's reference count decides.
// Deleting a blob decrements every chunk it references (see CommitDelete),
// and only when the last referencing blob is gone does RefCount reach zero
// and the chunk become collectable — which is exactly what keeps a chunk
// shared between blob A and blob B alive after A is deleted.
func (h *NamespaceHandle) compactPhase1(ctx context.Context) (CompactResult, error) {
	eng, err := h.store.beginNSOp(h.nsID)
	if err != nil {
		return CompactResult{}, err
	}
	defer h.store.endNSOp(h.nsID)

	var (
		deadChunks   []object.ChunkEntry   // physical pages to mark deleted
		deadChunkIDs []object.ChunkID    // index records with RefCount == 0 to remove
		bytesFreed   int64
		chunksFreed  int64
	)

	// indexCache avoids re-fetching the same chunk record from the index
	// once per physical page: ScanSegments visits every page of a chunk,
	// and a chunk shared across blobs is hit many times. A nil cached
	// entry marks a ChunkID already confirmed absent.
	indexCache := make(map[object.ChunkID]*object.ChunkEntry)
	deadIDSeen := make(map[object.ChunkID]struct{})

	lookupChunk := func(id object.ChunkID) *object.ChunkEntry {
		if c, ok := indexCache[id]; ok {
			return c
		}
		c, err := h.store.idx.GetChunk(ctx, id)
		if err != nil {
			indexCache[id] = nil
			return nil
		}
		indexCache[id] = c
		return c
	}

	if err := eng.ScanSegments(func(entry object.ChunkEntry, hdr volume.PageHeader) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if hdr.Flags&0x01 != 0 { // already deleted
			return nil
		}

		indexed := lookupChunk(entry.ChunkID)
		if indexed != nil && indexed.RefCount > 0 {
			return nil // live — referenced by at least one blob
		}

		deadChunks = append(deadChunks, entry)
		bytesFreed += entry.Length
		chunksFreed++
		if _, ok := deadIDSeen[entry.ChunkID]; !ok {
			deadIDSeen[entry.ChunkID] = struct{}{}
			deadChunkIDs = append(deadChunkIDs, entry.ChunkID)
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

	// Remove index records for chunks whose RefCount reached zero. Phantom
	// duplicate pages (dead but sharing a live chunk's content hash) leave
	// the index record alone — it belongs to the live canonical copy.
	for _, cid := range deadChunkIDs {
		if err := h.store.idx.DeleteChunk(ctx, cid); err != nil {
			return CompactResult{}, fmt.Errorf("store: delete chunk record: %w", err)
		}
	}

	// Sweep blob manifests nobody references anymore. Safe regardless of
	// chunk sharing: chunk records are keyed by content and own their own
	// refcounts, so removing a dead blob entry never drops live data.
	blobsRemoved, err := h.store.idx.PurgeDeadBlobs(ctx)
	if err != nil {
		return CompactResult{}, fmt.Errorf("store: purge dead blobs: %w", err)
	}

	return CompactResult{
		BlobsRemoved:  int64(blobsRemoved),
		ChunksRemoved: chunksFreed,
		BytesFreed:    bytesFreed,
	}, nil
}

// compactPhase2 physically rewrites sealed segments whose dead-byte ratio
// meets threshold, reclaiming disk space. See Compact's doc comment for
// the locking and crash-safety rationale.
func (h *NamespaceHandle) compactPhase2(ctx context.Context, threshold float64) (bytesFreed int64, segmentsCompacted int64, err error) {
	eng, err := h.store.beginExclusiveNSOp(h.nsID)
	if err != nil {
		return 0, 0, err
	}
	defer h.store.endExclusiveNSOp(h.nsID)

	stats, err := eng.SegmentStats()
	if err != nil {
		return 0, 0, fmt.Errorf("segment stats: %w", err)
	}
	activeID, hasActive := eng.ActiveSegmentID()

	for _, s := range stats {
		if ctx.Err() != nil {
			return bytesFreed, segmentsCompacted, ctx.Err()
		}
		if hasActive && s.SegmentID == activeID {
			continue // never rewrite the segment still being written to
		}
		if s.DeadRatio() < threshold {
			continue
		}

		rewriteResult, relocated, err := eng.RewriteSegment(s.SegmentID)
		if err != nil {
			return bytesFreed, segmentsCompacted, fmt.Errorf("rewrite segment %s: %w", s.SegmentID, err)
		}

		// Commit every relocated chunk's new location to the index BEFORE
		// removing the old segment's files. If the process crashes here,
		// both the old and new segment files still exist, so every
		// ChunkEntry — whether its index record has been updated yet or
		// not — still resolves to valid data; the only cost of a crash
		// mid-loop is that this segment's space isn't reclaimed yet, and
		// the next Compact call simply retries it.
		for _, entry := range relocated {
			if err := h.store.idx.PutChunk(ctx, entry); err != nil {
				return bytesFreed, segmentsCompacted, fmt.Errorf(
					"relocate chunk %s to segment %s: %w", entry.ChunkID, rewriteResult.NewSegmentID, err,
				)
			}
		}

		// Only now is it safe to remove the old segment's files.
		if err := eng.DeleteSegmentFile(s.SegmentID); err != nil {
			return bytesFreed, segmentsCompacted, fmt.Errorf("delete old segment %s: %w", s.SegmentID, err)
		}

		bytesFreed += rewriteResult.BytesFreed
		segmentsCompacted++
	}

	return bytesFreed, segmentsCompacted, nil
}

// CompactOptions tunes Compact's phase 2 (segment rewrite).
type CompactOptions struct {
	// RewriteThreshold is the minimum dead-byte ratio (0.0–1.0, by page
	// count) a sealed segment must reach before it is physically
	// rewritten to reclaim space. Zero uses
	// volume.DefaultSegmentRewriteThreshold (0.30).
	RewriteThreshold float64
}

func (o CompactOptions) withDefaults() CompactOptions {
	if o.RewriteThreshold == 0 {
		o.RewriteThreshold = volume.DefaultSegmentRewriteThreshold
	}
	return o
}

// CompactResult summarises what Compact reclaimed.
type CompactResult struct {
	BlobsRemoved      int64
	ChunksRemoved     int64
	BytesFreed        int64 // phase 1 (orphaned-chunk bytes) + phase 2 (physically reclaimed bytes)
	SegmentsCompacted int64 // sealed segments physically rewritten and removed by phase 2
}

// ── Content-type detection ───────────────────────────────────────────────────

// mimeSniffLimit is how many leading bytes of a blob are read for content-type
// detection. This matches mimetype.Detect's own default header size (3072
// bytes) — sniffing beyond what the detector itself would ever look at
// would just mean holding more of the stream in memory for no benefit.
const mimeSniffLimit = 3072

// detectContentType peeks up to mimeSniffLimit bytes from r to sniff its
// MIME type, then returns a reader that reproduces the exact original
// stream — the peeked prefix followed by whatever remains of r — so the
// caller (Put) can hand it to WriteBlob exactly as if no peeking had
// happened. r itself must not be read from again after this call; use only
// the returned reader.
//
// A blob shorter than mimeSniffLimit is not an error: io.ReadFull reports
// io.EOF (zero bytes available at all) or io.ErrUnexpectedEOF (fewer bytes
// than requested) in that case, and both are expected outcomes here, not
// failures — they just mean the entire blob was small enough to fit in the
// sniff buffer already. Any other read error is real and is propagated;
// mimetype.Detect itself never errors — its worst case is falling back to
// "application/octet-stream", the same default this replaces.
func detectContentType(r io.Reader) (io.Reader, string, error) {
	buf := make([]byte, mimeSniffLimit)
	n, err := io.ReadFull(r, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, "", err
	}
	peeked := buf[:n]
	detected := mimetype.Detect(peeked).String()
	return io.MultiReader(bytes.NewReader(peeked), r), detected, nil
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
	if q.MaxBlobSize > 0 && incomingSize > q.MaxBlobSize {
		return &bserrors.QuotaExceededError{
			NamespaceID: h.nsID,
			Dimension:   "blob_size",
			Limit:       q.MaxBlobSize,
			Requested:   incomingSize,
		}
	}
	return nil
}

// ── chunkReader — streaming reassembly ───────────────────────────────────────

// chunkReader implements io.ReadCloser (and, additionally, io.Seeker — see
// Seek below) by reading chunks from the volume engine in order, buffering
// one chunk at a time.
//
// It also carries ownership of the Store's read guard that
// NamespaceHandle.Get/GetSeekable acquired: release is called exactly
// once, when Close is called, so the guard stays held for as long as the
// caller might still call Read or Seek — not just for the duration of the
// Get/GetSeekable call that created this reader. See Store.Close's doc
// comment.
type chunkReader struct {
	engine      *volume.Engine
	locations   []object.ChunkEntry
	totalSize   int64 // sum of every location's Length, computed once at construction
	idx         int   // next chunk to load
	pendingSkip int64 // bytes to discard from the next-loaded chunk; set by Seek, consumed once
	buf         []byte // current chunk payload
	pos         int    // read position within buf
	absPos      int64  // absolute stream position, for Seek's SeekCurrent/SeekEnd math
	release     func()
	closeOnce   sync.Once
}

func newChunkReader(eng *volume.Engine, locations []object.ChunkEntry, release func()) *chunkReader {
	var total int64
	for _, loc := range locations {
		total += loc.Length
	}
	return &chunkReader{engine: eng, locations: locations, totalSize: total, release: release}
}

func (r *chunkReader) Read(p []byte) (int, error) {
	for {
		// Serve from buffer if available.
		if r.pos < len(r.buf) {
			n := copy(p, r.buf[r.pos:])
			r.pos += n
			r.absPos += int64(n)
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
		r.idx++
		if r.pendingSkip > 0 {
			// Left over from a Seek that landed partway into this chunk:
			// discard the leading bytes before this chunk's data is
			// served — resolveChunkOffset guarantees pendingSkip is
			// strictly less than this chunk's length, so this always
			// fully consumes it in one step.
			skip := r.pendingSkip
			if skip > int64(len(data)) {
				skip = int64(len(data)) // defensive; should not happen
			}
			data = data[skip:]
			r.pendingSkip -= skip
		}
		r.buf = data
		r.pos = 0
	}
}

// Seek implements io.Seeker, which is what lets a chunkReader be handed
// directly to http.ServeContent (see the streaming package) to get full
// HTTP Range / conditional-GET / multi-range-request support for free
// from the standard library, without this package needing to know
// anything about the HTTP Range protocol itself.
//
// whence follows the usual io.SeekStart / io.SeekCurrent / io.SeekEnd
// convention. Seeking to any offset is efficient regardless of target:
// chunk boundaries and lengths are already known from the blob's
// manifest (resolveChunkOffset works purely against metadata already
// held in r.locations), so no chunk before the target is ever read from
// disk — the currently buffered chunk, if any, is simply discarded, and
// the next Read call loads whichever chunk the target actually falls in.
func (r *chunkReader) Seek(offset int64, whence int) (int64, error) {
	var target int64
	switch whence {
	case io.SeekStart:
		target = offset
	case io.SeekCurrent:
		target = r.absPos + offset
	case io.SeekEnd:
		target = r.totalSize + offset
	default:
		return 0, fmt.Errorf("store: chunkReader.Seek: invalid whence %d", whence)
	}
	if target < 0 {
		return 0, fmt.Errorf("store: chunkReader.Seek: resulting offset must not be negative")
	}

	idx, intraOffset := resolveChunkOffset(r.locations, target)
	r.idx = idx
	r.pendingSkip = intraOffset
	r.buf = nil
	r.pos = 0
	r.absPos = target
	return target, nil
}

// resolveChunkOffset finds which chunk (by index into locations) contains
// byte offset, and the intra-chunk offset within that chunk's payload
// corresponding to offset. locations must be in blob order. An offset
// equal to the sum of every chunk's length (i.e. exactly at EOF) resolves
// to len(locations), 0 — meaning "no chunk left to read, already at the
// end," which Read already treats as io.EOF via its idx bounds check.
func resolveChunkOffset(locations []object.ChunkEntry, offset int64) (chunkIdx int, intraOffset int64) {
	var cumulative int64
	for i, loc := range locations {
		next := cumulative + loc.Length
		if offset < next {
			return i, offset - cumulative
		}
		cumulative = next
	}
	return len(locations), 0
}

func (r *chunkReader) Close() error {
	r.buf = nil
	r.idx = len(r.locations)
	r.pendingSkip = 0
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
