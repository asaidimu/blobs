package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asaidimu/blobs/encryption"
	bserrors "github.com/asaidimu/blobs/errors"
	"github.com/asaidimu/blobs/index"
	"github.com/asaidimu/blobs/object"
	"github.com/asaidimu/blobs/volume"
)

// nsState is the in-memory handle for one namespace incarnation. Everything
// the namespace owns on disk and in the index is keyed by uid, never by name.
type nsState struct {
	name string
	uid  string
	eng  *volume.Engine

	// life is held shared by every operation on this namespace for its whole
	// duration (including an open reader returned by Get). The final erase
	// takes it exclusively, which drains in-flight operations.
	life sync.RWMutex

	// rw is the engine guard: shared for ordinary engine operations,
	// exclusive for Compact phase 2's segment rewrite. Always acquired
	// after life.
	rw sync.RWMutex

	// quota serialises the quota-check + CommitPut window (VULN 8): without
	// it N concurrent Puts could all read the same stats, all pass the
	// check and together exceed the quota.
	quota sync.Mutex

	// unlinked is set, under Store.mu, the moment DeleteNamespace has
	// released the name. An operation that looked this state up before that
	// and acquires life afterwards sees it and reports NotFound.
	unlinked atomic.Bool

	closeOnce sync.Once
	closeErr  error
}

func (st *nsState) closeEngine() error {
	st.closeOnce.Do(func() { st.closeErr = st.eng.Close() })
	return st.closeErr
}

// ── identity ──────────────────────────────────────────────────────────────────

func newUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("store: generate namespace uid: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// validUID reports whether uid is safe to use as a directory name directly
// under DataDir. Generated UIDs are 32 hex characters; legacy namespaces use
// their (validated) name.
func validUID(uid string) bool {
	if uid == "" || len(uid) > 64 || uid[0] == '.' {
		return false
	}
	for _, c := range uid {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '_':
		default:
			return false
		}
	}
	return true
}

func uidOf(ns object.Namespace) string {
	if ns.UID != "" {
		return ns.UID
	}
	return ns.ID // namespace created before UIDs existed
}

// ── Open / Close ──────────────────────────────────────────────────────────────

// Open opens (creating if needed) a store. If the index was written by an
// older version it is upgraded in place first (see MigrationReport), and any
// namespace deleted before the last shutdown whose data was not yet erased is
// erased now (see CleanupDeletedNamespaces).
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
		states:  make(map[string]*nsState),
		pending: make(map[string]*nsState),
	}
	ctx := context.Background()

	if err := s.migrateLegacy(ctx); err != nil {
		return nil, fmt.Errorf("store: upgrade index: %w", err)
	}

	namespaces, err := s.idx.ListNamespaces(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: list namespaces on open: %w", err)
	}
	for _, ns := range namespaces {
		st, err := s.openEngine(ctx, ns)
		if err != nil {
			return nil, fmt.Errorf("store: open engine for %q: %w", ns.ID, err)
		}
		s.states[ns.ID] = st
	}

	// Finish erasing namespaces deleted before the last shutdown. Best
	// effort: a failure leaves the tombstone in place, the data unreachable,
	// and the erase retryable via CleanupDeletedNamespaces, which reports
	// the error.
	_, _ = s.CleanupDeletedNamespaces(ctx)

	return s, nil
}

// Close waits for every in-flight operation to finish, then closes all
// engines and the index backend.
//
// New operations are refused as soon as Close begins; operations already
// running are allowed to finish. Every operation — including an io.ReadCloser
// returned by NamespaceHandle.Get, until the caller closes it — is counted, so
// a caller that never closes a reader will make Close block, exactly as
// leaking any other closable handle would jam an equivalent shutdown.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return &bserrors.ClosedError{}
	}
	s.closed = true
	s.mu.Unlock()

	s.inflight.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()

	var errs []error
	for _, m := range []map[string]*nsState{s.states, s.pending} {
		for _, st := range m {
			if err := st.closeEngine(); err != nil {
				errs = append(errs, fmt.Errorf("engine %q: %w", st.uid, err))
			}
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

// ── operation guards ──────────────────────────────────────────────────────────

// beginOp registers a store-level operation (one that is not bound to a single
// namespace). It returns *errors.ClosedError once Close has begun.
func (s *Store) beginOp() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return &bserrors.ClosedError{}
	}
	s.inflight.Add(1)
	return nil
}

func (s *Store) endOp() { s.inflight.Done() }

// beginNSOp resolves name to its live namespace and registers an operation on
// it, for operations that touch only the index. The caller must call endNSOp
// exactly once. It returns NotFound if the namespace does not exist or has
// been deleted — including deleted while this call was in progress.
func (s *Store) beginNSOp(name string) (*nsState, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, &bserrors.ClosedError{}
	}
	st, ok := s.states[name]
	if !ok {
		s.mu.RUnlock()
		return nil, &bserrors.NotFoundError{NamespaceID: name}
	}
	s.inflight.Add(1)
	s.mu.RUnlock()

	// Taken after Store.mu is released: waiting here (behind an erase, say)
	// must never hold up the rest of the store.
	st.life.RLock()
	if st.unlinked.Load() {
		st.life.RUnlock()
		s.inflight.Done()
		return nil, &bserrors.NotFoundError{NamespaceID: name}
	}
	return st, nil
}

func (s *Store) endNSOp(st *nsState) {
	st.life.RUnlock()
	s.inflight.Done()
}

// beginEngineOp is beginNSOp plus the shared engine guard, for operations that
// read or write segment files (Put, Get, Verify, RebuildIndex, Compact phase 1).
func (s *Store) beginEngineOp(name string) (*nsState, error) {
	st, err := s.beginNSOp(name)
	if err != nil {
		return nil, err
	}
	st.rw.RLock()
	return st, nil
}

func (s *Store) endEngineOp(st *nsState) {
	st.rw.RUnlock()
	s.endNSOp(st)
}

// beginExclusiveEngineOp takes the exclusive engine guard, for Compact phase
// 2's physical segment rewrite. Scoped to this one namespace.
func (s *Store) beginExclusiveEngineOp(name string) (*nsState, error) {
	st, err := s.beginNSOp(name)
	if err != nil {
		return nil, err
	}
	st.rw.Lock()
	return st, nil
}

func (s *Store) endExclusiveEngineOp(st *nsState) {
	st.rw.Unlock()
	s.endNSOp(st)
}

// ── namespace management ──────────────────────────────────────────────────────

// CreateNamespaceOption configures optional behavior for CreateNamespace.
type CreateNamespaceOption func(*createNamespaceConfig)

type createNamespaceConfig struct {
	encrypt    bool
	recoverUID string
}

// WithEncryption enables encryption-at-rest for the namespace being created. A
// random 256-bit data-encryption key (DEK) is generated for it, wrapped under
// the current master key from Config.KeyProvider, and the wrapped form stored
// in the namespace record. It requires Config.KeyProvider to be set. This is
// decided once, at creation.
func WithEncryption() CreateNamespaceOption {
	return func(c *createNamespaceConfig) { c.encrypt = true }
}

// WithRecoveredUID is for disaster recovery only. It makes the new namespace
// adopt an existing directory DataDir/<uid> instead of getting a fresh UID — the
// way to reattach segment files to a namespace record after the index was lost
// (follow with NamespaceHandle.RebuildIndex).
//
// It is deliberately an explicit option and not a field callers can set on
// object.Namespace: adopting a directory hands over everything in it, so it
// must never be reachable from untrusted input. CreateNamespace refuses to
// adopt a directory that does not exist, that is already owned by a live
// namespace, or that belongs to a deleted namespace whose erase is pending. It
// cannot be combined with WithEncryption: an encrypted namespace's key lived in
// the lost index, so its data cannot be recovered by reattaching files.
func WithRecoveredUID(uid string) CreateNamespaceOption {
	return func(c *createNamespaceConfig) { c.recoverUID = uid }
}

// CreateNamespace creates a namespace named ns.ID.
//
// The store assigns the namespace a fresh UID (any ns.UID passed in is
// ignored). Its index records and its directory under DataDir are keyed by
// that UID, so a namespace created under the name of a previously deleted one
// shares nothing with it — not segment files, not chunk records, not stats,
// not encryption key material.
func (s *Store) CreateNamespace(ctx context.Context, ns object.Namespace, opts ...CreateNamespaceOption) error {
	if err := object.ValidateNamespaceID(ns.ID); err != nil {
		return err
	}
	var cfg createNamespaceConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := s.beginOp(); err != nil {
		return err
	}
	defer s.endOp()

	s.admin.Lock()
	defer s.admin.Unlock()

	s.mu.RLock()
	_, exists := s.states[ns.ID]
	s.mu.RUnlock()
	if exists {
		return &bserrors.AlreadyExistsError{NamespaceID: ns.ID}
	}
	if _, err := s.idx.GetNamespace(ctx, ns.ID); err == nil {
		return &bserrors.AlreadyExistsError{NamespaceID: ns.ID}
	} else if !index.IsNotFound(err) {
		return fmt.Errorf("store: check namespace: %w", err)
	}

	if cfg.encrypt && cfg.recoverUID != "" {
		return fmt.Errorf("store: WithRecoveredUID cannot be combined with WithEncryption: the namespace's data key was in the lost index")
	}
	if cfg.encrypt {
		if s.cfg.KeyProvider == nil {
			return fmt.Errorf("store: WithEncryption requires Config.KeyProvider to be set")
		}
		dek, err := encryption.GenerateKey()
		if err != nil {
			return fmt.Errorf("store: generate namespace key: %w", err)
		}
		masterKey, version, err := s.cfg.KeyProvider.CurrentKey()
		if err != nil {
			return fmt.Errorf("store: get current master key: %w", err)
		}
		wrapped, err := encryption.WrapKey(masterKey, dek)
		if err != nil {
			return fmt.Errorf("store: wrap namespace key: %w", err)
		}
		ns.Encryption = &object.EncryptionInfo{
			Enabled:    true,
			WrappedDEK: wrapped,
			KeyVersion: version,
		}
	}

	var uid string
	if cfg.recoverUID != "" {
		if err := s.checkRecoverable(ctx, cfg.recoverUID); err != nil {
			return err
		}
		uid = cfg.recoverUID
	} else {
		var err error
		if uid, err = newUID(); err != nil {
			return err
		}
	}
	ns.UID = uid
	ns.CreatedAt = time.Now().UTC()
	if err := s.idx.PutNamespace(ctx, ns); err != nil {
		return fmt.Errorf("store: persist namespace: %w", err)
	}

	st, err := s.openEngine(ctx, ns)
	if err != nil {
		// Roll back so a failed create leaves no half-created namespace.
		_ = s.idx.DeleteNamespace(ctx, ns.ID)
		if cfg.recoverUID == "" {
			// Never remove an adopted directory: it holds the operator's
			// recovered data, not something this call created.
			_ = s.removeNamespaceDir(uid)
		}
		return err
	}
	s.mu.Lock()
	s.states[ns.ID] = st
	s.mu.Unlock()
	return nil
}

// checkRecoverable verifies uid names an existing, unowned namespace directory.
func (s *Store) checkRecoverable(ctx context.Context, uid string) error {
	if !validUID(uid) {
		return fmt.Errorf("store: WithRecoveredUID: invalid uid %q", uid)
	}
	if st, err := os.Stat(filepath.Join(s.cfg.DataDir, uid)); err != nil || !st.IsDir() {
		return fmt.Errorf("store: WithRecoveredUID: no directory %q under DataDir", uid)
	}
	live, err := s.idx.ListNamespaces(ctx)
	if err != nil {
		return err
	}
	for _, ns := range live {
		if uidOf(ns) == uid {
			return fmt.Errorf("store: WithRecoveredUID: directory %q already belongs to namespace %q", uid, ns.ID)
		}
	}
	tombs, err := s.idx.ListTombstones(ctx)
	if err != nil {
		return err
	}
	for _, t := range tombs {
		if t.UID == uid {
			return fmt.Errorf("store: WithRecoveredUID: directory %q belongs to deleted namespace %q awaiting cleanup", uid, t.Name)
		}
	}
	return nil
}

// GetNamespace returns the namespace record for name.
func (s *Store) GetNamespace(ctx context.Context, name string) (*object.Namespace, error) {
	if err := s.beginOp(); err != nil {
		return nil, err
	}
	defer s.endOp()
	ns, err := s.idx.GetNamespace(ctx, name)
	if err != nil {
		if index.IsNotFound(err) {
			return nil, &bserrors.NotFoundError{NamespaceID: name}
		}
		return nil, err
	}
	return ns, nil
}

// DeleteNamespace deletes the namespace named name.
//
// Deletion is two-phase, and the first phase is atomic:
//
//  1. The name is unlinked and a tombstone written in a single index
//     transaction. From that instant the name is free to be created again, no
//     new operation can reach the old namespace, and — for an encrypted
//     namespace — its wrapped key is gone.
//  2. Once operations already running on the namespace (including readers
//     returned by Get) have finished, everything it owned is erased: index
//     records, segment files, WAL files, and the directory.
//
// Deleting a namespace never blocks or is blocked by operations on any other
// namespace.
//
// If phase 2 cannot complete — the backend failed, or ctx expired while
// waiting for in-flight operations — DeleteNamespace returns
// *errors.CleanupPendingError. The namespace is still deleted; only the erase
// is outstanding, and CleanupDeletedNamespaces (or the next Open) finishes it.
//
// Erasure removes the files; it does not overwrite their blocks. For data that
// must be unrecoverable from the underlying device, use an encrypted namespace:
// its key is destroyed in phase 1.
func (s *Store) DeleteNamespace(ctx context.Context, name string) error {
	if err := s.beginOp(); err != nil {
		return err
	}
	defer s.endOp()

	s.admin.Lock()
	s.mu.RLock()
	st, ok := s.states[name]
	s.mu.RUnlock()
	if !ok {
		s.admin.Unlock()
		return &bserrors.NotFoundError{NamespaceID: name}
	}
	if err := s.idx.UnlinkNamespace(ctx, name, st.uid, time.Now().UTC()); err != nil {
		s.admin.Unlock()
		if index.IsNotFound(err) {
			return &bserrors.NotFoundError{NamespaceID: name}
		}
		return fmt.Errorf("store: delete namespace %q: %w", name, err)
	}
	s.mu.Lock()
	delete(s.states, name)
	s.pending[st.uid] = st
	st.unlinked.Store(true)
	s.mu.Unlock()
	s.admin.Unlock()

	if err := s.purge(ctx, st); err != nil {
		return &bserrors.CleanupPendingError{NamespaceID: name, UID: st.uid, Err: err}
	}
	return nil
}

// CleanupDeletedNamespaces finishes erasing every namespace that was deleted
// but whose data is still on disk — because an earlier DeleteNamespace could
// not complete, or the process stopped part-way. It returns the number of
// namespaces erased. It is safe to call at any time and from any number of
// goroutines; it never touches a live namespace.
func (s *Store) CleanupDeletedNamespaces(ctx context.Context) (int, error) {
	if err := s.beginOp(); err != nil {
		return 0, err
	}
	defer s.endOp()

	tombs, err := s.idx.ListTombstones(ctx)
	if err != nil {
		return 0, fmt.Errorf("store: list deleted namespaces: %w", err)
	}
	var (
		n    int
		errs []error
	)
	for _, t := range tombs {
		s.mu.RLock()
		st := s.pending[t.UID]
		s.mu.RUnlock()

		var perr error
		if st != nil {
			perr = s.purge(ctx, st)
		} else {
			// No in-process state: the namespace was deleted before this
			// process started, so nothing can be using it.
			mu := s.purgeLock(t.UID)
			mu.Lock()
			perr = s.eraseUID(ctx, t.UID)
			mu.Unlock()
		}
		if perr != nil {
			errs = append(errs, fmt.Errorf("namespace %q (uid %s): %w", t.Name, t.UID, perr))
			continue
		}
		n++
	}
	return n, errors.Join(errs...)
}

func (s *Store) purgeLock(uid string) *sync.Mutex {
	v, _ := s.purging.LoadOrStore(uid, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// purge erases an unlinked namespace once its in-flight operations drain. It is
// idempotent and may be retried after any failure.
func (s *Store) purge(ctx context.Context, st *nsState) error {
	mu := s.purgeLock(st.uid)
	mu.Lock()
	defer mu.Unlock()

	// Drain: wait until nothing holds the namespace. Nothing new can start,
	// because the state is unlinked, so this ends when the last reader closes.
	delay := time.Millisecond
	for !st.life.TryLock() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		if delay < 50*time.Millisecond {
			delay *= 2
		}
	}
	defer st.life.Unlock()

	// A close error is deliberately not fatal: the files are removed next
	// regardless, and a handle that failed to close cannot be reused.
	_ = st.closeEngine()

	if err := s.eraseUID(ctx, st.uid); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.pending, st.uid)
	s.mu.Unlock()
	s.purging.Delete(st.uid)
	return nil
}

// eraseUID deletes everything owned by uid: its index records, then its
// directory, then the tombstone. Each step is idempotent, and the tombstone —
// which is what marks the erase as outstanding — goes last.
func (s *Store) eraseUID(ctx context.Context, uid string) error {
	if err := s.idx.PurgeOwner(ctx, uid); err != nil {
		return fmt.Errorf("erase index records: %w", err)
	}
	if err := s.removeNamespaceDir(uid); err != nil {
		return fmt.Errorf("erase files: %w", err)
	}
	if err := s.idx.DeleteTombstone(ctx, uid); err != nil && !index.IsNotFound(err) {
		return fmt.Errorf("remove tombstone: %w", err)
	}
	return nil
}

// removeNamespaceDir removes DataDir/<uid>. uid is validated so a corrupt or
// forged value can never escape DataDir.
func (s *Store) removeNamespaceDir(uid string) error {
	if !validUID(uid) {
		return fmt.Errorf("refusing to remove directory for invalid namespace uid %q", uid)
	}
	return os.RemoveAll(filepath.Join(s.cfg.DataDir, uid))
}

// Namespace returns a handle for the namespace named nsID. The handle holds
// only the name: every operation resolves it afresh, so a handle never keeps a
// deleted namespace alive, and one obtained before a delete/re-create refers
// to whatever namespace currently has that name.
func (s *Store) Namespace(nsID string) *NamespaceHandle {
	return &NamespaceHandle{store: s, nsID: nsID}
}

// Stats returns usage aggregated across all namespaces. NamespaceStats entries
// are labelled with namespace names.
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
		nsStats, err := s.idx.GetStats(ctx, uidOf(ns))
		if err != nil {
			return nil, err
		}
		nsStats.NamespaceID = ns.ID
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

// ── engine open / WAL replay ──────────────────────────────────────────────────

// openEngine opens ns's volume engine, in the directory named after its UID,
// and replays its WAL.
//
// Replay re-registers blobs whose chunks reached disk but whose index commit
// never happened (a crash between WriteBlob and CommitPut). They come back
// "presumed referenced" (RefCount 1): replay cannot know whether a ref was ever
// written, and a presumed-live blob that leaks space until an operator
// intervenes is a far better failure than silently deleting one that a
// recovery is still in the middle of re-linking.
func (s *Store) openEngine(ctx context.Context, ns object.Namespace) (*nsState, error) {
	uid := uidOf(ns)
	if !validUID(uid) {
		return nil, fmt.Errorf("store: namespace %q has invalid uid %q", ns.ID, uid)
	}
	cipher, err := s.resolveCipher(ns)
	if err != nil {
		return nil, err
	}
	eng, err := volume.Open(s.cfg.DataDir, uid, volume.Options{
		PageSize:       s.cfg.PageSize,
		ChunkSize:      s.cfg.ChunkSize,
		MaxSegmentSize: s.cfg.MaxSegmentSize,
		Cipher:         cipher,
	})
	if err != nil {
		return nil, err
	}
	if err := s.replayWAL(ctx, uid, eng); err != nil {
		_ = eng.Close()
		return nil, fmt.Errorf("replay WAL: %w", err)
	}
	return &nsState{name: ns.ID, uid: uid, eng: eng}, nil
}

// resolveCipher builds ns's cipher from its record: nil if unencrypted,
// otherwise its wrapped DEK unwrapped under the master key version it was
// wrapped with.
func (s *Store) resolveCipher(ns object.Namespace) (*encryption.Cipher, error) {
	if ns.Encryption == nil || !ns.Encryption.Enabled {
		return nil, nil
	}
	if s.cfg.KeyProvider == nil {
		return nil, fmt.Errorf("store: namespace %q is encrypted but Config.KeyProvider is not set", ns.ID)
	}
	masterKey, err := s.cfg.KeyProvider.Key(ns.Encryption.KeyVersion)
	if err != nil {
		return nil, fmt.Errorf("store: resolve master key (version %q) for namespace %q: %w", ns.Encryption.KeyVersion, ns.ID, err)
	}
	dek, err := encryption.UnwrapKey(masterKey, ns.Encryption.WrappedDEK)
	if err != nil {
		return nil, fmt.Errorf("store: unwrap DEK for namespace %q: %w", ns.ID, err)
	}
	cipher, err := encryption.New(dek)
	if err != nil {
		return nil, fmt.Errorf("store: init cipher for namespace %q: %w", ns.ID, err)
	}
	return cipher, nil
}

func (s *Store) replayWAL(ctx context.Context, uid string, eng *volume.Engine) error {
	segIDs, err := eng.ListSegmentIDs()
	if err != nil {
		return fmt.Errorf("list segments for %q: %w", uid, err)
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

			if _, err := s.idx.GetBlob(ctx, uid, entry.BlobID); err == nil {
				continue // already committed via CommitPut — nothing to replay
			} else if !index.IsNotFound(err) {
				return fmt.Errorf("check blob %s: %w", entry.BlobID, err)
			}

			var totalSize int64
			chunkIDs := make([]object.ChunkID, len(entry.Chunks))
			for _, c := range entry.Chunks {
				c.RefCount = 1
				if err := s.idx.PutChunk(ctx, c); err != nil {
					return fmt.Errorf("replay chunk %s: %w", c.ChunkID, err)
				}
				totalSize += c.Length
				chunkIDs[c.Seq] = c.ChunkID
			}

			if err := s.idx.PutBlob(ctx, uid, object.BlobEntry{
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
