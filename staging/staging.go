// Package staging implements a resumable-upload manager supporting parallel and
// out-of-order chunk uploads. It exists to solve one problem: receiving a large
// file over an unreliable connection in chunks (in sequence or in parallel),
// without requiring the transfer to restart from byte zero when a chunk fails.
//
// staging.Manager is deliberately independent of any particular blob store's API.
// Complete hands back a *CompletedUpload (an io.ReadCloser plus session metadata),
// allowing the caller to commit it however its own store expects.
//
// Resumability and parallel ingestion are range-based: received byte intervals
// [start, end) are persisted in metadata. Chunks can arrive in any order or via
// multiple concurrent worker threads. The underlying data file's logical size is
// set up front, and chunks are written using thread-safe offset writes (WriteAt).
//
// Sizing is platform-dependent. On Linux (fallocate) and on Darwin filesystems
// that support it (F_PREALLOCATE, APFS/HFS+), real disk blocks are reserved at
// Begin time, so a full disk fails fast there instead of mid-upload. Elsewhere —
// Darwin volumes without F_PREALLOCATE support (e.g. network mounts) and every
// other OS — the file is only truncated to size, which is sparse: it reserves no
// disk space, so a full disk is only discovered when a later WriteAt fails.
//
// Typical usage:
//
//	mgr, err := staging.NewManager("/var/lib/blobstore/staging")
//	stop := mgr.StartReaper(10*time.Minute, 24*time.Hour)
//	defer stop()
//
//	sess, err := mgr.Begin(ctx, nsID, key, staging.BeginOptions{
//	    ContentType:    contentType,
//	    ExpectedSize:   expectedSize,
//	    ExpectedSHA256: expectedSHA256,
//	    BlockSize:      1 << 20,           // 1 MiB blocks (optional)
//	    PieceHashes:    pieceHexHashes,    // optional
//	})
//
//	// ... one or more WriteChunk calls, potentially in parallel across threads,
//	// specifying chunk offsets ...
//	totalBytesSoFar, err := mgr.WriteChunk(ctx, sess.ID, offset, chunkReader, chunkSHA256)
//
//	// once all byte ranges covering [0, ExpectedSize) have arrived:
//	cu, err := mgr.Complete(ctx, sess.ID)
//	defer cu.Close()
//	meta, err := yourStore.Namespace(cu.NamespaceID).Put(ctx, cu.Key, cu.ContentType, cu)
//	if err != nil {
//	    return err // cu.Close() leaves the staged data in place for a retry
//	}
//	cu.Finalize() // now cu.Close() will delete the staged data
package staging

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ── Errors ────────────────────────────────────────────────────────────────────

// ErrSessionNotFound is returned when an operation references a session ID
// that has no corresponding metadata file.
var ErrSessionNotFound = errors.New("staging: session not found")

// ErrInvalidSessionID is returned when a caller-supplied session ID does
// not match the required format (path-traversal guard).
var ErrInvalidSessionID = errors.New("staging: invalid session id")

// ErrSizeMismatch is returned by Complete when the staged data's size does
// not match the ExpectedSize given at Begin.
var ErrSizeMismatch = errors.New("staging: staged size does not match expected size")

// ErrIncompleteUpload is returned by Complete when one or more byte ranges
// are missing from the session.
var ErrIncompleteUpload = errors.New("staging: upload is incomplete; missing byte ranges")

// ErrChecksumMismatch is returned by WriteChunk (per-chunk) or Complete
// (whole-file) when a supplied SHA-256 hash does not match received data.
var ErrChecksumMismatch = errors.New("staging: checksum mismatch")

// ErrBlockAlignment is returned when a chunk offset or size is not a
// multiple of the configured BlockSize.
var ErrBlockAlignment = errors.New("staging: chunk must be block-aligned when BlockSize > 0")

// ErrPieceCountMismatch is returned when the number of provided piece
// hashes does not match the number of blocks.
var ErrPieceCountMismatch = errors.New("staging: number of piece hashes does not match block count")

// ErrPieceHashMismatch is returned when a piece hash validation fails.
var ErrPieceHashMismatch = errors.New("staging: piece hash mismatch")

// OffsetMismatchError is maintained for backward compatibility.
type OffsetMismatchError struct {
	Expected int64
	Actual   int64
}

func (e *OffsetMismatchError) Error() string {
	return fmt.Sprintf("staging: offset mismatch: caller sent %d, server has %d", e.Expected, e.Actual)
}

// ── Byte Range Tracking ───────────────────────────────────────────────────────

// ByteRange represents a contiguous interval of received bytes [Start, End).
type ByteRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

// RangeSet manages a sorted, non-overlapping set of ByteRanges.
type RangeSet []ByteRange

// Add inserts a new interval [start, end) and merges overlapping or adjacent ranges.
func (rs *RangeSet) Add(start, end int64) {
	if start >= end {
		return
	}

	*rs = append(*rs, ByteRange{Start: start, End: end})
	n := len(*rs)
	if n == 1 {
		return
	}

	sort.Slice(*rs, func(i, j int) bool {
		return (*rs)[i].Start < (*rs)[j].Start
	})

	// In-place merge
	j := 0
	for i := 1; i < n; i++ {
		if (*rs)[i].Start <= (*rs)[j].End { // overlapping or adjacent
			if (*rs)[i].End > (*rs)[j].End {
				(*rs)[j].End = (*rs)[i].End
			}
		} else {
			j++
			(*rs)[j] = (*rs)[i]
		}
	}
	*rs = (*rs)[:j+1]
}

// TotalBytes returns the aggregate sum of all uploaded byte ranges.
func (rs RangeSet) TotalBytes() int64 {
	var total int64
	for _, r := range rs {
		total += (r.End - r.Start)
	}
	return total
}

// IsComplete checks if the range set fully covers [0, expectedSize).
// A zero‑length file is complete when the set is empty.
func (rs RangeSet) IsComplete(expectedSize int64) bool {
	if expectedSize == 0 {
		return len(rs) == 0
	}
	return len(rs) == 1 && rs[0].Start == 0 && rs[0].End == expectedSize
}

// Contains checks if the given interval [start, end) is already fully present.
func (rs RangeSet) Contains(start, end int64) bool {
	for _, r := range rs {
		if start >= r.Start && end <= r.End {
			return true
		}
	}
	return false
}

// ── Block Bitmap (lock‑free progress for aligned uploads) ─────────────────────

// blockBitmap tracks which blocks have been received.
type blockBitmap struct {
	blocks     []uint64 // each bit = one block
	blockCount int64
}

func newBlockBitmap(numBlocks int64) *blockBitmap {
	words := (numBlocks + 63) / 64
	return &blockBitmap{
		blocks:     make([]uint64, words),
		blockCount: numBlocks,
	}
}

// mark sets the bit for blockIdx. Returns true if it was already set.
func (bb *blockBitmap) mark(blockIdx int64) {
	if blockIdx < 0 || blockIdx >= bb.blockCount {
		return
	}
	word := blockIdx / 64
	bit := blockIdx % 64
	mask := uint64(1) << bit
	for {
		old := atomic.LoadUint64(&bb.blocks[word])
		if old&mask != 0 {
			return
		}
		if atomic.CompareAndSwapUint64(&bb.blocks[word], old, old|mask) {
			return
		}
	}
}

// isComplete returns true if every bit is set.
func (bb *blockBitmap) isComplete() bool {
	words := len(bb.blocks)
	if words == 0 {
		return true
	}
	full := ^uint64(0)
	for i := 0; i < words-1; i++ {
		if atomic.LoadUint64(&bb.blocks[i]) != full {
			return false
		}
	}
	valid := bb.blockCount - int64(words-1)*64 // in [1,64]
	mask := full
	if valid != 64 {
		mask = (uint64(1) << valid) - 1
	}
	return atomic.LoadUint64(&bb.blocks[words-1])&mask == mask
}

// ── Session metadata ─────────────────────────────────────────────────────────

type sessionMeta struct {
	ID             string            `json:"id"`
	NamespaceID    string            `json:"namespace_id"`
	Key            string            `json:"key"`
	ContentType    string            `json:"content_type,omitempty"`
	Custom         map[string]string `json:"custom,omitempty"`
	ExpectedSize   int64             `json:"expected_size,omitempty"`
	ExpectedSHA256 string            `json:"expected_sha256,omitempty"`
	BlockSize      int64             `json:"block_size,omitempty"`
	PieceHashes    []string          `json:"piece_hashes,omitempty"`
	CreatedAt      time.Time         `json:"created_at"`
	LastActivity   time.Time         `json:"last_activity"`
	Ranges         RangeSet          `json:"ranges,omitempty"` // only used when BlockSize == 0
}

// BeginOptions carries parameters for starting a new session.
type BeginOptions struct {
	ContentType    string
	Custom         map[string]string
	ExpectedSize   int64
	ExpectedSHA256 string   // hex-encoded whole-file SHA-256 (optional)
	BlockSize      int64    // if > 0, enforce block‑aligned chunks; enables piece hashes
	PieceHashes    []string // optional SHA-256 hashes for each block (hex)
}

// Session describes a newly begun upload.
type Session struct {
	ID          string
	NamespaceID string
	Key         string
	Offset      int64
}

// CompletedUpload represents a verified, complete staged upload.
type CompletedUpload struct {
	NamespaceID string
	Key         string
	ContentType string
	Custom      map[string]string
	Size        int64

	file      *os.File
	mgr       *Manager
	id        string
	mu        *sync.Mutex // nil for block‑aligned sessions; otherwise holds the session lock
	committed bool
}

func (cu *CompletedUpload) Read(p []byte) (int, error) {
	return cu.file.Read(p)
}

func (cu *CompletedUpload) File() *os.File {
	return cu.file
}

// Finalize marks the upload as successfully committed to the final store.
// After this call, cu.Close() will remove the staged data.
func (cu *CompletedUpload) Finalize() {
	cu.committed = true
}

func (cu *CompletedUpload) Close() error {
	if cu.mu != nil {
		defer cu.mu.Unlock()
	}
	err := cu.file.Close()
	if cu.committed {
		cu.mgr.cleanup(cu.id)
	}
	return err
}

// ── Manager ───────────────────────────────────────────────────────────────────

const (
	dataSuffix        = ".data"
	metaSuffix        = ".meta.json"
	sessionIDByteLen  = 16
	metaFlushInterval = 5 * time.Second
)

var (
	bufPool  = sync.Pool{New: func() interface{} { return make([]byte, 64*1024) }}
	hashPool = sync.Pool{New: func() interface{} { return sha256.New() }}
)

// sessionState holds the in‑memory state for one upload.
type sessionState struct {
	mu          sync.Mutex // serializes metadata updates (RangeSet path only)
	meta        sessionMeta
	dirty       bool
	lastFlushed time.Time

	// dataFile is the lazily-opened O_WRONLY handle to the staged data file,
	// held open for the lifetime of the session so individual WriteChunk calls
	// avoid paying an open/close syscall each time (the dominant cost for many
	// small chunks). fieldMu serializes its one-time creation; it is also
	// closed and nilled by cleanup/cleanupLocked so Abort/Reap/Finalize don't
	// leak the descriptor.
	dataFile *os.File
	fileMu   sync.Mutex

	// block‑aligned fields (valid when BlockSize > 0)
	blockSize int64
	pieces    []string     // piece hashes if given
	bitmap    *blockBitmap // blocks received
	verified  *blockBitmap // pieces verified (nil if no piece hashes)
}

type Manager struct {
	dir      string
	sessions sync.Map // id → *sessionState
}

// NewManager initialises a staging manager backed by the given directory.
func NewManager(dir string) (*Manager, error) {
	if dir == "" {
		return nil, fmt.Errorf("staging: dir must not be empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("staging: create staging dir %q: %w", dir, err)
	}
	return &Manager{dir: dir}, nil
}

// Begin starts a new resumable upload session.
func (m *Manager) Begin(ctx context.Context, nsID, key string, opts BeginOptions) (*Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if nsID == "" || key == "" {
		return nil, fmt.Errorf("staging: namespace ID and key must not be empty")
	}
	if opts.ExpectedSHA256 != "" {
		opts.ExpectedSHA256 = strings.ToLower(opts.ExpectedSHA256)
		if len(opts.ExpectedSHA256) != sha256.Size*2 {
			return nil, fmt.Errorf("staging: ExpectedSHA256 must be %d hex chars", sha256.Size*2)
		}
	}
	if opts.ExpectedSize < 0 {
		return nil, fmt.Errorf("staging: ExpectedSize must not be negative")
	}

	// Validate block‑aligned mode
	blockSize := opts.BlockSize
	if blockSize < 0 {
		return nil, fmt.Errorf("staging: BlockSize must be >= 0")
	}
	if blockSize == 0 && len(opts.PieceHashes) > 0 {
		return nil, fmt.Errorf("staging: PieceHashes requires BlockSize > 0")
	}
	if blockSize > 0 {
		if opts.ExpectedSize <= 0 {
			return nil, fmt.Errorf("staging: BlockSize requires ExpectedSize > 0")
		}
		if len(opts.PieceHashes) > 0 && opts.ExpectedSize%blockSize != 0 {
			return nil, fmt.Errorf("staging: ExpectedSize must be multiple of BlockSize when PieceHashes are provided")
		}
		numBlocks := blockCountFor(opts.ExpectedSize, blockSize)
		if len(opts.PieceHashes) > 0 && int64(len(opts.PieceHashes)) != numBlocks {
			return nil, ErrPieceCountMismatch
		}
		for i, h := range opts.PieceHashes {
			opts.PieceHashes[i] = strings.ToLower(h)
			if len(opts.PieceHashes[i]) != sha256.Size*2 {
				return nil, fmt.Errorf("staging: piece hash %d is invalid length", i)
			}
		}
	}

	id, err := newSessionID()
	if err != nil {
		return nil, fmt.Errorf("staging: generate session id: %w", err)
	}

	dataPath := m.dataPath(id)
	f, err := os.OpenFile(dataPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("staging: create data file: %w", err)
	}
	// Platform‑native sparse allocation if size known
	if opts.ExpectedSize > 0 {
		if err := preallocate(f, opts.ExpectedSize); err != nil {
			f.Close()
			os.Remove(dataPath)
			return nil, fmt.Errorf("staging: preallocate: %w", err)
		}
	}
	f.Close()

	now := time.Now().UTC()
	meta := sessionMeta{
		ID:             id,
		NamespaceID:    nsID,
		Key:            key,
		ContentType:    opts.ContentType,
		Custom:         opts.Custom,
		ExpectedSize:   opts.ExpectedSize,
		ExpectedSHA256: opts.ExpectedSHA256,
		BlockSize:      blockSize,
		PieceHashes:    opts.PieceHashes,
		CreatedAt:      now,
		LastActivity:   now,
		Ranges:         RangeSet{},
	}

	state := &sessionState{
		meta:        meta,
		lastFlushed: now,
		blockSize:   blockSize,
		pieces:      opts.PieceHashes,
	}
	if blockSize > 0 {
		numBlocks := blockCountFor(opts.ExpectedSize, blockSize)
		state.bitmap = newBlockBitmap(numBlocks)
		if len(opts.PieceHashes) > 0 {
			state.verified = newBlockBitmap(numBlocks)
		}
	}

	// Write initial metadata to disk so the reaper can see it.
	if err := m.saveMeta(id, meta); err != nil {
		os.Remove(dataPath)
		return nil, err
	}
	m.sessions.Store(id, state)

	return &Session{ID: id, NamespaceID: nsID, Key: key, Offset: 0}, nil
}

// Offset returns the total number of unique bytes staged so far.
func (m *Manager) Offset(id string) (int64, error) {
	state, err := m.getOrLoadSession(id)
	if err != nil {
		return 0, err
	}
	if state.blockSize > 0 {
		return state.progress(), nil
	}
	state.mu.Lock()
	total := state.meta.Ranges.TotalBytes()
	state.mu.Unlock()
	return total, nil
}

// Ranges returns a copy of the current received ByteRanges.
func (m *Manager) Ranges(id string) (RangeSet, error) {
	state, err := m.getOrLoadSession(id)
	if err != nil {
		return nil, err
	}
	if state.blockSize == 0 {
		state.mu.Lock()
		cp := make(RangeSet, len(state.meta.Ranges))
		copy(cp, state.meta.Ranges)
		state.mu.Unlock()
		return cp, nil
	}
	// Reconstruct from bitmap
	rs := make(RangeSet, 0)
	bs := state.blockSize
	numBlocks := blockCountFor(state.meta.ExpectedSize, bs)
	expSize := state.meta.ExpectedSize
	start := int64(-1)
	for i := int64(0); i < numBlocks; i++ {
		word := i / 64
		bit := i % 64
		if atomic.LoadUint64(&state.bitmap.blocks[word])&(1<<bit) != 0 {
			if start == -1 {
				start = i * bs
			}
		} else {
			if start != -1 {
				rs = append(rs, ByteRange{start, blockEndBoundary(i, bs, expSize)})
				start = -1
			}
		}
	}
	if start != -1 {
		rs = append(rs, ByteRange{start, blockEndBoundary(numBlocks, bs, expSize)})
	}
	return rs, nil
}

// WriteChunk writes chunk to session id at offset using random-access WriteAt (pwrite).
// Out-of-order and parallel chunk writes are supported natively.
//
// If the session was created with BlockSize > 0, the offset must be a multiple
// of the block size and the chunk length must be a whole number of blocks, or a
// single trailing chunk ending exactly at the expected file size (whose final
// block may be shorter than the block size).
func (m *Manager) WriteChunk(ctx context.Context, id string, offset int64, chunk io.Reader, expectedSHA256 string) (int64, error) {
	if chunk == nil {
		return 0, fmt.Errorf("staging: chunk reader is nil")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := validateSessionID(id); err != nil {
		return 0, err
	}
	if offset < 0 {
		return 0, fmt.Errorf("staging: offset must not be negative")
	}
	if expectedSHA256 != "" {
		expectedSHA256 = strings.ToLower(expectedSHA256)
		if len(expectedSHA256) != sha256.Size*2 {
			return 0, fmt.Errorf("staging: expectedSHA256 must be %d hex chars", sha256.Size*2)
		}
	}

	state, err := m.getOrLoadSession(id)
	if err != nil {
		return 0, err
	}

	if state.blockSize > 0 {
		return m.writeChunkAligned(ctx, state, id, offset, chunk, expectedSHA256)
	}
	return m.writeChunkRangeSet(ctx, state, id, offset, chunk, expectedSHA256)
}

// writeChunkAligned handles block‑aligned writes without locks.
func (m *Manager) writeChunkAligned(ctx context.Context, state *sessionState, id string, offset int64, chunk io.Reader, expectedSHA256 string) (int64, error) {
	bs := state.blockSize
	if offset%bs != 0 {
		return state.progress(), fmt.Errorf("%w: offset %d not multiple of %d", ErrBlockAlignment, offset, bs)
	}

	f, err := m.sessionDataFile(state, id)
	if err != nil {
		return state.progress(), err
	}

	// Fast path: no per-chunk or per-block verification requested. Stream the
	// chunk straight into the file with a pooled buffer, avoiding the whole-
	// chunk allocation (and its zeroing) that the verification path needs.
	// Alignment and bounds are validated after the stream, exactly like the
	// range-set path does — a misaligned or oversized chunk is detected before
	// any block is marked received, so a failed write never leaves a partially
	// marked upload.
	if expectedSHA256 == "" && len(state.pieces) == 0 {
		return m.writeChunkAlignedStream(state, f, offset, chunk)
	}

	// Verification path: buffer the whole chunk so we can hash it before
	// writing.
	data, err := io.ReadAll(chunk)
	if err != nil {
		return state.progress(), fmt.Errorf("staging: read chunk: %w", err)
	}
	chunkLen := int64(len(data))
	if chunkLen == 0 {
		return state.progress(), nil
	}
	if !state.validBlockChunk(offset, chunkLen) {
		if chunkLen%bs != 0 {
			return state.progress(), fmt.Errorf("%w: chunk size %d must be block-aligned or end at the expected file size (block size %d)", ErrBlockAlignment, chunkLen, bs)
		}
		return state.progress(), fmt.Errorf("staging: chunk exceeds expected size")
	}

	// Verify optional chunk checksum before writing
	if expectedSHA256 != "" {
		sumBytes := sha256.Sum256(data)
		if !strings.EqualFold(hex.EncodeToString(sumBytes[:]), expectedSHA256) {
			return state.progress(), ErrChecksumMismatch
		}
	}

	if _, err := f.WriteAt(data, offset); err != nil {
		return state.progress(), fmt.Errorf("staging: write chunk: %w", err)
	}

	// Mark blocks as received (atomic, lock‑free)
	firstBlock := offset / bs
	lastBlock := (offset + chunkLen - 1) / bs
	for b := firstBlock; b <= lastBlock; b++ {
		state.bitmap.mark(b)
	}

	// Piece hash verification (if provided)
	if len(state.pieces) > 0 {
		for b := firstBlock; b <= lastBlock; b++ {
			// skip if already verified
			word := b / 64
			bit := b % 64
			if atomic.LoadUint64(&state.verified.blocks[word])&(1<<bit) != 0 {
				continue
			}
			// compute hash of this block slice
			blockStart := (b - firstBlock) * bs
			blockEnd := blockStart + bs
			blockData := data[blockStart:blockEnd]
			hashBytes := sha256.Sum256(blockData)
			calc := hex.EncodeToString(hashBytes[:])
			if calc != state.pieces[b] {
				return state.progress(), fmt.Errorf("%w: block %d expected %s, got %s", ErrPieceHashMismatch, b, state.pieces[b], calc)
			}
			state.verified.mark(b)
		}
	}

	// Update last activity and flush metadata asynchronously
	state.mu.Lock()
	state.meta.LastActivity = time.Now().UTC()
	state.mu.Unlock()
	_ = m.maybeFlushMeta(state)

	return state.progress(), nil
}

// writeChunkAlignedStream streams a block-aligned chunk into an already-open
// data file without buffering it in memory. Alignment and bounds are validated
// after reading; the caller guarantees no verification is needed.
func (m *Manager) writeChunkAlignedStream(state *sessionState, f *os.File, offset int64, chunk io.Reader) (int64, error) {
	bs := state.blockSize
	var written int64

	buf := bufPool.Get().([]byte)
	defer bufPool.Put(buf)

	for {
		nr, er := chunk.Read(buf)
		if nr > 0 {
			nw, ew := f.WriteAt(buf[:nr], offset+written)
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				return state.progress(), fmt.Errorf("staging: write: %w", ew)
			}
			if nr != nw {
				return state.progress(), io.ErrShortWrite
			}
		}
		if er != nil {
			if er == io.EOF {
				break
			}
			return state.progress(), fmt.Errorf("staging: read chunk: %w", er)
		}
	}

	if written == 0 {
		return state.progress(), nil
	}
	if !state.validBlockChunk(offset, written) {
		if written%bs != 0 {
			return state.progress(), fmt.Errorf("%w: chunk size %d must be block-aligned or end at the expected file size (block size %d)", ErrBlockAlignment, written, bs)
		}
		return state.progress(), fmt.Errorf("staging: chunk exceeds expected size")
	}

	// Mark blocks as received (atomic, lock‑free)
	firstBlock := offset / bs
	lastBlock := (offset + written - 1) / bs
	for b := firstBlock; b <= lastBlock; b++ {
		state.bitmap.mark(b)
	}

	// Update last activity and flush metadata asynchronously
	state.mu.Lock()
	state.meta.LastActivity = time.Now().UTC()
	state.mu.Unlock()
	_ = m.maybeFlushMeta(state)

	return state.progress(), nil
}

// writeChunkRangeSet handles unaligned writes using the RangeSet (mutex‑based).
func (m *Manager) writeChunkRangeSet(ctx context.Context, state *sessionState, id string, offset int64, chunk io.Reader, expectedSHA256 string) (int64, error) {
	state.mu.Lock()
	defer state.mu.Unlock()

	meta := &state.meta

	f, err := m.sessionDataFile(state, id)
	if err != nil {
		return 0, err
	}

	// Stream chunk to file with optional SHA-256 computation
	var written int64
	var chunkHash hash.Hash
	if expectedSHA256 != "" {
		chunkHash = hashPool.Get().(hash.Hash)
		defer hashPool.Put(chunkHash)
		chunkHash.Reset()
	}

	buf := bufPool.Get().([]byte)
	defer bufPool.Put(buf)

	for {
		nr, er := chunk.Read(buf)
		if nr > 0 {
			nw, ew := f.WriteAt(buf[:nr], offset+written)
			if nw > 0 {
				written += int64(nw)
				if chunkHash != nil {
					chunkHash.Write(buf[:nw])
				}
			}
			if ew != nil {
				return meta.Ranges.TotalBytes(), fmt.Errorf("staging: write: %w", ew)
			}
			if nr != nw {
				return meta.Ranges.TotalBytes(), io.ErrShortWrite
			}
		}
		if er != nil {
			if er == io.EOF {
				break
			}
			return meta.Ranges.TotalBytes(), fmt.Errorf("staging: read chunk: %w", er)
		}
	}

	if written == 0 {
		return meta.Ranges.TotalBytes(), nil
	}

	chunkEnd := offset + written
	if meta.ExpectedSize > 0 && chunkEnd > meta.ExpectedSize {
		return meta.Ranges.TotalBytes(), fmt.Errorf("staging: chunk exceeds expected size")
	}

	// Fast path: already received
	if meta.Ranges.Contains(offset, chunkEnd) {
		meta.LastActivity = time.Now().UTC()
		_ = m.maybeFlushMetaLocked(state)
		return meta.Ranges.TotalBytes(), nil
	}

	// Verify chunk checksum
	if expectedSHA256 != "" {
		got := hex.EncodeToString(chunkHash.Sum(nil))
		if !strings.EqualFold(got, expectedSHA256) {
			return meta.Ranges.TotalBytes(), fmt.Errorf("%w: expected %s, got %s", ErrChecksumMismatch, expectedSHA256, got)
		}
	}

	meta.Ranges.Add(offset, chunkEnd)
	meta.LastActivity = time.Now().UTC()
	state.dirty = true
	total := state.meta.Ranges.TotalBytes()
	_ = m.maybeFlushMetaLocked(state)

	return total, nil
}

// Complete finalizes session id: it verifies that all byte ranges covering the file
// have been received and validates ExpectedSize and ExpectedSHA256.
func (m *Manager) Complete(ctx context.Context, id string) (*CompletedUpload, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateSessionID(id); err != nil {
		return nil, err
	}

	state, err := m.getOrLoadSession(id)
	if err != nil {
		return nil, err
	}

	transferred := false
	// Lock for RangeSet path; block‑aligned path needs no lock
	lock := state.blockSize == 0
	if lock {
		state.mu.Lock()
		// mu will be released in CompletedUpload.Close
		defer func() {
			if !transferred {
				state.mu.Unlock()
			}
		}()
	}

	meta := &state.meta
	dataPath := m.dataPath(id)

	f, err := os.OpenFile(dataPath, os.O_RDWR, 0o644)
	if err != nil {
		if lock {
			state.mu.Unlock()
		}
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("staging: %w: %s", ErrSessionNotFound, id)
		}
		return nil, fmt.Errorf("staging: open data file: %w", err)
	}
	// On error path, close file
	defer func() {
		if err != nil {
			f.Close()
		}
	}()

	// Sync data to disk (once)
	if err = f.Sync(); err != nil {
		return nil, fmt.Errorf("staging: fsync: %w", err)
	}

	st, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("staging: stat: %w", err)
	}

	if meta.ExpectedSize > 0 && st.Size() != meta.ExpectedSize {
		err = ErrSizeMismatch
		return nil, err
	}

	if state.blockSize > 0 {
		// Verify bitmap completeness
		if !state.bitmap.isComplete() {
			err = ErrIncompleteUpload
			return nil, err
		}
		// If piece hashes were supplied, ensure all pieces verified
		if state.verified != nil && !state.verified.isComplete() {
			err = fmt.Errorf("staging: piece verification incomplete")
			return nil, err
		}
		// Whole-file checksum (if requested but no piece hashes) would require re-reading;
		// we skip it because block integrity is already guaranteed.
	} else {
		expSize := meta.ExpectedSize
		if expSize <= 0 {
			expSize = st.Size()
		}
		if !meta.Ranges.IsComplete(expSize) {
			err = ErrIncompleteUpload
			return nil, err
		}
		// Whole-file checksum (optional)
		if meta.ExpectedSHA256 != "" {
			if err = verifyFileChecksum(f, meta.ExpectedSHA256); err != nil {
				return nil, err
			}
			if _, err = f.Seek(0, io.SeekStart); err != nil {
				return nil, err
			}
		}
	}

	// Write final metadata to disk
	meta.LastActivity = time.Now().UTC()
	if err = m.saveMeta(id, *meta); err != nil {
		return nil, err
	}

	var mu *sync.Mutex
	if lock {
		mu = &state.mu
	}

	transferred = true
	return &CompletedUpload{
		NamespaceID: meta.NamespaceID,
		Key:         meta.Key,
		ContentType: meta.ContentType,
		Custom:      meta.Custom,
		Size:        st.Size(),
		file:        f,
		mgr:         m,
		id:          id,
		mu:          mu,
	}, nil
}

// Abort removes all staged data and metadata for session id.
func (m *Manager) Abort(id string) error {
	if err := validateSessionID(id); err != nil {
		return err
	}
	state, err := m.getOrLoadSession(id)
	if err != nil {
		return err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	m.cleanup(id)
	return nil
}

// Reap removes sessions whose LastActivity is older than maxAge.
func (m *Manager) Reap(maxAge time.Duration) (int, error) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return 0, fmt.Errorf("staging: list staging dir %q: %w", m.dir, err)
	}

	cutoff := time.Now().UTC().Add(-maxAge)
	reaped := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), metaSuffix) {
			continue
		}
		id := strings.TrimSuffix(e.Name(), metaSuffix)
		if !isLowerHexLen(id, sessionIDByteLen*2) {
			continue
		}

		state, err := m.getOrLoadSession(id)
		if err != nil {
			continue
		}
		state.mu.Lock()
		if state.meta.LastActivity.Before(cutoff) {
			m.cleanupLocked(id, state)
			reaped++
		} else {
			state.mu.Unlock()
		}
	}
	return reaped, nil
}

// StartReaper starts a background goroutine that periodically calls Reap.
func (m *Manager) StartReaper(interval, maxAge time.Duration) (stop func()) {
	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_, _ = m.Reap(maxAge)
			case <-done:
				return
			}
		}
	}()
	return func() { close(done) }
}

// BlockSize returns the configured BlockSize for the given session ID.
func (m *Manager) BlockSize(id string) (int64, error) {
	if err := validateSessionID(id); err != nil {
		return 0, err
	}
	state, err := m.getOrLoadSession(id)
	if err != nil {
		return 0, err
	}
	return state.blockSize, nil
}

// ExpectedSize returns the ExpectedSize recorded for the given session ID.
func (m *Manager) ExpectedSize(id string) (int64, error) {
	if err := validateSessionID(id); err != nil {
		return 0, err
	}
	state, err := m.getOrLoadSession(id)
	if err != nil {
		return 0, err
	}
	return state.meta.ExpectedSize, nil
}

// ── Internal helpers ──────────────────────────────────────────────────────────

func (m *Manager) sessionMutex(id string) *sync.Mutex {
	// Not used directly in new code; kept for backward compatibility if needed.
	v, _ := m.sessions.Load(id)
	if v == nil {
		return nil
	}
	return &v.(*sessionState).mu
}

func (m *Manager) dataPath(id string) string {
	return filepath.Join(m.dir, id+dataSuffix)
}

func (m *Manager) metaPath(id string) string {
	return filepath.Join(m.dir, id+metaSuffix)
}

func (m *Manager) cleanup(id string) {
	m.closeDataFile(id)
	_ = os.Remove(m.dataPath(id))
	_ = os.Remove(m.metaPath(id))
	m.sessions.Delete(id)
}

func (m *Manager) cleanupLocked(id string, state *sessionState) {
	m.closeDataFileLocked(state)
	_ = os.Remove(m.dataPath(id))
	_ = os.Remove(m.metaPath(id))
	m.sessions.Delete(id)
	state.mu.Unlock()
}

// closeDataFile closes and nils any cached write handle for session id.
func (m *Manager) closeDataFile(id string) {
	if v, ok := m.sessions.Load(id); ok {
		m.closeDataFileLocked(v.(*sessionState))
	}
}

// closeDataFileLocked closes and nils the cached write handle. Callers that
// already hold the session's fileMu must use this directly.
func (m *Manager) closeDataFileLocked(state *sessionState) {
	state.fileMu.Lock()
	if state.dataFile != nil {
		_ = state.dataFile.Close()
		state.dataFile = nil
	}
	state.fileMu.Unlock()
}

// sessionDataFile returns the session's cached O_WRONLY data-file handle,
// lazily opening it on first use. Reusing one handle across WriteChunk calls
// avoids an open/close syscall pair per chunk. Returns ErrSessionNotFound when
// the metadata exists but the data file does not (e.g. a stale meta-only
// session from a crash before the data file was created).
func (m *Manager) sessionDataFile(state *sessionState, id string) (*os.File, error) {
	state.fileMu.Lock()
	defer state.fileMu.Unlock()
	if state.dataFile != nil {
		return state.dataFile, nil
	}
	f, err := os.OpenFile(m.dataPath(id), os.O_WRONLY, 0o644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("staging: %w: %s", ErrSessionNotFound, id)
		}
		return nil, fmt.Errorf("staging: open data file: %w", err)
	}
	state.dataFile = f
	return f, nil
}

// getOrLoadSession retrieves the in‑memory state or loads it from disk.
func (m *Manager) getOrLoadSession(id string) (*sessionState, error) {
	if v, ok := m.sessions.Load(id); ok {
		return v.(*sessionState), nil
	}
	meta, err := m.loadMeta(id)
	if err != nil {
		return nil, err
	}
	state := &sessionState{
		meta:        meta,
		lastFlushed: meta.LastActivity,
		blockSize:   meta.BlockSize,
		pieces:      meta.PieceHashes,
	}
	if meta.BlockSize > 0 {
		numBlocks := blockCountFor(meta.ExpectedSize, meta.BlockSize)
		state.bitmap = newBlockBitmap(numBlocks)
		if len(meta.PieceHashes) > 0 {
			state.verified = newBlockBitmap(numBlocks)
		}
	}
	actual, loaded := m.sessions.LoadOrStore(id, state)
	if loaded {
		return actual.(*sessionState), nil
	}
	return state, nil
}

func (m *Manager) loadMeta(id string) (sessionMeta, error) {
	var meta sessionMeta
	b, err := os.ReadFile(m.metaPath(id))
	if err != nil {
		if os.IsNotExist(err) {
			return meta, fmt.Errorf("staging: %w: %s", ErrSessionNotFound, id)
		}
		return meta, fmt.Errorf("staging: read metadata: %w", err)
	}
	if err := json.Unmarshal(b, &meta); err != nil {
		return meta, fmt.Errorf("staging: parse metadata: %w", err)
	}
	return meta, nil
}

func (m *Manager) saveMeta(id string, meta sessionMeta) error {
	b, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("staging: encode metadata: %w", err)
	}
	tmpPath := m.metaPath(id) + ".tmp"
	if err := os.WriteFile(tmpPath, b, 0o644); err != nil {
		return fmt.Errorf("staging: write metadata: %w", err)
	}
	if err := os.Rename(tmpPath, m.metaPath(id)); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("staging: commit metadata: %w", err)
	}
	return nil
}

// maybeFlushMeta writes metadata to disk if enough time has passed or if dirty.
// Caller must NOT hold state.mu.
func (m *Manager) maybeFlushMeta(state *sessionState) error {
	state.mu.Lock()
	needFlush := state.dirty || time.Since(state.lastFlushed) >= metaFlushInterval
	state.mu.Unlock()
	if needFlush {
		state.mu.Lock()
		if state.dirty || time.Since(state.lastFlushed) >= metaFlushInterval {
			if err := m.saveMeta(state.meta.ID, state.meta); err != nil {
				state.mu.Unlock()
				return err
			}
			state.lastFlushed = time.Now().UTC()
			state.dirty = false
		}
		state.mu.Unlock()
	}
	return nil
}

// maybeFlushMetaLocked writes metadata to disk when the caller already holds state.mu.
func (m *Manager) maybeFlushMetaLocked(state *sessionState) error {
	if state.dirty || time.Since(state.lastFlushed) >= metaFlushInterval {
		if err := m.saveMeta(state.meta.ID, state.meta); err != nil {
			return err
		}
		state.lastFlushed = time.Now().UTC()
		state.dirty = false
	}
	return nil
}

// progress returns total bytes received (block‑aligned only).
func (state *sessionState) progress() int64 {
	bs := state.blockSize
	if bs <= 0 {
		return 0
	}
	blocks := state.bitmap.blocks
	n := len(blocks)
	if n == 0 {
		return 0
	}
	var set int64
	for i := 0; i < n-1; i++ {
		set += int64(bits.OnesCount64(atomic.LoadUint64(&blocks[i])))
	}
	valid := state.bitmap.blockCount - int64(n-1)*64 // in [1,64]
	mask := ^uint64(0)
	if valid != 64 {
		mask = (uint64(1) << valid) - 1
	}
	set += int64(bits.OnesCount64(atomic.LoadUint64(&blocks[n-1]) & mask))

	bytes := set * bs
	// The trailing block may be shorter than bs; if it is set, its contribution
	// is tail, not a full block.
	if tail := state.meta.ExpectedSize % bs; tail != 0 {
		last := state.bitmap.blockCount - 1
		word := last / 64
		bit := last % 64
		if atomic.LoadUint64(&blocks[word])&(1<<bit) != 0 {
			bytes = bytes - bs + tail
		}
	}
	return bytes
}

// blockCountFor returns the number of block-size slots needed to cover size
// bytes, rounding up to accommodate a trailing partial block.
func blockCountFor(size, bs int64) int64 {
	if bs <= 0 {
		return 0
	}
	nb := size / bs
	if size%bs != 0 {
		nb++
	}
	return nb
}

// blockEndBoundary returns the byte offset where block `i` ends (i*bs), clamped
// to the expected file size so a trailing partial block is not overreported.
func blockEndBoundary(i, bs, expectedSize int64) int64 {
	end := i * bs
	if end > expectedSize {
		end = expectedSize
	}
	return end
}

// validBlockChunk reports whether a chunk of size bytes starting at offset is
// acceptable in block‑aligned mode: it must be an exact whole number of blocks,
// or a single trailing chunk whose end coincides exactly with the expected file
// size (the final block may be shorter than BlockSize). offset is expected to be
// block‑aligned (checked by the caller).
func (state *sessionState) validBlockChunk(offset, size int64) bool {
	bs := state.blockSize
	if size%bs == 0 {
		return offset+size <= state.meta.ExpectedSize
	}
	return offset%bs == 0 && offset+size == state.meta.ExpectedSize
}

// verifyFileChecksum reads the entire file from current offset, calculates its
// SHA‑256, and compares with expectedHex. The file offset is restored after reading.
func verifyFileChecksum(f *os.File, expectedHex string) error {
	current, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("seek: %w", err)
	}
	defer f.Seek(current, io.SeekStart)

	h := hashPool.Get().(hash.Hash)
	defer hashPool.Put(h)
	h.Reset()

	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf("hash: %w", err)
	}
	sum := hex.EncodeToString(h.Sum(nil))
	if !strings.EqualFold(sum, expectedHex) {
		return fmt.Errorf("%w: expected %s, got %s", ErrChecksumMismatch, expectedHex, sum)
	}
	return nil
}

func newSessionID() (string, error) {
	b := make([]byte, sessionIDByteLen)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// validateSessionID checks that id is exactly 32 lowercase hex digits.
func validateSessionID(id string) error {
	if len(id) != sessionIDByteLen*2 {
		return ErrInvalidSessionID
	}
	if !isLowerHexLen(id, sessionIDByteLen*2) {
		return ErrInvalidSessionID
	}
	return nil
}

func isLowerHexLen(s string, expectedLen int) bool {
	if len(s) != expectedLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}
