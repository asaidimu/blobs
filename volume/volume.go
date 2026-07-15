// Package volume implements the physical storage engine for the blobstore.
//
// # Design principles
//
//  1. Mechanical sympathy — 88-byte pageHeader, largest fields first,
//     no implicit padding. magicFlags packs sentinel + PageFlags in one word.
//
//  2. Minimal syscalls — one Write per page (pooled assembly buffer).
//     ReadChunk uses ReadAt per page. MarkDeleted: ReadAt + WriteAt on
//     the 4-byte magicFlags word. WAL file kept open beside the segment
//     (eliminates os.OpenFile + syscall.ByteSliceFromString per blob).
//
//  3. Zero-allocation hot paths:
//     - chunkBuf, pageBuf, sha256 hasher from sync.Pool
//     - blobHasher.Sum into a stack array — no heap digest slice
//     - BlobID encoded from stack buffer, converted directly to string
//     - ChunkID built from a pooled scratch buffer — no fmt.Sprintf
//     - patches slice pre-allocated cap(1); chunks slice pre-allocated cap(1)
//     - WAL write buffer grown once per blob, from pool
//
//  4. Lock scope minimised — io.Reader streaming, hashing, and CRC happen
//     with NO lock held. Lock acquired only for bw.Write(pageBuf) per chunk.
//
// # PageHeader on-disk layout (88 bytes, no implicit padding)
//
//	 0  dataLen      8    uint64
//	 8  chunkID     32    [32]byte
//	40  blobID      32    [32]byte
//	72  chunkSeq    4     uint32
//	76  totalChunks 4     uint32
//	80  crc32       4     uint32
//	84  magicFlags  4     upper 24: 0xB10B5E  lower 8: PageFlags
//	                      88 total
//
// # Segment file layout
//
//	[SegmentFileHeader  128 bytes]
//	[Page 0 … N]  each exactly pageSize bytes: header(88) + payload + padding
//
// # WAL entry layout (appended per blob, written to the kept-open wal file)
//
//	magic(4) | blobIDLen(2) | blobID | chunkCount(4) |
//	  per chunk: chunkIDLen(2) | chunkID | segSeq(8) | offset(8) | length(8)
package volume

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/asaidimu/blobs/errors"
	"github.com/asaidimu/blobs/object"
)

// ── Constants ─────────────────────────────────────────────────────────────────

const (
	walMagicVal      = uint32(0xBA1FACE)
	segmentFileMagic = uint32(0xB10B5EED)
	segmentVersion   = uint16(1)
	pageHeaderSize   = 88
	segHeaderSize    = 128
	segWriteBufSize  = 256 * 1024

	magicSentinel = uint32(0xB10B5E) << 8 // 0xB10B5E00
	magicMask     = uint32(0xFFFFFF00)
	flagsMask     = uint32(0x000000FF)

	// sha256HexLen is the byte length of a hex-encoded SHA-256 digest.
	sha256HexLen = sha256.Size * 2 // 64

	// hashPrefix is prepended to every content address.
	hashPrefix    = "sha256:"
	hashPrefixLen = len(hashPrefix) // 7

	// blobIDLen is the total length of a BlobID string: "sha256:" + 64 hex chars.
	blobIDLen = hashPrefixLen + sha256HexLen // 71

	// maxChunkSeq is the largest valid 0-based chunk sequence number.
	// chunkIDFromBlobID uses variable-width decimal; this cap ensures
	// no two distinct sequence numbers produce the same string, since
	// the scratch buffer holds up to 10 decimal digits (uint32 max).
	maxChunkSeq = math.MaxUint32 // 4 294 967 295

	// chunkIDScratchLen is the max length of a ChunkID string:
	// blobIDLen(71) + "#"(1) + max uint32 decimal(10) = 82.
	chunkIDScratchLen = blobIDLen + 1 + 10 // 82
)

// Default tuning values.
const (
	DefaultPageSize       = 16 * 1024
	DefaultChunkSize      = 4 * 1024 * 1024
	DefaultMaxSegmentSize = 512 * 1024 * 1024
)

// ── PageFlags ─────────────────────────────────────────────────────────────────

// PageFlags occupies the lower 8 bits of the magicFlags word.
//
//	0  FlagDeleted    unreferenced; eligible for compaction
//	1  FlagLastChunk  final chunk of the blob
//	2  FlagCompressed reserved
type PageFlags uint8

const (
	FlagDeleted    PageFlags = 1 << 0
	FlagLastChunk  PageFlags = 1 << 1
	FlagCompressed PageFlags = 1 << 2
)

func (f PageFlags) IsDeleted() bool                { return f&FlagDeleted != 0 }
func (f PageFlags) IsLastChunk() bool              { return f&FlagLastChunk != 0 }
func (f PageFlags) Set(flag PageFlags) PageFlags   { return f | flag }
func (f PageFlags) Clear(flag PageFlags) PageFlags { return f &^ flag }

func encodeMagicFlags(flags PageFlags) uint32 { return magicSentinel | uint32(flags) }

func decodeMagicFlags(v uint32) (PageFlags, error) {
	if v&magicMask != magicSentinel {
		return 0, fmt.Errorf("volume: bad page magic %08x", v)
	}
	return PageFlags(v & flagsMask), nil
}

// ── Content address encoding — zero allocation ────────────────────────────────

// blobIDFromDigest encodes a raw SHA-256 digest into a BlobID.
// Uses a fixed-size stack buffer; the string conversion is the only
// unavoidable allocation (Go always copies []byte→string).
func blobIDFromDigest(digest *[sha256.Size]byte) object.BlobID {
	var buf [blobIDLen]byte
	copy(buf[:hashPrefixLen], hashPrefix)
	hex.Encode(buf[hashPrefixLen:], digest[:])
	return object.BlobID(buf[:]) // one allocation: string header
}

// chunkIDPool pools scratch buffers for ChunkID construction.
var chunkIDPool = sync.Pool{
	New: func() any {
		b := make([]byte, chunkIDScratchLen)
		return &b
	},
}

// chunkIDFromBlobID constructs a ChunkID from a BlobID and 0-based sequence.
// Format: "<blobID>#<decimal_seq>".
//
// Security invariants enforced here:
//  1. seq must be <= maxChunkSeq (uint32 max). A seq value beyond this would
//     require > 4 billion chunks per blob — physically impossible — but we
//     guard it explicitly so future callers cannot produce colliding ChunkIDs.
//  2. len(blobID) must equal blobIDLen (71). An oversized BlobID would
//     overflow the scratch buffer and silently truncate the seq field,
//     causing two different chunks to share the same ChunkID (index aliasing).
func chunkIDFromBlobID(blobID object.BlobID, seq int) object.ChunkID {
	if seq < 0 || seq > maxChunkSeq {
		panic(fmt.Sprintf("volume: chunk sequence %d out of range [0, %d]", seq, maxChunkSeq))
	}
	if len(blobID) != blobIDLen {
		panic(fmt.Sprintf("volume: BlobID length %d != expected %d — non-canonical BlobID would corrupt ChunkID", len(blobID), blobIDLen))
	}

	p := chunkIDPool.Get().(*[]byte)
	scratch := *p

	n := copy(scratch, blobID)
	scratch[n] = '#'
	n++
	n += appendDecimal(scratch[n:], uint64(seq))

	result := object.ChunkID(scratch[:n]) // one allocation: string copy
	chunkIDPool.Put(p)
	return result
}

// appendDecimal writes v as a decimal integer into dst and returns the number
// of bytes written. Variable-width — no leading zeros, no fixed-width truncation.
// Uses no allocation. dst must be at least 20 bytes (max uint64 decimal length).
func appendDecimal(dst []byte, v uint64) int {
	if v == 0 {
		dst[0] = '0'
		return 1
	}
	// Write digits in reverse, then flip.
	var tmp [20]byte
	pos := 20
	for v > 0 {
		pos--
		tmp[pos] = byte('0' + v%10)
		v /= 10
	}
	n := copy(dst, tmp[pos:])
	return n
}

// ── pageHeader ────────────────────────────────────────────────────────────────

// pageHeader is the internal 88-byte prefix of every page.
// Field order: uint64 → [32]byte×2 → uint32×4. No implicit padding.
type pageHeader struct {
	DataLen     uint64   //  0
	ChunkID     [32]byte //  8
	BlobID      [32]byte // 40
	ChunkSeq    uint32   // 72
	TotalChunks uint32   // 76
	CRC32       uint32   // 80
	MagicFlags  uint32   // 84
}

// PageHeader is the exported view used in ScanSegments callbacks.
type PageHeader struct {
	ChunkID     [32]byte
	BlobID      [32]byte
	ChunkSeq    uint32
	TotalChunks uint32
	DataLen     uint64
	Flags       PageFlags
}

// On-disk byte offsets — named so encode/decode/patch stay in sync.
const (
	phOffDataLen     = int64(0)
	phOffChunkID     = int64(8)
	phOffBlobID      = int64(40)
	phOffChunkSeq    = int64(72)
	phOffTotalChunks = int64(76)
	phOffCRC32       = int64(80)
	phOffMagicFlags  = int64(84)
)

// encodePageHeader serialises h into dst[:pageHeaderSize]. No allocation.
func encodePageHeader(h pageHeader, dst []byte) {
	binary.LittleEndian.PutUint64(dst[0:], h.DataLen)
	copy(dst[8:40], h.ChunkID[:])
	copy(dst[40:72], h.BlobID[:])
	binary.LittleEndian.PutUint32(dst[72:], h.ChunkSeq)
	binary.LittleEndian.PutUint32(dst[76:], h.TotalChunks)
	binary.LittleEndian.PutUint32(dst[80:], h.CRC32)
	binary.LittleEndian.PutUint32(dst[84:], h.MagicFlags)
}

func decodePageHeader(b []byte) (pageHeader, error) {
	if len(b) < pageHeaderSize {
		return pageHeader{}, fmt.Errorf("volume: page header too short (%d bytes)", len(b))
	}
	mf := binary.LittleEndian.Uint32(b[84:])
	if _, err := decodeMagicFlags(mf); err != nil {
		return pageHeader{}, err
	}
	h := pageHeader{
		DataLen:     binary.LittleEndian.Uint64(b[0:]),
		ChunkSeq:    binary.LittleEndian.Uint32(b[72:]),
		TotalChunks: binary.LittleEndian.Uint32(b[76:]),
		CRC32:       binary.LittleEndian.Uint32(b[80:]),
		MagicFlags:  mf,
	}
	copy(h.ChunkID[:], b[8:40])
	copy(h.BlobID[:], b[40:72])
	return h, nil
}

// ── Segment file header ───────────────────────────────────────────────────────

type segFileHeader struct {
	Magic     uint32
	Version   uint16
	SegSeq    uint64
	PageSize  uint32
	CreatedAt int64
	NSID      [64]byte
}

func encodeSegFileHeader(h segFileHeader) []byte {
	b := make([]byte, segHeaderSize)
	binary.LittleEndian.PutUint32(b[0:], h.Magic)
	binary.LittleEndian.PutUint16(b[4:], h.Version)
	binary.LittleEndian.PutUint64(b[8:], h.SegSeq)
	binary.LittleEndian.PutUint32(b[16:], h.PageSize)
	binary.LittleEndian.PutUint64(b[20:], uint64(h.CreatedAt))
	copy(b[28:], h.NSID[:])
	return b
}

// ── Options ───────────────────────────────────────────────────────────────────

// Options configures a volume Engine.
type Options struct {
	PageSize       int
	ChunkSize      int64
	MaxSegmentSize int64
}

func (o Options) withDefaults() Options {
	if o.PageSize == 0 {
		o.PageSize = DefaultPageSize
	}
	if o.ChunkSize == 0 {
		o.ChunkSize = DefaultChunkSize
	}
	if o.MaxSegmentSize == 0 {
		o.MaxSegmentSize = DefaultMaxSegmentSize
	}
	return o
}

// Validate reports whether o is safe to use. A zero field is never
// rejected — it means "use the package default", and the defaults are
// always valid — but an explicitly-set nonsensical value is rejected
// before it can reach the paging/segment code and cause a panic or silent
// corruption at runtime. This is deliberately called both here (via Open)
// and by store.Open, so a bad Config fails immediately, before any disk
// I/O, regardless of whether the caller goes through package store or
// uses package volume directly.
func (o Options) Validate() error {
	if o.PageSize < 0 {
		return fmt.Errorf("volume: Options.PageSize must not be negative, got %d", o.PageSize)
	}
	if o.ChunkSize < 0 {
		return fmt.Errorf("volume: Options.ChunkSize must not be negative, got %d", o.ChunkSize)
	}
	if o.MaxSegmentSize < 0 {
		return fmt.Errorf("volume: Options.MaxSegmentSize must not be negative, got %d", o.MaxSegmentSize)
	}

	// A page must have room for at least one byte of payload beyond its
	// own header, or every WriteBlob/ReadChunk call on it would panic or
	// corrupt data (this is the exact PageSize=50 example from the
	// production readiness report — 50 < pageHeaderSize=88).
	if o.PageSize != 0 && o.PageSize <= pageHeaderSize {
		return fmt.Errorf(
			"volume: Options.PageSize (%d) must be greater than the page header size (%d); a page must have room for at least one byte of payload",
			o.PageSize, pageHeaderSize,
		)
	}

	// PageSize is encoded into the segment header as a uint32
	// (encodePageHeader writes it via binary.LittleEndian.PutUint32). A
	// PageSize beyond uint32 range would be silently truncated on encode
	// — the same class of bug VULN 1/VULN 2 already fixed for ChunkID and
	// BlobID, just in a different field.
	if o.PageSize != 0 && uint64(o.PageSize) > math.MaxUint32 {
		return fmt.Errorf(
			"volume: Options.PageSize (%d) exceeds uint32 range and would be silently truncated when encoded into the segment header",
			o.PageSize,
		)
	}

	// Cross-field checks run against the resolved (defaults-applied)
	// values: an interaction between an explicit setting and the default
	// for the other field is just as nonsensical as a single bad field,
	// e.g. MaxSegmentSize=1MB with ChunkSize left at its 4MB default.
	resolved := o.withDefaults()
	if resolved.ChunkSize > resolved.MaxSegmentSize {
		return fmt.Errorf(
			"volume: ChunkSize (%d) must not exceed MaxSegmentSize (%d): a single chunk must fit within one segment",
			resolved.ChunkSize, resolved.MaxSegmentSize,
		)
	}
	if int64(resolved.PageSize) > resolved.MaxSegmentSize {
		return fmt.Errorf(
			"volume: PageSize (%d) must not exceed MaxSegmentSize (%d): a segment must be able to hold at least one page",
			resolved.PageSize, resolved.MaxSegmentSize,
		)
	}
	return nil
}

// ── Buffer and hasher pools ───────────────────────────────────────────────────

var chunkBufPool = sync.Pool{
	New: func() any { b := make([]byte, DefaultChunkSize); return &b },
}

var pageBufPool = sync.Pool{
	New: func() any { b := make([]byte, DefaultPageSize); return &b },
}

var hasherPool = sync.Pool{
	New: func() any { return sha256.New() },
}

// walBufPool pools WAL write buffers. Each entry is ~100–200 bytes for a
// typical single-chunk blob; we pool to avoid one alloc per blob.
var walBufPool = sync.Pool{
	New: func() any { b := make([]byte, 0, 256); return &b },
}

func getChunkBuf(size int64) []byte {
	if size == DefaultChunkSize {
		return (*chunkBufPool.Get().(*[]byte))[:size]
	}
	return make([]byte, size)
}
func putChunkBuf(b []byte) {
	if int64(cap(b)) == DefaultChunkSize {
		chunkBufPool.Put(&b)
	}
}

func getPageBuf(size int) []byte {
	if size == DefaultPageSize {
		return (*pageBufPool.Get().(*[]byte))[:size]
	}
	return make([]byte, size)
}
func putPageBuf(b []byte) {
	if cap(b) == DefaultPageSize {
		pageBufPool.Put(&b)
	}
}

func getHasher() hash.Hash { h := hasherPool.Get().(hash.Hash); h.Reset(); return h }
func putHasher(h hash.Hash) { hasherPool.Put(h) }

func getWALBuf() *[]byte  { return walBufPool.Get().(*[]byte) }
func putWALBuf(p *[]byte) { *p = (*p)[:0]; walBufPool.Put(p) }

// ── WriteResult ───────────────────────────────────────────────────────────────

// WriteResult is returned by WriteBlob after a blob is durably written.
type WriteResult struct {
	BlobID    object.BlobID
	TotalSize int64
	Chunks    []object.ChunkEntry
}

// ── Engine ────────────────────────────────────────────────────────────────────

// Engine manages segment files for a single namespace. Safe for concurrent use.
//
// Cache line layout:
//   - mu + 40-byte explicit pad = exactly 64 bytes (one cache line).
//     Prevents false sharing with active/nextSeq which change per write.
type Engine struct {
	mu  sync.RWMutex
	_   [40]byte // pad to 64-byte cache line (RWMutex=24b, pad=40b)

	active  *segFile
	nextSeq uint64

	dir  string // read-only after Open
	nsID string
	opts Options
}

// Open opens (or creates) a volume engine rooted at rootDir for namespace nsID.
func Open(rootDir, nsID string, opts Options) (*Engine, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	opts = opts.withDefaults()
	dir := filepath.Join(rootDir, nsID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("volume: mkdir %s: %w", dir, err)
	}

	e := &Engine{dir: dir, nsID: nsID, opts: opts}
	if err := e.markDirty(); err != nil {
		return nil, err
	}

	seq, err := e.highestSegSeq()
	if err != nil {
		return nil, err
	}
	if seq > 0 {
		sf, err := openSegFile(e.segPath(seq), opts.PageSize)
		if err != nil {
			return nil, fmt.Errorf("volume: open active segment seq %d: %w", seq, err)
		}
		e.active = sf
		e.nextSeq = seq + 1
	} else {
		e.nextSeq = 1
	}
	return e, nil
}

// IsDirty reports whether the engine was not cleanly closed on last run.
func (e *Engine) IsDirty() bool {
	_, err := os.Stat(e.dirtyPath())
	return err == nil
}

// WriteBlob reads all bytes from r, splits into chunks, writes durably.
//
// Allocation budget per blob:
//   - 1 × WriteResult escape (unavoidable — caller receives a pointer)
//   - 1 × []ChunkEntry backing array (result.Chunks)
//   - 2 × string header per chunk (BlobID + ChunkID — Go string-from-[]byte)
//   - Pool borrows (chunkBuf, pageBuf, hasher, walBuf, chunkIDScratch): zero net
//
// Lock is held only during bw.Write(pageBuf) — not during I/O or hashing.
func (e *Engine) WriteBlob(r io.Reader) (*WriteResult, error) {
	chunkBuf := getChunkBuf(e.opts.ChunkSize)
	defer putChunkBuf(chunkBuf)

	blobHasher := getHasher()
	defer putHasher(blobHasher)

	// Pre-allocate cap(1) — covers the common single-chunk case without growth.
	chunks := make([]object.ChunkEntry, 0, 1)

	type patchTarget struct {
		segSeq     uint64
		pageOffset int64
	}
	patches := make([]patchTarget, 0, 1)

	var (
		total    int64
		chunkSeq int
	)

	for {
		n, readErr := io.ReadFull(r, chunkBuf)
		if n == 0 && readErr == io.EOF {
			break
		}
		if readErr != nil && readErr != io.ErrUnexpectedEOF && readErr != io.EOF {
			return nil, fmt.Errorf("volume: read input: %w", readErr)
		}

		payload := chunkBuf[:n]
		total += int64(n)

		// Hash and CRC computed outside the lock.
		blobHasher.Write(payload)
		chunkHashArr := sha256.Sum256(payload)
		crc := crc32.ChecksumIEEE(payload)

		// Lock only for the segment write.
		e.mu.Lock()
		if err := e.ensureActive(); err != nil {
			e.mu.Unlock()
			return nil, err
		}
		if err := e.rollIfNeeded(); err != nil {
			e.mu.Unlock()
			return nil, err
		}

		hdr := pageHeader{
			DataLen:    uint64(n),
			ChunkID:    chunkHashArr,
			ChunkSeq:   uint32(chunkSeq),
			CRC32:      crc,
			MagicFlags: encodeMagicFlags(0), // BlobID, TotalChunks, Flags patched below
		}

		pageOffset, pageCount, err := e.active.appendChunk(hdr, payload, e.opts.PageSize)
		segSeq := e.active.seq
		e.mu.Unlock()

		if err != nil {
			return nil, fmt.Errorf("volume: append chunk seq %d: %w", chunkSeq, err)
		}

		chunks = append(chunks, object.ChunkEntry{
			SegmentID:   object.SegmentID(segSeq),
			NamespaceID: e.nsID,
			PageOffset:  pageOffset,
			PageCount:   pageCount,
			Length:      int64(n),
			Seq:         chunkSeq,
		})
		patches = append(patches, patchTarget{segSeq, pageOffset})
		chunkSeq++

		if readErr == io.ErrUnexpectedEOF || readErr == io.EOF {
			break
		}
	}

	if len(chunks) == 0 {
		return nil, fmt.Errorf("volume: refusing to store empty blob")
	}

	// Sum into a stack array — no heap allocation for the digest slice.
	var digest [sha256.Size]byte
	blobHasher.Sum(digest[:0])

	blobID := blobIDFromDigest(&digest)
	totalChunks := uint32(len(chunks))

	// Flush before patching so the bufio.Writer buffer has reached the kernel.
	e.mu.Lock()
	flushErr := e.active.flushBuf()
	e.mu.Unlock()
	if flushErr != nil {
		return nil, fmt.Errorf("volume: flush before patch: %w", flushErr)
	}

	// Patch blobID, totalChunks, and flags into every written page.
	for i, pt := range patches {
		flags := PageFlags(0)
		if i == len(patches)-1 {
			flags = FlagLastChunk
		}
		if err := e.patchPage(pt.segSeq, pt.pageOffset, digest, totalChunks, flags); err != nil {
			return nil, fmt.Errorf("volume: patch page %d: %w", i, err)
		}
		chunks[i].BlobID = blobID
		chunks[i].ChunkID = chunkIDFromBlobID(blobID, i)
	}

	// fsync and WAL under lock so the active pointer is stable.
	e.mu.Lock()
	syncErr := e.active.sync()
	var walErr error
	if syncErr == nil {
		walErr = e.active.appendWAL(blobID, chunks)
	}
	e.mu.Unlock()

	if syncErr != nil {
		return nil, fmt.Errorf("volume: fsync: %w", syncErr)
	}
	if walErr != nil {
		return nil, fmt.Errorf("volume: write WAL: %w", walErr)
	}

	return &WriteResult{BlobID: blobID, TotalSize: total, Chunks: chunks}, nil
}

// ReadChunk reads and CRC-verifies the payload of a single chunk.
// RLock — concurrent reads never block each other.
// One ReadAt per page.
func (e *Engine) ReadChunk(entry object.ChunkEntry) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	path := e.segPath(uint64(entry.SegmentID))
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &errors.CorruptionError{
				SegmentID: entry.SegmentID.String(),
				Offset:    entry.PageOffset,
				Detail:    "segment file missing",
			}
		}
		return nil, fmt.Errorf("volume: open segment: %w", err)
	}
	defer f.Close()

	pageSize := e.opts.PageSize
	capacity := pageSize - pageHeaderSize
	totalPages := entry.PageCount
	if totalPages < 1 {
		totalPages = 1
	}

	pageBuf := getPageBuf(pageSize)
	defer putPageBuf(pageBuf)

	data := make([]byte, 0, entry.Length)
	var storedCRC uint32

	for page := 0; page < totalPages; page++ {
		pageOffset := entry.PageOffset + int64(page)*int64(pageSize)

		n, err := f.ReadAt(pageBuf[:pageSize], pageOffset)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("volume: ReadAt page %d: %w", page, err)
		}
		if n < pageHeaderSize {
			return nil, fmt.Errorf("volume: page %d too short (%d bytes)", page, n)
		}

		hdr, err := decodePageHeader(pageBuf[:pageHeaderSize])
		if err != nil {
			return nil, err
		}

		flags, _ := decodeMagicFlags(hdr.MagicFlags)
		if flags.IsDeleted() {
			return nil, fmt.Errorf("volume: chunk %s is deleted", entry.ChunkID)
		}
		if page == 0 {
			storedCRC = hdr.CRC32
		}

		payloadLen := int(hdr.DataLen)
		if payloadLen > capacity {
			return nil, fmt.Errorf("volume: page %d: DataLen %d exceeds capacity", page, payloadLen)
		}
		data = append(data, pageBuf[pageHeaderSize:pageHeaderSize+payloadLen]...)
	}

	if crc32.ChecksumIEEE(data) != storedCRC {
		return nil, &errors.CorruptionError{
			SegmentID: entry.SegmentID.String(),
			Offset:    entry.PageOffset,
			Detail:    fmt.Sprintf("CRC mismatch for chunk %s", entry.ChunkID),
		}
	}
	return data, nil
}

// MarkDeleted sets the deleted flag via ReadAt + WriteAt on the magicFlags word.
func (e *Engine) MarkDeleted(entry object.ChunkEntry) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	path := e.segPath(uint64(entry.SegmentID))
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("volume: open segment for mark-deleted: %w", err)
	}
	defer f.Close()

	var buf4 [4]byte
	if _, err := f.ReadAt(buf4[:], entry.PageOffset+phOffMagicFlags); err != nil {
		return fmt.Errorf("volume: ReadAt magicFlags: %w", err)
	}
	flags, err := decodeMagicFlags(binary.LittleEndian.Uint32(buf4[:]))
	if err != nil {
		return fmt.Errorf("volume: bad magicFlags on mark-deleted: %w", err)
	}
	binary.LittleEndian.PutUint32(buf4[:], encodeMagicFlags(flags.Set(FlagDeleted)))
	if _, err := f.WriteAt(buf4[:], entry.PageOffset+phOffMagicFlags); err != nil {
		return fmt.Errorf("volume: WriteAt magicFlags: %w", err)
	}
	return f.Sync()
}

// ScanSegments iterates every page in every segment file.
func (e *Engine) ScanSegments(fn func(object.ChunkEntry, PageHeader) error) error {
	e.mu.RLock()
	entries, err := os.ReadDir(e.dir)
	e.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("volume: read dir %s: %w", e.dir, err)
	}

	for _, de := range entries {
		if de.IsDir() || filepath.Ext(de.Name()) != ".vol" {
			continue
		}
		var seq uint64
		if _, err := fmt.Sscanf(de.Name(), "seg-%016x.vol", &seq); err != nil {
			continue
		}
		if err := e.scanSegFile(filepath.Join(e.dir, de.Name()), seq, fn); err != nil {
			return err
		}
	}
	return nil
}

// Close flushes and closes the active segment, clears the dirty flag.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.active != nil {
		if err := e.active.close(); err != nil {
			return fmt.Errorf("volume: close active segment: %w", err)
		}
		e.active = nil
	}
	return e.clearDirty()
}

// ── Internal helpers ──────────────────────────────────────────────────────────

func (e *Engine) dirtyPath() string { return filepath.Join(e.dir, ".dirty") }

func (e *Engine) markDirty() error {
	f, err := os.OpenFile(e.dirtyPath(), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("volume: mark dirty: %w", err)
	}
	return f.Close()
}

func (e *Engine) clearDirty() error {
	if err := os.Remove(e.dirtyPath()); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("volume: clear dirty: %w", err)
	}
	return nil
}

func (e *Engine) segPath(seq uint64) string {
	return filepath.Join(e.dir, fmt.Sprintf("seg-%016x.vol", seq))
}

func (e *Engine) highestSegSeq() (uint64, error) {
	entries, err := os.ReadDir(e.dir)
	if err != nil {
		return 0, fmt.Errorf("volume: read dir: %w", err)
	}
	var max uint64
	for _, de := range entries {
		if de.IsDir() || filepath.Ext(de.Name()) != ".vol" {
			continue
		}
		var seq uint64
		if _, err := fmt.Sscanf(de.Name(), "seg-%016x.vol", &seq); err != nil {
			continue
		}
		if seq > max {
			max = seq
		}
	}
	return max, nil
}

// ensureActive and rollIfNeeded must be called with e.mu held.

func (e *Engine) ensureActive() error {
	if e.active != nil {
		return nil
	}
	sf, err := createSegFile(e.segPath(e.nextSeq), e.nextSeq, e.nsID, e.opts.PageSize)
	if err != nil {
		return err
	}
	e.active = sf
	e.nextSeq++
	return nil
}

func (e *Engine) rollIfNeeded() error {
	size, err := e.active.currentSize()
	if err != nil {
		return err
	}
	if size < e.opts.MaxSegmentSize {
		return nil
	}
	if err := e.active.close(); err != nil {
		return fmt.Errorf("volume: seal segment: %w", err)
	}
	sf, err := createSegFile(e.segPath(e.nextSeq), e.nextSeq, e.nsID, e.opts.PageSize)
	if err != nil {
		return err
	}
	e.active = sf
	e.nextSeq++
	return nil
}

// patchPage writes blobID, totalChunks, and flags into a page header.
// Uses WriteAt on the raw *os.File — bypasses the bufio.Writer.
// Must be called after flushBuf().
//
// VULN 6 fix: the full Lock is held for the entire patch sequence.
// Previously RLock was taken only to read e.active.f, then released before
// WriteAt. A concurrent rollIfNeeded (under Lock) could close and reopen
// e.active.f between RUnlock and WriteAt, and if the OS reused the file
// descriptor number, WriteAt would silently write into the wrong segment.
func (e *Engine) patchPage(segSeq uint64, pageOffset int64, blobID [sha256.Size]byte, totalChunks uint32, flags PageFlags) error {
	var f *os.File
	e.mu.Lock()
	isActive := e.active != nil && e.active.seq == segSeq
	if isActive {
		f = e.active.f
	}
	// Hold the lock through all WriteAt calls below when patching the active
	// segment. For sealed segments we open a separate fd under lock then
	// release before the I/O (sealed segments are never modified otherwise).
	if !isActive {
		e.mu.Unlock()
		var err error
		f, err = os.OpenFile(e.segPath(segSeq), os.O_RDWR, 0o644)
		if err != nil {
			return err
		}
		defer f.Close()
	} else {
		defer e.mu.Unlock()
	}

	var buf4 [4]byte

	// TotalChunks at offset 76.
	binary.LittleEndian.PutUint32(buf4[:], totalChunks)
	if _, err := f.WriteAt(buf4[:], pageOffset+phOffTotalChunks); err != nil {
		return fmt.Errorf("volume: patch totalChunks: %w", err)
	}

	// BlobID at offset 40.
	if _, err := f.WriteAt(blobID[:], pageOffset+phOffBlobID); err != nil {
		return fmt.Errorf("volume: patch blobID: %w", err)
	}

	// Read-modify-write magicFlags at offset 84 to set flags.
	if _, err := f.ReadAt(buf4[:], pageOffset+phOffMagicFlags); err != nil {
		return fmt.Errorf("volume: ReadAt magicFlags for patch: %w", err)
	}
	existingFlags, err := decodeMagicFlags(binary.LittleEndian.Uint32(buf4[:]))
	if err != nil {
		return fmt.Errorf("volume: bad magicFlags during patch: %w", err)
	}
	binary.LittleEndian.PutUint32(buf4[:], encodeMagicFlags(existingFlags|flags))
	if _, err := f.WriteAt(buf4[:], pageOffset+phOffMagicFlags); err != nil {
		return fmt.Errorf("volume: patch magicFlags: %w", err)
	}
	return nil
}

func (e *Engine) scanSegFile(path string, seq uint64, fn func(object.ChunkEntry, PageHeader) error) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("volume: open %s: %w", path, err)
	}
	defer f.Close()

	pageSize := int64(e.opts.PageSize)
	offset := int64(segHeaderSize)
	hdrBuf := make([]byte, pageHeaderSize)

	for {
		if _, err := f.ReadAt(hdrBuf, offset); err != nil {
			break
		}
		hdr, err := decodePageHeader(hdrBuf)
		if err != nil {
			break
		}

		flags, _ := decodeMagicFlags(hdr.MagicFlags)
		blobIDStr := blobIDFromDigest((*[sha256.Size]byte)(hdr.BlobID[:]))
		chunkID := chunkIDFromBlobID(blobIDStr, int(hdr.ChunkSeq))

		capacity := pageSize - pageHeaderSize
		pagesForChunk := (int64(hdr.DataLen) + capacity - 1) / capacity
		if pagesForChunk < 1 {
			pagesForChunk = 1
		}

		entry := object.ChunkEntry{
			ChunkID:     chunkID,
			BlobID:      blobIDStr,
			SegmentID:   object.SegmentID(seq),
			NamespaceID: e.nsID,
			PageOffset:  offset,
			PageCount:   int(pagesForChunk),
			Length:      int64(hdr.DataLen),
			Seq:         int(hdr.ChunkSeq),
		}
		pub := PageHeader{
			ChunkID:     hdr.ChunkID,
			BlobID:      hdr.BlobID,
			ChunkSeq:    hdr.ChunkSeq,
			TotalChunks: hdr.TotalChunks,
			DataLen:     hdr.DataLen,
			Flags:       flags,
		}

		if err := fn(entry, pub); err != nil {
			return err
		}
		offset += pagesForChunk * pageSize
	}
	return nil
}

// ── segFile ───────────────────────────────────────────────────────────────────

// segFile is a handle to an open segment + its co-located WAL file.
//
// The WAL file is kept open alongside the segment to eliminate the
// os.OpenFile + syscall.ByteSliceFromString allocation that occurred on every
// blob write when we opened the WAL by path each time.
type segFile struct {
	f      *os.File       // segment data file
	bw     *bufio.Writer  // buffered writer over f
	wal    *os.File       // WAL file — kept open for the segment's lifetime
	seq    uint64
	offset int64
}

func createSegFile(path string, seq uint64, nsID string, pageSize int) (*segFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if os.IsExist(err) {
			return openSegFile(path, pageSize)
		}
		return nil, fmt.Errorf("volume: create segment %s: %w", path, err)
	}

	var nsArr [64]byte
	copy(nsArr[:], nsID)
	hdr := encodeSegFileHeader(segFileHeader{
		Magic:     segmentFileMagic,
		Version:   segmentVersion,
		SegSeq:    seq,
		PageSize:  uint32(pageSize),
		CreatedAt: time.Now().UTC().Unix(),
		NSID:      nsArr,
	})
	if _, err := f.Write(hdr); err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("volume: write segment header: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(path)
		return nil, err
	}

	// Open the WAL file once and keep it open.
	walPath := walPathFromSegPath(path)
	wal, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		f.Close()
		os.Remove(path)
		return nil, fmt.Errorf("volume: open WAL %s: %w", walPath, err)
	}

	return &segFile{
		f:      f,
		bw:     bufio.NewWriterSize(f, segWriteBufSize),
		wal:    wal,
		seq:    seq,
		offset: segHeaderSize,
	}, nil
}

func openSegFile(path string, pageSize int) (*segFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("volume: open segment %s: %w", path, err)
	}
	hdrBuf := make([]byte, segHeaderSize)
	if _, err := io.ReadFull(f, hdrBuf); err != nil {
		f.Close()
		return nil, fmt.Errorf("volume: read segment header: %w", err)
	}
	seq := binary.LittleEndian.Uint64(hdrBuf[8:])
	endOff, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return nil, err
	}

	walPath := walPathFromSegPath(path)
	wal, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("volume: open WAL %s: %w", walPath, err)
	}

	return &segFile{
		f:      f,
		bw:     bufio.NewWriterSize(f, segWriteBufSize),
		wal:    wal,
		seq:    seq,
		offset: endOff,
	}, nil
}

func walPathFromSegPath(segPath string) string {
	// Replace ".vol" suffix with ".wal".
	base := segPath[:len(segPath)-4]
	return base + ".wal"
}

// appendChunk assembles header + payload + padding into a pooled page buffer
// and issues exactly one Write per page. Zero allocation.
func (sf *segFile) appendChunk(hdr pageHeader, payload []byte, pageSize int) (offset int64, pageCount int, err error) {
	offset = sf.offset
	capacity := pageSize - pageHeaderSize
	remaining := payload

	pageBuf := getPageBuf(pageSize)
	defer putPageBuf(pageBuf)

	firstPage := true
	for len(remaining) > 0 || firstPage {
		slice := remaining
		if len(slice) > capacity {
			slice = slice[:capacity]
		}
		remaining = remaining[len(slice):]

		ph := hdr
		ph.DataLen = uint64(len(slice))
		if !firstPage {
			ph.CRC32 = 0
		}

		encodePageHeader(ph, pageBuf[:pageHeaderSize])
		copy(pageBuf[pageHeaderSize:], slice)
		// Zero the padding region using clear() — compiler-optimised to SIMD
		// memset. Prevents stale pool data from a previous tenant's write
		// leaking into the on-disk padding bytes (cross-tenant information leak).
		clear(pageBuf[pageHeaderSize+len(slice) : pageSize])

		if _, werr := sf.bw.Write(pageBuf[:pageSize]); werr != nil {
			return 0, 0, fmt.Errorf("volume: write page: %w", werr)
		}

		sf.offset += int64(pageSize)
		pageCount++
		firstPage = false
		if len(remaining) == 0 {
			break
		}
	}
	return offset, pageCount, nil
}

// appendWAL writes a WAL entry using the kept-open wal file.
// Uses a pooled buffer — zero per-call allocation.
func (sf *segFile) appendWAL(blobID object.BlobID, chunks []object.ChunkEntry) error {
	p := getWALBuf()
	buf := *p

	blobIDBytes := []byte(blobID)
	buf = binary.LittleEndian.AppendUint32(buf, walMagicVal)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(blobIDBytes)))
	buf = append(buf, blobIDBytes...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(chunks)))

	for _, c := range chunks {
		cidBytes := []byte(c.ChunkID)
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(cidBytes)))
		buf = append(buf, cidBytes...)
		buf = binary.LittleEndian.AppendUint64(buf, uint64(c.SegmentID))
		buf = binary.LittleEndian.AppendUint64(buf, uint64(c.PageOffset))
		buf = binary.LittleEndian.AppendUint64(buf, uint64(c.Length))
	}

	if _, err := sf.wal.Write(buf); err != nil {
		*p = buf
		putWALBuf(p)
		return fmt.Errorf("volume: WAL write: %w", err)
	}
	*p = buf
	putWALBuf(p)
	// fsync the WAL — without this a crash between segment fsync and WAL flush
	// would leave a durable segment with no WAL entry. Recovery via ScanSegments
	// still works (blobID is patched into page headers before segment fsync),
	// but an unsynced WAL gives a false sense of durability to future readers.
	if err := sf.wal.Sync(); err != nil {
		return fmt.Errorf("volume: WAL sync: %w", err)
	}
	return nil
}

func (sf *segFile) flushBuf() error { return sf.bw.Flush() }

func (sf *segFile) currentSize() (int64, error) { return sf.offset, nil }

func (sf *segFile) sync() error {
	if err := sf.bw.Flush(); err != nil {
		return fmt.Errorf("volume: flush before sync: %w", err)
	}
	return sf.f.Sync()
}

func (sf *segFile) close() error {
	var errs []error
	if err := sf.bw.Flush(); err != nil {
		errs = append(errs, fmt.Errorf("volume: flush on close: %w", err))
	}
	if err := sf.f.Close(); err != nil {
		errs = append(errs, fmt.Errorf("volume: close segment: %w", err))
	}
	if sf.wal != nil {
		if err := sf.wal.Close(); err != nil {
			errs = append(errs, fmt.Errorf("volume: close WAL: %w", err))
		}
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}
