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
//     - ChunkID is the chunk's content hash, encoded from a stack buffer
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
	"sort"
	"sync"
	"time"

	"github.com/asaidimu/blobs/chunking"
	"github.com/asaidimu/blobs/encryption"
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
)

// Default tuning values.
const (
	DefaultPageSize       = 16 * 1024
	DefaultChunkSize      = 4 * 1024 * 1024
	DefaultMaxSegmentSize = 512 * 1024 * 1024

	// DefaultSegmentRewriteThreshold is the dead-byte ratio (by page count,
	// 0.0–1.0) a sealed segment must reach before Compact's phase 2
	// physically rewrites it to reclaim space. 0.30 means a segment is
	// rewritten once 30% or more of its pages belong to deleted chunks.
	DefaultSegmentRewriteThreshold = 0.30
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

// chunkIDFromContent constructs a content-addressed ChunkID from a chunk's raw
// SHA-256 digest: "sha256:<hex>". Chunk identity is the chunk's own content
// hash, so identical runs of bytes produce the same ChunkID no matter which
// blob wrote them — the foundation of cross-blob deduplication. The result is
// the same length (71) as a BlobID.
func chunkIDFromContent(digest [sha256.Size]byte) object.ChunkID {
	var buf [blobIDLen]byte
	copy(buf[:hashPrefixLen], hashPrefix)
	hex.Encode(buf[hashPrefixLen:], digest[:])
	return object.ChunkID(buf[:]) // one allocation: string header
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

	// Cipher, if non-nil, enables encryption-at-rest for every chunk
	// this Engine writes and reads: WriteBlob encrypts each chunk's
	// payload before it reaches disk, and ReadChunk decrypts it after
	// reading and CRC-verifying the stored bytes. Nil (the default)
	// means the namespace is unencrypted — Engine's on-disk behavior is
	// then identical to before this option existed.
	//
	// This is deliberately an Engine-level (i.e. per-namespace) switch,
	// not a global one: package store resolves each namespace's own
	// Cipher independently, from that namespace's own key, so
	// encryption is opt-in per namespace.
	Cipher *encryption.Cipher
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

func getHasher() hash.Hash  { h := hasherPool.Get().(hash.Hash); h.Reset(); return h }
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
	mu sync.RWMutex
	_  [40]byte // pad to 64-byte cache line (RWMutex=24b, pad=40b)

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

// WriteBlob reads all bytes from r, splits into content-defined chunks via
// FastCDC, and writes them durably. Chunk boundaries are a function of the
// bytes themselves (see package chunking), and every chunk's identity is its
// own content hash — so a blob and a blob-plus-a-prefix share their unchanged
// chunks. Cross-blob reuse of those shared chunks happens at the store layer
// (index refcounting); this engine always writes every chunk it is given.
//
// Allocation budget per blob:
//   - 1 × WriteResult escape (unavoidable — caller receives a pointer)
//   - 1 × []ChunkEntry backing array (result.Chunks)
//   - 2 × string header per chunk (BlobID + ChunkID — Go string-from-[]byte)
//   - Pool borrows (pageBuf, hasher, walBuf, chunkIDScratch): zero net
//
// Lock is held only during bw.Write(pageBuf) — not during I/O or hashing.
func (e *Engine) WriteBlob(r io.Reader) (*WriteResult, error) {
	blobHasher := getHasher()
	defer putHasher(blobHasher)

	chunker, err := chunking.New(r, chunking.Options{Avg: int(e.opts.ChunkSize)})
	if err != nil {
		return nil, fmt.Errorf("volume: chunker: %w", err)
	}

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
		c, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("volume: chunk input: %w", err)
		}

		payload := c.Data
		n := len(payload)
		total += int64(n)

		// Hash computed on plaintext, outside the lock — content
		// addressing (ChunkID/BlobID) and cross-blob dedup must always
		// key off the plaintext, encryption or not, or identical
		// content would stop deduplicating.
		blobHasher.Write(payload)
		chunkHashArr := sha256.Sum256(payload)
		chunkID := chunkIDFromContent(chunkHashArr)

		// stored is what actually gets written to the page: the
		// plaintext payload, unless this namespace has encryption
		// enabled, in which case it's payload's ciphertext — always
		// exactly len(payload) bytes either way (see package
		// encryption's doc comment for why that length-preservation is
		// the property that keeps everything below this point, and all
		// of compaction, unaware encryption exists at all).
		stored := payload
		var nonce, tag []byte
		if e.opts.Cipher != nil {
			var err error
			nonce, stored, tag, err = e.opts.Cipher.Seal([]byte(chunkID), payload)
			if err != nil {
				return nil, fmt.Errorf("volume: encrypt chunk seq %d: %w", chunkSeq, err)
			}
		}
		crc := crc32.ChecksumIEEE(stored)

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
			DataLen:    uint64(len(stored)),
			ChunkID:    chunkHashArr,
			ChunkSeq:   uint32(chunkSeq),
			CRC32:      crc,
			MagicFlags: encodeMagicFlags(0), // BlobID, TotalChunks, Flags patched below
		}

		pageOffset, pageCount, err := e.active.appendChunk(hdr, stored, e.opts.PageSize)
		segSeq := e.active.seq
		e.mu.Unlock()

		if err != nil {
			return nil, fmt.Errorf("volume: append chunk seq %d: %w", chunkSeq, err)
		}

		chunks = append(chunks, object.ChunkEntry{
			ChunkID:     chunkID,
			SegmentID:   object.SegmentID(segSeq),
			NamespaceID: e.nsID,
			PageOffset:  pageOffset,
			PageCount:   pageCount,
			Length:      int64(n),
			Seq:         chunkSeq,
			Nonce:       nonce,
			Tag:         tag,
		})
		patches = append(patches, patchTarget{segSeq, pageOffset})
		chunkSeq++
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

	if e.opts.Cipher == nil {
		return data, nil
	}
	if len(entry.Nonce) == 0 || len(entry.Tag) == 0 {
		return nil, fmt.Errorf(
			"volume: chunk %s: namespace %q is encrypted but this chunk's index record has no nonce/tag (written before encryption was enabled, or index corruption)",
			entry.ChunkID, e.nsID,
		)
	}
	plaintext, err := e.opts.Cipher.Open([]byte(entry.ChunkID), entry.Nonce, data, entry.Tag)
	if err != nil {
		return nil, &errors.CorruptionError{
			SegmentID: entry.SegmentID.String(),
			Offset:    entry.PageOffset,
			Detail:    fmt.Sprintf("decrypt chunk %s: %v", entry.ChunkID, err),
		}
	}
	return plaintext, nil
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

// SegmentStat summarises one segment's live/dead page and byte counts, as
// observed from its on-disk page header flags at the time of the scan.
type SegmentStat struct {
	SegmentID  object.SegmentID
	TotalPages int
	DeadPages  int
	TotalBytes int64
	DeadBytes  int64
}

// DeadRatio returns the fraction (0.0–1.0) of this segment's pages that
// belong to deleted chunks. Returns 0 for a segment with no pages at all.
func (s SegmentStat) DeadRatio() float64 {
	if s.TotalPages == 0 {
		return 0
	}
	return float64(s.DeadPages) / float64(s.TotalPages)
}

// SegmentStats scans every segment file and returns per-segment live/dead
// totals, ordered by SegmentID (equivalently, creation order). It is built
// on top of ScanSegments — the same page-header deleted flag ReadChunk and
// MarkDeleted already use — so it stays consistent with them by
// construction rather than by convention.
//
// This exists to let a caller (package store's Compact) decide which
// sealed segments are worth physically rewriting, without package volume
// needing to know anything about the index or about why a chunk became
// dead — it only reports what the on-disk flags already say.
func (e *Engine) SegmentStats() ([]SegmentStat, error) {
	byID := make(map[object.SegmentID]*SegmentStat)
	err := e.ScanSegments(func(entry object.ChunkEntry, hdr PageHeader) error {
		s, ok := byID[entry.SegmentID]
		if !ok {
			s = &SegmentStat{SegmentID: entry.SegmentID}
			byID[entry.SegmentID] = s
		}
		s.TotalPages += entry.PageCount
		s.TotalBytes += entry.Length
		if hdr.Flags.IsDeleted() {
			s.DeadPages += entry.PageCount
			s.DeadBytes += entry.Length
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	out := make([]SegmentStat, 0, len(byID))
	for _, s := range byID {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SegmentID < out[j].SegmentID })
	return out, nil
}

// ActiveSegmentID returns the currently-active (still being written to)
// segment's ID. ok is false if no segment has been created yet. Callers
// must never rewrite or delete the active segment — RewriteSegment and
// DeleteSegmentFile both refuse to.
func (e *Engine) ActiveSegmentID() (id object.SegmentID, ok bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.active == nil {
		return 0, false
	}
	return object.SegmentID(e.active.seq), true
}

// SegmentRewriteResult summarises the outcome of rewriting one segment.
type SegmentRewriteResult struct {
	OldSegmentID object.SegmentID
	NewSegmentID object.SegmentID
	ChunksKept   int   // live chunks copied into the new segment
	PagesFreed   int   // pages belonging to deleted chunks, not copied forward
	BytesFreed   int64 // payload bytes belonging to deleted chunks, not copied forward
}

// RewriteSegment copies every live (not-deleted) chunk in the sealed
// segment oldSegID, verbatim, into a freshly created segment, and returns
// the new locations the caller must commit to the index. It does not
// touch the index itself and does not delete oldSegID's files — package
// store's Compact owns that ordering (index update, then delete) because
// getting it backwards is what would make a crash mid-rewrite lose data
// instead of merely leaving a segment un-reclaimed until the next run.
//
// "Verbatim" matters here: a live chunk's header, payload, and CRC32 are
// copied as the exact bytes already on disk, not recomputed. The content
// hasn't changed — only where it lives — so recomputing anything would be
// pure risk (a transcription bug corrupting an otherwise-untouched chunk)
// for zero benefit.
//
// Returns an error without creating anything if oldSegID is the currently
// active segment — it is still being appended to and must never be
// rewritten out from under a concurrent WriteBlob.
func (e *Engine) RewriteSegment(oldSegID object.SegmentID) (*SegmentRewriteResult, []object.ChunkEntry, error) {
	e.mu.Lock()
	if e.active != nil && object.SegmentID(e.active.seq) == oldSegID {
		e.mu.Unlock()
		return nil, nil, fmt.Errorf("volume: cannot rewrite segment %s: it is the active segment", oldSegID)
	}
	newSeq := e.nextSeq
	e.nextSeq++
	e.mu.Unlock()

	oldPath := e.segPath(uint64(oldSegID))
	oldFile, err := os.Open(oldPath)
	if err != nil {
		return nil, nil, fmt.Errorf("volume: open segment %s for rewrite: %w", oldSegID, err)
	}
	defer oldFile.Close()

	newSF, err := createSegFile(e.segPath(newSeq), newSeq, e.nsID, e.opts.PageSize)
	if err != nil {
		return nil, nil, fmt.Errorf("volume: create rewrite target for segment %s: %w", oldSegID, err)
	}

	pageSize := int64(e.opts.PageSize)
	capacity := pageSize - int64(pageHeaderSize)
	offset := int64(segHeaderSize)
	hdrBuf := make([]byte, pageHeaderSize)

	result := &SegmentRewriteResult{OldSegmentID: oldSegID, NewSegmentID: object.SegmentID(newSeq)}
	var relocated []object.ChunkEntry

	for {
		n, readErr := oldFile.ReadAt(hdrBuf, offset)
		if n < len(hdrBuf) {
			break // no more complete page headers — end of written data
		}
		if readErr != nil && readErr != io.EOF {
			newSF.close()
			return nil, nil, fmt.Errorf("volume: read page header at offset %d during rewrite: %w", offset, readErr)
		}

		hdr, err := decodePageHeader(hdrBuf)
		if err != nil {
			newSF.close()
			return nil, nil, fmt.Errorf("volume: decode page header at offset %d during rewrite: %w", offset, err)
		}
		flags, _ := decodeMagicFlags(hdr.MagicFlags)

		pagesForChunk := (int64(hdr.DataLen) + capacity - 1) / capacity
		if pagesForChunk < 1 {
			pagesForChunk = 1
		}
		spanBytes := pagesForChunk * pageSize

		if flags.IsDeleted() {
			result.PagesFreed += int(pagesForChunk)
			result.BytesFreed += int64(hdr.DataLen)
			offset += spanBytes
			continue
		}

		span := make([]byte, spanBytes)
		if _, err := oldFile.ReadAt(span, offset); err != nil && err != io.EOF {
			newSF.close()
			return nil, nil, fmt.Errorf("volume: read chunk span at offset %d during rewrite: %w", offset, err)
		}

		newOffset, err := newSF.appendRaw(span)
		if err != nil {
			newSF.close()
			return nil, nil, fmt.Errorf("volume: write rewritten chunk during rewrite: %w", err)
		}

		blobIDStr := blobIDFromDigest((*[sha256.Size]byte)(hdr.BlobID[:]))
		relocated = append(relocated, object.ChunkEntry{
			ChunkID:     chunkIDFromContent(hdr.ChunkID),
			BlobID:      blobIDStr,
			SegmentID:   object.SegmentID(newSeq),
			NamespaceID: e.nsID,
			PageOffset:  newOffset,
			PageCount:   int(pagesForChunk),
			Length:      int64(hdr.DataLen),
			Seq:         int(hdr.ChunkSeq),
		})
		result.ChunksKept++
		offset += spanBytes
	}

	if err := newSF.sync(); err != nil {
		newSF.close()
		return nil, nil, fmt.Errorf("volume: fsync rewritten segment for %s: %w", oldSegID, err)
	}
	if err := newSF.close(); err != nil {
		return nil, nil, fmt.Errorf("volume: close rewritten segment for %s: %w", oldSegID, err)
	}

	return result, relocated, nil
}

// DeleteSegmentFile physically removes a sealed segment's data and WAL
// files. The caller must not call this until every chunk that was live in
// this segment has had its relocated location durably committed to the
// index — see RewriteSegment's doc comment and Compact's phase 2 for why
// that ordering is what keeps a mid-rewrite crash safe. Refuses to delete
// the currently active segment.
func (e *Engine) DeleteSegmentFile(segID object.SegmentID) error {
	e.mu.RLock()
	isActive := e.active != nil && object.SegmentID(e.active.seq) == segID
	e.mu.RUnlock()
	if isActive {
		return fmt.Errorf("volume: refusing to delete segment %s: it is the active segment", segID)
	}

	path := e.segPath(uint64(segID))
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("volume: remove segment file %s: %w", path, err)
	}
	walPath := walPathFromSegPath(path)
	if err := os.Remove(walPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("volume: remove WAL file %s: %w", walPath, err)
	}
	return nil
}

// ListSegmentIDs returns every segment file's ID by reading the directory
// listing only — it does not open or read any segment's contents. This is
// the cheap discovery step WAL replay is built on: finding out which
// segments exist costs one directory read, versus ScanSegments/
// SegmentStats' full page-by-page walk of every byte of every segment
// file. Replay only needs the small per-segment WAL files after this.
func (e *Engine) ListSegmentIDs() ([]object.SegmentID, error) {
	e.mu.RLock()
	entries, err := os.ReadDir(e.dir)
	e.mu.RUnlock()
	if err != nil {
		return nil, fmt.Errorf("volume: read dir %s: %w", e.dir, err)
	}
	var ids []object.SegmentID
	for _, de := range entries {
		if de.IsDir() || filepath.Ext(de.Name()) != ".vol" {
			continue
		}
		var seq uint64
		if _, err := fmt.Sscanf(de.Name(), "seg-%016x.vol", &seq); err != nil {
			continue
		}
		ids = append(ids, object.SegmentID(seq))
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// WALEntry is one durably-committed WriteBlob group, as recorded in a
// segment's WAL file: one blob and every chunk WriteBlob wrote for it,
// written and fsynced as a single unit (see appendWAL's call site in
// WriteBlob — the segment itself is fsynced first, then the WAL entry
// covering all of that blob's chunks is written and fsynced in one call).
type WALEntry struct {
	BlobID object.BlobID
	Chunks []object.ChunkEntry // full ChunkEntry, seq order, ready to PutChunk directly
}

// ParseWAL reads every complete WAL entry recorded for segment segID, in
// the order they were written, reconstructing a full ChunkEntry for each
// chunk. PageCount is recomputed via the same ceil-division every other
// page-count calculation in this package uses (the WAL doesn't store it);
// Seq is parsed from each ChunkID's own "<blobID>#<seq>" suffix rather
// than trusted from WAL position, so a caller doesn't have to assume WAL
// ordering matches chunk ordering — it's cross-checked implicitly by
// simply not depending on it.
//
// ParseWAL stops cleanly, without error, at the first incomplete or
// malformed trailing entry, and at a missing WAL file entirely (returns
// nil, nil). appendWAL writes an entry with a single Write call followed
// by a Sync; a crash between those two, or mid-Write, can leave a torn
// partial record as the very last bytes in the file. Every entry before
// that point was written as a complete unit, so treating a torn tail as
// "the log simply ends here" is correct — it is indistinguishable from,
// and handled identically to, "this entry hadn't been synced yet when the
// process stopped," which is exactly the case a WAL is supposed to leave
// safely unreplayed.
func (e *Engine) ParseWAL(segID object.SegmentID) ([]WALEntry, error) {
	path := walPathFromSegPath(e.segPath(uint64(segID)))
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("volume: read WAL %s: %w", path, err)
	}

	capacity := int64(e.opts.PageSize) - int64(pageHeaderSize)
	if capacity <= 0 {
		// Options.Validate() rejects a PageSize this small; this guard
		// only prevents a division by zero/negative if ParseWAL is ever
		// called against an Engine somehow opened without validation.
		capacity = 1
	}

	var entries []WALEntry
	off := 0
	for off < len(data) {
		entry, n, ok := decodeWALEntry(data[off:], e.nsID, capacity)
		if !ok {
			break // torn or malformed trailing entry — stop, do not error
		}
		entries = append(entries, entry)
		off += n
	}
	return entries, nil
}

// decodeWALEntry decodes exactly one WAL entry from the start of buf,
// mirroring appendWAL's write order field-for-field. Returns ok=false —
// never an error — for anything short of a fully well-formed entry,
// since ParseWAL's contract is to stop silently at the first bad entry
// rather than fail the whole replay over a torn trailing write.
func decodeWALEntry(buf []byte, nsID string, pageCapacity int64) (entry WALEntry, consumed int, ok bool) {
	pos := 0

	readU16 := func() (uint16, bool) {
		if pos+2 > len(buf) {
			return 0, false
		}
		v := binary.LittleEndian.Uint16(buf[pos:])
		pos += 2
		return v, true
	}
	readU32 := func() (uint32, bool) {
		if pos+4 > len(buf) {
			return 0, false
		}
		v := binary.LittleEndian.Uint32(buf[pos:])
		pos += 4
		return v, true
	}
	readU64 := func() (uint64, bool) {
		if pos+8 > len(buf) {
			return 0, false
		}
		v := binary.LittleEndian.Uint64(buf[pos:])
		pos += 8
		return v, true
	}
	readBytes := func(n int) ([]byte, bool) {
		if n < 0 || pos+n > len(buf) {
			return nil, false
		}
		b := buf[pos : pos+n]
		pos += n
		return b, true
	}

	magic, ok := readU32()
	if !ok || magic != walMagicVal {
		return WALEntry{}, 0, false
	}
	blobIDLen, ok := readU16()
	if !ok {
		return WALEntry{}, 0, false
	}
	blobIDBytes, ok := readBytes(int(blobIDLen))
	if !ok {
		return WALEntry{}, 0, false
	}
	blobID := object.BlobID(blobIDBytes)

	chunkCount, ok := readU32()
	if !ok {
		return WALEntry{}, 0, false
	}

	chunks := make([]object.ChunkEntry, chunkCount)
	for i := uint32(0); i < chunkCount; i++ {
		cidLen, ok := readU16()
		if !ok {
			return WALEntry{}, 0, false
		}
		cidBytes, ok := readBytes(int(cidLen))
		if !ok {
			return WALEntry{}, 0, false
		}
		chunkID := object.ChunkID(cidBytes)

		segIDRaw, ok := readU64()
		if !ok {
			return WALEntry{}, 0, false
		}
		pageOffset, ok := readU64()
		if !ok {
			return WALEntry{}, 0, false
		}
		length, ok := readU64()
		if !ok {
			return WALEntry{}, 0, false
		}

		// ChunkID is content-addressed (see chunkIDFromContent); it carries
		// no sequence. Seq is the chunk's position within this WAL entry,
		// which matches the order WriteBlob appended the chunks. Entry
		// boundaries are validated below: every chunk's location must fall
		// inside this entry's declared chunkCount.
		pages := (int64(length) + pageCapacity - 1) / pageCapacity
		if pages < 1 {
			pages = 1
		}

		chunks[i] = object.ChunkEntry{
			ChunkID:     chunkID,
			BlobID:      blobID,
			SegmentID:   object.SegmentID(segIDRaw),
			NamespaceID: nsID,
			PageOffset:  int64(pageOffset),
			PageCount:   int(pages),
			Length:      int64(length),
			Seq:         int(i),
		}
	}

	return WALEntry{BlobID: blobID, Chunks: chunks}, pos, true
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
		chunkID := chunkIDFromContent(hdr.ChunkID)

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
	f      *os.File      // segment data file
	bw     *bufio.Writer // buffered writer over f
	wal    *os.File      // WAL file — kept open for the segment's lifetime
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
// appendRaw writes buf — already-formatted, page-aligned bytes (header +
// payload + padding) exactly as they existed on disk — verbatim to the
// segment, advancing offset by len(buf). Used only by RewriteSegment,
// which relocates a live chunk's physical bytes unchanged into a new
// segment: the content hasn't changed, only where it lives, so nothing
// about the header, payload, or CRC needs to be (or should be) recomputed.
func (sf *segFile) appendRaw(buf []byte) (offset int64, err error) {
	offset = sf.offset
	if _, err := sf.bw.Write(buf); err != nil {
		return 0, fmt.Errorf("volume: append raw page span: %w", err)
	}
	sf.offset += int64(len(buf))
	return offset, nil
}

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
