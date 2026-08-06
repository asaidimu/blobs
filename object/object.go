// Package object defines the core data types for the blobstore.
// This package has no I/O and no external dependencies — it is pure data.
package object

import (
	"fmt"
	"regexp"
	"time"
)

// ── Identity types ────────────────────────────────────────────────────────────

// BlobID is a content-addressed identifier: "sha256:<hex>".
// Two blobs with identical bytes always produce the same BlobID.
// This is the foundation of deduplication and idempotent replication.
type BlobID string

func (id BlobID) String() string  { return string(id) }
func (id BlobID) IsZero() bool    { return id == "" }

// ChunkID uniquely identifies a chunk within the volume engine.
//
// Production chunk IDs are content-addressed: "sha256:<hex>" over the chunk's
// own bytes (see volume.chunkIDFromContent), so identical runs of bytes in
// different blobs share one ChunkID — the foundation of cross-blob
// deduplication. NewChunkID below remains available as a legacy/utility
// constructor for the older "<blobID>#<seq>" format.
type ChunkID string

// canonicalBlobIDLen is the expected byte length of a canonical BlobID:
// "sha256:" (7) + 64 hex chars = 71.
const canonicalBlobIDLen = 71

// maxChunkSeq is the largest valid 0-based chunk sequence number.
// Sequence numbers beyond uint32 max cannot occur in practice (a blob of
// 4MB chunks would need > 4 billion chunks), but the guard is explicit so
// no two distinct sequences ever produce the same ChunkID string.
const maxChunkSeq = 1<<32 - 1 // math.MaxUint32 without importing math

// NewChunkID constructs a ChunkID from a BlobID and 0-based sequence number.
// Format: "<blobID>#<decimal_seq>" using variable-width decimal.
//
// Panics if:
//   - seq < 0 or seq > maxChunkSeq: would require more than 4 billion chunks.
//   - len(blobID) != canonicalBlobIDLen: a non-canonical BlobID would overflow
//     internal scratch buffers and silently truncate the sequence field,
//     causing two different chunk sequences to share the same ChunkID
//     (index aliasing / silent data corruption).
func NewChunkID(blobID BlobID, seq int) ChunkID {
	if seq < 0 || seq > maxChunkSeq {
		panic(fmt.Sprintf("object: chunk sequence %d out of range [0, %d]", seq, maxChunkSeq))
	}
	if len(blobID) != canonicalBlobIDLen {
		panic(fmt.Sprintf(
			"object: BlobID length %d != expected %d — non-canonical BlobID would corrupt ChunkID (index aliasing)",
			len(blobID), canonicalBlobIDLen,
		))
	}
	return ChunkID(fmt.Sprintf("%s#%d", blobID, seq))
}

func (id ChunkID) String() string { return string(id) }

// SegmentID identifies a segment file. It is a monotonically increasing
// uint64 formatted as a zero-padded 16-char hex string.
type SegmentID uint64

func (id SegmentID) String() string { return fmt.Sprintf("%016x", uint64(id)) }

var nsIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9\-]{0,61}[a-z0-9]$`)

// ValidateNamespaceID returns an error if id is not a valid namespace identifier.
func ValidateNamespaceID(id string) error {
	if !nsIDPattern.MatchString(id) {
		return fmt.Errorf(
			"invalid namespace id %q: must be lowercase alphanumeric + hyphens, 2–63 chars",
			id,
		)
	}
	return nil
}

// ── Metadata ──────────────────────────────────────────────────────────────────

// Metadata holds all descriptive information about a stored blob.
// Stored in the index alongside the ref — never inside the volume.
// This means metadata can evolve without rewriting blob bytes.
type Metadata struct {
	ContentType string            `json:"content_type,omitempty"` // MIME type; default "application/octet-stream"
	Size        int64             `json:"size"`                   // total logical blob size in bytes
	BlobID      BlobID            `json:"blob_id"`
	ChunkCount  int               `json:"chunk_count"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	Custom      map[string]string `json:"custom,omitempty"` // caller-defined; keys starting "_bs_" are reserved
}

// ── Index record types ────────────────────────────────────────────────────────

// RefEntry is the index record for a (namespaceID, key) → BlobID mapping.
// It is the only mutable layer — the blob it points at is immutable.
type RefEntry struct {
	NamespaceID string   `json:"namespace_id"`
	Key         string   `json:"key"`
	BlobID      BlobID   `json:"blob_id"`
	Metadata    Metadata `json:"metadata"`
}

// BlobEntry is the index record for a content-addressed blob.
// Shared across all namespaces that reference the same content.
// RefCount tracks how many RefEntries point here; zero means GC-eligible.
type BlobEntry struct {
	BlobID    BlobID    `json:"blob_id"`
	ChunkIDs  []ChunkID `json:"chunk_ids"`  // ordered; reassemble in this order
	TotalSize int64     `json:"total_size"`
	RefCount  int64     `json:"ref_count"`
	CreatedAt time.Time `json:"created_at"`
}

// ChunkEntry is the index record for a chunk's physical location on disk.
type ChunkEntry struct {
	ChunkID     ChunkID   `json:"chunk_id"`
	BlobID      BlobID    `json:"blob_id"`
	SegmentID   SegmentID `json:"segment_id"`
	NamespaceID string    `json:"namespace_id"`
	PageOffset  int64     `json:"page_offset"` // byte offset of the page in the segment file
	PageCount   int       `json:"page_count"`  // number of pages this chunk occupies
	Length      int64     `json:"length"`      // payload byte length (excludes page headers/padding)
	Seq         int       `json:"seq"`         // 0-based position within the blob
	RefCount    int64     `json:"ref_count"`   // blobs referencing this content; zero means GC-eligible
}

// SegmentState describes the lifecycle of a segment file.
type SegmentState int

const (
	SegmentActive     SegmentState = iota // currently being written to
	SegmentSealed                         // full and read-only
	SegmentCompacting                     // being rewritten; read-only
	SegmentDead                           // all live data migrated; awaiting deletion
)

// SegmentEntry is the index record for a segment file.
type SegmentEntry struct {
	SegmentID   SegmentID    `json:"segment_id"`
	NamespaceID string       `json:"namespace_id"`
	State       SegmentState `json:"state"`
	PageSize    int          `json:"page_size"`
	PageCount   int64        `json:"page_count"`
	BytesUsed   int64        `json:"bytes_used"`   // live payload bytes
	BytesTotal  int64        `json:"bytes_total"`  // total file size
	CreatedAt   time.Time    `json:"created_at"`
	SealedAt    *time.Time   `json:"sealed_at,omitempty"`
}

// ── Namespace and quotas ──────────────────────────────────────────────────────

// Namespace is an isolated logical partition within a Store.
type Namespace struct {
	ID          string            `json:"id"`
	DisplayName string            `json:"display_name,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	Quota       *Quota            `json:"quota,omitempty"` // nil = unlimited
	Custom      map[string]string `json:"custom,omitempty"`
}

// Quota defines resource limits for a namespace.
// Zero values mean unlimited for that dimension.
type Quota struct {
	MaxBytes     int64 `json:"max_bytes,omitempty"`
	MaxBlobCount int64 `json:"max_blob_count,omitempty"`
	MaxBlobSize  int64 `json:"max_blob_size,omitempty"`
}

// ── Stats ─────────────────────────────────────────────────────────────────────

// NamespaceStats holds live usage metrics for a single namespace.
// Maintained incrementally on every write/delete — never computed by scanning.
type NamespaceStats struct {
	NamespaceID   string    `json:"namespace_id"`
	BlobCount     int64     `json:"blob_count"`      // live refs
	BytesStored   int64     `json:"bytes_stored"`    // logical bytes (sum of blob sizes)
	BytesPhysical int64     `json:"bytes_physical"`  // actual bytes on disk (after dedup)
	ChunkCount    int64     `json:"chunk_count"`     // live chunks
	DeadBytes     int64     `json:"dead_bytes"`      // unreferenced bytes awaiting compaction
	DeadChunks    int64     `json:"dead_chunks"`     // unreferenced chunks awaiting compaction
	SegmentCount  int64     `json:"segment_count"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// StoreStats aggregates metrics across all namespaces.
type StoreStats struct {
	NamespaceCount     int64           `json:"namespace_count"`
	TotalBlobCount     int64           `json:"total_blob_count"`
	TotalBytesStored   int64           `json:"total_bytes_stored"`
	TotalBytesPhysical int64           `json:"total_bytes_physical"`
	TotalDeadBytes     int64           `json:"total_dead_bytes"`
	SegmentCount       int64           `json:"segment_count"`
	DeduplicationRatio float64         `json:"deduplication_ratio"` // >1 means dedup is saving space
	PerNamespace       []NamespaceStats `json:"per_namespace"`
	ComputedAt         time.Time        `json:"computed_at"`
}

// ── Public-facing API types ───────────────────────────────────────────────────

// BlobInfo is what callers see — a key plus its resolved metadata.
type BlobInfo struct {
	Key         string   `json:"key"`
	NamespaceID string   `json:"namespace_id"`
	Metadata    Metadata `json:"metadata"`
}
