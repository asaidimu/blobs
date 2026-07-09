// volume_export_test.go exposes package internals for white-box testing.
// This file is compiled only during testing (package volume_test imports it
// via the same package boundary). It does not appear in production builds.
package volume

// PageHeaderSize returns the pageHeaderSize constant so tests can verify
// the on-disk wire size without depending on the unexported constant directly.
func PageHeaderSize() int { return pageHeaderSize }

// EngineMutexPadding returns the combined size of the Engine's RWMutex and
// its explicit cache-line padding. Tests verify this equals 64 bytes.
func EngineMutexPadding() int {
	// sync.RWMutex = 24 bytes, explicit pad = 40 bytes → 64 bytes total.
	const rwmutexSize = 24
	const padSize = 40
	return rwmutexSize + padSize
}

// EncodeMagicFlags and DecodeMagicFlags are exported so tests can exercise
// the packing/validation logic directly without going through WriteBlob.
func EncodeMagicFlags(flags PageFlags) uint32 { return encodeMagicFlags(flags) }
func DecodeMagicFlags(v uint32) (PageFlags, error) { return decodeMagicFlags(v) }

// TestPageHeader is an exported mirror of the internal pageHeader used by
// encode/decode tests. Field names and types match exactly.
type TestPageHeader struct {
	DataLen     uint64
	ChunkID     [32]byte
	BlobID      [32]byte
	ChunkSeq    uint32
	TotalChunks uint32
	CRC32       uint32
	Flags       PageFlags // stored as MagicFlags on wire
}

// EncodeTestPageHeader serialises a TestPageHeader using the production
// encodePageHeader path so tests verify the real encode logic.
func EncodeTestPageHeader(h TestPageHeader) []byte {
	internal := pageHeader{
		DataLen:     h.DataLen,
		ChunkID:     h.ChunkID,
		BlobID:      h.BlobID,
		ChunkSeq:    h.ChunkSeq,
		TotalChunks: h.TotalChunks,
		CRC32:       h.CRC32,
		MagicFlags:  encodeMagicFlags(h.Flags),
	}
	dst := make([]byte, pageHeaderSize)
	encodePageHeader(internal, dst)
	return dst
}

// DecodeTestPageHeader parses a wire-format header using the production
// decodePageHeader path and returns a TestPageHeader.
func DecodeTestPageHeader(b []byte) (TestPageHeader, error) {
	h, err := decodePageHeader(b)
	if err != nil {
		return TestPageHeader{}, err
	}
	flags, _ := decodeMagicFlags(h.MagicFlags)
	return TestPageHeader{
		DataLen:     h.DataLen,
		ChunkID:     h.ChunkID,
		BlobID:      h.BlobID,
		ChunkSeq:    h.ChunkSeq,
		TotalChunks: h.TotalChunks,
		CRC32:       h.CRC32,
		Flags:       flags,
	}, nil
}
