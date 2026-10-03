package index

import (
	"context"
	"errors"

	"github.com/asaidimu/blobs/object"
)

// Format 1 (store v1.3.x and earlier) kept blob manifests and chunk records in
// two global key spaces that no namespace owned:
//
//	blob:<blobID>   → BlobEntry
//	chunk:<chunkID> → ChunkEntry
//
// The accessors below read and remove those records. They exist only for the
// one-time upgrade to format 2; nothing else should call them.

func keyLegacyBlob(id object.BlobID) []byte   { return []byte("blob:" + string(id)) }
func keyLegacyChunk(id object.ChunkID) []byte { return []byte("chunk:" + string(id)) }

var (
	prefixLegacyBlob  = []byte("blob:")
	prefixLegacyChunk = []byte("chunk:")
)

// GetLegacyBlob reads a format-1 global blob manifest.
func (idx *Index) GetLegacyBlob(ctx context.Context, id object.BlobID) (*object.BlobEntry, error) {
	v, err := idx.b.Get(ctx, keyLegacyBlob(id))
	if err != nil {
		return nil, err
	}
	var e object.BlobEntry
	return &e, unmarshal(v, &e)
}

// GetLegacyChunk reads a format-1 global chunk record.
func (idx *Index) GetLegacyChunk(ctx context.Context, id object.ChunkID) (*object.ChunkEntry, error) {
	v, err := idx.b.Get(ctx, keyLegacyChunk(id))
	if err != nil {
		return nil, err
	}
	var e object.ChunkEntry
	return &e, unmarshal(v, &e)
}

// ScanLegacyBlobs visits every format-1 global blob manifest.
func (idx *Index) ScanLegacyBlobs(ctx context.Context, fn func(object.BlobEntry) error) error {
	return idx.b.Scan(ctx, prefixLegacyBlob, func(_, v []byte) error {
		var e object.BlobEntry
		if err := unmarshal(v, &e); err != nil {
			return err
		}
		return fn(e)
	})
}

// ScanLegacyChunks visits every format-1 global chunk record.
func (idx *Index) ScanLegacyChunks(ctx context.Context, fn func(object.ChunkEntry) error) error {
	return idx.b.Scan(ctx, prefixLegacyChunk, func(_, v []byte) error {
		var e object.ChunkEntry
		if err := unmarshal(v, &e); err != nil {
			return err
		}
		return fn(e)
	})
}

// HasLegacyRecords reports whether any format-1 global record remains.
func (idx *Index) HasLegacyRecords(ctx context.Context) (bool, error) {
	for _, p := range [][]byte{prefixLegacyBlob, prefixLegacyChunk} {
		found := false
		err := idx.b.Scan(ctx, p, func(_, _ []byte) error {
			found = true
			return errStopScan
		})
		if err != nil && !errors.Is(err, errStopScan) {
			return false, err
		}
		if found {
			return true, nil
		}
	}
	return false, nil
}

// DeleteLegacyRecords removes every format-1 global blob and chunk record.
func (idx *Index) DeleteLegacyRecords(ctx context.Context) error {
	for _, p := range [][]byte{prefixLegacyBlob, prefixLegacyChunk} {
		if err := idx.deletePrefix(ctx, p); err != nil {
			return err
		}
	}
	return nil
}
