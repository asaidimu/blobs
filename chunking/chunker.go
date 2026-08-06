// Package chunking provides content-defined chunking (CDC) for the blobstore.
//
// Chunk boundaries are a pure function of the bytes themselves rather than of
// their offset in the stream, so a boundary that appears in one blob appears at
// the same content in every blob that contains the same run of bytes. This is
// what lets the store deduplicate a blob against another blob that is
// byte-identical except for a prefix (or suffix, or interior) insertion: the
// unchanged bytes still produce the same chunks, while only the chunks touching
// the edit point change.
//
// The implementation is the FastCDC 2020 algorithm
// (https://ieeexplore.ieee.org/document/9055082), provided by the dependency
// github.com/kalbasit/fastcdc. This package is a thin, stable wrapper:
// it owns the configuration defaults (min = avg/4, max = avg*4, normalized
// chunking at level 2) and exposes a streaming Next loop that the volume engine
// consumes, keeping the store code independent of the upstream API.
package chunking

import (
	"io"

	"github.com/kalbasit/fastcdc"
)

// DefaultNormalization is the FastCDC "normalized chunking" level used when
// Options.Normalization is zero. Level 2 keeps nearly all chunks close to the
// average size (see the upstream WithNormalization docs). Zero disables
// normalization, which yields chunk sizes that follow an offset exponential
// distribution instead.
const DefaultNormalization = 2

// Options configures a Chunker. Zero values fall back to sensible defaults.
type Options struct {
	// Min is the lower bound for chunk sizes. Defaults to Avg/4.
	// The last chunk of a stream may still undercut Min.
	Min int
	// Avg is the target average chunk size. Required; must be >= 4 (the
	// default Min = Avg/4 must stay >= 1). No power-of-two constraint.
	Avg int
	// Max is the upper bound for chunk sizes. Defaults to Avg*4. A boundary
	// is forced at Max even if the content never suggests one.
	Max int
	// Normalization is the FastCDC normalized-chunking level, 0–3. Zero means
	// DefaultNormalization. Higher values tighten the size distribution.
	Normalization int
}

// Chunk is a single content-defined region of the input stream. Data is only
// valid until the next call to Next (the underlying buffer is reused).
type Chunk struct {
	Offset int64  // absolute byte offset of the chunk in the stream
	Size   int    // length of the chunk in bytes
	Data   []byte // the chunk bytes; see the type comment for validity
}

// Chunker splits an io.Reader into content-defined chunks.
type Chunker struct {
	inner *fastcdc.Chunker
	avg   int
	min   int
	max   int
}

// New returns a Chunker that emits content-defined chunks from r.
func New(r io.Reader, opts Options) (*Chunker, error) {
	if opts.Avg == 0 {
		opts.Avg = 256 * 1024
	}
	if opts.Min == 0 {
		opts.Min = opts.Avg >> 2
	}
	if opts.Max == 0 {
		opts.Max = opts.Avg << 2
	}
	if opts.Normalization == 0 {
		opts.Normalization = DefaultNormalization
	}

	inner, err := fastcdc.NewChunker(
		r,
		fastcdc.WithTargetSize(uint32(opts.Avg)),
		fastcdc.WithMinSize(uint32(opts.Min)),
		fastcdc.WithMaxSize(uint32(opts.Max)),
		fastcdc.WithNormalization(uint8(opts.Normalization)),
	)
	if err != nil {
		return nil, err
	}
	return &Chunker{
		inner: inner,
		avg:   opts.Avg,
		min:   opts.Min,
		max:   opts.Max,
	}, nil
}

// Reset reinitializes the chunker to read from a new stream. Configuration is
// preserved; buffers are reused.
func (c *Chunker) Reset(r io.Reader) {
	c.inner.Reset(r)
}

// Avg returns the configured average chunk size in bytes.
func (c *Chunker) Avg() int { return c.avg }

// Min returns the configured minimum chunk size in bytes.
func (c *Chunker) Min() int { return c.min }

// Max returns the configured maximum chunk size in bytes.
func (c *Chunker) Max() int { return c.max }

// Next returns the next chunk, or io.EOF when the stream is exhausted and all
// buffered bytes have been emitted. The returned Chunk's Data is only valid
// until the next call to Next.
func (c *Chunker) Next() (*Chunk, error) {
	chunk, err := c.inner.Next()
	if err != nil {
		return nil, err
	}
	return &Chunk{
		Offset: int64(chunk.Offset),
		Size:   int(chunk.Length),
		Data:   chunk.Data,
	}, nil
}
