package chunking

import (
	"bytes"
	"io"
	"math/rand"
	"testing"
)

const MiB = 1024 * 1024

// testBytes returns a deterministic pseudo-random buffer seeded by seed. Using a
// fixed-seed PRNG (not the global source) keeps the test reproducible across
// runs and parallel-safe.
func testBytes(seed int64, size int) []byte {
	buf := make([]byte, size)
	rand.New(rand.NewSource(seed)).Read(buf)
	return buf
}

func chunkBoundaries(t *testing.T, data []byte, opts Options) []Chunk {
	t.Helper()
	c, err := New(bytes.NewReader(data), opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var chunks []Chunk
	for {
		ch, err := c.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		chunks = append(chunks, *ch)
	}
	return chunks
}

// boundaries returns the absolute end offsets of every chunk (the cut points).
func boundaries(chunks []Chunk) []int64 {
	offs := make([]int64, 0, len(chunks))
	for _, ch := range chunks {
		offs = append(offs, ch.Offset+int64(ch.Size))
	}
	return offs
}

func TestDeterministic(t *testing.T) {
	data := testBytes(1, 8*MiB)
	opts := Options{Avg: 256 * 1024}

	one := chunkBoundaries(t, data, opts)
	two := chunkBoundaries(t, data, opts)
	if len(one) != len(two) {
		t.Fatalf("boundary count differs across runs: %d vs %d", len(one), len(two))
	}
	for i := range one {
		if one[i].Offset != two[i].Offset || one[i].Size != two[i].Size {
			t.Fatalf("boundary %d differs: (%d,%d) vs (%d,%d)",
				i, one[i].Offset, one[i].Size, two[i].Offset, two[i].Size)
		}
	}
}

func TestBounds(t *testing.T) {
	const avg = 256 * 1024
	opts := Options{Avg: avg}
	data := testBytes(2, 16*MiB)
	chunks := chunkBoundaries(t, data, opts)

	min, max := opts.Min, opts.Max
	if min == 0 {
		min = avg >> 2
	}
	if max == 0 {
		max = avg << 2
	}

	for i, ch := range chunks {
		if ch.Size > max {
			t.Fatalf("chunk %d size %d exceeds max %d", i, ch.Size, max)
		}
		// Every chunk except the final one must respect the minimum; the
		// last chunk of a stream may legitimately undercut it.
		if i < len(chunks)-1 && ch.Size < min {
			t.Fatalf("chunk %d size %d below min %d (idx %d of %d)", i, ch.Size, min, i, len(chunks))
		}
		if ch.Size < 1 {
			t.Fatalf("chunk %d has empty size", i)
		}
	}

	var total, n int
	for _, ch := range chunks {
		total += ch.Size
		n++
	}
	if total != len(data) {
		t.Fatalf("chunks reassemble to %d bytes, want %d", total, len(data))
	}
	gotAvg := float64(total) / float64(n)
	if gotAvg < float64(avg)/2 || gotAvg > float64(avg)*2 {
		t.Fatalf("average chunk size %.0f far from target %d", gotAvg, avg)
	}
}

// TestBoundaryStabilityUnderPrefixInsertion is the dedup property the store
// actually cares about: adding a different prefix in front of a run of bytes
// must NOT shift the chunk boundaries inside that run. This is exactly the case
// that fixed-size chunking fails and that motivated content-defined chunking —
// a blob and "blob plus a prefix" should share almost all of their chunks.
func TestBoundaryStabilityUnderPrefixInsertion(t *testing.T) {
	const (
		data   = 24 * MiB
		prefix = 6 * MiB
	)
	opts := Options{Avg: 256 * 1024}
	opts.Min, opts.Max = opts.Avg>>2, opts.Avg<<2 // explicit for the warmup threshold

	run := testBytes(3, data)

	runEnds := boundaries(chunkBoundaries(t, run, opts))
	prefixedEnds := boundaries(chunkBoundaries(t, append(testBytes(4, prefix), run...), opts))

	endSet := make(map[int64]bool, len(prefixedEnds))
	for _, e := range prefixedEnds {
		endSet[e] = true
	}

	// Boundaries deep inside `run` (well past both the splice point and any
	// rolling-hash warm-up) must reappear, shifted by the prefix length.
	warm := int64(opts.Max * 4)
	matched := 0
	for _, e := range runEnds {
		if e < warm {
			continue
		}
		if !endSet[e+int64(prefix)] {
			t.Fatalf("boundary at run offset %d (%d+%d) disappeared after prefix insertion", e, int64(prefix), e)
		}
		matched++
	}
	if matched < 10 {
		t.Fatalf("too few deep boundaries (%d) to make the assertion meaningful", matched)
	}
}

func TestShortInput_SingleChunk(t *testing.T) {
	data := testBytes(5, 1024) // well under avg 256KB
	chunks := chunkBoundaries(t, data, Options{Avg: 256 * 1024})
	if len(chunks) != 1 {
		t.Fatalf("short stream produced %d chunks, want 1", len(chunks))
	}
	if chunks[0].Size != len(data) {
		t.Fatalf("short chunk size %d, want %d", chunks[0].Size, len(data))
	}
	if chunks[0].Offset != 0 {
		t.Fatalf("first chunk offset %d, want 0", chunks[0].Offset)
	}
}

func TestEmptyInput_EarlyEOF(t *testing.T) {
	c, err := New(bytes.NewReader(nil), Options{Avg: 256 * 1024})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := c.Next(); err != io.EOF {
		t.Fatalf("Next on empty input = %v, want io.EOF", err)
	}
}

func TestNew_RejectsInvalidAverage(t *testing.T) {
	for _, avg := range []int{1, 2, 3, -8} {
		if _, err := New(bytes.NewReader(nil), Options{Avg: avg}); err == nil {
			t.Errorf("Avg=%d: expected an error (default Min = Avg/4 must be >= 1)", avg)
		}
	}
}

func TestNew_AcceptsNonPowerOfTwoAverage(t *testing.T) {
	for _, avg := range []int{5, 100000, 600 * 1024} {
		if _, err := New(bytes.NewReader(nil), Options{Avg: avg}); err != nil {
			t.Errorf("Avg=%d: unexpected error %v", avg, err)
		}
	}
}

func TestReset_ReusesConfiguration(t *testing.T) {
	dataA := testBytes(6, 4*MiB)
	dataB := testBytes(7, 4*MiB)
	opts := Options{Avg: 512 * 1024}

	c, err := New(bytes.NewReader(dataA), opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	a1, err := c.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	_ = a1 // consume at least once before Reset

	c.Reset(bytes.NewReader(dataB))
	var got uint64
	for {
		ch, err := c.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next after Reset: %v", err)
		}
		got += uint64(ch.Size)
	}
	if got != uint64(len(dataB)) {
		t.Fatalf("Reset reassembled %d bytes, want %d", got, len(dataB))
	}
}
