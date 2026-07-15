package volume_test

import (
	"strings"
	"testing"

	"github.com/asaidimu/blobs/volume"
)

// pageHeaderSize mirrors the unexported constant of the same name in
// package volume (currently 88 bytes: see volume.go's pageHeader layout
// comment). It is duplicated here, as a literal, only because this test
// lives in the external volume_test package and therefore cannot import
// an unexported identifier — if the header layout ever changes, the
// corresponding boundary tests below need updating too.
const pageHeaderSize = 88

func TestOptionsValidate_ZeroValueIsValid(t *testing.T) {
	if err := (volume.Options{}).Validate(); err != nil {
		t.Fatalf("zero-value Options.Validate() = %v, want nil (zero fields fall back to defaults, which must be valid)", err)
	}
}

func TestOptionsValidate_ExplicitValidValuesPass(t *testing.T) {
	opts := volume.Options{
		PageSize:       32 * 1024,
		ChunkSize:      1024 * 1024,
		MaxSegmentSize: 64 * 1024 * 1024,
	}
	if err := opts.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil for a sensible explicit config", err)
	}
}

func TestOptionsValidate_NegativeFieldsRejected(t *testing.T) {
	cases := []struct {
		name string
		opts volume.Options
	}{
		{"NegativePageSize", volume.Options{PageSize: -1}},
		{"NegativeChunkSize", volume.Options{ChunkSize: -1}},
		{"NegativeMaxSegmentSize", volume.Options{MaxSegmentSize: -1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.opts.Validate(); err == nil {
				t.Fatalf("Validate() = nil for %+v, want an error", tc.opts)
			}
		})
	}
}

// TestOptionsValidate_PageSizeBelowHeaderSize is the exact scenario named
// in the production readiness report (section 5.8): PageSize=50 is
// smaller than pageHeaderSize=88 and must be rejected instead of causing
// a panic or silent corruption at runtime.
func TestOptionsValidate_PageSizeBelowHeaderSize(t *testing.T) {
	err := volume.Options{PageSize: 50}.Validate()
	if err == nil {
		t.Fatal("Validate() = nil for PageSize=50 (< pageHeaderSize=88), want an error")
	}
	if !strings.Contains(err.Error(), "50") {
		t.Errorf("error %q does not mention the offending value 50", err.Error())
	}
}

func TestOptionsValidate_PageSizeAtHeaderSizeBoundary(t *testing.T) {
	// Exactly pageHeaderSize: zero bytes of payload capacity. Must still
	// be rejected — a page needs room for at least one payload byte.
	if err := (volume.Options{PageSize: pageHeaderSize}).Validate(); err == nil {
		t.Fatalf("Validate() = nil for PageSize == pageHeaderSize (%d), want an error (zero payload capacity)", pageHeaderSize)
	}
	// One byte over: the smallest valid PageSize. ChunkSize must also be
	// set small here, or it would default to 4MB and trip the unrelated
	// "ChunkSize must fit within MaxSegmentSize" cross-field check instead
	// of testing the PageSize boundary in isolation.
	small := volume.Options{PageSize: pageHeaderSize + 1, ChunkSize: 1, MaxSegmentSize: pageHeaderSize + 1}
	if err := small.Validate(); err != nil {
		t.Fatalf("Validate() = %v for PageSize == pageHeaderSize+1, want nil (one byte of payload capacity is valid)", err)
	}
}

func TestOptionsValidate_PageSizeOverflowsUint32(t *testing.T) {
	// PageSize is encoded as a uint32 in the segment header. A value
	// beyond uint32 range must be rejected rather than silently truncated
	// on encode (the same bug class as VULN 1 / VULN 2, different field).
	const tooLarge = int(1) << 33 // 8 GiB — comfortably beyond uint32 range on a 64-bit int
	if err := (volume.Options{PageSize: tooLarge}).Validate(); err == nil {
		t.Fatalf("Validate() = nil for PageSize=%d (overflows uint32), want an error", tooLarge)
	}
}

func TestOptionsValidate_ChunkSizeExceedingMaxSegmentSizeRejected(t *testing.T) {
	// A single chunk must fit within one segment. This is a cross-field
	// check evaluated on the *resolved* (defaults-applied) values, so it
	// also catches an explicit MaxSegmentSize that's smaller than the
	// default ChunkSize even though ChunkSize itself was never set.
	err := volume.Options{MaxSegmentSize: 1024 * 1024}.Validate() // 1MB segment, default 4MB chunk
	if err == nil {
		t.Fatal("Validate() = nil for MaxSegmentSize smaller than the default ChunkSize, want an error")
	}

	err = volume.Options{ChunkSize: 10 * 1024 * 1024, MaxSegmentSize: 1024 * 1024}.Validate()
	if err == nil {
		t.Fatal("Validate() = nil for explicit ChunkSize > explicit MaxSegmentSize, want an error")
	}
}

func TestOptionsValidate_PageSizeExceedingMaxSegmentSizeRejected(t *testing.T) {
	// A segment must be able to hold at least one page.
	err := volume.Options{
		PageSize:       64 * 1024,
		ChunkSize:      1024,
		MaxSegmentSize: 32 * 1024, // smaller than PageSize
	}.Validate()
	if err == nil {
		t.Fatal("Validate() = nil for PageSize > MaxSegmentSize, want an error")
	}
}

func TestOptionsValidate_DefaultsAreInternallyConsistent(t *testing.T) {
	// Sanity check: the package defaults themselves must always pass
	// Validate(), or every zero-value Options{} would be broken.
	err := volume.Options{
		PageSize:       volume.DefaultPageSize,
		ChunkSize:      volume.DefaultChunkSize,
		MaxSegmentSize: volume.DefaultMaxSegmentSize,
	}.Validate()
	if err != nil {
		t.Fatalf("Validate() = %v for the package's own defaults, want nil", err)
	}
}
