package index_test

import (
	"testing"

	"github.com/asaidimu/blobs/index"
	"github.com/asaidimu/blobs/index/indextest"
)

// TestMemoryBackendCompliance exercises index.MemoryBackend against the
// same contract every index.Backend implementation must satisfy. This is
// the test file called out as missing in the production readiness report
// (section 5.4) — running it here, and against backends.BboltBackend in
// index/backends/bbolt_test.go, guarantees the two never quietly diverge.
func TestMemoryBackendCompliance(t *testing.T) {
	indextest.Run(t, func(t *testing.T) index.Backend {
		return index.NewMemoryBackend()
	})
}
