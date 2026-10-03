package store_test

// Migration from the pre-UID layout (v1.3.x and earlier).
//
// The old layout keyed everything by namespace *name*: the directory was
// DataDir/<name>, refs/segments/stats were keyed by name, and blob manifests
// and chunk records lived in two global, un-owned key spaces ("blob:<id>" and
// "chunk:<id>"). These tests construct that layout by hand — real segment
// files via volume.Engine, hand-written index records in the old key format —
// and check that Open upgrades it in place without losing data.
//
// A legacy namespace's UID is its own name, so its directory does not move.

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/asaidimu/blobs/index"
	"github.com/asaidimu/blobs/index/backend"
	"github.com/asaidimu/blobs/object"
	"github.com/asaidimu/blobs/store"
	"github.com/asaidimu/blobs/volume"
)

type legacyBlob struct {
	key  string
	data []byte
}

func legacyPut(t *testing.T, b index.Backend, key string, v any) {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %s: %v", key, err)
	}
	if err := b.Put(context.Background(), []byte(key), raw); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
}

func legacyGetChunk(t *testing.T, b index.Backend, id object.ChunkID) (object.ChunkEntry, bool) {
	t.Helper()
	raw, err := b.Get(context.Background(), []byte("chunk:"+string(id)))
	if err != nil {
		return object.ChunkEntry{}, false
	}
	var c object.ChunkEntry
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	return c, true
}

func legacyGetBlob(t *testing.T, b index.Backend, id object.BlobID) (object.BlobEntry, bool) {
	t.Helper()
	raw, err := b.Get(context.Background(), []byte("blob:"+string(id)))
	if err != nil {
		return object.BlobEntry{}, false
	}
	var e object.BlobEntry
	if err := json.Unmarshal(raw, &e); err != nil {
		t.Fatal(err)
	}
	return e, true
}

// writeLegacyStore lays out spec (namespace → blobs) exactly as store v1.3.x
// did: one directory per namespace *name*, global blob:/chunk: records.
//
// When two namespaces write the same chunk, the later one overwrites the
// global chunk record with its own location and RefCount 1 — the (buggy)
// behaviour of the old CommitPut, which leaves the earlier namespace's blob
// pointing at the later namespace's location.
func writeLegacyStore(t *testing.T, dir string, b index.Backend, spec map[string][]legacyBlob) {
	t.Helper()

	names := make([]string, 0, len(spec))
	for n := range spec {
		names = append(names, n)
	}
	sort.Strings(names)

	for _, name := range names {
		legacyPut(t, b, "ns:"+name, object.Namespace{ID: name, CreatedAt: time.Now().UTC()})

		eng, err := volume.Open(dir, name, volume.Options{})
		if err != nil {
			t.Fatalf("volume.Open(%q): %v", name, err)
		}
		stats := object.NamespaceStats{NamespaceID: name}

		for _, lb := range spec[name] {
			res, err := eng.WriteBlob(bytes.NewReader(lb.data))
			if err != nil {
				t.Fatalf("WriteBlob: %v", err)
			}
			now := time.Now().UTC()
			legacyPut(t, b, "ref:"+name+":"+lb.key, object.RefEntry{
				NamespaceID: name,
				Key:         lb.key,
				BlobID:      res.BlobID,
				Metadata: object.Metadata{
					ContentType: "application/octet-stream",
					Size:        res.TotalSize,
					BlobID:      res.BlobID,
					ChunkCount:  len(res.Chunks),
					CreatedAt:   now,
					UpdatedAt:   now,
				},
			})

			be, ok := legacyGetBlob(t, b, res.BlobID)
			if ok {
				be.RefCount++
			} else {
				be = object.BlobEntry{BlobID: res.BlobID, TotalSize: res.TotalSize, RefCount: 1, CreatedAt: now}
				for _, c := range res.Chunks {
					be.ChunkIDs = append(be.ChunkIDs, c.ChunkID)
				}
			}
			legacyPut(t, b, "blob:"+string(res.BlobID), be)

			for _, c := range res.Chunks {
				c.RefCount = 1
				legacyPut(t, b, "chunk:"+string(c.ChunkID), c) // overwrite == clobber
				stats.ChunkCount++
				stats.BytesPhysical += c.Length
			}
			stats.BlobCount++
			stats.BytesStored += res.TotalSize
		}
		if err := eng.Close(); err != nil {
			t.Fatalf("engine close: %v", err)
		}
		legacyPut(t, b, "stats:"+name, stats)
	}
}

func openMigrated(t *testing.T, dir string, b index.Backend) *store.Store {
	t.Helper()
	s, err := store.Open(store.Config{DataDir: dir, Index: b})
	if err != nil {
		t.Fatalf("store.Open on legacy layout: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func assertNoLegacyKeys(t *testing.T, b index.Backend) {
	t.Helper()
	for _, prefix := range []string{"blob:", "chunk:"} {
		n := 0
		_ = b.Scan(context.Background(), []byte(prefix), func(k, _ []byte) error { n++; return nil })
		if n != 0 {
			t.Errorf("%d legacy %q records left in the index after migration", n, prefix)
		}
	}
}

// ── tests ────────────────────────────────────────────────────────────────────

func TestMigration_PreservesEveryBlobAndKeepsDirectories(t *testing.T) {
	dir := t.TempDir()
	b := index.NewMemoryBackend()

	spec := map[string][]legacyBlob{
		"tenant-a": {{"docs/one", randBytes(3000)}, {"docs/two", randBytes(40 << 10)}},
		"tenant-b": {{"img", randBytes(20 << 10)}},
	}
	writeLegacyStore(t, dir, b, spec)

	s := openMigrated(t, dir, b)
	ctx := context.Background()

	rep := s.MigrationReport()
	if !rep.Ran {
		t.Fatal("MigrationReport.Ran = false on a legacy store")
	}
	if rep.Namespaces != 2 {
		t.Errorf("MigrationReport.Namespaces = %d, want 2", rep.Namespaces)
	}
	if len(rep.Unrecoverable) != 0 {
		t.Errorf("unexpected unrecoverable chunks: %+v", rep.Unrecoverable)
	}

	for name, blobs := range spec {
		// A legacy namespace keeps its directory: UID == name.
		if uid := nsUID(t, s, name); uid != name {
			t.Errorf("legacy namespace %q should keep UID == name, got %q", name, uid)
		}
		ns := s.Namespace(name)
		for _, lb := range blobs {
			if got := getBytes(t, ns, lb.key); !bytes.Equal(got, lb.data) {
				t.Errorf("%s/%s: bytes differ after migration", name, lb.key)
			}
			info, err := ns.Head(ctx, lb.key)
			if err != nil || info.NamespaceID != name {
				t.Errorf("%s/%s: Head = (%+v, %v); BlobInfo.NamespaceID must be the namespace name", name, lb.key, info, err)
			}
		}
		if err := ns.Verify(ctx); err != nil {
			t.Errorf("%s: Verify after migration: %v", name, err)
		}
		st, err := ns.Stats(ctx)
		if err != nil || st.NamespaceID != name || st.BlobCount != int64(len(blobs)) {
			t.Errorf("%s: Stats = (%+v, %v)", name, st, err)
		}
	}
	assertNoLegacyKeys(t, b)
}

func TestMigration_IsIdempotentAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(t.TempDir(), "index.bbolt")

	bb, err := backend.Open(backend.Options{Path: idxPath})
	if err != nil {
		t.Fatal(err)
	}
	data := randBytes(10 << 10)
	writeLegacyStore(t, dir, bb, map[string][]legacyBlob{"tenant": {{"k", data}}})

	s1, err := store.Open(store.Config{DataDir: dir, Index: bb})
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if !s1.MigrationReport().Ran {
		t.Fatal("first Open should migrate")
	}
	if got := getBytes(t, s1.Namespace("tenant"), "k"); !bytes.Equal(got, data) {
		t.Fatal("data differs after first Open")
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	bb2, err := backend.Open(backend.Options{Path: idxPath})
	if err != nil {
		t.Fatal(err)
	}
	s2, err := store.Open(store.Config{DataDir: dir, Index: bb2})
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer s2.Close()
	if s2.MigrationReport().Ran {
		t.Error("second Open migrated again")
	}
	if got := getBytes(t, s2.Namespace("tenant"), "k"); !bytes.Equal(got, data) {
		t.Fatal("data differs after reopen")
	}
}

// The old CommitPut let a second namespace overwrite the first one's chunk
// record. Such a store has live blobs whose chunk locations point into the
// wrong namespace. Migration must repair that from the namespaces' own
// segment files.
func TestMigration_RepairsChunkRecordsClobberedAcrossNamespaces(t *testing.T) {
	dir := t.TempDir()
	b := index.NewMemoryBackend()

	shared := randBytes(48 << 10)
	filler := randBytes(30 << 10)
	spec := map[string][]legacyBlob{
		// tenant-a stores filler first so `shared` is at a different offset
		// in a's segment than in b's.
		"tenant-a": {{"filler", filler}, {"shared", shared}},
		"tenant-b": {{"shared", shared}},
	}
	writeLegacyStore(t, dir, b, spec)

	// Sanity: the fixture really is clobbered — the global record for the
	// shared chunk is owned by tenant-b.
	be, ok := legacyGetBlob(t, b, blobIDOfBytes(t, shared))
	if !ok {
		t.Fatal("fixture: shared blob manifest missing")
	}
	if c, ok := legacyGetChunk(t, b, be.ChunkIDs[0]); !ok || c.NamespaceID != "tenant-b" {
		t.Fatalf("fixture is not clobbered as intended: %+v", c)
	}

	s := openMigrated(t, dir, b)
	ctx := context.Background()

	rep := s.MigrationReport()
	if rep.ChunksRebuilt == 0 {
		t.Error("expected migration to rebuild at least one clobbered chunk record from segments")
	}
	if len(rep.Unrecoverable) != 0 {
		t.Errorf("unrecoverable: %+v", rep.Unrecoverable)
	}

	a, bn := s.Namespace("tenant-a"), s.Namespace("tenant-b")
	if got := getBytes(t, a, "shared"); !bytes.Equal(got, shared) {
		t.Fatal("tenant-a/shared wrong after migration")
	}
	if got := getBytes(t, a, "filler"); !bytes.Equal(got, filler) {
		t.Fatal("tenant-a/filler wrong after migration")
	}
	if got := getBytes(t, bn, "shared"); !bytes.Equal(got, shared) {
		t.Fatal("tenant-b/shared wrong after migration")
	}
	for _, ns := range []*store.NamespaceHandle{a, bn} {
		if err := ns.Verify(ctx); err != nil {
			t.Errorf("Verify: %v", err)
		}
	}

	// And the repaired namespaces are isolated from here on.
	if err := bn.Delete(ctx, "shared"); err != nil {
		t.Fatal(err)
	}
	if _, err := bn.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	if got := getBytes(t, a, "shared"); !bytes.Equal(got, shared) {
		t.Fatal("tenant-a lost data when tenant-b compacted after migration")
	}
}

// A legacy namespace can be deleted and re-created like any other; the old
// directory (named after the namespace) is erased.
func TestMigration_LegacyNamespaceCanBeDeletedAndRecreated(t *testing.T) {
	dir := t.TempDir()
	b := index.NewMemoryBackend()
	marker := []byte("LEGACY-NAMESPACE-MARKER-0007")
	writeLegacyStore(t, dir, b, map[string][]legacyBlob{
		"old-tenant": {{"k", secretBlob(string(marker), 4096)}},
	})

	s := openMigrated(t, dir, b)
	ctx := context.Background()
	if !dataDirContains(t, dir, marker) {
		t.Fatal("fixture: marker not on disk")
	}

	if err := s.DeleteNamespace(ctx, "old-tenant"); err != nil {
		t.Fatalf("DeleteNamespace: %v", err)
	}
	if dataDirContains(t, dir, marker) {
		t.Fatal("legacy namespace's bytes survived deletion")
	}
	if dirExists(filepath.Join(dir, "old-tenant")) {
		t.Fatal("legacy namespace directory survived deletion")
	}

	fresh := mustCreate(t, s, "old-tenant")
	if uid := nsUID(t, s, "old-tenant"); uid == "old-tenant" {
		t.Error("re-created namespace reused the legacy name-as-UID")
	}
	items, _ := fresh.List(ctx, store.ListOptions{})
	if len(items) != 0 {
		t.Errorf("re-created namespace has %d keys", len(items))
	}
	assertNoLegacyKeys(t, b)
}

// Blobs that were recovered by RebuildIndex or WAL replay are "presumed
// referenced" (RefCount 1, no ref) so Compact will not reap them while an
// operator re-establishes refs. Migration must not downgrade that.
func TestMigration_KeepsPresumedReferencedOrphansSafeFromCompact(t *testing.T) {
	dir := t.TempDir()
	b := index.NewMemoryBackend()
	ctx := context.Background()

	// A namespace with one normal blob, plus an orphan written straight to
	// the volume and registered the way WAL replay would register it.
	writeLegacyStore(t, dir, b, map[string][]legacyBlob{
		"tenant": {{"live", randBytes(2048)}},
	})
	eng, err := volume.Open(dir, "tenant", volume.Options{})
	if err != nil {
		t.Fatal(err)
	}
	orphanData := randBytes(8 << 10)
	res, err := eng.WriteBlob(bytes.NewReader(orphanData))
	if err != nil {
		t.Fatal(err)
	}
	eng.Close()
	ob := object.BlobEntry{BlobID: res.BlobID, TotalSize: res.TotalSize, RefCount: 1}
	for _, c := range res.Chunks {
		c.RefCount = 1
		legacyPut(t, b, "chunk:"+string(c.ChunkID), c)
		ob.ChunkIDs = append(ob.ChunkIDs, c.ChunkID)
	}
	legacyPut(t, b, "blob:"+string(res.BlobID), ob)

	s := openMigrated(t, dir, b)
	ns := s.Namespace("tenant")

	if _, err := ns.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	// Re-establishing the ref by re-Putting the same content must succeed
	// and read back, which requires the orphan's chunks to have survived.
	putBytes(t, ns, "recovered", orphanData)
	if got := getBytes(t, ns, "recovered"); !bytes.Equal(got, orphanData) {
		t.Fatal("recovered orphan reads back wrong")
	}
	if err := ns.Verify(ctx); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

// A brand-new store must not be reported as migrated.
func TestMigration_FreshStoreIsNotMigrated(t *testing.T) {
	s, _ := openStoreIn(t)
	if s.MigrationReport().Ran {
		t.Error("a fresh store reports that it ran a migration")
	}
}

func blobIDOfBytes(t *testing.T, data []byte) object.BlobID {
	t.Helper()
	dir := t.TempDir()
	eng, err := volume.Open(dir, "probe", volume.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	res, err := eng.WriteBlob(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return res.BlobID
}
