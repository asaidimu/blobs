package store

import (
	"context"
	"fmt"
	"sort"

	"github.com/asaidimu/blobs/index"
	"github.com/asaidimu/blobs/object"
	"github.com/asaidimu/blobs/volume"
)

// migrateBatch bounds how many records one migration transaction writes.
const migrateBatch = 500

// UnrecoverableChunk describes a chunk that a live blob needs but that the
// upgrade could not reconstruct. The blob's key still exists; reading it will
// fail, exactly as it did before the upgrade.
type UnrecoverableChunk struct {
	Namespace string
	Key       string
	ChunkID   object.ChunkID
	Reason    string
}

// MigrationReport describes what Open did to upgrade an index written by an
// older version. A store that needed no upgrade reports Ran == false.
type MigrationReport struct {
	// Ran is true if this Open upgraded a format-1 index.
	Ran bool
	// Namespaces, Blobs and Chunks count the namespaces, blob manifests and
	// chunk records written in the new layout.
	Namespaces, Blobs, Chunks int
	// ChunksRebuilt counts chunk records that had to be recovered from the
	// namespace's own segment pages because the old global record had been
	// overwritten by a different namespace storing identical content.
	ChunksRebuilt int
	// Unrecoverable lists chunks that could not be recovered; see
	// UnrecoverableChunk. Empty in the normal case.
	Unrecoverable []UnrecoverableChunk
}

// MigrationReport returns what Open did to upgrade the index. It is the zero
// value unless the index needed upgrading.
func (s *Store) MigrationReport() MigrationReport { return s.migration }

// migrateLegacy upgrades a format-1 index to format 2 in place.
//
// Format 1 kept blob manifests and chunk records in two global key spaces that
// no namespace owned. Format 2 gives every namespace its own. Namespace
// directories do not move: a pre-UID namespace's UID is its name.
//
// For each namespace the upgrade:
//
//   - recomputes blob and chunk reference counts from the namespace's actual
//     refs (this also repairs counts inflated by overwriting a key with
//     identical content), and
//   - copies the chunk records it owns, rebuilding from its own segment pages
//     any record the old code had let another namespace overwrite.
//
// Blobs and chunks that no ref points at (recovered by RebuildIndex or WAL
// replay, presumed referenced) keep their reference counts, so Compact
// treats them exactly as before.
//
// The upgrade writes the new records first, then the format marker, then
// deletes the old ones, so a crash at any point either repeats the whole
// upgrade (idempotent) or merely leaves old records to be deleted on the next
// Open.
func (s *Store) migrateLegacy(ctx context.Context) error {
	format, err := s.idx.Format(ctx)
	if err != nil {
		return err
	}
	switch format {
	case index.FormatCurrent:
		return s.dropLegacyRecords(ctx)
	case "":
	default:
		return fmt.Errorf("index format %q is newer than this version understands (%q)", format, index.FormatCurrent)
	}

	namespaces, err := s.idx.ListNamespaces(ctx)
	if err != nil {
		return err
	}
	hasLegacy, err := s.idx.HasLegacyRecords(ctx)
	if err != nil {
		return err
	}
	if len(namespaces) == 0 && !hasLegacy {
		return s.idx.SetFormat(ctx, index.FormatCurrent) // fresh store
	}

	rep := MigrationReport{Ran: true}
	owners := make(map[string]object.Namespace, len(namespaces))
	for i := range namespaces {
		ns := namespaces[i]
		if ns.UID == "" {
			ns.UID = ns.ID
			if err := s.idx.PutNamespace(ctx, ns); err != nil {
				return fmt.Errorf("assign uid to namespace %q: %w", ns.ID, err)
			}
		}
		owners[ns.UID] = ns
	}

	referenced := make(map[object.BlobID]bool)
	names := make([]string, 0, len(owners))
	for uid := range owners {
		names = append(names, uid)
	}
	sort.Strings(names)
	for _, uid := range names {
		if err := s.migrateNamespace(ctx, owners[uid], referenced, &rep); err != nil {
			return fmt.Errorf("namespace %q: %w", uid, err)
		}
		rep.Namespaces++
	}
	if err := s.migrateOrphans(ctx, owners, referenced, &rep); err != nil {
		return fmt.Errorf("unreferenced records: %w", err)
	}

	if err := s.idx.SetFormat(ctx, index.FormatCurrent); err != nil {
		return err
	}
	if err := s.dropLegacyRecords(ctx); err != nil {
		return err
	}
	s.migration = rep
	return nil
}

func (s *Store) dropLegacyRecords(ctx context.Context) error {
	has, err := s.idx.HasLegacyRecords(ctx)
	if err != nil || !has {
		return err
	}
	return s.idx.DeleteLegacyRecords(ctx)
}

func (s *Store) migrateNamespace(ctx context.Context, ns object.Namespace, referenced map[object.BlobID]bool, rep *MigrationReport) error {
	uid := ns.UID

	refs, err := s.idx.ListRefs(ctx, uid, "")
	if err != nil {
		return err
	}
	refsPerBlob := make(map[object.BlobID]int64)
	keyOfBlob := make(map[object.BlobID]string)
	for _, r := range refs {
		refsPerBlob[r.BlobID]++
		if _, ok := keyOfBlob[r.BlobID]; !ok {
			keyOfBlob[r.BlobID] = r.Key
		}
		referenced[r.BlobID] = true
	}

	manifests := make(map[object.BlobID]object.BlobEntry, len(refsPerBlob))
	chunkRefs := make(map[object.ChunkID]int64)
	for id, n := range refsPerBlob {
		bm, err := s.idx.GetLegacyBlob(ctx, id)
		if err != nil {
			if index.IsNotFound(err) {
				rep.Unrecoverable = append(rep.Unrecoverable, UnrecoverableChunk{
					Namespace: ns.ID, Key: keyOfBlob[id], Reason: "blob manifest missing from index",
				})
				continue
			}
			return err
		}
		bm.RefCount = n
		manifests[id] = *bm
		for _, cid := range bm.ChunkIDs {
			chunkRefs[cid] += n
		}
	}

	resolved := make(map[object.ChunkID]object.ChunkEntry, len(chunkRefs))
	var missing []object.ChunkID
	for cid := range chunkRefs {
		lc, err := s.idx.GetLegacyChunk(ctx, cid)
		switch {
		case err == nil && lc.NamespaceID == uid:
			resolved[cid] = *lc
		case err == nil || index.IsNotFound(err):
			// Absent, or overwritten by another namespace storing the same
			// content: this namespace's own copy is still on disk.
			missing = append(missing, cid)
		default:
			return err
		}
	}

	if len(missing) > 0 {
		onDisk, err := s.scanLivePages(uid)
		if err != nil {
			return fmt.Errorf("scan segments: %w", err)
		}
		encrypted := ns.Encryption != nil && ns.Encryption.Enabled
		for _, cid := range missing {
			e, ok := onDisk[cid]
			switch {
			case !ok:
				rep.Unrecoverable = append(rep.Unrecoverable, UnrecoverableChunk{
					Namespace: ns.ID, ChunkID: cid, Reason: "chunk record lost and no live page found in segments",
				})
			case encrypted:
				// The nonce and tag live only in the lost record.
				rep.Unrecoverable = append(rep.Unrecoverable, UnrecoverableChunk{
					Namespace: ns.ID, ChunkID: cid, Reason: "encrypted chunk's nonce/tag were in the overwritten record",
				})
			default:
				resolved[cid] = e
				rep.ChunksRebuilt++
			}
		}
	}

	var blobs []object.BlobEntry
	var chunks []object.ChunkEntry
	flush := func() error {
		if len(blobs) == 0 && len(chunks) == 0 {
			return nil
		}
		err := s.idx.PutRecords(ctx, uid, blobs, chunks)
		blobs, chunks = blobs[:0], chunks[:0]
		return err
	}
	for _, bm := range manifests {
		blobs = append(blobs, bm)
		rep.Blobs++
		if len(blobs)+len(chunks) >= migrateBatch {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	for cid, e := range resolved {
		e.RefCount = chunkRefs[cid]
		chunks = append(chunks, e)
		rep.Chunks++
		if len(blobs)+len(chunks) >= migrateBatch {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

// migrateOrphans carries over records no ref points at: blobs and chunks that
// RebuildIndex or WAL replay registered as presumed-referenced, and dead
// records awaiting Compact. Records belonging to namespaces that no longer
// exist are dropped with the old key space.
func (s *Store) migrateOrphans(ctx context.Context, owners map[string]object.Namespace, referenced map[object.BlobID]bool, rep *MigrationReport) error {
	var chunks []object.ChunkEntry
	if err := s.idx.ScanLegacyChunks(ctx, func(lc object.ChunkEntry) error {
		if _, ok := owners[lc.NamespaceID]; ok {
			chunks = append(chunks, lc)
		}
		return nil
	}); err != nil {
		return err
	}
	byOwner := make(map[string][]object.ChunkEntry)
	for _, lc := range chunks {
		if _, err := s.idx.GetChunk(ctx, lc.NamespaceID, lc.ChunkID); err == nil {
			continue // already migrated as part of a live blob
		} else if !index.IsNotFound(err) {
			return err
		}
		byOwner[lc.NamespaceID] = append(byOwner[lc.NamespaceID], lc)
	}

	type orphanBlob struct {
		owner string
		blob  object.BlobEntry
	}
	var blobs []orphanBlob
	var legacyBlobs []object.BlobEntry
	if err := s.idx.ScanLegacyBlobs(ctx, func(b object.BlobEntry) error {
		if !referenced[b.BlobID] && len(b.ChunkIDs) > 0 {
			legacyBlobs = append(legacyBlobs, b)
		}
		return nil
	}); err != nil {
		return err
	}
	for _, b := range legacyBlobs {
		first, err := s.idx.GetLegacyChunk(ctx, b.ChunkIDs[0])
		if err != nil {
			if index.IsNotFound(err) {
				continue
			}
			return err
		}
		if _, ok := owners[first.NamespaceID]; ok {
			blobs = append(blobs, orphanBlob{first.NamespaceID, b})
		}
	}

	byOwnerBlobs := make(map[string][]object.BlobEntry)
	for _, ob := range blobs {
		byOwnerBlobs[ob.owner] = append(byOwnerBlobs[ob.owner], ob.blob)
	}
	for owner := range owners {
		cs, bs := byOwner[owner], byOwnerBlobs[owner]
		for len(cs) > 0 || len(bs) > 0 {
			nc, nb := min(len(cs), migrateBatch), min(len(bs), migrateBatch)
			if err := s.idx.PutRecords(ctx, owner, bs[:nb], cs[:nc]); err != nil {
				return err
			}
			rep.Chunks += nc
			rep.Blobs += nb
			cs, bs = cs[nc:], bs[nb:]
		}
	}
	return nil
}

// scanLivePages returns, for a namespace's directory, the first live (not
// flagged deleted) physical copy found for every chunk ID, as a ChunkEntry that
// ReadChunk can use directly. It reads page headers only, so it works for
// encrypted namespaces too.
//
// volume.Engine.ScanSegments reports every *page* as its own entry. A chunk
// larger than one page therefore appears as several entries: the first page
// carries the chunk's start (its page header's TotalChunks is non-zero) and
// each continuation page repeats the ChunkID with TotalChunks == 0. A record
// taken from a single page would describe only that page, so consecutive
// pages of the same chunk are merged here into one entry covering them all.
func (s *Store) scanLivePages(uid string) (map[object.ChunkID]object.ChunkEntry, error) {
	eng, err := volume.Open(s.cfg.DataDir, uid, volume.Options{
		PageSize:       s.cfg.PageSize,
		ChunkSize:      s.cfg.ChunkSize,
		MaxSegmentSize: s.cfg.MaxSegmentSize,
	})
	if err != nil {
		return nil, err
	}
	defer eng.Close()

	out := make(map[object.ChunkID]object.ChunkEntry)
	var (
		cur     object.ChunkEntry
		have    bool
		curDead bool
	)
	flush := func() {
		if have && !curDead {
			if _, ok := out[cur.ChunkID]; !ok {
				out[cur.ChunkID] = cur
			}
		}
		have = false
	}
	err = eng.ScanSegments(func(entry object.ChunkEntry, hdr volume.PageHeader) error {
		if hdr.TotalChunks > 0 { // first page of a chunk
			flush()
			cur, have, curDead = entry, true, hdr.Flags.IsDeleted()
			return nil
		}
		if have && entry.ChunkID == cur.ChunkID && entry.SegmentID == cur.SegmentID {
			cur.PageCount += entry.PageCount
			cur.Length += entry.Length
		}
		return nil
	})
	flush()
	return out, err
}
