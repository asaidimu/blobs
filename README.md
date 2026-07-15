# blobs

[![Go Version](https://img.shields.io/github/go-mod/go-version/asaidimu/blobs)](https://go.dev)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE.md)

`blobs` is a high-performance, content-addressed blob storage engine for Go.
Data is immutable and addressed by SHA-256 hash — identical content is stored once.
Metadata lives in a pluggable index; raw data lives in segment files with a WAL
for crash recovery.

---

## Features

- **Content addressing** — blobs keyed by `sha256:<hex>`; deduplication is automatic.
- **Namespace isolation** — logical partitions with optional per-namespace quotas.
- **Pluggable index** — `MemoryBackend` for tests, `BboltBackend` for production (ACID, single-file, crash-safe).
- **Segment-based storage** — fixed-size pages (default 16 KB), cache-aligned headers, zero-allocation read path.
- **Write-Ahead Log (WAL)** — fsync'd before return; survive process crash after `Put` succeeds.
- **Crash recovery** — `RebuildIndex` reconstructs chunk locations by scanning segment files.
- **Compaction** — marks dead chunks in-place (phase 1); segment rewriting reclaims disk (phase 2).
- **Data verification** — CRC-32 per page + SHA-256 content hash; `Verify` endpoint checks integrity.
- **MIME auto-detection** — when `ContentType` is empty, `Put` sniffs the first 3072 bytes via the `mimetype` library; no more manual `ContentType` boilerplate.
- **Graceful shutdown** — `Close` drains in-flight operations before tearing down; `Get`'s reader holds a read guard until the caller closes it.
- **Eager config validation** — negative sizes, page ≤ header, chunk > segment, and other nonsensical options are rejected at `Open` time, before any disk I/O.
- **Security audited** — 10 findings (2 critical, 2 high, 4 medium, 2 info) all fixed and regression-tested.

---

## Index Backends

| Backend | Package | Use |
|---|---|---|
| `MemoryBackend` | `index.NewMemoryBackend()` | Tests only — data lost on restart. |
| `BboltBackend` | `index/backend.Open(...)` | Production — embedded, ACID, single-file, survives restart. |

---

## Opening a Store

```go
import (
    "github.com/asaidimu/blobs/index/backend"
    "github.com/asaidimu/blobs/store"
)

// ── With bbolt (production) ──────────────────────────────────────────
idx, err := backend.Open(backend.Options{Path: "/data/blobs/index.bbolt"})
s, err := store.Open(store.Config{DataDir: "/data/blobs", Index: idx})
defer s.Close()        // also closes the index

// ── With MemoryBackend (tests) ──────────────────────────────────────
s, err := store.Open(store.Config{
    DataDir: t.TempDir(),
    Index:   index.NewMemoryBackend(),
})
```

`Close` drains all in-flight operations before tearing down the index and
volume engines. For `Get`, the returned reader holds a read guard on the
store — always call `rc.Close()` to release it, or `Close` will block
indefinitely.

`Config` fields:

| Field | Default | Description |
|---|---|---|---|
| `DataDir` | required | Root for segment/WAL files. One subdirectory per namespace. |
| `Index` | required | Pluggable `Backend` — see table above. |
| `PageSize` | 16384 | Size of each page in segment files. Must be > 88 (header size) and ≤ uint32 max. |
| `ChunkSize` | 4 MB | Target chunk size before splitting. Must not exceed `MaxSegmentSize`. |
| `MaxSegmentSize` | 512 MB | Max size before rolling to a new segment file. |

All options are validated eagerly at `Open` — a bad config fails before any disk I/O.

A `"default"` namespace is created automatically on first open.

---

## Namespaces

```go
// Create a new namespace with a quota.
s.CreateNamespace(ctx, object.Namespace{
    ID:          "my-app",
    DisplayName: "My Application",
    Quota:       &object.Quota{MaxBytes: 1 << 30},
})

// List all namespaces.
nsList, _ := s.ListNamespaces(ctx)

// Get a scoped handle (cheap — no I/O).
ns := s.Namespace("my-app")

// Delete a namespace and all its refs (default cannot be deleted).
s.DeleteNamespace(ctx, "my-app")
```

Namespace IDs must match `^[a-z0-9][a-z0-9\-]{0,61}[a-z0-9]$` (2–63 chars,
lowercase alphanumeric plus hyphens). `"default"` is reserved.

---

## CRUD

### Put — write a blob

```go
info, err := ns.Put(ctx, "photos/vacation.jpg", file, store.PutOptions{
    Custom: map[string]string{"album": "2026"},
})
// info.Key, info.Metadata.BlobID, info.Metadata.Size, info.Metadata.ContentType ...
```

If `ContentType` is empty (default), `Put` auto-detects it by sniffing the first
3072 bytes via the [`mimetype`](https://github.com/gabriel-vasile/mimetype)
library. You can still override it explicitly:

```go
info, err := ns.Put(ctx, "script.sh", file, store.PutOptions{
    ContentType: "text/x-shellscript",
})
```

`Put` streams the reader through SHA-256, splits into chunks, writes them to a
segment with CRC-32, fsyncs, appends the WAL, then atomically commits ref + blob
manifest + chunk locations + stats to the index.

If the key already exists, the old ref is replaced and the previous blob becomes
GC-eligible (ref-counted). Duplicate content is automatically deduplicated.

### Get — read a blob

```go
rc, err := ns.Get(ctx, "photos/vacation.jpg")
defer rc.Close()
data, err := io.ReadAll(rc)
```

Returns a streaming reader that reassembles chunks on-the-fly.
Returns `*errors.NotFoundError` if the key doesn't exist.

### Head — get metadata without reading data

```go
info, err := ns.Head(ctx, "photos/vacation.jpg")
// info.Metadata.Size, info.Metadata.BlobID, info.Metadata.CreatedAt ...
```

### Delete — remove a ref

```go
err := ns.Delete(ctx, "photos/vacation.jpg")    // idempotent — returns nil if missing
```

Removes the ref entry. The underlying blob's ref count is decremented; chunks
are not reclaimed until `Compact` runs.

### List — enumerate keys

```go
// All keys
items, _ := ns.List(ctx, store.ListOptions{})

// Prefix filter
items, _ := ns.List(ctx, store.ListOptions{KeyPrefix: "photos/"})

// Pagination (lexicographic)
items, _ := ns.List(ctx, store.ListOptions{After: "photos/vacation.jpg", Limit: 50})
```

Returns `[]object.BlobInfo` in lexicographic order.

---

## Stats

```go
// Per-namespace
s, _ := ns.Stats(ctx)
// s.BlobCount, s.BytesStored, s.BytesPhysical, s.ChunkCount,
// s.DeadBytes, s.SegmentCount, s.UpdatedAt

// Aggregate across all namespaces
global, _ := store.Stats(ctx)
// global.TotalBlobCount, global.TotalBytesStored, global.TotalBytesPhysical,
// global.DeduplicationRatio, global.PerNamespace
```

---

## Maintenance

### Verify — check data integrity

```go
err := ns.Verify(ctx)
// Returns first *errors.CorruptionError found, or nil.
```

Reads every chunk for every blob in the namespace and verifies its CRC-32.
Expensive — run offline.

### RebuildIndex — reconstruct chunk index from segment files

```go
err := ns.RebuildIndex(ctx)
```

Scans all segment files on disk and repopulates chunk entries in the index.
Does not reconstruct refs (those come from the WAL or a backup). Call after
`Open` when the dirty flag indicates an unclean shutdown.

### Compact — reclaim dead space (phase 1)

```go
result, err := ns.Compact(ctx)
// result.BlobsRemoved, result.ChunksRemoved, result.BytesFreed
```

Walks all blobs with `RefCount == 0`, marks their page headers as deleted in
the segment files, and removes blob/chunk entries from the index. Phase 2
(segment rewriting to reclaim disk space) is not yet implemented.

---

## Error Handling

All public API errors are typed in the `errors` package:

```go
import bserrors "github.com/asaidimu/blobs/errors"

var target *bserrors.NotFoundError
if errors.As(err, &target) {
    fmt.Println("not found:", target.NamespaceID, target.Key)
}
```

| Type | When |
|---|---|
| `*NotFoundError` | Key or namespace doesn't exist. |
| `*AlreadyExistsError` | Creating a namespace that already exists. |
| `*QuotaExceededError` | Put would exceed namespace limits. |
| `*CorruptionError` | CRC-32 mismatch — data corruption on disk. |
| `*InvalidKeyError` | Empty or malformed key. |
| `*InvalidNamespaceIDError` | Namespace ID fails validation. |
| `*ClosedError` | Operation on a closed Store. |

---

## Full Example

See [`examples/basic/main.go`](examples/basic/main.go) for a walkthrough that
writes a blob, reads it back, lists keys, checks stats, closes the store, and
reopens it to prove data survives a restart.

```go
import (
    "github.com/asaidimu/blobs/index/backend"
    "github.com/asaidimu/blobs/store"
)

idx, _ := backend.Open(backend.Options{Path: "index.bbolt"})
s, _     := store.Open(store.Config{DataDir: "data", Index: idx})
defer s.Close()

ns := s.Namespace("default")
ns.Put(ctx, "key", reader, store.PutOptions{})
rc, _      := ns.Get(ctx, "key")
items, _   := ns.List(ctx, store.ListOptions{})
stats, _   := ns.Stats(ctx)
ns.Delete(ctx, "key")
```

---

## Benchmarks

Intel Xeon @ 2.80 GHz, Linux/amd64:

| Operation | Throughput |
|---|---|
| WriteBlob (4 MB) | 101 MB/s |
| ReadChunk (4 MB) | 3,813 MB/s |
| ReadChunk concurrent (64 KB) | 3,190 MB/s |

Write throughput is fsync-bound; expect 400–800 MB/s on NVMe with
battery-backed write cache.

---

## Development

```bash
make build      # go build -v ./...
make test       # go clean -testcache && go test -v ./...
go test -race ./...
```

All 119 tests pass with no race conditions.

---

## License

MIT — see [LICENSE.md](LICENSE.md).
