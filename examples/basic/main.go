// Command basicusage demonstrates the full public API of the blobstore:
// opening a Store backed by bbolt, writing and reading a blob, listing keys,
// checking stats, and — the actual point of the bbolt work — closing the
// store and reopening it to prove data survives a restart.
//
// Run it with:
//
//	go run ./examples/basic
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/asaidimu/blobs/index/backend"
	"github.com/asaidimu/blobs/store"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	// A real deployment points DataDir and the bbolt file at persistent,
	// durable storage. This example uses a temp dir so it's runnable
	// standalone and cleans up after itself.
	dataDir, err := os.MkdirTemp("", "blobstore-example-")
	if err != nil {
		return fmt.Errorf("make temp dir: %w", err)
	}
	defer os.RemoveAll(dataDir)

	indexPath := filepath.Join(dataDir, "index.bbolt")

	// ── First run: open, write a blob, read it back, close ──────────────

	if err := writeAndClose(ctx, dataDir, indexPath); err != nil {
		return err
	}

	// ── Second run: reopen the SAME index file and data dir ─────────────
	// Nothing above is re-written here. If this reads back the same bytes,
	// the index survived the process boundary — which is the entire
	// reason the bbolt backend exists (index.MemoryBackend would lose
	// everything at the `writeAndClose` return above).

	if err := reopenAndRead(ctx, dataDir, indexPath); err != nil {
		return err
	}

	fmt.Println("OK: data survived close + reopen via the bbolt index backend")
	return nil
}

func writeAndClose(ctx context.Context, dataDir, indexPath string) error {
	idx, err := backend.Open(backend.Options{Path: indexPath})
	if err != nil {
		return fmt.Errorf("open bbolt index: %w", err)
	}
	// Store.Close calls idx.Close() for us (see store.Config.Index doc
	// comment), so no separate defer idx.Close() here.

	s, err := store.Open(store.Config{
		DataDir: dataDir,
		Index:   idx,
	})
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer s.Close()

	ns := s.Namespace("default") // "default" always exists; no CreateNamespace call needed

	info, err := ns.Put(ctx, "hello.txt", bytes.NewReader([]byte("hello, blobstore")), store.PutOptions{
		ContentType: "text/plain",
	})
	if err != nil {
		return fmt.Errorf("put: %w", err)
	}
	fmt.Printf("wrote %q: %d bytes, blob id %s\n", info.Key, info.Metadata.Size, info.Metadata.BlobID)

	rc, err := ns.Get(ctx, "hello.txt")
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}
	fmt.Printf("read back: %q\n", string(data))

	items, err := ns.List(ctx, store.ListOptions{})
	if err != nil {
		return fmt.Errorf("list: %w", err)
	}
	fmt.Printf("namespace has %d key(s)\n", len(items))

	stats, err := ns.Stats(ctx)
	if err != nil {
		return fmt.Errorf("stats: %w", err)
	}
	fmt.Printf("namespace stats: %d blob(s), %d bytes stored\n", stats.BlobCount, stats.BytesStored)

	return nil
}

func reopenAndRead(ctx context.Context, dataDir, indexPath string) error {
	idx, err := backend.Open(backend.Options{Path: indexPath})
	if err != nil {
		return fmt.Errorf("reopen bbolt index: %w", err)
	}

	s, err := store.Open(store.Config{
		DataDir: dataDir,
		Index:   idx,
	})
	if err != nil {
		return fmt.Errorf("reopen store: %w", err)
	}
	defer s.Close()

	rc, err := s.Namespace("default").Get(ctx, "hello.txt")
	if err != nil {
		return fmt.Errorf("get after reopen: %w", err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("read after reopen: %w", err)
	}
	fmt.Printf("after reopen, read back: %q\n", string(data))
	return nil
}
