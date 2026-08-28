// Command movieapp is a self-contained movie streaming application built on
// top of github.com/asaidimu/blobs. It stores video files and poster images
// as content-addressed blobs (deduplicated, chunked, crash-safe), uses the
// blobs "staging" package for resumable/chunked uploads of large video
// files, and uses the blobs "streaming" package to serve video playback with
// full HTTP Range support (206 Partial Content, ETags, seek/scrub in the
// browser's <video> element).
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/asaidimu/blobs/index"
	"github.com/asaidimu/blobs/index/backend"
	"github.com/asaidimu/blobs/object"
	"github.com/asaidimu/blobs/staging"
	"github.com/asaidimu/blobs/store"
)

// Namespace names. Videos and posters are kept in separate namespaces so
// that a poster's key can mirror its movie's key (same key, different
// namespace) without any collision risk, and so namespace-level stats stay
// meaningful (e.g. "how many bytes of video vs. poster art are stored").
const (
	moviesNamespace  = "movies"
	postersNamespace = "posters"
)

// app bundles together every piece of shared state the HTTP handlers need.
// A single instance is constructed in main and its methods are registered
// as the mux's handlers (see routes.go).
type app struct {
	blobStore *store.Store
	movies    *store.NamespaceHandle
	posters   *store.NamespaceHandle
	staging   *staging.Manager
}

func main() {
	dataDir := envOr("MOVIEAPP_DATA_DIR", "./data")
	addr := envOr("MOVIEAPP_ADDR", ":8080")

	blobDataDir := dataDir + "/blobs"
	indexPath := dataDir + "/index.bbolt"
	stagingDir := dataDir + "/staging"

	if err := os.MkdirAll(blobDataDir, 0o755); err != nil {
		log.Fatalf("movieapp: create blob data dir: %v", err)
	}
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		log.Fatalf("movieapp: create staging dir: %v", err)
	}

	// Bbolt is the production index backend: a single ACID file that
	// survives process restarts, unlike index.NewMemoryBackend() (which
	// the blobs examples use only for tests/demos).
	idx, err := backend.Open(backend.Options{Path: indexPath})
	if err != nil {
		log.Fatalf("movieapp: open index: %v", err)
	}

	blobStore, err := store.Open(store.Config{
		DataDir: blobDataDir,
		Index:   idx,
	})
	if err != nil {
		log.Fatalf("movieapp: open blob store: %v", err)
	}
	defer blobStore.Close() // also closes idx

	ctx := context.Background()
	if err := ensureNamespace(ctx, blobStore, moviesNamespace, "Movies"); err != nil {
		log.Fatalf("movieapp: ensure %q namespace: %v", moviesNamespace, err)
	}
	if err := ensureNamespace(ctx, blobStore, postersNamespace, "Posters"); err != nil {
		log.Fatalf("movieapp: ensure %q namespace: %v", postersNamespace, err)
	}

	mgr, err := staging.NewManager(stagingDir)
	if err != nil {
		log.Fatalf("movieapp: init staging manager: %v", err)
	}
	// Reap abandoned upload sessions: check every 5 minutes, discard
	// anything idle for more than 6 hours (a movie upload can legitimately
	// take a while on a slow connection with pauses in between).
	stopReaper := mgr.StartReaper(5*time.Minute, 6*time.Hour)
	defer stopReaper()

	a := &app{
		blobStore: blobStore,
		movies:    blobStore.Namespace(moviesNamespace),
		posters:   blobStore.Namespace(postersNamespace),
		staging:   mgr,
	}

	server := &http.Server{
		Addr:         addr,
		Handler:      a.routes(),
		ReadTimeout:  0, // large chunk uploads and long video reads must not be cut off
		WriteTimeout: 0,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Printf("movieapp: listening on http://localhost%s", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("movieapp: server error: %v", err)
		}
	}()

	// Graceful shutdown: stop accepting new connections, then let Store's
	// Close() drain in-flight Puts/Gets before the process exits.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	log.Println("movieapp: shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("movieapp: server shutdown error: %v", err)
	}
}

// ensureNamespace creates the namespace if it doesn't already exist. Safe to
// call on every startup (idempotent), matching the pattern used by the
// blobs staging example.
func ensureNamespace(ctx context.Context, s *store.Store, id, displayName string) error {
	if _, err := s.GetNamespace(ctx, id); err != nil {
		if !index.IsNotFound(err) {
			return err
		}
		return s.CreateNamespace(ctx, object.Namespace{ID: id, DisplayName: displayName})
	}
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
