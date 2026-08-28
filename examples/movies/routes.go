package main

import "net/http"

// routes builds the full mux for the application. Go 1.22's net/http
// ServeMux supports method matching and {wildcard} path segments natively,
// so no third-party router is needed.
func (a *app) routes() http.Handler {
	mux := http.NewServeMux()

	// Frontend (embedded static assets: index.html, app.js, style.css).
	mux.Handle("GET /", staticHandler())

	// Catalog.
	mux.HandleFunc("GET /api/movies", a.handleListMovies)
	mux.HandleFunc("GET /api/movies/{key}", a.handleGetMovie)
	mux.HandleFunc("DELETE /api/movies/{key}", a.handleDeleteMovie)

	// Playback (Range-enabled streaming).
	mux.HandleFunc("GET /api/movies/{key}/stream", a.handleStreamMovie)
	mux.HandleFunc("GET /api/movies/{key}/poster", a.handleGetPoster)
	mux.HandleFunc("PUT /api/movies/{key}/poster", a.handleUploadPoster)

	// Resumable upload protocol (mirrors the blobs staging example, ported
	// to net/http so it can share a single server with streaming.ServeBlob,
	// which requires the standard net/http interfaces).
	mux.HandleFunc("POST /api/upload/begin", a.handleUploadBegin)
	mux.HandleFunc("POST /api/upload/chunk", a.handleUploadChunk)
	mux.HandleFunc("POST /api/upload/complete", a.handleUploadComplete)
	mux.HandleFunc("GET /api/upload/progress", a.handleUploadProgress)
	mux.HandleFunc("POST /api/upload/abort", a.handleUploadAbort)

	mux.HandleFunc("GET /api/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	})

	return withLogging(mux)
}
