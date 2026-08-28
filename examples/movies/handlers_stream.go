package main

import (
	"net/http"

	"github.com/asaidimu/blobs/index"
	"github.com/asaidimu/blobs/store"
	"github.com/asaidimu/blobs/streaming"
)

// maxPosterUploadBytes bounds poster image uploads. Posters are small
// (cover art, not video), so a generous still-safe ceiling is used to reject
// obviously-wrong uploads without needing the resumable staging protocol.
const maxPosterUploadBytes = 20 << 20 // 20 MiB

// handleStreamMovie serves GET /api/movies/{key}/stream.
//
// This is the entire "streaming" part of the app: streaming.ServeBlob wraps
// the store's seekable reader in the standard library's http.ServeContent,
// which handles byte-range requests, conditional GET (If-Range /
// If-None-Match / If-Modified-Since), and multi-range requests — exactly
// what a browser's <video> element needs to seek/scrub through a movie
// without downloading the whole file first.
func (a *app) handleStreamMovie(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	streaming.ServeBlob(w, r, a.movies, key)
}

// handleGetPoster serves GET /api/movies/{key}/poster. Reusing
// streaming.ServeBlob here (rather than a plain Get+io.Copy) means posters
// also get correct ETags and conditional-GET caching for free, even though
// they're small enough that Range support rarely matters in practice.
func (a *app) handleGetPoster(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	streaming.ServeBlob(w, r, a.posters, key)
}

// handleUploadPoster serves PUT /api/movies/{key}/poster with the raw image
// bytes as the request body. Posters are small enough to write directly
// (no resumable/staged upload needed, unlike movie video files).
func (a *app) handleUploadPoster(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	key := r.PathValue("key")

	if _, err := a.movies.Head(ctx, key); err != nil {
		if index.IsNotFound(err) {
			writeError(w, http.StatusNotFound, "movie not found")
			return
		}
		mapStoreError(w, err)
		return
	}

	body := http.MaxBytesReader(w, r.Body, maxPosterUploadBytes)
	defer body.Close()

	// ContentType is left empty so Put sniffs it from the image bytes
	// themselves via the mimetype library.
	info, err := a.posters.Put(ctx, key, body, store.PutOptions{})
	if err != nil {
		mapStoreError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":      "ok",
		"key":         info.Key,
		"size":        info.Metadata.Size,
		"contentType": info.Metadata.ContentType,
	})
}
