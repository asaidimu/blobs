// Package streaming serves blobs over HTTP with full support for byte
// Range requests, conditional GET, and multi-range requests — for any
// content type, not just video. It exists to answer "how do I stream
// bytes efficiently from the store" as a general capability rather than
// a format-specific one (HLS/DASH manifests, if you need them, are just
// more blobs stored and served the same way as everything else; this
// package doesn't know or care about them).
//
// The actual protocol logic — parsing Range headers, deciding between a
// 200 and a 206, building multipart/byteranges for multi-range requests,
// honoring If-Range/If-None-Match/If-Modified-Since — is handled entirely
// by the standard library's http.ServeContent. This package's only job is
// to give it a seekable reader (store.NamespaceHandle.GetSeekable) and
// the right response headers. That division of labor is deliberate: this
// package is a thin adapter, not a reimplementation of RFC 7233.
package streaming

import (
	"errors"
	"fmt"
	"net/http"

	bserrors "github.com/asaidimu/blobs/errors"
	"github.com/asaidimu/blobs/store"
)

// ServeBlob writes the blob stored under key in ns to w, honoring any
// Range/If-Range/If-None-Match/If-Modified-Since headers on r exactly as
// http.ServeContent would for a local file — because that's exactly what
// this delegates to.
//
// The ETag is derived from the blob's content-addressed BlobID: the same
// bytes always produce the same BlobID, and a different BlobID always
// means different bytes, which is exactly what a strong ETag promises —
// with no extra bookkeeping, no separate hash computation, and no risk of
// it drifting out of sync with the actual content, since it IS the
// content's identity in this store.
//
// This function is format-agnostic on purpose: it works identically for
// a movie, a song, an image, or an arbitrary large document, because HTTP
// Range support is a property of the transport, not of what's inside.
func ServeBlob(w http.ResponseWriter, r *http.Request, ns *store.NamespaceHandle, key string) {
	ctx := r.Context()

	info, err := ns.Head(ctx, key)
	if err != nil {
		writeBlobError(w, err)
		return
	}

	rsc, err := ns.GetSeekable(ctx, key)
	if err != nil {
		writeBlobError(w, err)
		return
	}
	defer rsc.Close()

	if info.Metadata.ContentType != "" {
		w.Header().Set("Content-Type", info.Metadata.ContentType)
	}
	// Setting ETag before calling http.ServeContent is intentional: the
	// standard library checks whatever ETag is already on the response
	// when evaluating If-None-Match/If-Range, but it never sets one
	// itself — only Last-Modified, which it derives from the modtime
	// argument below. Both are provided here so either kind of client
	// (ETag-based or Last-Modified-based caching) gets a correct answer.
	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, info.Metadata.BlobID))

	http.ServeContent(w, r, key, info.Metadata.UpdatedAt, rsc)
}

// writeBlobError maps a store error to an HTTP status. This is
// deliberately minimal — adapt it to whatever error-mapping convention
// the rest of your HTTP layer already uses (mapBlobError,
// common.NewSystemError, structured JSON error bodies, etc.) rather than
// treating this as the final word on error responses.
func writeBlobError(w http.ResponseWriter, err error) {
	var notFound *bserrors.NotFoundError
	if errors.As(err, &notFound) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	http.Error(w, "internal error", http.StatusInternalServerError)
}
