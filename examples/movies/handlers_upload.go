package main

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/asaidimu/blobs/staging"
	"github.com/asaidimu/blobs/store"
)

// defaultUploadBlockSize is used when the client doesn't request a specific
// block size in the begin request. 8 MiB balances two things: large enough
// that per-chunk HTTP overhead is negligible for a multi-gigabyte movie
// file, small enough that a dropped connection only costs a few seconds of
// re-upload.
const defaultUploadBlockSize = 8 << 20 // 8 MiB

// maxUploadChunkBytes bounds a single chunk upload request body. Set well
// above defaultUploadBlockSize so a client-supplied custom block size still
// has headroom.
const maxUploadChunkBytes = 256 << 20 // 256 MiB

// beginUploadRequest is the JSON body of POST /api/upload/begin.
type beginUploadRequest struct {
	Title       string `json:"title"`
	Genre       string `json:"genre"`
	Year        string `json:"year"`
	Description string `json:"description"`
	Filename    string `json:"filename"`
	Size        int64  `json:"size"`
	ContentType string `json:"contentType"`
	BlockSize   int64  `json:"blockSize,omitempty"`
}

// handleUploadBegin serves POST /api/upload/begin. It picks a unique
// storage key from the movie's title, opens a staging session for it, and
// carries the catalog metadata (title/genre/year/description) through
// staging.BeginOptions.Custom so it arrives automatically on
// staging.CompletedUpload.Custom once the upload finishes — no separate
// "save metadata" step is needed.
func (a *app) handleUploadBegin(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req beginUploadRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}

	title := strings.TrimSpace(req.Title)
	if title == "" {
		title = strings.TrimSpace(req.Filename)
	}
	if title == "" {
		writeError(w, http.StatusBadRequest, "title or filename is required")
		return
	}
	if req.Size <= 0 {
		writeError(w, http.StatusBadRequest, "size must be greater than 0")
		return
	}
	if req.BlockSize < 0 || req.BlockSize > req.Size {
		writeError(w, http.StatusBadRequest, "blockSize must be between 0 and the file size")
		return
	}

	key, err := uniqueMovieKey(ctx, a.movies, title)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed choosing storage key")
		return
	}

	blockSize := req.BlockSize
	if blockSize == 0 {
		blockSize = defaultUploadBlockSize
		if blockSize > req.Size {
			blockSize = req.Size
		}
	}

	sess, err := a.staging.Begin(ctx, moviesNamespace, key, staging.BeginOptions{
		ContentType:  req.ContentType,
		ExpectedSize: req.Size,
		BlockSize:    blockSize,
		Custom: map[string]string{
			customTitle:       title,
			customGenre:       strings.TrimSpace(req.Genre),
			customYear:        strings.TrimSpace(req.Year),
			customDescription: strings.TrimSpace(req.Description),
		},
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"sessionId":    sess.ID,
		"key":          sess.Key,
		"offset":       0,
		"expectedSize": req.Size,
		"blockSize":    blockSize,
	})
}

// handleUploadChunk serves POST /api/upload/chunk. The chunk's raw bytes are
// the request body; its logical position and (optional) integrity check
// travel as headers, matching the protocol the blobs staging example
// defines for its JS uploader.
func (a *app) handleUploadChunk(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	sessionID := r.Header.Get("X-Session-ID")
	if sessionID == "" {
		writeError(w, http.StatusBadRequest, "missing X-Session-ID header")
		return
	}

	offset, err := strconv.ParseInt(r.Header.Get("X-Offset"), 10, 64)
	if err != nil || offset < 0 {
		writeError(w, http.StatusBadRequest, "invalid or missing X-Offset header")
		return
	}

	expectedSHA := r.Header.Get("X-Chunk-SHA256")
	body := http.MaxBytesReader(w, r.Body, maxUploadChunkBytes)
	defer body.Close()

	total, err := a.staging.WriteChunk(ctx, sessionID, offset, body, expectedSHA)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]int64{"total": total})
}

// handleUploadComplete serves POST /api/upload/complete. It finalizes the
// staged upload and streams it straight into the blob store's
// chunking/hashing pipeline in a single pass — CompletedUpload satisfies
// io.Reader, so there's no intermediate copy.
func (a *app) handleUploadComplete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	sessionID := r.Header.Get("X-Session-ID")
	if sessionID == "" {
		writeError(w, http.StatusBadRequest, "missing X-Session-ID header")
		return
	}

	cu, err := a.staging.Complete(ctx, sessionID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer cu.Close()

	info, err := a.movies.Put(ctx, cu.Key, cu, store.PutOptions{
		ContentType: cu.ContentType,
		Custom:      cu.Custom,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed storing movie: "+err.Error())
		return
	}
	cu.Finalize()

	writeJSON(w, http.StatusOK, movieFromBlobInfo(info, false))
}

// handleUploadProgress serves GET /api/upload/progress?id=<sessionId>,
// reporting which byte ranges have already been received so an interrupted
// upload can resume instead of restarting from zero.
func (a *app) handleUploadProgress(w http.ResponseWriter, r *http.Request) {
	sessionID := r.URL.Query().Get("id")
	if sessionID == "" {
		writeError(w, http.StatusBadRequest, "missing ?id= query parameter")
		return
	}

	ranges, err := a.staging.Ranges(sessionID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	blockSize, _ := a.staging.BlockSize(sessionID)
	expectedSize, _ := a.staging.ExpectedSize(sessionID)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"total":        ranges.TotalBytes(),
		"ranges":       ranges,
		"blockSize":    blockSize,
		"expectedSize": expectedSize,
	})
}

// handleUploadAbort serves POST /api/upload/abort, discarding a staged
// upload's data and metadata.
func (a *app) handleUploadAbort(w http.ResponseWriter, r *http.Request) {
	sessionID := r.Header.Get("X-Session-ID")
	if sessionID == "" {
		writeError(w, http.StatusBadRequest, "missing X-Session-ID header")
		return
	}
	if err := a.staging.Abort(sessionID); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "aborted"})
}
