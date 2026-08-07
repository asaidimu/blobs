package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/asaidimu/blobs/index"
	"github.com/asaidimu/blobs/object"
	"github.com/asaidimu/blobs/staging"
	"github.com/asaidimu/blobs/store"
	"github.com/valyala/fasthttp"
)

var (
	mgr       *staging.Manager
	blobStore *store.Store
	ns        *store.NamespaceHandle
)

// storeNamespace is the single namespace uploaded files live under. Chosen
// to match the "default" bucket name the staging manager was already using.
const storeNamespace = "default"

// fasthttp treats MaxRequestBodySize<=0 as "use the 4MB DefaultMaxRequestBodySize",
// NOT as "unlimited". Since chunk sizes here can reach 8MB (see GOOD_BLOCK_SIZE in
// the client), we need an explicit, generous ceiling or every large chunk gets
// rejected with a body-size error.
const maxRequestBodySize = 10 << 30 // 10 GiB per request/chunk

func main() {
	var err error
	mgr, err = staging.NewManager("./staging_data")
	if err != nil {
		log.Fatalf("failed to initialize staging manager: %v", err)
	}
	stopReaper := mgr.StartReaper(5*time.Minute, 1*time.Hour)
	defer stopReaper()

	// NOTE: index.NewMemoryBackend() is what the store package's own doc
	// comment uses as a placeholder ("swap for bbolt, badger, etc."). It is
	// NOT persisted across restarts -- only the segment files under
	// blobDataDir survive a restart, and with an in-memory index every blob
	// they contain becomes invisible again until RebuildIndex is run. Swap
	// this for a real persistent index.Backend before relying on this in
	// production.
	blobStore, err = store.Open(store.Config{
		DataDir: "./blob_data",
		Index:   index.NewMemoryBackend(),
	})
	if err != nil {
		log.Fatalf("failed to open blob store: %v", err)
	}
	defer blobStore.Close()

	bgCtx := context.Background()
	if _, err := blobStore.GetNamespace(bgCtx, storeNamespace); err != nil {
		if !index.IsNotFound(err) {
			log.Fatalf("failed to check blob store namespace: %v", err)
		}
		if err := blobStore.CreateNamespace(bgCtx, object.Namespace{ID: storeNamespace}); err != nil {
			log.Fatalf("failed to create blob store namespace: %v", err)
		}
	}
	ns = blobStore.Namespace(storeNamespace)

	server := &fasthttp.Server{
		Handler:            requestHandler,
		StreamRequestBody:  true,
		MaxRequestBodySize: maxRequestBodySize,
		ReadTimeout:        30 * time.Second,
		WriteTimeout:       30 * time.Second,
		IdleTimeout:        120 * time.Second,
	}

	log.Println("Server running on http://localhost:8080")
	if err := server.ListenAndServe(":8080"); err != nil {
		log.Fatal(err)
	}
}

// ── Router & Helpers ────────────────────────────────────────────────────────

func requestHandler(ctx *fasthttp.RequestCtx) {
	path := string(ctx.Path())
	method := string(ctx.Method())

	switch {
	case path == "/" && method == fasthttp.MethodGet:
		serveHTML(ctx)
	case path == "/api/begin" && method == fasthttp.MethodPost:
		handleBegin(ctx)
	case path == "/api/upload" && method == fasthttp.MethodPost:
		handleUpload(ctx)
	case path == "/api/complete" && method == fasthttp.MethodPost:
		handleComplete(ctx)
	case path == "/api/progress" && method == fasthttp.MethodGet:
		handleProgress(ctx)
	case path == "/api/abort" && method == fasthttp.MethodPost:
		handleAbort(ctx)
	case path == "/api/ping" && method == fasthttp.MethodGet:
		handlePing(ctx)
	case path == "/api/probe" && method == fasthttp.MethodPost:
		handleProbe(ctx)
	case path == "/api/files" && method == fasthttp.MethodGet:
		handleListFiles(ctx)
	case path == "/api/download" && method == fasthttp.MethodGet:
		handleDownload(ctx)
	case path == "/api/delete" && (method == fasthttp.MethodDelete || method == fasthttp.MethodPost):
		handleDelete(ctx)
	default:
		writeError(ctx, fasthttp.StatusNotFound, "route not found")
	}
}

func writeJSON(ctx *fasthttp.RequestCtx, statusCode int, v interface{}) {
	ctx.SetStatusCode(statusCode)
	ctx.SetContentType("application/json; charset=utf-8")
	if err := json.NewEncoder(ctx).Encode(v); err != nil {
		log.Printf("json encode error: %v", err)
	}
}

func writeError(ctx *fasthttp.RequestCtx, statusCode int, message string) {
	writeJSON(ctx, statusCode, map[string]string{"error": message})
}

func sanitizeFilename(name string) (string, error) {
	if name == "" {
		return "", errors.New("filename required")
	}
	if strings.ContainsRune(name, 0) {
		return "", errors.New("filename contains invalid characters")
	}
	base := filepath.Base(filepath.Clean(name))
	if base == "" || base == "." || base == ".." || base == string(filepath.Separator) {
		return "", errors.New("invalid filename")
	}
	return base, nil
}

// uniqueStoreKey returns a key for name that doesn't already exist in the
// store's namespace, appending " (1)", " (2)", etc. before the extension if
// needed -- the same collision-avoidance uniqueFinalPath used to do against
// the filesystem, done here against the store instead so two uploads with
// the same filename don't silently overwrite each other (Put's own
// semantics: "if key already exists, its ref is updated to point at the
// new blob").
func uniqueStoreKey(ctx context.Context, name string) (string, error) {
	if _, err := ns.Head(ctx, name); err != nil {
		if index.IsNotFound(err) {
			return name, nil
		}
		return "", err
	}

	ext := filepath.Ext(name)
	base := strings.TrimSuffix(name, ext)
	for i := 1; i < 10000; i++ {
		candidate := fmt.Sprintf("%s (%d)%s", base, i, ext)
		if _, err := ns.Head(ctx, candidate); err != nil {
			if index.IsNotFound(err) {
				return candidate, nil
			}
			return "", err
		}
	}
	return "", errors.New("could not find a unique storage key")
}

func serveHTML(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("text/html; charset=utf-8")
	ctx.WriteString(indexHTML)
}

// ── API Handlers ─────────────────────────────────────────────────────────────

type beginRequest struct {
	Filename    string `json:"filename"`
	Size        int64  `json:"size"`
	ContentType string `json:"contentType"`
	BlockSize   int64  `json:"blockSize,omitempty"`
}

type fileInfoResponse struct {
	Name    string `json:"name"`
	Size    int64  `json:"size"`
	ModTime string `json:"modTime"`
}

func handleBegin(ctx *fasthttp.RequestCtx) {
	var req beginRequest
	if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
		writeError(ctx, fasthttp.StatusBadRequest, "invalid JSON payload")
		return
	}

	filename, err := sanitizeFilename(req.Filename)
	if err != nil {
		writeError(ctx, fasthttp.StatusBadRequest, err.Error())
		return
	}

	blockSize := req.BlockSize
	if blockSize < 0 || blockSize > req.Size {
		writeError(ctx, fasthttp.StatusBadRequest, "blockSize must be between 0 and the file size")
		return
	}

	sess, err := mgr.Begin(context.Background(), "default", filename, staging.BeginOptions{
		ContentType:  req.ContentType,
		ExpectedSize: req.Size,
		BlockSize:    blockSize,
	})
	if err != nil {
		writeError(ctx, fasthttp.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(ctx, fasthttp.StatusOK, map[string]interface{}{
		"id":            sess.ID,
		"offset":        0,
		"expected_size": req.Size,
		"block_size":    blockSize,
	})
}

func handleUpload(ctx *fasthttp.RequestCtx) {
	sessionID := string(ctx.Request.Header.Peek("X-Session-ID"))
	if sessionID == "" {
		writeError(ctx, fasthttp.StatusBadRequest, "missing X-Session-ID header")
		return
	}

	offsetStr := string(ctx.Request.Header.Peek("X-Offset"))
	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil || offset < 0 {
		writeError(ctx, fasthttp.StatusBadRequest, "invalid or missing X-Offset header")
		return
	}

	expectedSHA := string(ctx.Request.Header.Peek("X-Chunk-SHA256"))
	bodyStream := ctx.RequestBodyStream()
	if bodyStream == nil {
		writeError(ctx, fasthttp.StatusBadRequest, "request body empty")
		return
	}

	total, err := mgr.WriteChunk(context.Background(), sessionID, offset, bodyStream, expectedSHA)
	if err != nil {
		writeError(ctx, fasthttp.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(ctx, fasthttp.StatusOK, map[string]int64{"total": total})
}

func handleComplete(ctx *fasthttp.RequestCtx) {
	sessionID := string(ctx.Request.Header.Peek("X-Session-ID"))
	if sessionID == "" {
		writeError(ctx, fasthttp.StatusBadRequest, "missing X-Session-ID header")
		return
	}

	bgCtx := context.Background()

	cu, err := mgr.Complete(bgCtx, sessionID)
	if err != nil {
		writeError(ctx, fasthttp.StatusInternalServerError, err.Error())
		return
	}
	defer cu.Close()

	key, err := uniqueStoreKey(bgCtx, cu.Key)
	if err != nil {
		writeError(ctx, fasthttp.StatusInternalServerError, "failed choosing storage key")
		return
	}

	// cu already satisfies io.Reader (see the staging package), so Put
	// streams it straight into the blob store's chunking/hashing pipeline
	// in a single pass -- no intermediate file, no os.Rename, no fallback
	// copy loop, and no plain "./uploads" directory at all. ContentType is
	// left empty so the store sniffs it itself.
	info, err := ns.Put(bgCtx, key, cu, store.PutOptions{})
	if err != nil {
		writeError(ctx, fasthttp.StatusInternalServerError, "failed storing blob: "+err.Error())
		return
	}
	cu.Finalize()

	writeJSON(ctx, fasthttp.StatusOK, map[string]interface{}{
		"status":      "ok",
		"key":         info.Key,
		"size":        info.Metadata.Size,
		"contentType": info.Metadata.ContentType,
	})
}

func handleProgress(ctx *fasthttp.RequestCtx) {
	sessionID := string(ctx.QueryArgs().Peek("id"))
	if sessionID == "" {
		writeError(ctx, fasthttp.StatusBadRequest, "missing ?id= query parameter")
		return
	}

	ranges, err := mgr.Ranges(sessionID)
	if err != nil {
		writeError(ctx, fasthttp.StatusNotFound, err.Error())
		return
	}

	blockSize, _ := mgr.BlockSize(sessionID)
	expectedSize, _ := mgr.ExpectedSize(sessionID)

	var total int64
	for _, r := range ranges {
		total += r.End - r.Start
	}

	writeJSON(ctx, fasthttp.StatusOK, map[string]interface{}{
		"total":         total,
		"ranges":        ranges,
		"block_size":    blockSize,
		"expected_size": expectedSize,
	})
}

func handleAbort(ctx *fasthttp.RequestCtx) {
	sessionID := string(ctx.Request.Header.Peek("X-Session-ID"))
	if sessionID == "" {
		writeError(ctx, fasthttp.StatusBadRequest, "missing X-Session-ID header")
		return
	}

	if err := mgr.Abort(sessionID); err != nil {
		writeError(ctx, fasthttp.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(ctx, fasthttp.StatusOK, map[string]string{"status": "aborted"})
}

func handleListFiles(ctx *fasthttp.RequestCtx) {
	blobs, err := ns.List(context.Background(), store.ListOptions{})
	if err != nil {
		writeError(ctx, fasthttp.StatusInternalServerError, "failed listing stored files")
		return
	}

	files := make([]fileInfoResponse, 0, len(blobs))
	for _, b := range blobs {
		files = append(files, fileInfoResponse{
			Name:    b.Key,
			Size:    b.Metadata.Size,
			ModTime: b.Metadata.UpdatedAt.Format(time.RFC3339),
		})
	}

	writeJSON(ctx, fasthttp.StatusOK, files)
}

func handleDownload(ctx *fasthttp.RequestCtx) {
	name := string(ctx.QueryArgs().Peek("name"))
	cleanName, err := sanitizeFilename(name)
	if err != nil {
		writeError(ctx, fasthttp.StatusBadRequest, "invalid filename")
		return
	}

	bgCtx := context.Background()

	info, err := ns.Head(bgCtx, cleanName)
	if err != nil {
		if index.IsNotFound(err) {
			writeError(ctx, fasthttp.StatusNotFound, "file not found")
			return
		}
		writeError(ctx, fasthttp.StatusInternalServerError, err.Error())
		return
	}

	rc, err := ns.Get(bgCtx, cleanName)
	if err != nil {
		if index.IsNotFound(err) {
			writeError(ctx, fasthttp.StatusNotFound, "file not found")
			return
		}
		writeError(ctx, fasthttp.StatusInternalServerError, err.Error())
		return
	}

	contentType := info.Metadata.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	ctx.SetContentType(contentType)
	ctx.Response.Header.Set("Content-Disposition", "attachment; filename=\""+cleanName+"\"")
	// fasthttp closes rc for us once the streamed body has been written in
	// full, since it implements io.Closer -- same contract os.File already
	// satisfied when ServeFile was used here before.
	ctx.SetBodyStream(rc, int(info.Metadata.Size))
}

func handleDelete(ctx *fasthttp.RequestCtx) {
	name := string(ctx.QueryArgs().Peek("name"))
	cleanName, err := sanitizeFilename(name)
	if err != nil {
		writeError(ctx, fasthttp.StatusBadRequest, "invalid filename")
		return
	}

	if err := ns.Delete(context.Background(), cleanName); err != nil {
		writeError(ctx, fasthttp.StatusInternalServerError, "failed deleting file")
		return
	}

	writeJSON(ctx, fasthttp.StatusOK, map[string]string{"status": "deleted"})
}

func handlePing(ctx *fasthttp.RequestCtx) {
	writeJSON(ctx, fasthttp.StatusOK, map[string]bool{"pong": true})
}

const maxProbeBytes = 2 << 20

func handleProbe(ctx *fasthttp.RequestCtx) {
	var n int64
	if body := ctx.RequestBodyStream(); body != nil {
		n, _ = io.Copy(io.Discard, io.LimitReader(body, maxProbeBytes))
	} else {
		n = int64(len(ctx.PostBody()))
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]int64{"received": n})
}

// ── Redesigned Modern UI Dashboard ───────────────────────────────────────────

const indexHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TransferEngine - High Performance Resumable Uploads</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-base: #0a0d14;
            --bg-surface: rgba(18, 24, 38, 0.75);
            --bg-card: rgba(255, 255, 255, 0.03);
            --bg-card-hover: rgba(255, 255, 255, 0.06);
            --border-subtle: rgba(255, 255, 255, 0.08);
            --border-accent: rgba(99, 102, 241, 0.3);
            --primary: #6366f1;
            --primary-hover: #4f46e5;
            --primary-glow: rgba(99, 102, 241, 0.35);
            --success: #10b981;
            --warning: #f59e0b;
            --danger: #f43f5e;
            --text-main: #f8fafc;
            --text-muted: #94a3b8;
            --text-dim: #64748b;
            --font-sans: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, sans-serif;
            --font-mono: 'JetBrains Mono', monospace;
        }

        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            background-color: var(--bg-base);
            background-image:
                radial-gradient(at 0% 0%, rgba(99, 102, 241, 0.12) 0px, transparent 50%),
                radial-gradient(at 100% 100%, rgba(16, 185, 129, 0.08) 0px, transparent 50%);
            background-attachment: fixed;
            font-family: var(--font-sans);
            color: var(--text-main);
            min-height: 100vh;
            padding: 32px 24px;
            line-height: 1.5;
        }

        .header-bar {
            max-width: 1320px;
            margin: 0 auto 28px auto;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }

        .brand {
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .brand-icon {
            width: 40px;
            height: 40px;
            background: linear-gradient(135deg, var(--primary), #8b5cf6);
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            box-shadow: 0 0 20px var(--primary-glow);
        }

        .brand-icon svg { width: 22px; height: 22px; fill: none; stroke: #fff; stroke-width: 2.2; }

        .brand-text h1 {
            font-size: 20px;
            font-weight: 800;
            letter-spacing: -0.02em;
            background: linear-gradient(135deg, #ffffff, #cbd5e1);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .brand-text p {
            font-size: 12px;
            color: var(--text-muted);
            font-weight: 500;
        }

        .system-badge {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            background: rgba(16, 185, 129, 0.1);
            border: 1px solid rgba(16, 185, 129, 0.25);
            color: #34d399;
            padding: 6px 14px;
            border-radius: 9999px;
            font-size: 12px;
            font-weight: 600;
        }

        .pulse-dot {
            width: 8px;
            height: 8px;
            background-color: var(--success);
            border-radius: 50%;
            box-shadow: 0 0 10px var(--success);
            animation: pulse 2s infinite;
        }

        @keyframes pulse {
            0% { transform: scale(0.95); opacity: 0.8; }
            50% { transform: scale(1.15); opacity: 1; }
            100% { transform: scale(0.95); opacity: 0.8; }
        }

        .app-layout {
            max-width: 1320px;
            margin: 0 auto;
            display: grid;
            grid-template-columns: 1fr 420px;
            gap: 28px;
        }

        @media (max-width: 1080px) {
            .app-layout { grid-template-columns: 1fr; }
        }

        .card {
            background: var(--bg-surface);
            backdrop-filter: blur(20px);
            -webkit-backdrop-filter: blur(20px);
            border: 1px solid var(--border-subtle);
            border-radius: 24px;
            padding: 28px;
            box-shadow: 0 20px 40px rgba(0, 0, 0, 0.4);
            transition: border-color 0.2s ease;
        }

        .card-title {
            font-size: 16px;
            font-weight: 700;
            color: var(--text-main);
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            letter-spacing: -0.01em;
        }

        .drop-zone {
            position: relative;
            border: 2px dashed rgba(255, 255, 255, 0.12);
            border-radius: 20px;
            padding: 36px 24px;
            text-align: center;
            cursor: pointer;
            transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
            background: var(--bg-card);
            margin-bottom: 20px;
            overflow: hidden;
        }

        .drop-zone:hover, .drop-zone.dragover {
            border-color: var(--primary);
            background: rgba(99, 102, 241, 0.08);
            transform: translateY(-2px);
            box-shadow: 0 12px 30px rgba(99, 102, 241, 0.15);
        }

        .drop-icon {
            width: 52px;
            height: 52px;
            margin: 0 auto 14px auto;
            background: rgba(255, 255, 255, 0.05);
            border-radius: 16px;
            display: flex;
            align-items: center;
            justify-content: center;
            color: var(--primary);
            transition: transform 0.2s;
        }

        .drop-zone:hover .drop-icon {
            transform: scale(1.1);
            background: var(--primary);
            color: #fff;
        }

        .drop-zone h3 { font-size: 15px; font-weight: 600; margin-bottom: 4px; }
        .drop-zone p { font-size: 13px; color: var(--text-muted); }
        .drop-zone input[type="file"] { display: none; }

        .queue-summary {
            display: none;
            align-items: center;
            justify-content: space-between;
            gap: 16px;
            background: var(--bg-card);
            border: 1px solid var(--border-subtle);
            border-radius: 16px;
            padding: 14px 18px;
            margin-bottom: 18px;
        }

        .qs-left { flex: 1; min-width: 0; }
        .qs-left .qs-headline { font-size: 13px; font-weight: 600; color: var(--text-main); display: block; margin-bottom: 8px; }
        .qs-track { background: rgba(255, 255, 255, 0.06); border-radius: 9999px; height: 6px; overflow: hidden; }
        .qs-fill { height: 100%; width: 0%; background: linear-gradient(90deg, var(--primary), #a855f7); border-radius: 9999px; transition: width 0.25s ease; }
        .qs-actions { display: flex; gap: 8px; flex-shrink: 0; flex-wrap: wrap; }

        .btn-sm {
            background: var(--bg-card-hover);
            border: 1px solid var(--border-subtle);
            color: var(--text-main);
            padding: 7px 12px;
            border-radius: 9px;
            font-family: var(--font-sans);
            font-weight: 600;
            font-size: 11px;
            cursor: pointer;
            white-space: nowrap;
            transition: all 0.15s ease;
        }
        .btn-sm:hover:not(:disabled) { background: rgba(255, 255, 255, 0.1); }
        .btn-sm:disabled { opacity: 0.35; cursor: not-allowed; }

        .job-list { display: flex; flex-direction: column; gap: 10px; margin-bottom: 4px; max-height: 440px; overflow-y: auto; padding-right: 4px; }
        .job-list::-webkit-scrollbar { width: 6px; }
        .job-list::-webkit-scrollbar-thumb { background: rgba(255, 255, 255, 0.15); border-radius: 3px; }

        .job-row {
            display: flex;
            align-items: center;
            gap: 14px;
            background: var(--bg-card);
            border: 1px solid var(--border-subtle);
            border-radius: 14px;
            padding: 12px 14px;
            transition: border-color 0.2s ease, opacity 0.2s ease;
            animation: fadeIn 0.25s ease;
        }

        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(4px); }
            to { opacity: 1; transform: translateY(0); }
        }

        .job-row.is-done { border-color: rgba(16, 185, 129, 0.3); }
        .job-row.is-error { border-color: rgba(244, 63, 94, 0.35); }
        .job-row.is-cancelled { opacity: 0.5; }

        .job-badge {
            flex-shrink: 0;
            width: 38px;
            height: 38px;
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 10px;
            font-weight: 800;
            font-family: var(--font-mono);
            letter-spacing: 0.02em;
            background: rgba(148, 163, 184, 0.12);
            color: var(--text-muted);
        }
        .job-badge.image { background: rgba(168, 85, 247, 0.15); color: #c084fc; }
        .job-badge.video { background: rgba(244, 63, 94, 0.15); color: #fda4af; }
        .job-badge.audio { background: rgba(245, 158, 11, 0.15); color: #fbbf24; }
        .job-badge.archive { background: rgba(100, 116, 139, 0.2); color: #cbd5e1; }
        .job-badge.doc { background: rgba(99, 102, 241, 0.15); color: #a5b4fc; }
        .job-badge.code { background: rgba(16, 185, 129, 0.15); color: #6ee7b7; }

        .job-main { flex: 1; min-width: 0; }
        .job-top { display: flex; align-items: baseline; justify-content: space-between; gap: 10px; margin-bottom: 7px; }
        .job-name { font-size: 13px; font-weight: 600; color: var(--text-main); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 260px; }
        .job-size { font-size: 11px; color: var(--text-dim); font-family: var(--font-mono); flex-shrink: 0; }

        .job-progress-track { background: rgba(255, 255, 255, 0.06); border-radius: 9999px; height: 5px; overflow: hidden; margin-bottom: 6px; }
        .job-progress-fill { height: 100%; width: 0%; background: linear-gradient(90deg, var(--primary), #a855f7); border-radius: 9999px; transition: width 0.25s ease; }
        .job-progress-fill.hashing { background: linear-gradient(90deg, var(--warning), #fbbf24); }
        .job-progress-fill.done { background: var(--success); }
        .job-progress-fill.errored { background: var(--danger); }

        .job-status { font-size: 11px; color: var(--text-muted); font-family: var(--font-mono); }
        .job-status.status-completed { color: #34d399; }
        .job-status.status-error { color: #fb7185; }
        .job-status.status-cancelled { color: var(--text-dim); }

        .job-actions { display: flex; gap: 6px; flex-shrink: 0; }
        .job-btn {
            width: 30px;
            height: 30px;
            border-radius: 9px;
            border: 1px solid var(--border-subtle);
            background: var(--bg-card-hover);
            color: var(--text-muted);
            display: flex;
            align-items: center;
            justify-content: center;
            cursor: pointer;
            transition: all 0.15s ease;
        }
        .job-btn:hover { background: rgba(255, 255, 255, 0.12); color: var(--text-main); }
        .job-btn.danger:hover { background: rgba(244, 63, 94, 0.2); color: #f43f5e; border-color: rgba(244, 63, 94, 0.3); }

        .log-toggle {
            background: none;
            border: none;
            color: var(--text-dim);
            font-size: 11px;
            font-weight: 600;
            cursor: pointer;
            padding: 6px 0 2px 0;
        }
        .log-toggle:hover { color: var(--text-muted); }

        .terminal-log {
            background: rgba(6, 9, 15, 0.85);
            border: 1px solid var(--border-subtle);
            border-radius: 14px;
            padding: 14px;
            height: 140px;
            overflow-y: auto;
            font-family: var(--font-mono);
            font-size: 12px;
            color: var(--text-muted);
            margin-top: 8px;
        }

        .terminal-log .line { padding: 2px 0; }
        .terminal-log .ts { color: var(--primary); margin-right: 8px; opacity: 0.8; }

        .empty-box {
            text-align: center;
            color: var(--text-dim);
            font-size: 13px;
            padding: 40px 16px;
            border: 1px dashed var(--border-subtle);
            border-radius: 16px;
        }

        .file-list { display: flex; flex-direction: column; gap: 10px; max-height: 600px; overflow-y: auto; padding-right: 2px; }

        .file-card {
            background: var(--bg-card);
            border: 1px solid var(--border-subtle);
            border-radius: 14px;
            padding: 14px 16px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 12px;
            transition: all 0.2s ease;
        }

        .file-card:hover {
            background: var(--bg-card-hover);
            border-color: rgba(255, 255, 255, 0.15);
            transform: translateX(2px);
        }

        .file-card-info { overflow: hidden; flex: 1; }
        .file-card-name { font-size: 13px; font-weight: 600; color: var(--text-main); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .file-card-meta { font-size: 11px; color: var(--text-dim); margin-top: 2px; }

        .file-card-actions { display: flex; gap: 8px; }

        .action-btn {
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid var(--border-subtle);
            color: var(--text-main);
            width: 34px;
            height: 34px;
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: center;
            cursor: pointer;
            text-decoration: none;
            transition: all 0.15s ease;
        }

        .action-btn svg { width: 16px; height: 16px; stroke: currentColor; fill: none; stroke-width: 2; }
        .action-btn:hover { background: rgba(255, 255, 255, 0.12); }
        .action-btn.del:hover { background: rgba(244, 63, 94, 0.2); color: #f43f5e; border-color: rgba(244, 63, 94, 0.3); }
    </style>
</head>
<body>

<div class="header-bar">
    <div class="brand">
        <div class="brand-icon">
            <svg viewBox="0 0 24 24"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4M17 8l-5-5-5 5M12 3v12"/></svg>
        </div>
        <div class="brand-text">
            <h1>TransferEngine</h1>
            <p>Resumable · Parallel · Deterministic Chunking</p>
        </div>
    </div>
    <div class="system-badge">
        <div class="pulse-dot"></div>
        Engine Active
    </div>
</div>

<div class="app-layout">
    <div class="card">
        <div class="card-title">
            <span>Upload Console</span>
        </div>

        <div class="drop-zone" id="dropZone">
            <div class="drop-icon">
                <svg viewBox="0 0 24 24" width="26" height="26" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 14.899A7 7 0 1 1 15.71 8h1.79a4.5 4.5 0 0 1 2.5 8.242M12 12v9M8 17l4-4 4 4"/></svg>
            </div>
            <h3>Drag and drop files here</h3>
            <p>or click anywhere to browse local filesystem · multiple files supported</p>
            <input type="file" id="fileInput" multiple>
        </div>

        <div class="queue-summary" id="queueSummary">
            <div class="qs-left">
                <span class="qs-headline" id="qsHeadline">—</span>
                <div class="qs-track"><div class="qs-fill" id="qsFill"></div></div>
            </div>
            <div class="qs-actions">
                <button class="btn-sm" id="pauseAllBtn">Pause All</button>
                <button class="btn-sm" id="resumeAllBtn">Resume All</button>
                <button class="btn-sm" id="clearCompletedBtn">Clear Completed</button>
            </div>
        </div>

        <div class="job-list" id="jobList">
            <div class="job-rows" id="jobRows"></div>
            <div class="empty-box" id="jobListEmpty">No active transfers. Drop files above to begin.</div>
        </div>

        <button class="log-toggle" id="logToggle">Show activity log ▾</button>
        <div class="terminal-log" id="log" style="display:none;"></div>
    </div>

    <div class="card">
        <div class="card-title">
            <span>Server Storage</span>
            <button id="refreshFilesBtn" class="action-btn" title="Refresh files list">
                <svg viewBox="0 0 24 24"><path d="M23 4v6h-6M1 20v-6h6M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg>
            </button>
        </div>
        <div class="file-list" id="fileList">
            <div class="empty-box">Querying server directory...</div>
        </div>
    </div>
</div>

<script>
    // ── Tunables ─────────────────────────────────────────────────────────────
    const PING_COUNT = 3;
    const PROBE_BYTES = 256 * 1024;
    const MIN_BLOCK_SIZE = 256 * 1024;
    const MEDIUM_BLOCK_SIZE = 2 * 1024 * 1024;
    const GOOD_BLOCK_SIZE = 8 * 1024 * 1024;
    const MIN_CONCURRENCY = 1;
    const MAX_CONCURRENCY = 6;
    const AIMD_SUCCESS_STREAK = 3;
    const AIMD_BACKOFF_COOLDOWN_MS = 2500;
    const EWMA_ALPHA = 0.3;
    const MAX_RETRIES = 3;
    const RETRY_DELAY_MS = 1500;
    const MAX_CONCURRENT_JOBS = 3;
    const RENDER_THROTTLE_MS = 80;

    // ── Icons ────────────────────────────────────────────────────────────────
    const ICON_PAUSE = '<svg viewBox="0 0 24 24" width="14" height="14" fill="currentColor"><path d="M6 19h4V5H6v14zm8-14v14h4V5h-4z"/></svg>';
    const ICON_PLAY = '<svg viewBox="0 0 24 24" width="14" height="14" fill="currentColor"><path d="M8 5v14l11-7z"/></svg>';
    const ICON_RETRY = '<svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2"><path d="M23 4v6h-6M1 20v-6h6M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg>';
    const ICON_CLOSE = '<svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 6 6 18M6 6l12 12"/></svg>';

    // ── DOM References ────────────────────────────────────────────────────────
    const fileInput = document.getElementById('fileInput');
    const dropZone = document.getElementById('dropZone');
    const queueSummary = document.getElementById('queueSummary');
    const qsHeadline = document.getElementById('qsHeadline');
    const qsFill = document.getElementById('qsFill');
    const pauseAllBtn = document.getElementById('pauseAllBtn');
    const resumeAllBtn = document.getElementById('resumeAllBtn');
    const clearCompletedBtn = document.getElementById('clearCompletedBtn');
    const jobRowsEl = document.getElementById('jobRows');
    const jobListEmpty = document.getElementById('jobListEmpty');
    const logToggle = document.getElementById('logToggle');
    const logEl = document.getElementById('log');
    const fileListEl = document.getElementById('fileList');
    const refreshFilesBtn = document.getElementById('refreshFilesBtn');

    // ── Global State ──────────────────────────────────────────────────────────
    const jobs = new Map();       // jobId -> job object
    const queue = [];             // jobIds waiting to start
    let activeJobCount = 0;
    let sharedNetworkProfile = null;
    let sharedThroughputEstimate = 256 * 1024;

    // ── IndexedDB Storage (one row per job, keyed by job id) ────────────────
    const IDB_NAME = 'UploadEngineDB';
    const IDB_VERSION = 2;
    const IDB_STORE = 'jobs';
    const IDB_LEGACY_STORE = 'active_session';

    function openIDB() {
        return new Promise((resolve, reject) => {
            const req = indexedDB.open(IDB_NAME, IDB_VERSION);
            req.onupgradeneeded = (e) => {
                const db = e.target.result;
                if (!db.objectStoreNames.contains(IDB_STORE)) {
                    db.createObjectStore(IDB_STORE);
                }
                if (db.objectStoreNames.contains(IDB_LEGACY_STORE)) {
                    db.deleteObjectStore(IDB_LEGACY_STORE);
                }
            };
            req.onsuccess = () => resolve(req.result);
            req.onerror = () => reject(req.error);
        });
    }

    function serializeJob(job) {
        return {
            id: job.id,
            file: job.file,
            name: job.name,
            size: job.size,
            type: job.type,
            status: job.status,
            sessionId: job.sessionId,
            blockSize: job.blockSize,
            blockHashes: job.precomputedBlockHashes,
            fingerprint: job.fingerprint,
            uploadedBytes: job.uploadedBytes,
            error: job.error
        };
    }

    async function persistJob(job) {
        try {
            const db = await openIDB();
            const tx = db.transaction(IDB_STORE, 'readwrite');
            tx.objectStore(IDB_STORE).put(serializeJob(job), job.id);
        } catch (e) {
            console.warn('IndexedDB write failed:', e);
        }
    }

    async function clearPersistedJob(id) {
        try {
            const db = await openIDB();
            const tx = db.transaction(IDB_STORE, 'readwrite');
            tx.objectStore(IDB_STORE).delete(id);
        } catch (e) {}
    }

    async function loadPersistedJobs() {
        try {
            const db = await openIDB();
            return new Promise((resolve) => {
                const tx = db.transaction(IDB_STORE, 'readonly');
                const store = tx.objectStore(IDB_STORE);
                const results = [];
                const req = store.openCursor();
                req.onsuccess = (e) => {
                    const cursor = e.target.result;
                    if (cursor) {
                        results.push(cursor.value);
                        cursor.continue();
                    } else {
                        resolve(results);
                    }
                };
                req.onerror = () => resolve(results);
            });
        } catch (e) {
            return [];
        }
    }

    // ── Utilities ─────────────────────────────────────────────────────────────
    function log(msg) {
        const entry = document.createElement('div');
        entry.className = 'line';
        const ts = new Date().toLocaleTimeString();
        entry.innerHTML = '<span class="ts">[' + ts + ']</span>' + msg;
        logEl.appendChild(entry);
        logEl.scrollTop = logEl.scrollHeight;
    }

    function formatBytes(b) {
        if (!b || b <= 0) return '0 B';
        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(b) / Math.log(1024));
        return (b / Math.pow(1024, i)).toFixed(1) + ' ' + units[i];
    }

    function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

    function escapeHtml(s) {
        return String(s).replace(/[&<>"']/g, c => ({
            '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
        }[c]));
    }

    function genId() {
        if (window.crypto && window.crypto.randomUUID) return window.crypto.randomUUID();
        return 'job_' + Date.now() + '_' + Math.random().toString(36).slice(2);
    }

    async function computeSHA256(arrayBuffer) {
        const hashBuf = await crypto.subtle.digest('SHA-256', arrayBuffer);
        return Array.from(new Uint8Array(hashBuf))
            .map(b => b.toString(16).padStart(2, '0')).join('');
    }

    function chooseBlockSize(fileSize, preferred) {
        // No requirement that blockSize evenly divide fileSize: every chunk
        // except the last is exactly blockSize, and the last is whatever
        // remains. Searching for an exact divisor here used to mean up to
        // "preferred" synchronous modulo checks (millions, for an 8MB
        // preference) with zero yielding -- a guaranteed tab freeze on any
        // file size that isn't a clean power of two.
        if (fileSize <= 0) return 0;
        return Math.min(preferred, fileSize);
    }

    function getMissingRanges(ranges, fileSize) {
        const missing = [];
        let expected = 0;
        for (const r of ranges) {
            if (r.start > expected) missing.push({ start: expected, end: r.start });
            if (r.end > expected) expected = r.end;
        }
        if (expected < fileSize) missing.push({ start: expected, end: fileSize });
        return missing;
    }

    function getFileCategory(name) {
        const ext = (name.split('.').pop() || '').toLowerCase();
        const map = {
            image: ['png', 'jpg', 'jpeg', 'gif', 'webp', 'svg', 'bmp', 'heic'],
            video: ['mp4', 'mov', 'mkv', 'avi', 'webm'],
            audio: ['mp3', 'wav', 'flac', 'aac', 'ogg'],
            archive: ['zip', 'rar', '7z', 'tar', 'gz'],
            doc: ['pdf', 'doc', 'docx', 'txt', 'md', 'ppt', 'pptx', 'xls', 'xlsx'],
            code: ['js', 'ts', 'py', 'go', 'java', 'c', 'cpp', 'rs', 'html', 'css', 'json']
        };
        for (const cat in map) {
            if (map[cat].indexOf(ext) !== -1) return { cat: cat, ext: ext };
        }
        return { cat: 'other', ext: ext || 'file' };
    }

    // ── Network Measurement (shared across jobs) ─────────────────────────────
    async function measureNetwork() {
        const samples = [];
        for (let i = 0; i < PING_COUNT; i++) {
            const t0 = performance.now();
            try {
                await fetch('/api/ping', { cache: 'no-store' });
                samples.push(performance.now() - t0);
            } catch (e) {}
        }
        let rtt = 300, jitter = 200;
        if (samples.length) {
            samples.sort((a, b) => a - b);
            rtt = samples[Math.floor(samples.length / 2)];
            jitter = samples[samples.length - 1] - samples[0];
        }

        let throughput = 256 * 1024;
        try {
            const probeData = new Uint8Array(PROBE_BYTES);
            const t0 = performance.now();
            await fetch('/api/probe', { method: 'POST', body: probeData });
            const elapsedSec = (performance.now() - t0) / 1000;
            if (elapsedSec > 0.001) throughput = PROBE_BYTES / elapsedSec;
        } catch (e) {}

        return { rtt, jitter, throughput };
    }

    function classifyNetwork(sample) {
        const rtt = sample.rtt, jitter = sample.jitter, throughput = sample.throughput;
        const mbps = throughput / (1024 * 1024);
        if (rtt < 120 && jitter < 60 && mbps > 3) {
            return { label: 'High Speed', blockSize: GOOD_BLOCK_SIZE, concurrency: 4 };
        }
        if (rtt < 350 && jitter < 200 && mbps > 0.8) {
            return { label: 'Moderate', blockSize: MEDIUM_BLOCK_SIZE, concurrency: 2 };
        }
        return { label: 'Constrained', blockSize: MIN_BLOCK_SIZE, concurrency: 1 };
    }

    async function ensureNetworkProfile() {
        if (!sharedNetworkProfile) {
            const sample = await measureNetwork();
            sharedNetworkProfile = classifyNetwork(sample);
            sharedThroughputEstimate = sample.throughput;
            log('Network profile: ' + sharedNetworkProfile.label + ' (' + formatBytes(sample.throughput) + '/s est.)');
        }
        return sharedNetworkProfile;
    }

    function createNetController(profile) {
        return {
            blockSize: profile.blockSize,
            maxConcurrency: profile.concurrency,
            label: profile.label,
            activeWorkers: 0,
            consecutiveSuccesses: 0,
            lastBackoff: 0,
            estThroughput: () => sharedThroughputEstimate,
            recordSuccess() {
                this.consecutiveSuccesses++;
                if (this.consecutiveSuccesses >= AIMD_SUCCESS_STREAK) {
                    this.consecutiveSuccesses = 0;
                    this.maxConcurrency = Math.min(MAX_CONCURRENCY, this.maxConcurrency + 1);
                }
            },
            recordFailure() {
                const now = Date.now();
                if (now - this.lastBackoff < AIMD_BACKOFF_COOLDOWN_MS) return;
                this.lastBackoff = now;
                this.consecutiveSuccesses = 0;
                this.maxConcurrency = Math.max(MIN_CONCURRENCY, Math.floor(this.maxConcurrency / 2));
            }
        };
    }

    function createChunkCursor(missingRanges, blockSize) {
        let ri = 0;
        let pos = missingRanges.length ? missingRanges[0].start : 0;
        function advance() {
            while (ri < missingRanges.length && pos >= missingRanges[ri].end) {
                ri++;
                if (ri < missingRanges.length) pos = missingRanges[ri].start;
            }
        }
        return {
            hasMore() { advance(); return ri < missingRanges.length; },
            next() {
                advance();
                if (ri >= missingRanges.length) return null;
                const end = Math.min(pos + blockSize, missingRanges[ri].end);
                const range = { start: pos, end: end };
                pos = end;
                return range;
            }
        };
    }

    // ── Job Creation ──────────────────────────────────────────────────────────
    function createJobFromFile(f) {
        return {
            id: genId(),
            file: f,
            name: f.name,
            size: f.size,
            type: f.type,
            status: 'queued',
            sessionId: null,
            blockSize: null,
            precomputedBlockHashes: [],
            fingerprint: '',
            uploadedBytes: 0,
            hashProgress: 0,
            error: null,
            controller: null,
            abortControllers: new Set(),
            retryCount: 0,
            ewmaThroughput: 0,
            speedText: '—',
            etaText: '—',
            lastProgressTime: 0,
            lastProgressBytes: 0,
            speedSamples: [],
            pauseRequested: false,
            cancelRequested: false
        };
    }

    // ── Pre-Hashing (per job) ────────────────────────────────────────────────
    async function preHashJobBlocks(job) {
        const blockSize = job.blockSize;
        const totalBlocks = Math.ceil(job.file.size / blockSize);
        job.precomputedBlockHashes = new Array(totalBlocks);
        const cumulativeHashes = [];

        log('Pre-hashing ' + totalBlocks + ' block(s) for "' + job.name + '"...');

        let lastYield = performance.now();

        for (let i = 0; i < totalBlocks; i++) {
            if (job.cancelRequested) throw new Error('Pre-hashing cancelled');
            while (job.pauseRequested && !job.cancelRequested) await sleep(200);
            if (job.cancelRequested) throw new Error('Pre-hashing cancelled');

            const start = i * blockSize;
            const end = Math.min(start + blockSize, job.file.size);
            const slice = job.file.slice(start, end);
            const buf = await slice.arrayBuffer();
            const blockHash = await computeSHA256(buf);

            job.precomputedBlockHashes[i] = blockHash;
            cumulativeHashes.push(blockHash);

            job.hashProgress = Math.round(((i + 1) / totalBlocks) * 100);
            if (i === totalBlocks - 1) {
                renderJob(job);
            } else {
                renderJobThrottled(job, RENDER_THROTTLE_MS);
            }

            const now = performance.now();
            if (now - lastYield > 12) {
                await sleep(0);
                lastYield = performance.now();
            }
        }

        const concatenated = cumulativeHashes.join('');
        const fpBuf = new TextEncoder().encode(concatenated);
        job.fingerprint = await computeSHA256(fpBuf);
        log('Fingerprint for "' + job.name + '": ' + job.fingerprint);

        await persistJob(job);
    }

    // ── Chunk Upload (per job) ───────────────────────────────────────────────
    function onJobProgress(job, bytes, elapsedSec, serverTotal) {
        if (elapsedSec > 0.05) {
            const instant = bytes / elapsedSec;
            sharedThroughputEstimate = sharedThroughputEstimate * (1 - EWMA_ALPHA) + instant * EWMA_ALPHA;
        }
        job.uploadedBytes = serverTotal;

        const now = Date.now();
        if (!job.lastProgressTime) {
            job.lastProgressTime = now;
            job.lastProgressBytes = serverTotal;
        } else if (now - job.lastProgressTime > 1000) {
            const deltaBytes = serverTotal - job.lastProgressBytes;
            const deltaSec = (now - job.lastProgressTime) / 1000;
            if (deltaSec > 0) {
                const speed = deltaBytes / deltaSec;
                job.speedSamples.push(speed);
                if (job.speedSamples.length > 5) job.speedSamples.shift();
                const avgSpeed = job.speedSamples.reduce((a, b) => a + b, 0) / job.speedSamples.length;
                job.ewmaThroughput = avgSpeed;
                const remaining = job.size - serverTotal;
                const eta = avgSpeed > 0 ? remaining / avgSpeed : 0;
                job.speedText = formatBytes(avgSpeed) + '/s';
                job.etaText = eta > 0 ? Math.ceil(eta) + 's' : '—';
            }
            job.lastProgressTime = now;
            job.lastProgressBytes = serverTotal;
        }

        renderJobThrottled(job, RENDER_THROTTLE_MS);
    }

    async function uploadOneChunk(job, range) {
        const start = range.start, end = range.end;
        const chunk = job.file.slice(start, end);

        if (job.cancelRequested || job.pauseRequested) return { success: false, aborted: true };

        const blockIndex = Math.floor(start / job.blockSize);
        const chunkHash = job.precomputedBlockHashes[blockIndex];

        for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            if (job.cancelRequested || job.pauseRequested) return { success: false, aborted: true };

            const ac = new AbortController();
            job.abortControllers.add(ac);
            const estBps = Math.max(job.controller.estThroughput(), 32 * 1024);
            const timeoutMs = Math.max(6000, ((end - start) / estBps) * 1000 * 3);
            const timeoutId = setTimeout(() => ac.abort(), timeoutMs);

            const t0 = performance.now();
            try {
                const resp = await fetch('/api/upload', {
                    method: 'POST',
                    headers: {
                        'X-Session-ID': job.sessionId,
                        'X-Offset': String(start),
                        'X-Chunk-SHA256': chunkHash
                    },
                    body: chunk,
                    signal: ac.signal
                });
                clearTimeout(timeoutId);
                job.abortControllers.delete(ac);

                if (!resp.ok) {
                    const errRes = await resp.json().catch(() => ({ error: 'Upload failed' }));
                    throw new Error(errRes.error || 'Upload failed');
                }

                const data = await resp.json();
                const elapsedSec = (performance.now() - t0) / 1000;
                job.controller.recordSuccess();
                onJobProgress(job, end - start, elapsedSec, data.total || 0);
                return { success: true };
            } catch (e) {
                clearTimeout(timeoutId);
                job.abortControllers.delete(ac);

                if (job.cancelRequested) return { success: false, aborted: true };
                if (job.pauseRequested && e.name === 'AbortError') return { success: false, aborted: true };

                job.controller.recordFailure();
                job.retryCount++;

                if (attempt < MAX_RETRIES && !job.cancelRequested && !job.pauseRequested) {
                    await sleep(RETRY_DELAY_MS * Math.pow(2, attempt - 1));
                }
            }
        }
        return { success: false, aborted: false };
    }

    function runUploadPool(job, missingRanges) {
        return new Promise((resolve, reject) => {
            const cursor = createChunkCursor(missingRanges, job.blockSize);
            let finished = false;
            let permanentFailure = null;
            let sawPause = false;

            function finishIfDone() {
                if (finished || job.controller.activeWorkers > 0) return;
                finished = true;
                if (permanentFailure) reject(permanentFailure);
                else resolve({ paused: sawPause && !job.cancelRequested, cancelled: job.cancelRequested });
            }

            function spawn() {
                job.controller.activeWorkers++;
                renderJob(job);
                (async () => {
                    while (true) {
                        if (job.cancelRequested) break;
                        if (job.pauseRequested) { sawPause = true; break; }
                        if (permanentFailure) break;
                        if (job.controller.activeWorkers > job.controller.maxConcurrency) break;

                        const range = cursor.next();
                        if (!range) break;

                        const result = await uploadOneChunk(job, range);
                        if (!result.success) {
                            if (!result.aborted) {
                                permanentFailure = new Error('Chunk failed at ' + formatBytes(range.start));
                            } else if (job.pauseRequested) {
                                sawPause = true;
                            }
                            break;
                        }
                    }
                })().finally(() => {
                    job.controller.activeWorkers--;
                    if (!job.cancelRequested && !job.pauseRequested && !permanentFailure &&
                        job.controller.activeWorkers < job.controller.maxConcurrency && cursor.hasMore()) {
                        spawn();
                    }
                    finishIfDone();
                });
            }

            const initialWorkers = Math.max(1, job.controller.maxConcurrency);
            let spawned = 0;
            for (let i = 0; i < initialWorkers && cursor.hasMore(); i++) { spawn(); spawned++; }
            if (spawned === 0) finishIfDone();
        });
    }

    // ── Per-Job Orchestration ─────────────────────────────────────────────────
    async function runJob(job) {
        job.cancelRequested = false;
        job.pauseRequested = false;
        job.status = 'uploading';
        renderJob(job);

        try {
            const profile = await ensureNetworkProfile();

            if (!job.controller) {
                job.controller = createNetController(profile);
            }

            if (job.blockSize == null) {
                job.blockSize = chooseBlockSize(job.file.size, profile.blockSize);
                job.controller.blockSize = job.blockSize;
                await persistJob(job);
            } else {
                job.controller.blockSize = job.blockSize;
            }

            if (job.precomputedBlockHashes.length === 0 && job.blockSize > 0) {
                job.status = 'hashing';
                renderJob(job);
                await preHashJobBlocks(job);
                job.status = 'uploading';
                renderJob(job);
            }

            if (!job.sessionId) {
                const resp = await fetch('/api/begin', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        filename: job.name,
                        size: job.size,
                        contentType: job.type || 'application/octet-stream',
                        blockSize: job.blockSize
                    })
                });
                if (!resp.ok) {
                    const errRes = await resp.json().catch(() => ({ error: 'Failed to begin session' }));
                    throw new Error(errRes.error || 'Failed to begin session');
                }
                const data = await resp.json();
                job.sessionId = data.id;
                await persistJob(job);
                log('Session started for "' + job.name + '": ' + job.sessionId);
            }

            while (!job.cancelRequested) {
                if (job.pauseRequested) break;

                const progResp = await fetch('/api/progress?id=' + encodeURIComponent(job.sessionId));
                if (!progResp.ok) throw new Error('Failed to fetch progress');
                const prog = await progResp.json();
                job.uploadedBytes = prog.total || 0;
                renderJob(job);

                if (job.uploadedBytes >= job.size) break;

                const missing = getMissingRanges(prog.ranges || [], job.size);
                if (missing.length === 0) break;

                const result = await runUploadPool(job, missing);
                if (result.cancelled) break;
                if (result.paused) {
                    job.status = 'paused';
                    renderJob(job);
                    await persistJob(job);
                    return;
                }
            }

            if (job.cancelRequested) {
                job.status = 'cancelled';
                renderJob(job);
                await clearPersistedJob(job.id);
                return;
            }

            job.status = 'completing';
            renderJob(job);

            const completeResp = await fetch('/api/complete', {
                method: 'POST',
                headers: { 'X-Session-ID': job.sessionId }
            });
            if (!completeResp.ok) {
                const errRes = await completeResp.json().catch(() => ({ error: 'Failed to finalize upload' }));
                throw new Error(errRes.error || 'Failed to finalize upload');
            }

            job.status = 'completed';
            job.uploadedBytes = job.size;
            renderJob(job);
            await clearPersistedJob(job.id);
            log('"' + job.name + '" completed successfully.');
            fetchUploadedFiles();
        } catch (err) {
            job.status = 'error';
            job.error = err.message || String(err);
            renderJob(job);
            await persistJob(job);
            log('Error uploading "' + job.name + '": ' + job.error);
        } finally {
            activeJobCount--;
            pumpQueue();
            updateSummary();
        }
    }

    // ── Scheduler ─────────────────────────────────────────────────────────────
    function enqueueJob(job) {
        jobs.set(job.id, job);
        queue.push(job.id);
        job.status = 'queued';
        renderJob(job);
        persistJob(job);
        pumpQueue();
    }

    function pumpQueue() {
        while (activeJobCount < MAX_CONCURRENT_JOBS && queue.length) {
            const id = queue.shift();
            const job = jobs.get(id);
            if (!job || job.status === 'cancelled') continue;
            activeJobCount++;
            runJob(job);
        }
        updateSummary();
    }

    function handleFilesSelected(fileList) {
        const files = Array.from(fileList || []);
        if (files.length === 0) return;
        for (const f of files) {
            const job = createJobFromFile(f);
            enqueueJob(job);
        }
        log(files.length + ' file' + (files.length > 1 ? 's' : '') + ' added to queue.');
    }

    function pauseJob(id) {
        const job = jobs.get(id);
        if (!job) return;
        if (job.status !== 'uploading' && job.status !== 'hashing' && job.status !== 'queued') return;

        job.pauseRequested = true;
        for (const ac of job.abortControllers) ac.abort();

        if (job.status === 'queued') {
            const idx = queue.indexOf(id);
            if (idx !== -1) queue.splice(idx, 1);
            job.status = 'paused';
            renderJob(job);
            persistJob(job);
        }
        // Otherwise runJob's own loop detects pauseRequested and transitions to 'paused'.
    }

    function resumeJob(id) {
        const job = jobs.get(id);
        if (!job) return;
        if (job.status !== 'paused' && job.status !== 'error') return;

        job.pauseRequested = false;
        job.cancelRequested = false;
        job.error = null;
        job.status = 'queued';
        renderJob(job);
        queue.push(id);
        pumpQueue();
    }

    function retryJob(id) {
        resumeJob(id);
    }

    async function cancelJob(id) {
        const job = jobs.get(id);
        if (!job) return;

        if (job.status === 'completed' || job.status === 'cancelled') {
            removeJobRow(id);
            jobs.delete(id);
            updateSummary();
            return;
        }

        if (!confirm('Cancel "' + job.name + '"? This cannot be undone.')) return;

        const wasActive = job.status === 'uploading' || job.status === 'hashing' || job.status === 'completing';
        job.cancelRequested = true;
        for (const ac of job.abortControllers) ac.abort();

        const qIdx = queue.indexOf(id);
        if (qIdx !== -1) queue.splice(qIdx, 1);

        if (job.sessionId) {
            try {
                await fetch('/api/abort', { method: 'POST', headers: { 'X-Session-ID': job.sessionId } });
            } catch (e) {}
        }
        await clearPersistedJob(id);

        if (!wasActive) {
            job.status = 'cancelled';
            renderJob(job);
            updateSummary();
        }
        // If wasActive, runJob's own loop will notice cancelRequested and finalize the status.
    }

    function pauseAllJobs() {
        for (const job of Array.from(jobs.values())) {
            if (job.status === 'uploading' || job.status === 'hashing' || job.status === 'queued') {
                pauseJob(job.id);
            }
        }
    }

    function resumeAllJobs() {
        for (const job of Array.from(jobs.values())) {
            if (job.status === 'paused') resumeJob(job.id);
        }
    }

    function clearCompletedJobs() {
        for (const [id, job] of Array.from(jobs.entries())) {
            if (job.status === 'completed' || job.status === 'cancelled') {
                removeJobRow(id);
                jobs.delete(id);
            }
        }
        updateSummary();
    }

    // ── Rendering ─────────────────────────────────────────────────────────────
    function statusLabel(job) {
        switch (job.status) {
            case 'queued': return 'Queued';
            case 'hashing': return 'Preparing… ' + (job.hashProgress || 0) + '%';
            case 'uploading': return job.speedText + ' · ETA ' + job.etaText;
            case 'paused': return 'Paused';
            case 'completing': return 'Finalizing…';
            case 'completed': return 'Complete';
            case 'error': return 'Failed: ' + (job.error || 'Unknown error');
            case 'cancelled': return 'Cancelled';
            default: return '';
        }
    }

    function ensureJobRow(job) {
        let row = jobRowsEl.querySelector('[data-job-id="' + job.id + '"]');
        if (row) return row;

        const info = getFileCategory(job.name);
        row = document.createElement('div');
        row.className = 'job-row';
        row.setAttribute('data-job-id', job.id);
        row.innerHTML =
            '<div class="job-badge ' + info.cat + '">' + escapeHtml(info.ext.slice(0, 4).toUpperCase()) + '</div>' +
            '<div class="job-main">' +
                '<div class="job-top">' +
                    '<span class="job-name" title="' + escapeHtml(job.name) + '">' + escapeHtml(job.name) + '</span>' +
                    '<span class="job-size">' + formatBytes(job.size) + '</span>' +
                '</div>' +
                '<div class="job-progress-track"><div class="job-progress-fill" data-role="fill"></div></div>' +
                '<div class="job-status" data-role="status"></div>' +
            '</div>' +
            '<div class="job-actions">' +
                '<button class="job-btn" data-action="pause" title="Pause">' + ICON_PAUSE + '</button>' +
                '<button class="job-btn" data-action="resume" title="Resume">' + ICON_PLAY + '</button>' +
                '<button class="job-btn" data-action="retry" title="Retry">' + ICON_RETRY + '</button>' +
                '<button class="job-btn danger" data-action="cancel" title="Cancel">' + ICON_CLOSE + '</button>' +
            '</div>';

        row.querySelector('[data-action="pause"]').addEventListener('click', () => pauseJob(job.id));
        row.querySelector('[data-action="resume"]').addEventListener('click', () => resumeJob(job.id));
        row.querySelector('[data-action="retry"]').addEventListener('click', () => retryJob(job.id));
        row.querySelector('[data-action="cancel"]').addEventListener('click', () => cancelJob(job.id));

        jobRowsEl.appendChild(row);
        return row;
    }

    function renderJob(job) {
        const row = ensureJobRow(job);
        const fill = row.querySelector('[data-role="fill"]');
        const statusEl = row.querySelector('[data-role="status"]');

        let pct = 0;
        if (job.status === 'hashing') {
            pct = job.hashProgress || 0;
            fill.className = 'job-progress-fill hashing';
        } else {
            pct = job.size > 0 ? Math.min(100, (job.uploadedBytes / job.size) * 100) : (job.status === 'completed' ? 100 : 0);
            fill.className = 'job-progress-fill' +
                (job.status === 'error' ? ' errored' : job.status === 'completed' ? ' done' : '');
        }
        fill.style.width = pct + '%';

        statusEl.textContent = statusLabel(job);
        statusEl.className = 'job-status status-' + job.status;

        row.classList.toggle('is-done', job.status === 'completed');
        row.classList.toggle('is-error', job.status === 'error');
        row.classList.toggle('is-cancelled', job.status === 'cancelled');

        const btnPause = row.querySelector('[data-action="pause"]');
        const btnResume = row.querySelector('[data-action="resume"]');
        const btnRetry = row.querySelector('[data-action="retry"]');
        const btnCancel = row.querySelector('[data-action="cancel"]');

        btnPause.style.display = (job.status === 'uploading' || job.status === 'hashing' || job.status === 'queued') ? 'flex' : 'none';
        btnResume.style.display = (job.status === 'paused') ? 'flex' : 'none';
        btnRetry.style.display = (job.status === 'error') ? 'flex' : 'none';
        btnCancel.title = (job.status === 'completed' || job.status === 'cancelled') ? 'Remove' : 'Cancel';

        updateSummary();
    }

    // renderJob() walks the DOM for this row and recomputes the whole queue
    // summary. That's fine at the rate chunks normally complete, but a
    // pre-hash or upload loop over thousands of small blocks calling it
    // unconditionally per-block is a real CPU sink (forced style/layout
    // recalculation thousands of times back to back). Throttle it to a
    // fixed cadence for high-frequency callers; callers that need an
    // immediate, un-throttled update (status transitions, completion) should
    // keep calling renderJob(job) directly.
    function renderJobThrottled(job, minIntervalMs) {
        const now = performance.now();
        if (!job._lastRenderAt || now - job._lastRenderAt >= minIntervalMs) {
            job._lastRenderAt = now;
            renderJob(job);
        }
    }

    function removeJobRow(id) {
        const row = jobRowsEl.querySelector('[data-job-id="' + id + '"]');
        if (row) row.remove();
    }

    function updateSummary() {
        const all = Array.from(jobs.values());

        if (all.length === 0) {
            queueSummary.style.display = 'none';
            jobListEmpty.style.display = 'block';
            return;
        }
        jobListEmpty.style.display = 'none';
        queueSummary.style.display = 'flex';

        const activeStates = ['uploading', 'hashing', 'queued', 'completing'];
        const active = all.filter(j => activeStates.indexOf(j.status) !== -1);
        const paused = all.filter(j => j.status === 'paused');
        const done = all.filter(j => j.status === 'completed');
        const cancelled = all.filter(j => j.status === 'cancelled');
        const failed = all.filter(j => j.status === 'error');

        let totalBytes = 0, uploadedBytes = 0, speedSum = 0;
        for (const j of active) {
            totalBytes += j.size;
            uploadedBytes += j.uploadedBytes;
            speedSum += j.ewmaThroughput || 0;
        }
        const pct = totalBytes > 0 ? Math.min(100, (uploadedBytes / totalBytes) * 100) : (active.length ? 0 : 100);
        qsFill.style.width = pct + '%';

        let headline;
        if (active.length > 0) {
            const remaining = Math.max(0, totalBytes - uploadedBytes);
            const eta = speedSum > 0 ? remaining / speedSum : 0;
            headline = active.length + ' active · ' + formatBytes(speedSum) + '/s' +
                (eta > 0 ? ' · ETA ' + Math.ceil(eta) + 's' : '');
        } else if (paused.length > 0) {
            headline = paused.length + ' paused';
        } else if (failed.length > 0) {
            headline = failed.length + ' failed';
        } else {
            headline = 'All transfers complete';
        }
        if (done.length > 0) headline += ' · ' + done.length + ' done';
        if (failed.length > 0 && (active.length + paused.length) > 0) headline += ' · ' + failed.length + ' failed';
        qsHeadline.textContent = headline;

        pauseAllBtn.disabled = all.filter(j => j.status === 'uploading' || j.status === 'hashing' || j.status === 'queued').length === 0;
        resumeAllBtn.disabled = paused.length === 0;
        clearCompletedBtn.disabled = (done.length + cancelled.length) === 0;
    }

    // ── Server Storage Panel ─────────────────────────────────────────────────
    async function fetchUploadedFiles() {
        try {
            const resp = await fetch('/api/files');
            if (!resp.ok) throw new Error('Failed to list files');
            const files = await resp.json();
            renderFileList(files);
        } catch (e) {
            fileListEl.innerHTML = '<div class="empty-box">Could not reach server storage.</div>';
        }
    }

    function renderFileList(files) {
        if (!files || files.length === 0) {
            fileListEl.innerHTML = '<div class="empty-box">No files uploaded yet.</div>';
            return;
        }
        fileListEl.innerHTML = files.map(f => {
            const safeName = escapeHtml(f.name);
            const encoded = encodeURIComponent(f.name);
            return (
                '<div class="file-card">' +
                    '<div class="file-card-info">' +
                        '<div class="file-card-name">' + safeName + '</div>' +
                        '<div class="file-card-meta">' + formatBytes(f.size) + ' · ' + new Date(f.modTime).toLocaleString() + '</div>' +
                    '</div>' +
                    '<div class="file-card-actions">' +
                        '<a class="action-btn" href="/api/download?name=' + encoded + '" title="Download">' +
                            '<svg viewBox="0 0 24 24"><path d="M12 3v12M7 10l5 5 5-5M5 21h14"/></svg>' +
                        '</a>' +
                        '<button class="action-btn del" data-delete="' + encoded + '" title="Delete">' +
                            '<svg viewBox="0 0 24 24"><path d="M3 6h18M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2m3 0-1 14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2L4 6"/></svg>' +
                        '</button>' +
                    '</div>' +
                '</div>'
            );
        }).join('');

        fileListEl.querySelectorAll('[data-delete]').forEach(btn => {
            btn.addEventListener('click', async () => {
                const encoded = btn.getAttribute('data-delete');
                if (!confirm('Delete this file from the server?')) return;
                try {
                    const resp = await fetch('/api/delete?name=' + encoded, { method: 'DELETE' });
                    if (!resp.ok) throw new Error('Delete failed');
                    fetchUploadedFiles();
                } catch (e) {
                    alert('Failed to delete file.');
                }
            });
        });
    }

    // ── Page Wiring ───────────────────────────────────────────────────────────
    dropZone.addEventListener('click', () => fileInput.click());

    dropZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        dropZone.classList.add('dragover');
    });

    dropZone.addEventListener('dragleave', () => {
        dropZone.classList.remove('dragover');
    });

    dropZone.addEventListener('drop', (e) => {
        e.preventDefault();
        dropZone.classList.remove('dragover');
        handleFilesSelected(e.dataTransfer.files);
    });

    fileInput.addEventListener('change', () => {
        if (fileInput.files && fileInput.files.length) {
            handleFilesSelected(fileInput.files);
        }
        fileInput.value = '';
    });

    pauseAllBtn.addEventListener('click', pauseAllJobs);
    resumeAllBtn.addEventListener('click', resumeAllJobs);
    clearCompletedBtn.addEventListener('click', clearCompletedJobs);
    refreshFilesBtn.addEventListener('click', fetchUploadedFiles);

    logToggle.addEventListener('click', () => {
        const showing = logEl.style.display !== 'none';
        logEl.style.display = showing ? 'none' : 'block';
        logToggle.textContent = showing ? 'Show activity log ▾' : 'Hide activity log ▴';
    });

    window.addEventListener('load', async () => {
        fetchUploadedFiles();

        const restored = await loadPersistedJobs();
        for (const raw of restored) {
            if (!raw || !raw.file) continue;
            const job = createJobFromFile(raw.file);
            job.id = raw.id;
            job.sessionId = raw.sessionId || null;
            job.blockSize = (raw.blockSize != null) ? raw.blockSize : null;
            job.precomputedBlockHashes = raw.blockHashes || [];
            job.fingerprint = raw.fingerprint || '';
            job.uploadedBytes = raw.uploadedBytes || 0;
            job.status = raw.status === 'error' ? 'error' : 'paused';
            job.error = raw.error || null;
            jobs.set(job.id, job);
            renderJob(job);
        }

        if (restored.length > 0) {
            log(restored.length + ' session(s) restored from a previous visit. Click resume to continue.');
        }
        updateSummary();
    });
</script>
</body>
</html>
`
