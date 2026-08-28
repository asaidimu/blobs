package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	bserrors "github.com/asaidimu/blobs/errors"
)

// writeJSON encodes v as the JSON response body with the given status code.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("movieapp: json encode error: %v", err)
	}
}

// writeError writes a JSON error body: {"error": message}.
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

// mapStoreError inspects an error returned by the blobs store/staging
// packages and maps it to the HTTP status code and message that should be
// sent back to the client. It mirrors the mapping streaming.ServeBlob's
// internal writeBlobError performs, extended to cover the additional typed
// errors this application can encounter (quota, already-exists, corruption).
func mapStoreError(w http.ResponseWriter, err error) {
	var notFound *bserrors.NotFoundError
	if errors.As(err, &notFound) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	var alreadyExists *bserrors.AlreadyExistsError
	if errors.As(err, &alreadyExists) {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	var quota *bserrors.QuotaExceededError
	if errors.As(err, &quota) {
		writeError(w, http.StatusInsufficientStorage, err.Error())
		return
	}
	var invalidKey *bserrors.InvalidKeyError
	if errors.As(err, &invalidKey) {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var corruption *bserrors.CorruptionError
	if errors.As(err, &corruption) {
		writeError(w, http.StatusInternalServerError, "stored data failed integrity check")
		return
	}
	var closed *bserrors.ClosedError
	if errors.As(err, &closed) {
		writeError(w, http.StatusServiceUnavailable, "store is shutting down")
		return
	}
	writeError(w, http.StatusInternalServerError, err.Error())
}

// withLogging logs method, path, status, and duration for every request.
func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		log.Printf("%s %s %d %s", r.Method, r.URL.Path, sw.status, time.Since(start))
	})
}

// statusWriter captures the status code written so withLogging can report it.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (sw *statusWriter) WriteHeader(status int) {
	sw.status = status
	sw.ResponseWriter.WriteHeader(status)
}
