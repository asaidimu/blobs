// Package errors defines the typed error hierarchy for the blobstore.
// All errors returned by the public API are either one of these types
// or wrap one of them — callers can use errors.As to inspect them.
package errors

import "fmt"

// NotFoundError is returned when a key or namespace does not exist.
type NotFoundError struct {
	NamespaceID string
	Key         string
}

func (e *NotFoundError) Error() string {
	if e.Key == "" {
		return fmt.Sprintf("namespace %q not found", e.NamespaceID)
	}
	return fmt.Sprintf("key %q not found in namespace %q", e.Key, e.NamespaceID)
}

// AlreadyExistsError is returned when creating a namespace that already exists.
type AlreadyExistsError struct {
	NamespaceID string
}

func (e *AlreadyExistsError) Error() string {
	return fmt.Sprintf("namespace %q already exists", e.NamespaceID)
}

// QuotaExceededError is returned when a write would exceed a namespace quota.
type QuotaExceededError struct {
	NamespaceID string
	Dimension   string // "bytes", "blob_count", "blob_size"
	Limit       int64
	Requested   int64
}

func (e *QuotaExceededError) Error() string {
	return fmt.Sprintf(
		"namespace %q quota exceeded: %s limit is %d, requested %d",
		e.NamespaceID, e.Dimension, e.Limit, e.Requested,
	)
}

// CorruptionError is returned when a CRC or integrity check fails.
// This indicates physical data corruption in a segment file.
type CorruptionError struct {
	SegmentID string
	Offset    int64
	Detail    string
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf(
		"corruption in segment %s at offset %d: %s",
		e.SegmentID, e.Offset, e.Detail,
	)
}

// InvalidKeyError is returned when a key contains illegal characters
// or violates naming constraints.
type InvalidKeyError struct {
	Key    string
	Reason string
}

func (e *InvalidKeyError) Error() string {
	return fmt.Sprintf("invalid key %q: %s", e.Key, e.Reason)
}

// InvalidNamespaceIDError is returned when a namespace ID is malformed.
type InvalidNamespaceIDError struct {
	ID     string
	Reason string
}

func (e *InvalidNamespaceIDError) Error() string {
	return fmt.Sprintf("invalid namespace ID %q: %s", e.ID, e.Reason)
}

// ClosedError is returned when an operation is attempted on a closed Store.
type ClosedError struct{}

func (e *ClosedError) Error() string { return "store is closed" }
