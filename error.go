package cereal

import "errors"

// Common errors that providers may return.
var (
	// ErrNotFound is returned when a resource doesn't exist.
	ErrNotFound = errors.New("resource not found")

	// ErrNotSupported is returned when an operation isn't supported by a provider.
	ErrNotSupported = errors.New("operation not supported")

	// ErrInvalidURI is returned when a URI is malformed or invalid.
	ErrInvalidURI = errors.New("invalid URI")

	// ErrReadOnly is returned when attempting to modify a read-only provider.
	ErrReadOnly = errors.New("provider is read-only")

	// ErrSyncNotSupported is returned when sync/watch isn't supported by a provider.
	ErrSyncNotSupported = errors.New("sync not supported")
)
