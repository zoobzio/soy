package cereal

import "errors"

// Common errors returned by providers.
var (
	// ErrNotFound is returned when a requested resource doesn't exist.
	ErrNotFound = errors.New("resource not found")

	// ErrQueryNotFound is returned when a named query doesn't exist.
	ErrQueryNotFound = errors.New("query not found")

	// ErrQueryExists is returned when trying to register a query that already exists.
	ErrQueryExists = errors.New("query already exists")

	// ErrBatchSizeMismatch is returned when batch operation arrays have different lengths.
	ErrBatchSizeMismatch = errors.New("batch operation size mismatch")
)
