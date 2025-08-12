package cereal

import (
	"context"
)

// Provider defines the common interface that all data providers must implement.
// Providers use ASTQL to build queries internally but expose a simple CRUD API.
type Provider interface {
	// Basic CRUD operations with explicit table/key parameters
	Get(ctx context.Context, table, keyField, keyValue string) ([]byte, error)
	Set(ctx context.Context, table, keyField, keyValue string, data []byte) error
	Delete(ctx context.Context, table, keyField, keyValue string) error
	Exists(ctx context.Context, table, keyField, keyValue string) (bool, error)

	// Batch operations
	BatchGet(ctx context.Context, table, keyField string, keyValues []string) ([][]byte, []error)
	BatchSet(ctx context.Context, table, keyField string, keyValues []string, data [][]byte) []error
	BatchDelete(ctx context.Context, table, keyField string, keyValues []string) []error

	// Execute runs a named query with parameters
	Execute(ctx context.Context, name string, parameters map[string]interface{}) ([]byte, error)

	// Provider lifecycle
	Close() error
}
