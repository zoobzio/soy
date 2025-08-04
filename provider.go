package cereal

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

// Provider defines the common interface that all data providers must implement.
type Provider interface {
	// Basic CRUD operations
	Get(ctx context.Context, uri URI) ([]byte, error)
	Set(ctx context.Context, uri URI, data []byte) error
	Delete(ctx context.Context, uri URI) error
	Exists(ctx context.Context, uri URI) (bool, error)

	// Batch operations
	BatchGet(ctx context.Context, uris []URI) ([][]byte, []error)
	BatchSet(ctx context.Context, uris []URI, data [][]byte) []error
	BatchDelete(ctx context.Context, uris []URI) []error

	// Execute an action with a payload
	Execute(ctx context.Context, uri URI, payload []byte) ([]byte, error)

	// Watch for changes
	Sync(ctx context.Context, uri URI, callback func([]byte)) (stop func(), err error)

	// Provider lifecycle
	Close() error
}

// Use this to ensure your provider correctly implements the interface.
func TestProvider(t *testing.T, providerName string, provider Provider) {
	ctx := context.Background()
	testURI := NewURI(providerName + "://test/data/item1")
	testData := []byte("test data")

	// Test Set and Get
	t.Run("SetAndGet", func(t *testing.T) {
		err := provider.Set(ctx, testURI, testData)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		got, err := provider.Get(ctx, testURI)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if !bytes.Equal(got, testData) {
			t.Errorf("Got %q, want %q", got, testData)
		}
	})

	// Test Exists
	t.Run("Exists", func(t *testing.T) {
		exists, err := provider.Exists(ctx, testURI)
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if !exists {
			t.Error("Expected resource to exist")
		}
	})

	// Test Delete
	t.Run("Delete", func(t *testing.T) {
		err := provider.Delete(ctx, testURI)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		_, err = provider.Get(ctx, testURI)
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Expected ErrNotFound, got %v", err)
		}
	})

	// Test Sync (if supported)
	t.Run("Sync", func(t *testing.T) {
		// Set initial data
		syncURI := NewURI("test://sync/item")
		syncData := []byte("initial data")
		if err := provider.Set(ctx, syncURI, syncData); err != nil {
			t.Fatalf("Failed to set sync data: %v", err)
		}

		// Start syncing
		stop, err := provider.Sync(ctx, syncURI, func(_ []byte) {
			// Callback received
		})

		if errors.Is(err, ErrSyncNotSupported) {
			t.Skip("Provider does not support sync")
			return
		}

		if err != nil {
			t.Fatalf("Sync failed: %v", err)
		}
		defer stop()

		// Wait for initial callback
		// Note: real tests might need better synchronization
		// This is simplified for the example
	})

	// Test Batch operations
	t.Run("BatchOperations", func(t *testing.T) {
		// Prepare test data
		uris := []URI{
			NewURI(providerName + "://test/batch/item1"),
			NewURI(providerName + "://test/batch/item2"),
			NewURI(providerName + "://test/batch/item3"),
		}
		data := [][]byte{
			[]byte("data1"),
			[]byte("data2"),
			[]byte("data3"),
		}

		// Test BatchSet
		errs := provider.BatchSet(ctx, uris, data)
		for i, err := range errs {
			if err != nil {
				t.Errorf("BatchSet[%d] failed: %v", i, err)
			}
		}

		// Test BatchGet
		results, errs := provider.BatchGet(ctx, uris)
		for i, err := range errs {
			if err != nil {
				t.Errorf("BatchGet[%d] failed: %v", i, err)
			}
			if !bytes.Equal(results[i], data[i]) {
				t.Errorf("BatchGet[%d] got %q, want %q", i, results[i], data[i])
			}
		}

		// Test BatchDelete
		errs = provider.BatchDelete(ctx, uris)
		for i, err := range errs {
			if err != nil {
				t.Errorf("BatchDelete[%d] failed: %v", i, err)
			}
		}

		// Verify deleted
		_, errs = provider.BatchGet(ctx, uris)
		for i, err := range errs {
			if !errors.Is(err, ErrNotFound) {
				t.Errorf("BatchGet[%d] after delete: expected ErrNotFound, got %v", i, err)
			}
		}
	})

	// Cleanup
	provider.Close()
}
