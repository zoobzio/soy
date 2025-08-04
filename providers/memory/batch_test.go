package memory_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/zoobzio/cereal"
	"github.com/zoobzio/cereal/providers/memory"
)

func TestBatchOperations(t *testing.T) {
	ctx := context.Background()
	provider := memory.New()
	defer provider.Close()

	t.Run("BatchSetSizeValidation", func(t *testing.T) {
		uris := []cereal.URI{
			cereal.NewURI("memory://test/1"),
			cereal.NewURI("memory://test/2"),
		}
		data := [][]byte{
			[]byte("data1"),
			// Missing second data element
		}

		errors := provider.BatchSet(ctx, uris, data)
		for i, err := range errors {
			if err == nil {
				t.Errorf("Expected error for mismatched batch sizes at index %d", i)
			}
		}
	})

	t.Run("PartialFailure", func(t *testing.T) {
		// Set up some data
		existingURI := cereal.NewURI("memory://test/exists")
		if err := provider.Set(ctx, existingURI, []byte("existing")); err != nil {
			t.Fatalf("Failed to set existing data: %v", err)
		}

		// Try to delete mix of existing and non-existing
		uris := []cereal.URI{
			existingURI,
			cereal.NewURI("memory://test/not-exists"),
			cereal.NewURI("memory://test/also-not-exists"),
		}

		errs := provider.BatchDelete(ctx, uris)

		// First should succeed
		if errs[0] != nil {
			t.Errorf("Expected success for existing item, got %v", errs[0])
		}

		// Others should fail with NotFound
		if !errors.Is(errs[1], cereal.ErrNotFound) {
			t.Errorf("Expected ErrNotFound for non-existing item, got %v", errs[1])
		}
		if !errors.Is(errs[2], cereal.ErrNotFound) {
			t.Errorf("Expected ErrNotFound for non-existing item, got %v", errs[2])
		}
	})

	t.Run("EmptyBatch", func(t *testing.T) {
		// Empty batches should return nil
		results, errors := provider.BatchGet(ctx, nil)
		if results != nil || errors != nil {
			t.Error("Expected nil returns for empty batch")
		}

		errors = provider.BatchSet(ctx, nil, nil)
		if errors != nil {
			t.Error("Expected nil return for empty batch")
		}

		errors = provider.BatchDelete(ctx, nil)
		if errors != nil {
			t.Error("Expected nil return for empty batch")
		}
	})

	t.Run("BatchWithWatchers", func(t *testing.T) {
		// Batch operations should trigger watchers
		watchedURIs := []cereal.URI{
			cereal.NewURI("memory://test/watch1"),
			cereal.NewURI("memory://test/watch2"),
		}

		var mu sync.Mutex
		callCount := 0
		wg := sync.WaitGroup{}
		wg.Add(2)

		var stopFuncs []func()
		for _, uri := range watchedURIs {
			stop, err := provider.Sync(ctx, uri, func(_ []byte) {
				mu.Lock()
				callCount++
				mu.Unlock()
				wg.Done()
			})
			if err != nil {
				t.Fatalf("Failed to sync: %v", err)
			}
			stopFuncs = append(stopFuncs, stop)
		}
		defer func() {
			for _, stop := range stopFuncs {
				stop()
			}
		}()

		// Batch set should trigger all watchers
		data := [][]byte{
			[]byte("data1"),
			[]byte("data2"),
		}

		errs := provider.BatchSet(ctx, watchedURIs, data)
		for i, err := range errs {
			if err != nil {
				t.Fatalf("BatchSet failed at index %d: %v", i, err)
			}
		}

		// Wait for all callbacks with timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			mu.Lock()
			if callCount != 2 {
				t.Errorf("Expected 2 callbacks, got %d", callCount)
			}
			mu.Unlock()
		case <-time.After(1 * time.Second):
			t.Error("Timeout waiting for callbacks")
		}
	})
}
