package memory_test

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/zoobzio/cereal"
	"github.com/zoobzio/cereal/providers/memory"
)

func TestMemorySync(t *testing.T) {
	ctx := context.Background()
	provider := memory.New()
	defer provider.Close()

	uri := cereal.NewURI("memory://test/data/item")

	// Test initial callback when resource exists
	t.Run("InitialCallback", func(t *testing.T) {
		initialData := []byte("initial")
		if err := provider.Set(ctx, uri, initialData); err != nil {
			t.Fatalf("Failed to set initial data: %v", err)
		}

		called := make(chan []byte, 1)
		stop, err := provider.Sync(ctx, uri, func(data []byte) {
			called <- data
		})
		if err != nil {
			t.Fatalf("Sync failed: %v", err)
		}
		defer stop()

		// Should get initial callback
		select {
		case data := <-called:
			if !bytes.Equal(data, initialData) {
				t.Errorf("Expected %q, got %q", initialData, data)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("Initial callback not received")
		}
	})

	// Test callbacks on Set
	t.Run("CallbackOnSet", func(t *testing.T) {
		called := make(chan []byte, 10)
		stop, err := provider.Sync(ctx, uri, func(data []byte) {
			called <- data
		})
		if err != nil {
			t.Fatalf("Sync failed: %v", err)
		}
		defer stop()

		// Drain initial callback
		select {
		case <-called:
			// Got initial callback
		case <-time.After(100 * time.Millisecond):
			// No initial callback, that's ok
		}

		// Update data
		newData := []byte("updated")
		if err := provider.Set(ctx, uri, newData); err != nil {
			t.Fatalf("Failed to set new data: %v", err)
		}

		// Should get callback
		select {
		case data := <-called:
			if !bytes.Equal(data, newData) {
				t.Errorf("Expected %q, got %q", newData, data)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("Callback not received after Set")
		}
	})

	// Test callbacks on Delete
	t.Run("CallbackOnDelete", func(t *testing.T) {
		if err := provider.Set(ctx, uri, []byte("data")); err != nil {
			t.Fatalf("Failed to set data: %v", err)
		}

		called := make(chan []byte, 10)
		stop, err := provider.Sync(ctx, uri, func(data []byte) {
			called <- data
		})
		if err != nil {
			t.Fatalf("Sync failed: %v", err)
		}
		defer stop()

		// Drain initial callback
		select {
		case <-called:
			// Got initial callback
		case <-time.After(100 * time.Millisecond):
			// No initial callback, that's ok
		}

		// Delete data
		if err := provider.Delete(ctx, uri); err != nil {
			t.Fatalf("Failed to delete data: %v", err)
		}

		// Should get callback with nil
		select {
		case data := <-called:
			if data != nil {
				t.Errorf("Expected nil for deleted resource, got %q", data)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("Callback not received after Delete")
		}
	})

	// Test multiple watchers
	t.Run("MultipleWatchers", func(t *testing.T) {
		var mu sync.Mutex
		count1, count2 := 0, 0
		wg := sync.WaitGroup{}
		wg.Add(2)

		stop1, err := provider.Sync(ctx, uri, func(_ []byte) {
			mu.Lock()
			count1++
			mu.Unlock()
			wg.Done()
		})
		if err != nil {
			t.Fatalf("Failed to sync 1: %v", err)
		}
		defer stop1()

		stop2, err := provider.Sync(ctx, uri, func(_ []byte) {
			mu.Lock()
			count2++
			mu.Unlock()
			wg.Done()
		})
		if err != nil {
			t.Fatalf("Failed to sync 2: %v", err)
		}
		defer stop2()

		// Both should get notified
		if err := provider.Set(ctx, uri, []byte("test")); err != nil {
			t.Fatalf("Failed to set test data: %v", err)
		}

		// Wait for callbacks with timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			mu.Lock()
			if count1 != 1 || count2 != 1 {
				t.Errorf("Expected both watchers to be called once, got %d and %d", count1, count2)
			}
			mu.Unlock()
		case <-time.After(1 * time.Second):
			t.Error("Timeout waiting for callbacks")
		}
	})

	// Test stop function
	t.Run("StopWatching", func(t *testing.T) {
		called := make(chan struct{}, 10)
		stop, err := provider.Sync(ctx, uri, func(_ []byte) {
			called <- struct{}{}
		})
		if err != nil {
			t.Fatalf("Failed to sync: %v", err)
		}

		// Wait for any initial callback
		select {
		case <-called:
			// Drain initial callback
		case <-time.After(50 * time.Millisecond):
			// No initial callback
		}

		// Stop watching
		stop()

		// Give stop time to take effect
		time.Sleep(10 * time.Millisecond)

		// Should not get callbacks after stop
		if err := provider.Set(ctx, uri, []byte("after stop")); err != nil {
			t.Fatalf("Failed to set after stop: %v", err)
		}

		select {
		case <-called:
			t.Error("Callback called after stop")
		case <-time.After(50 * time.Millisecond):
			// Good, no callback
		}
	})
}
