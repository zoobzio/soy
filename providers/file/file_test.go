package file_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zoobzio/cereal"
	"github.com/zoobzio/cereal/providers/file"
)

func TestFileProvider(t *testing.T) {
	tmpDir := t.TempDir()
	provider, err := file.New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create provider: %v", err)
	}

	cereal.TestProvider(t, "file", provider)
}

func TestFileSync(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	provider, err := file.New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create file provider: %v", err)
	}
	defer provider.Close()

	uri := cereal.NewURI("file://test/data/watch.txt")

	// Create initial file
	initialData := []byte("initial content")
	if err := provider.Set(ctx, uri, initialData); err != nil {
		t.Fatalf("Failed to set initial data: %v", err)
	}

	// Test sync with file changes
	t.Run("FileModification", func(t *testing.T) {
		called := make(chan []byte, 10)
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

		// Modify file directly
		filePath := filepath.Join(tmpDir, "test", "data", "watch.txt")
		newData := []byte("modified content")
		err = os.WriteFile(filePath, newData, 0o600)
		if err != nil {
			t.Fatalf("Failed to write file: %v", err)
		}

		// Should get callback
		select {
		case data := <-called:
			if !bytes.Equal(data, newData) {
				t.Errorf("Expected %q, got %q", newData, data)
			}
		case <-time.After(200 * time.Millisecond):
			t.Error("Callback not received after file modification")
		}
	})

	// Test file deletion
	t.Run("FileDeletion", func(t *testing.T) {
		// Create a new file to watch
		deleteURI := cereal.NewURI("file://test/data/delete.txt")
		if err := provider.Set(ctx, deleteURI, []byte("will be deleted")); err != nil {
			t.Fatalf("Failed to set data for deletion: %v", err)
		}

		called := make(chan []byte, 10)
		stop, err := provider.Sync(ctx, deleteURI, func(data []byte) {
			called <- data
		})
		if err != nil {
			t.Fatalf("Sync failed: %v", err)
		}
		defer stop()

		// Drain initial callback
		<-called

		// Delete file
		if err := provider.Delete(ctx, deleteURI); err != nil {
			t.Fatalf("Failed to delete: %v", err)
		}

		// Should get callback with nil
		select {
		case data := <-called:
			if data != nil {
				t.Errorf("Expected nil for deleted file, got %q", data)
			}
		case <-time.After(200 * time.Millisecond):
			t.Error("Callback not received after file deletion")
		}
	})
}
