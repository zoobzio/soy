package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
	
	"github.com/zoobzio/cereal"
	"github.com/zoobzio/cereal/providers/file"
)

func main() {
	ctx := context.Background()
	tmpDir := "/tmp/cereal-demo"
	os.RemoveAll(tmpDir)
	
	provider, err := file.New(tmpDir)
	if err != nil {
		panic(err)
	}
	
	uri := cereal.NewURI("config://app.json")
	
	// Create initial file
	fmt.Println("Creating initial file...")
	provider.Set(ctx, uri, []byte(`{"version": 1}`))
	
	// Set up sync
	fmt.Println("Setting up file watch...")
	stop, err := provider.Sync(ctx, uri, func(data []byte) {
		if data == nil {
			fmt.Println("-> File deleted")
		} else {
			fmt.Printf("-> File changed: %s\n", data)
		}
	})
	if err != nil {
		panic(err)
	}
	defer stop()
	
	// Give watcher time to start
	time.Sleep(100 * time.Millisecond)
	
	// Modify file directly
	fmt.Println("\nModifying file directly...")
	filePath := filepath.Join(tmpDir, "config", "app.json")
	os.WriteFile(filePath, []byte(`{"version": 2}`), 0644)
	
	time.Sleep(100 * time.Millisecond)
	
	// Delete via provider
	fmt.Println("\nDeleting file...")
	provider.Delete(ctx, uri)
	
	time.Sleep(100 * time.Millisecond)
	
	fmt.Println("\nDone!")
}