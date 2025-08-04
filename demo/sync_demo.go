package main

import (
	"context"
	"fmt"
	"time"
	
	"github.com/zoobzio/cereal"
	"github.com/zoobzio/cereal/providers/memory"
)

func main() {
	ctx := context.Background()
	mem := memory.New()
	uri := cereal.NewURI("test://data")
	
	// Set up sync
	fmt.Println("Setting up sync...")
	stop, err := mem.Sync(ctx, uri, func(data []byte) {
		if data == nil {
			fmt.Println("-> Data deleted")
		} else {
			fmt.Printf("-> Data changed: %s\n", data)
		}
	})
	if err != nil {
		panic(err)
	}
	defer stop()
	
	// Test operations
	fmt.Println("\nTesting Set operations:")
	mem.Set(ctx, uri, []byte("first"))
	time.Sleep(50 * time.Millisecond)
	
	mem.Set(ctx, uri, []byte("second"))
	time.Sleep(50 * time.Millisecond)
	
	fmt.Println("\nTesting Delete:")
	mem.Delete(ctx, uri)
	time.Sleep(50 * time.Millisecond)
	
	fmt.Println("\nDone!")
}