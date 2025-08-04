package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/zoobzio/cereal"
)

// Provider implements cereal.Provider using in-memory storage.
type Provider struct {
	store    map[string][]byte
	watchers map[string]map[string]func([]byte)
	nextID   int64
	mu       sync.RWMutex
}

// New creates a new memory provider.
func New() *Provider {
	return &Provider{
		store:    make(map[string][]byte),
		watchers: make(map[string]map[string]func([]byte)),
	}
}

// Get retrieves data from memory.
func (p *Provider) Get(_ context.Context, uri cereal.URI) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	key := uri.String()
	data, exists := p.store[key]
	if !exists {
		return nil, cereal.ErrNotFound
	}

	// Return a copy to prevent external modification
	result := make([]byte, len(data))
	copy(result, data)
	return result, nil
}

// Set stores data in memory.
func (p *Provider) Set(_ context.Context, uri cereal.URI, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Store a copy to prevent external modification
	stored := make([]byte, len(data))
	copy(stored, data)
	p.store[uri.String()] = stored

	// Notify watchers
	p.notifyWatchers(uri.String(), stored)

	return nil
}

// Delete removes data from memory.
func (p *Provider) Delete(_ context.Context, uri cereal.URI) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := uri.String()
	if _, exists := p.store[key]; !exists {
		return cereal.ErrNotFound
	}

	delete(p.store, key)

	// Notify watchers with nil data (deleted)
	p.notifyWatchers(key, nil)

	return nil
}

// Exists checks if a resource exists in memory.
func (p *Provider) Exists(_ context.Context, uri cereal.URI) (bool, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	_, exists := p.store[uri.String()]
	return exists, nil
}

// Execute performs an action based on the URI path.
func (p *Provider) Execute(ctx context.Context, uri cereal.URI, payload []byte) ([]byte, error) {
	// Route based on the last path segment
	path := uri.Path()
	if len(path) == 0 {
		return nil, cereal.ErrNotSupported
	}

	action := path[len(path)-1]

	switch action {
	case "list":
		return p.executeList(ctx, uri, payload)
	case "search":
		return p.executeSearch(ctx, uri, payload)
	default:
		return nil, cereal.ErrNotSupported
	}
}

// executeList returns all values with keys matching a prefix.
func (p *Provider) executeList(_ context.Context, uri cereal.URI, _ []byte) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Build prefix from URI (namespace + path minus action)
	path := uri.Path()
	prefixParts := []string{uri.Namespace()}
	if len(path) > 1 {
		prefixParts = append(prefixParts, path[:len(path)-1]...)
	}
	prefix := strings.Join(prefixParts, "/")

	// Collect matching values
	var values []json.RawMessage
	for key, data := range p.store {
		// Remove provider prefix for comparison
		keyWithoutProvider := strings.TrimPrefix(key, uri.Provider()+"://")
		if strings.HasPrefix(keyWithoutProvider, prefix) {
			// Add raw JSON data (assuming it's already JSON encoded)
			values = append(values, json.RawMessage(data))
		}
	}

	// Return as JSON array
	return json.Marshal(values)
}

// executeSearch performs a more complex search (placeholder for now).
func (*Provider) executeSearch(_ context.Context, _ cereal.URI, _ []byte) ([]byte, error) {
	// For now, just return empty results
	// Future: parse payload for search criteria
	return json.Marshal([][]byte{})
}

// Sync watches a resource for changes.
func (p *Provider) Sync(_ context.Context, uri cereal.URI, callback func([]byte)) (func(), error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := uri.String()
	watcherID := fmt.Sprintf("watcher-%d", atomic.AddInt64(&p.nextID, 1))

	// Initialize watcher map for this URI if needed
	if p.watchers[key] == nil {
		p.watchers[key] = make(map[string]func([]byte))
	}

	// Register the callback
	p.watchers[key][watcherID] = callback

	// If resource exists, immediately call callback with current value
	if data, exists := p.store[key]; exists {
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)
		go callback(dataCopy)
	}

	// Return stop function
	stop := func() {
		p.mu.Lock()
		defer p.mu.Unlock()

		if watchers, exists := p.watchers[key]; exists {
			delete(watchers, watcherID)
			// Clean up empty watcher maps
			if len(watchers) == 0 {
				delete(p.watchers, key)
			}
		}
	}

	return stop, nil
}

// Must be called with lock held.
func (p *Provider) notifyWatchers(key string, data []byte) {
	if watchers, exists := p.watchers[key]; exists {
		// Make a copy of data for each watcher
		var dataCopy []byte
		if data != nil {
			dataCopy = make([]byte, len(data))
			copy(dataCopy, data)
		}

		// Call callbacks in separate goroutines to avoid blocking
		for _, callback := range watchers {
			cb := callback
			go cb(dataCopy)
		}
	}
}

// BatchGet retrieves multiple values from memory.
func (p *Provider) BatchGet(_ context.Context, uris []cereal.URI) ([][]byte, []error) {
	if len(uris) == 0 {
		return nil, nil
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	results := make([][]byte, len(uris))
	errors := make([]error, len(uris))

	for i, uri := range uris {
		key := uri.String()
		if data, exists := p.store[key]; exists {
			// Return a copy
			result := make([]byte, len(data))
			copy(result, data)
			results[i] = result
		} else {
			errors[i] = cereal.ErrNotFound
		}
	}

	return results, errors
}

// BatchSet stores multiple values in memory.
func (p *Provider) BatchSet(_ context.Context, uris []cereal.URI, data [][]byte) []error {
	if len(uris) == 0 {
		return nil
	}

	if len(uris) != len(data) {
		// Return error for all
		errors := make([]error, len(uris))
		for i := range errors {
			errors[i] = fmt.Errorf("batch size mismatch: %d uris, %d data", len(uris), len(data))
		}
		return errors
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	errors := make([]error, len(uris))

	for i, uri := range uris {
		key := uri.String()
		// Store a copy
		stored := make([]byte, len(data[i]))
		copy(stored, data[i])
		p.store[key] = stored

		// Notify watchers
		p.notifyWatchers(key, stored)
	}

	return errors // All nil since memory set can't fail
}

// BatchDelete removes multiple values from memory.
func (p *Provider) BatchDelete(_ context.Context, uris []cereal.URI) []error {
	if len(uris) == 0 {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	errors := make([]error, len(uris))

	for i, uri := range uris {
		key := uri.String()
		if _, exists := p.store[key]; exists {
			delete(p.store, key)
			// Notify watchers
			p.notifyWatchers(key, nil)
		} else {
			errors[i] = cereal.ErrNotFound
		}
	}

	return errors
}

// Close clears the memory store.
func (p *Provider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.store = make(map[string][]byte)
	p.watchers = make(map[string]map[string]func([]byte))
	return nil
}

// Ensure Provider implements cereal.Provider.
var _ cereal.Provider = (*Provider)(nil)
