package file

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
	"github.com/zoobzio/cereal"
)

// Provider implements cereal.Provider using filesystem storage.
type Provider struct {
	baseDir string
}

// New creates a new file provider with the given base directory.
func New(baseDir string) (*Provider, error) {
	// Ensure base directory exists
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, err
	}

	// Get absolute path
	absPath, err := filepath.Abs(baseDir)
	if err != nil {
		return nil, err
	}

	return &Provider{
		baseDir: absPath,
	}, nil
}

// uriToPath converts a URI to a filesystem path.
func (p *Provider) uriToPath(uri cereal.URI) string {
	// Join namespace and path segments
	segments := append([]string{p.baseDir, uri.Namespace()}, uri.Path()...)
	return filepath.Join(segments...)
}

// Get reads a file from disk.
func (p *Provider) Get(_ context.Context, uri cereal.URI) ([]byte, error) {
	path := p.uriToPath(uri)

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, cereal.ErrNotFound
		}
		return nil, err
	}

	return data, nil
}

// Set writes data to disk.
func (p *Provider) Set(_ context.Context, uri cereal.URI, data []byte) error {
	path := p.uriToPath(uri)

	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	return os.WriteFile(path, data, 0o600)
}

// Delete removes a file from disk.
func (p *Provider) Delete(_ context.Context, uri cereal.URI) error {
	path := p.uriToPath(uri)

	err := os.Remove(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cereal.ErrNotFound
		}
		return err
	}

	return nil
}

// Exists checks if a file exists.
func (p *Provider) Exists(_ context.Context, uri cereal.URI) (bool, error) {
	path := p.uriToPath(uri)

	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	return true, nil
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
	case "glob":
		return p.executeGlob(ctx, uri, payload)
	default:
		return nil, cereal.ErrNotSupported
	}
}

// executeList returns all files in a directory.
func (p *Provider) executeList(_ context.Context, uri cereal.URI, _ []byte) ([]byte, error) {
	// Build directory path from URI (namespace + path minus action)
	path := uri.Path()
	pathParts := []string{p.baseDir, uri.Namespace()}
	if len(path) > 1 {
		pathParts = append(pathParts, path[:len(path)-1]...)
	}
	dirPath := filepath.Join(pathParts...)

	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	results := make([][]byte, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filePath := filepath.Join(dirPath, entry.Name())
		data, err := os.ReadFile(filePath)
		if err != nil {
			continue
		}

		results = append(results, data)
	}

	return json.Marshal(results)
}

// executeGlob performs glob pattern matching.
func (p *Provider) executeGlob(_ context.Context, uri cereal.URI, payload []byte) ([]byte, error) {
	// Parse payload for glob pattern
	var globReq struct {
		Pattern string `json:"pattern"`
	}
	if err := json.Unmarshal(payload, &globReq); err != nil {
		return nil, fmt.Errorf("invalid glob request: %w", err)
	}

	// Build base path from URI
	path := uri.Path()
	pathParts := []string{p.baseDir, uri.Namespace()}
	if len(path) > 1 {
		pathParts = append(pathParts, path[:len(path)-1]...)
	}
	basePath := filepath.Join(pathParts...)

	// Apply pattern
	globPattern := filepath.Join(basePath, globReq.Pattern)
	matches, err := filepath.Glob(globPattern)
	if err != nil {
		return nil, err
	}

	results := make([][]byte, 0, len(matches))
	for _, match := range matches {
		// Skip directories
		info, err := os.Stat(match)
		if err != nil || info.IsDir() {
			continue
		}

		data, err := os.ReadFile(match)
		if err != nil {
			continue
		}

		results = append(results, data)
	}

	return json.Marshal(results)
}

// Sync watches a file for changes.
func (p *Provider) Sync(ctx context.Context, uri cereal.URI, callback func([]byte)) (func(), error) {
	path := p.uriToPath(uri)

	// Check if file exists
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil, cereal.ErrNotFound
		}
		return nil, err
	}

	// Create watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	// Add file to watcher
	err = watcher.Add(path)
	if err != nil {
		watcher.Close()
		return nil, err
	}

	// Read initial content and notify
	if data, err := os.ReadFile(path); err == nil {
		go callback(data)
	}

	// Start watching in goroutine
	stopChan := make(chan struct{})
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				// Notify on write or create events
				if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
					if data, err := os.ReadFile(path); err == nil {
						callback(data)
					}
				} else if event.Op&fsnotify.Remove == fsnotify.Remove || event.Op&fsnotify.Rename == fsnotify.Rename {
					// File was deleted or renamed
					callback(nil)
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				// Log error but continue watching
				_ = err
			case <-stopChan:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	// Return stop function
	stop := func() {
		close(stopChan)
		watcher.Close()
	}

	return stop, nil
}

// BatchGet reads multiple files from disk.
func (p *Provider) BatchGet(ctx context.Context, uris []cereal.URI) ([][]byte, []error) {
	if len(uris) == 0 {
		return nil, nil
	}

	results := make([][]byte, len(uris))
	errors := make([]error, len(uris))

	for i, uri := range uris {
		data, err := p.Get(ctx, uri)
		results[i] = data
		errors[i] = err
	}

	return results, errors
}

// BatchSet writes multiple files to disk.
func (p *Provider) BatchSet(ctx context.Context, uris []cereal.URI, data [][]byte) []error {
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

	errors := make([]error, len(uris))

	for i, uri := range uris {
		errors[i] = p.Set(ctx, uri, data[i])
	}

	return errors
}

// BatchDelete removes multiple files from disk.
func (p *Provider) BatchDelete(ctx context.Context, uris []cereal.URI) []error {
	if len(uris) == 0 {
		return nil
	}

	errors := make([]error, len(uris))

	for i, uri := range uris {
		errors[i] = p.Delete(ctx, uri)
	}

	return errors
}

// Close is a no-op for file provider.
func (*Provider) Close() error {
	return nil
}

// Ensure Provider implements cereal.Provider.
var _ cereal.Provider = (*Provider)(nil)
