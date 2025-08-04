package cereal

import (
	"fmt"
	"strings"
)

// URI represents a resource identifier with provider routing
// Format: provider://namespace/path/to/resource
// Examples:
//   - memory://users/profiles/123
//   - redis://sessions/active/user-456
//   - file://config/app.json
type URI struct {
	raw       string
	provider  string   // Storage provider (memory, redis, file)
	namespace string   // Logical namespace (users, orders, sessions)
	path      []string // Path segments
}

// Panics if the URI format is invalid.
func NewURI(uri string) URI {
	parsed, err := ParseURI(uri)
	if err != nil {
		panic(fmt.Sprintf("invalid URI: %v", err))
	}
	return parsed
}

// ParseURI creates a URI with error handling.
func ParseURI(uri string) (URI, error) {
	// Validate basic format
	if !strings.Contains(uri, "://") {
		return URI{}, fmt.Errorf("missing '://' separator in URI: %s", uri)
	}

	// Split provider and rest
	parts := strings.SplitN(uri, "://", 2)
	if len(parts) != 2 {
		return URI{}, fmt.Errorf("invalid URI format: %s", uri)
	}

	provider := parts[0]
	remainder := parts[1]

	// Validate provider
	if provider == "" {
		return URI{}, fmt.Errorf("empty provider in URI: %s", uri)
	}

	// Split namespace and path
	pathParts := strings.SplitN(remainder, "/", 2)
	if len(pathParts) == 0 || pathParts[0] == "" {
		return URI{}, fmt.Errorf("missing namespace in URI: %s", uri)
	}

	namespace := pathParts[0]
	var path []string

	if len(pathParts) > 1 && pathParts[1] != "" {
		path = strings.Split(pathParts[1], "/")
	}

	return URI{
		raw:       uri,
		provider:  provider,
		namespace: namespace,
		path:      path,
	}, nil
}

// String returns the URI as a string.
func (u URI) String() string {
	return u.raw
}

// Namespace returns the namespace part of the URI.
func (u URI) Namespace() string {
	return u.namespace
}

// Path returns the path segments.
func (u URI) Path() []string {
	return u.path
}

// PathString returns the path as a string.
func (u URI) PathString() string {
	return strings.Join(u.path, "/")
}

// Provider returns the provider part of the URI.
func (u URI) Provider() string {
	return u.provider
}
