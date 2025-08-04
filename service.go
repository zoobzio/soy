package cereal

import (
	"sync"
)

// Service manages providers and configuration.
type Service struct {
	providers map[string]Provider
	mu        sync.RWMutex
}

// Global default service.
var defaultService = NewService()

// NewService creates a new service instance.
func NewService() *Service {
	return &Service{
		providers: make(map[string]Provider),
	}
}

// Mount adds a provider to the service with a given name.
func (s *Service) Mount(name string, provider Provider) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.providers[name] = provider
}

// Unmount removes a provider from the service.
func (s *Service) Unmount(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.providers, name)
}

// GetProvider retrieves a provider by name.
func (s *Service) GetProvider(name string) (Provider, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	provider, ok := s.providers[name]
	return provider, ok
}

// Package-level functions that delegate to the default service

// Mount adds a provider to the global service.
func Mount(name string, provider Provider) {
	defaultService.Mount(name, provider)
}

// Unmount removes a provider from the global service.
func Unmount(name string) {
	defaultService.Unmount(name)
}

// GetProvider retrieves a provider from the global service.
func GetProvider(name string) (Provider, bool) {
	return defaultService.GetProvider(name)
}
