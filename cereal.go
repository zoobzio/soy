package cereal

import (
	"context"
	"fmt"

	"sentinel"
)

// Get retrieves and decodes a value from the specified URI.
func Get[T any](ctx context.Context, uri string) (T, error) {
	var zero T

	// Parse URI
	parsed, err := ParseURI(uri)
	if err != nil {
		return zero, fmt.Errorf("invalid URI: %w", err)
	}

	// Get provider
	provider, ok := defaultService.GetProvider(parsed.Provider())
	if !ok {
		return zero, fmt.Errorf("provider %s not found", parsed.Provider())
	}

	// Get raw data
	data, err := provider.Get(ctx, parsed)
	if err != nil {
		return zero, err
	}

	// Decode data
	return decode[T](data)
}

// Set encodes and stores a value at the specified URI.
func Set[T any](ctx context.Context, uri string, value T) error {
	// Parse URI
	parsed, err := ParseURI(uri)
	if err != nil {
		return fmt.Errorf("invalid URI: %w", err)
	}

	// Get provider
	provider, ok := defaultService.GetProvider(parsed.Provider())
	if !ok {
		return fmt.Errorf("provider %s not found", parsed.Provider())
	}

	// Encode value
	data, err := encode(value)
	if err != nil {
		return fmt.Errorf("encode failed: %w", err)
	}

	// Store data
	return provider.Set(ctx, parsed, data)
}

// Delete removes a value at the specified URI.
func Delete(ctx context.Context, uri string) error {
	// Parse URI
	parsed, err := ParseURI(uri)
	if err != nil {
		return fmt.Errorf("invalid URI: %w", err)
	}

	// Get provider
	provider, ok := defaultService.GetProvider(parsed.Provider())
	if !ok {
		return fmt.Errorf("provider %s not found", parsed.Provider())
	}

	return provider.Delete(ctx, parsed)
}

// Exec executes an action with a payload and returns the result.
func Exec[Result any, Payload any](ctx context.Context, uri string, payload Payload) (Result, error) {
	var zero Result

	// Parse URI
	parsed, err := ParseURI(uri)
	if err != nil {
		return zero, fmt.Errorf("invalid URI: %w", err)
	}

	// Get provider
	provider, ok := defaultService.GetProvider(parsed.Provider())
	if !ok {
		return zero, fmt.Errorf("provider %s not found", parsed.Provider())
	}

	// Encode payload
	payloadData, err := encode(payload)
	if err != nil {
		return zero, fmt.Errorf("encode payload failed: %w", err)
	}

	// Execute
	resultData, err := provider.Execute(ctx, parsed, payloadData)
	if err != nil {
		return zero, err
	}

	// Decode result
	return decode[Result](resultData)
}

// encode serializes a value to bytes using codec selection.
func encode[T any](value T) ([]byte, error) {
	// Get codecs from Sentinel metadata
	codecs := getCodecs[T]()

	if len(codecs) == 0 {
		return nil, fmt.Errorf("no codecs configured for type %T", value)
	}

	// Try each codec in order
	var lastErr error
	for _, codecName := range codecs {
		codec, ok := GetCodec(codecName)
		if !ok {
			lastErr = fmt.Errorf("codec %s not found in registry", codecName)
			continue
		}

		data, err := codec.Encode(value)
		if err == nil {
			return data, nil
		}
		lastErr = err
	}

	if lastErr != nil {
		return nil, fmt.Errorf("all codecs failed, last error: %w", lastErr)
	}
	return nil, fmt.Errorf("no codecs available for type %T", value)
}

// decode deserializes bytes to a value using codec selection.
func decode[T any](data []byte) (T, error) {
	var result T

	// Get codecs from Sentinel metadata
	codecs := getCodecs[T]()

	var lastErr error
	for _, codecName := range codecs {
		codec, ok := GetCodec(codecName)
		if !ok {
			continue
		}

		err := codec.Decode(data, &result)
		if err == nil {
			return result, nil
		}
		lastErr = err
	}

	if lastErr != nil {
		return result, fmt.Errorf("all codecs failed, last error: %w", lastErr)
	}
	return result, fmt.Errorf("no codecs available")
}

// getCodecs retrieves the codec order from Sentinel metadata.
func getCodecs[T any]() []string {
	// Get metadata from Sentinel
	metadata := sentinel.Inspect[T]()

	// Use codecs from metadata or fall back to defaults
	if len(metadata.Codecs) > 0 {
		return metadata.Codecs
	}

	return DefaultCodecs()
}
