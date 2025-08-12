package cereal

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/zoobzio/sentinel"
)

// ServiceConfig holds configuration for creating a Service.
type ServiceConfig struct {
	Codec    Codec // Optional - defaults to JSONCodec if not specified
	Table    string
	KeyField string
}

// Service wraps a Provider with type-safe serialization for type T.
type Service[T any] struct {
	provider Provider
	codec    Codec
	table    string
	keyField string
	metadata sentinel.ModelMetadata
}

// NewService creates a type-safe service wrapper for the given type T.
// It uses sentinel metadata to automatically determine table name and primary key.
func NewService[T any](provider Provider) (*Service[T], error) {
	// Get metadata from sentinel
	metadata := sentinel.Inspect[T]()

	// Derive table name from type name (pluralized lowercase)
	table := strings.ToLower(metadata.TypeName) + "s"

	// Find primary key field
	keyField := ""
	for _, field := range metadata.Fields {
		// Check for constraints tag with "pk"
		if constraints, ok := field.Tags["constraints"]; ok {
			if strings.Contains(constraints, "pk") {
				keyField = field.Tags["db"]
				if keyField == "" {
					keyField = strings.ToLower(field.Name)
				}
				break
			}
		}
	}

	// Fallback to common field names
	if keyField == "" {
		for _, field := range metadata.Fields {
			fieldName := strings.ToLower(field.Name)
			if fieldName == "id" || fieldName == "uuid" || fieldName == "guid" {
				keyField = field.Tags["db"]
				if keyField == "" {
					keyField = fieldName
				}
				break
			}
		}
	}

	if keyField == "" {
		return nil, fmt.Errorf("no primary key field found for type %s", metadata.TypeName)
	}

	// Select codec from metadata or default to JSON
	codec := JSONCodec
	if len(metadata.Codecs) > 0 {
		switch metadata.Codecs[0] {
		case "msgpack":
			codec = MsgPackCodec
		case "toml":
			codec = TOMLCodec
		}
	}

	return &Service[T]{
		provider: provider,
		codec:    codec,
		table:    table,
		keyField: keyField,
		metadata: metadata,
	}, nil
}

// NewServiceWithConfig creates a type-safe service wrapper with explicit configuration.
// This allows overriding the automatic detection from sentinel metadata.
func NewServiceWithConfig[T any](provider Provider, config ServiceConfig) (*Service[T], error) {
	if config.Table == "" {
		return nil, fmt.Errorf("table name is required")
	}

	if config.KeyField == "" {
		return nil, fmt.Errorf("key field is required")
	}

	// Get metadata even for explicit config (might be useful later)
	metadata := sentinel.Inspect[T]()

	// Use provided codec or default to JSON
	codec := config.Codec
	if codec == nil {
		codec = JSONCodec
	}

	return &Service[T]{
		provider: provider,
		codec:    codec,
		table:    config.Table,
		keyField: config.KeyField,
		metadata: metadata,
	}, nil
}

// Get retrieves a single record by key value.
func (s *Service[T]) Get(ctx context.Context, keyValue string) (*T, error) {
	data, err := s.provider.Get(ctx, s.table, s.keyField, keyValue)
	if err != nil {
		return nil, err
	}

	var result T
	if err := s.codec.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal: %w", err)
	}

	return &result, nil
}

// Set stores a record, extracting the key value from the struct.
func (s *Service[T]) Set(ctx context.Context, record *T) error {
	// Extract key value using reflection
	keyValue, err := s.extractKey(record)
	if err != nil {
		return fmt.Errorf("failed to extract key: %w", err)
	}

	data, err := s.codec.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal: %w", err)
	}

	return s.provider.Set(ctx, s.table, s.keyField, keyValue, data)
}

// Delete removes a record by key value.
func (s *Service[T]) Delete(ctx context.Context, keyValue string) error {
	return s.provider.Delete(ctx, s.table, s.keyField, keyValue)
}

// Exists checks if a record exists by key value.
func (s *Service[T]) Exists(ctx context.Context, keyValue string) (bool, error) {
	return s.provider.Exists(ctx, s.table, s.keyField, keyValue)
}

// BatchGet retrieves multiple records by key values.
func (s *Service[T]) BatchGet(ctx context.Context, keyValues []string) ([]T, []error) {
	dataList, errs := s.provider.BatchGet(ctx, s.table, s.keyField, keyValues)

	results := make([]T, len(dataList))
	for i, data := range dataList {
		if errs[i] != nil {
			continue
		}

		if err := s.codec.Unmarshal(data, &results[i]); err != nil {
			errs[i] = fmt.Errorf("unmarshal error: %w", err)
		}
	}

	return results, errs
}

// BatchSet stores multiple records.
func (s *Service[T]) BatchSet(ctx context.Context, records []T) []error {
	keyValues := make([]string, len(records))
	dataList := make([][]byte, len(records))
	errs := make([]error, len(records))

	// Extract keys and marshal each record
	for i := range records {
		keyValue, err := s.extractKey(&records[i])
		if err != nil {
			errs[i] = fmt.Errorf("failed to extract key: %w", err)
			continue
		}
		keyValues[i] = keyValue

		data, err := s.codec.Marshal(&records[i])
		if err != nil {
			errs[i] = fmt.Errorf("failed to marshal: %w", err)
			continue
		}
		dataList[i] = data
	}

	// Call provider's batch set
	providerErrs := s.provider.BatchSet(ctx, s.table, s.keyField, keyValues, dataList)

	// Merge errors
	for i, err := range providerErrs {
		if errs[i] == nil {
			errs[i] = err
		}
	}

	return errs
}

// BatchDelete removes multiple records by key values.
func (s *Service[T]) BatchDelete(ctx context.Context, keyValues []string) []error {
	return s.provider.BatchDelete(ctx, s.table, s.keyField, keyValues)
}

// List retrieves multiple records using the "list" query.
func (s *Service[T]) List(ctx context.Context, parameters map[string]interface{}) ([]T, error) {
	data, err := s.provider.Execute(ctx, "list", parameters)
	if err != nil {
		return nil, err
	}

	// Try to unmarshal as array first
	var results []T
	if err := s.codec.Unmarshal(data, &results); err != nil {
		// If that fails, try as single result and wrap in array
		var single T
		if err := s.codec.Unmarshal(data, &single); err != nil {
			return nil, fmt.Errorf("failed to unmarshal results: %w", err)
		}
		results = []T{single}
	}

	return results, nil
}

// GetProvider returns the underlying provider for direct access if needed.
func (s *Service[T]) GetProvider() Provider {
	return s.provider
}

// GetTable returns the table name for this service.
func (s *Service[T]) GetTable() string {
	return s.table
}

// GetKeyField returns the primary key field name.
func (s *Service[T]) GetKeyField() string {
	return s.keyField
}

// GetMetadata returns the sentinel metadata for the type.
func (s *Service[T]) GetMetadata() sentinel.ModelMetadata {
	return s.metadata
}

// extractKey extracts the primary key value from a struct instance.
func (s *Service[T]) extractKey(record *T) (string, error) {
	v := reflect.ValueOf(record)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	t := v.Type()

	// Look for field with matching db tag
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if dbTag := field.Tag.Get("db"); dbTag == s.keyField {
			fieldValue := v.Field(i)
			return fmt.Sprintf("%v", fieldValue.Interface()), nil
		}
	}

	// Fallback to field name
	if field := v.FieldByName(s.keyField); field.IsValid() {
		return fmt.Sprintf("%v", field.Interface()), nil
	}

	return "", fmt.Errorf("key field %s not found in %T", s.keyField, record)
}
