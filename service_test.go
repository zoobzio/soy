package cereal

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/zoobzio/sentinel"
)

// Test types.
type TestUser struct {
	ID     string `json:"id" db:"id" constraints:"pk"`
	Name   string `json:"name" db:"name"`
	Email  string `json:"email" db:"email" constraints:"unique"`
	Active bool   `json:"active" db:"active"`
}

type TestProduct struct {
	SKU   string  `json:"sku" db:"sku" constraints:"pk,unique"`
	Name  string  `json:"name" db:"name"`
	Price float64 `json:"price" db:"price"`
}

type TestOrder struct {
	OrderID string  `json:"order_id" db:"order_id" constraints:"pk"`
	Status  string  `json:"status" db:"status"`
	Total   float64 `json:"total" db:"total"`
}

// mockProvider implements Provider for testing.
type mockProvider struct {
	data          map[string]map[string][]byte // table -> key -> data
	lastOperation string
	lastTable     string
	lastKeyField  string
	lastKeyValue  string
	returnError   error
}

func newMockProvider() *mockProvider {
	return &mockProvider{
		data: make(map[string]map[string][]byte),
	}
}

func (m *mockProvider) Get(_ context.Context, table, keyField, keyValue string) ([]byte, error) {
	m.lastOperation = "Get"
	m.lastTable = table
	m.lastKeyField = keyField
	m.lastKeyValue = keyValue

	if m.returnError != nil {
		return nil, m.returnError
	}

	if tableData, ok := m.data[table]; ok {
		if data, ok := tableData[keyValue]; ok {
			return data, nil
		}
	}
	return nil, ErrNotFound
}

func (m *mockProvider) Set(_ context.Context, table, keyField, keyValue string, data []byte) error {
	m.lastOperation = "Set"
	m.lastTable = table
	m.lastKeyField = keyField
	m.lastKeyValue = keyValue

	if m.returnError != nil {
		return m.returnError
	}

	if m.data[table] == nil {
		m.data[table] = make(map[string][]byte)
	}
	m.data[table][keyValue] = data
	return nil
}

func (m *mockProvider) Delete(_ context.Context, table, keyField, keyValue string) error {
	m.lastOperation = "Delete"
	m.lastTable = table
	m.lastKeyField = keyField
	m.lastKeyValue = keyValue

	if m.returnError != nil {
		return m.returnError
	}

	if tableData, ok := m.data[table]; ok {
		delete(tableData, keyValue)
	}
	return nil
}

func (m *mockProvider) Exists(_ context.Context, table, keyField, keyValue string) (bool, error) {
	m.lastOperation = "Exists"
	m.lastTable = table
	m.lastKeyField = keyField
	m.lastKeyValue = keyValue

	if m.returnError != nil {
		return false, m.returnError
	}

	if tableData, ok := m.data[table]; ok {
		_, exists := tableData[keyValue]
		return exists, nil
	}
	return false, nil
}

func (m *mockProvider) BatchGet(ctx context.Context, table, keyField string, keyValues []string) ([][]byte, []error) {
	results := make([][]byte, len(keyValues))
	errs := make([]error, len(keyValues))

	for i, key := range keyValues {
		data, err := m.Get(ctx, table, keyField, key)
		results[i] = data
		errs[i] = err
	}

	return results, errs
}

func (m *mockProvider) BatchSet(ctx context.Context, table, keyField string, keyValues []string, data [][]byte) []error {
	if len(keyValues) != len(data) {
		errs := make([]error, len(keyValues))
		for i := range errs {
			errs[i] = ErrBatchSizeMismatch
		}
		return errs
	}

	errs := make([]error, len(keyValues))
	for i := range keyValues {
		errs[i] = m.Set(ctx, table, keyField, keyValues[i], data[i])
	}

	return errs
}

func (m *mockProvider) BatchDelete(ctx context.Context, table, keyField string, keyValues []string) []error {
	errs := make([]error, len(keyValues))
	for i, key := range keyValues {
		errs[i] = m.Delete(ctx, table, keyField, key)
	}
	return errs
}

func (*mockProvider) Execute(_ context.Context, name string, _ map[string]interface{}) ([]byte, error) {
	// For testing, return mock data based on query name
	if name == "list" {
		users := []TestUser{
			{ID: "1", Name: "User1", Email: "user1@example.com"},
			{ID: "2", Name: "User2", Email: "user2@example.com"},
		}
		return json.Marshal(users)
	}
	return nil, errors.New("unknown query")
}

func (*mockProvider) Close() error {
	return nil
}

func init() {
	// Register tags needed for DDL
	sentinel.Tag("constraints")
	sentinel.Tag("type")

	// Create admin and seal for testing
	adm, err := sentinel.NewAdmin()
	if err != nil {
		panic(err)
	}
	adm.Seal()
}

func TestNewService(t *testing.T) {
	provider := newMockProvider()

	t.Run("automatic metadata extraction", func(t *testing.T) {
		service, err := NewService[TestUser](provider)
		if err != nil {
			t.Fatalf("NewService failed: %v", err)
		}

		if service.GetTable() != "testusers" {
			t.Errorf("Expected table=testusers, got %s", service.GetTable())
		}

		if service.GetKeyField() != "id" {
			t.Errorf("Expected keyField=id, got %s", service.GetKeyField())
		}

		// Should use JSON codec by default
		if service.codec != JSONCodec {
			t.Error("Expected JSON codec by default")
		}
	})

	t.Run("pk from constraints tag", func(t *testing.T) {
		service, err := NewService[TestProduct](provider)
		if err != nil {
			t.Fatalf("NewService failed: %v", err)
		}

		if service.GetTable() != "testproducts" {
			t.Errorf("Expected table=testproducts, got %s", service.GetTable())
		}

		if service.GetKeyField() != "sku" {
			t.Errorf("Expected keyField=sku, got %s", service.GetKeyField())
		}
	})

	t.Run("explicit config override", func(t *testing.T) {
		service, err := NewServiceWithConfig[TestUser](provider, ServiceConfig{
			Table:    "custom_users",
			KeyField: "uuid",
			Codec:    MsgPackCodec,
		})
		if err != nil {
			t.Fatalf("NewServiceWithConfig failed: %v", err)
		}

		if service.GetTable() != "custom_users" {
			t.Errorf("Expected table=custom_users, got %s", service.GetTable())
		}

		if service.GetKeyField() != "uuid" {
			t.Errorf("Expected keyField=uuid, got %s", service.GetKeyField())
		}

		if service.codec != MsgPackCodec {
			t.Error("Expected MsgPack codec")
		}
	})

	t.Run("metadata access", func(t *testing.T) {
		service, err := NewService[TestUser](provider)
		if err != nil {
			t.Fatalf("NewService failed: %v", err)
		}

		metadata := service.GetMetadata()
		if metadata.TypeName != "TestUser" {
			t.Errorf("Expected TypeName=TestUser, got %s", metadata.TypeName)
		}

		// Check that fields were extracted
		if len(metadata.Fields) == 0 {
			t.Error("Expected metadata to have fields")
		}

		// Check that constraints tag was extracted
		hasConstraintsTag := false
		for _, field := range metadata.Fields {
			if _, ok := field.Tags["constraints"]; ok {
				hasConstraintsTag = true
				break
			}
		}
		if !hasConstraintsTag {
			t.Error("Expected at least one field with constraints tag")
		}
	})

	t.Run("missing table name in config", func(t *testing.T) {
		_, err := NewServiceWithConfig[TestOrder](provider, ServiceConfig{
			KeyField: "order_id",
		})
		if err == nil {
			t.Error("Expected error for missing table name")
		}
	})

	t.Run("missing key field in config", func(t *testing.T) {
		_, err := NewServiceWithConfig[TestOrder](provider, ServiceConfig{
			Table: "orders",
		})
		if err == nil {
			t.Error("Expected error for missing key field")
		}
	})
}

func TestServiceCRUD(t *testing.T) {
	provider := newMockProvider()
	service, err := NewService[TestUser](provider)
	if err != nil {
		t.Fatalf("Failed to create service: %v", err)
	}

	ctx := context.Background()

	t.Run("Set and Get", func(t *testing.T) {
		user := &TestUser{
			ID:     "123",
			Name:   "John Doe",
			Email:  "john@example.com",
			Active: true,
		}

		// Set
		err := service.Set(ctx, user)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		// Verify provider was called correctly
		if provider.lastTable != "testusers" {
			t.Errorf("Expected table=testusers, got %s", provider.lastTable)
		}
		if provider.lastKeyField != "id" {
			t.Errorf("Expected keyField=id, got %s", provider.lastKeyField)
		}
		if provider.lastKeyValue != "123" {
			t.Errorf("Expected keyValue=123, got %s", provider.lastKeyValue)
		}

		// Get
		retrieved, err := service.Get(ctx, "123")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if retrieved.ID != user.ID {
			t.Errorf("Expected ID=%s, got %s", user.ID, retrieved.ID)
		}
		if retrieved.Name != user.Name {
			t.Errorf("Expected Name=%s, got %s", user.Name, retrieved.Name)
		}
	})

	t.Run("Exists", func(t *testing.T) {
		exists, err := service.Exists(ctx, "123")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if !exists {
			t.Error("Expected record to exist")
		}

		exists, err = service.Exists(ctx, "999")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if exists {
			t.Error("Expected record to not exist")
		}
	})

	t.Run("Delete", func(t *testing.T) {
		err := service.Delete(ctx, "123")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// Verify it's gone
		_, err = service.Get(ctx, "123")
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Expected ErrNotFound, got %v", err)
		}
	})
}

func TestServiceBatch(t *testing.T) {
	provider := newMockProvider()
	service, err := NewService[TestUser](provider)
	if err != nil {
		t.Fatalf("Failed to create service: %v", err)
	}

	ctx := context.Background()

	t.Run("BatchSet and BatchGet", func(t *testing.T) {
		users := []TestUser{
			{ID: "1", Name: "User1", Email: "user1@example.com"},
			{ID: "2", Name: "User2", Email: "user2@example.com"},
			{ID: "3", Name: "User3", Email: "user3@example.com"},
		}

		// Batch set
		errs := service.BatchSet(ctx, users)
		for i, err := range errs {
			if err != nil {
				t.Errorf("BatchSet[%d] failed: %v", i, err)
			}
		}

		// Batch get
		keys := []string{"1", "2", "3", "999"}
		results, errs := service.BatchGet(ctx, keys)

		// Check first 3 succeeded
		for i := 0; i < 3; i++ {
			if errs[i] != nil {
				t.Errorf("BatchGet[%d] failed: %v", i, errs[i])
			}
			if results[i].ID != users[i].ID {
				t.Errorf("BatchGet[%d] wrong ID: got %s, want %s", i, results[i].ID, users[i].ID)
			}
		}

		// Check last one failed
		if !errors.Is(errs[3], ErrNotFound) {
			t.Errorf("Expected ErrNotFound for key 999, got %v", errs[3])
		}
	})

	t.Run("BatchDelete", func(t *testing.T) {
		keys := []string{"1", "2", "3"}
		errs := service.BatchDelete(ctx, keys)

		for i, err := range errs {
			if err != nil {
				t.Errorf("BatchDelete[%d] failed: %v", i, err)
			}
		}

		// Verify all deleted
		for _, key := range keys {
			exists, err := service.Exists(ctx, key)
			if err != nil {
				t.Fatalf("Exists failed: %v", err)
			}
			if exists {
				t.Errorf("Key %s still exists after delete", key)
			}
		}
	})
}

func TestServiceQuery(t *testing.T) {
	provider := newMockProvider()
	service, err := NewService[TestUser](provider)
	if err != nil {
		t.Fatalf("Failed to create service: %v", err)
	}

	ctx := context.Background()

	t.Run("List multiple results", func(t *testing.T) {
		results, err := service.List(ctx, map[string]interface{}{})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}
	})
}

func TestExtractKey(t *testing.T) {
	provider := newMockProvider()
	service, err := NewService[TestUser](provider)
	if err != nil {
		t.Fatalf("Failed to create service: %v", err)
	}

	t.Run("extract from pointer", func(t *testing.T) {
		user := &TestUser{ID: "test123"}
		key, err := service.extractKey(user)
		if err != nil {
			t.Fatalf("extractKey failed: %v", err)
		}
		if key != "test123" {
			t.Errorf("Expected key=test123, got %s", key)
		}
	})

	t.Run("extract from value", func(t *testing.T) {
		user := TestUser{ID: "test456"}
		key, err := service.extractKey(&user)
		if err != nil {
			t.Fatalf("extractKey failed: %v", err)
		}
		if key != "test456" {
			t.Errorf("Expected key=test456, got %s", key)
		}
	})
}
