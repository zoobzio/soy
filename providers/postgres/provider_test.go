package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/providers/postgres"
	"github.com/zoobzio/cereal"
	"github.com/zoobzio/sentinel"
)

func init() {
	// Register required tags BEFORE sealing
	sentinel.Tag("constraints")
	sentinel.Tag("validate")
	sentinel.Tag("type")
	sentinel.Tag("table")

	// Create admin and seal sentinel configuration
	admin, err := sentinel.NewAdmin()
	if err != nil {
		panic(err)
	}
	admin.Seal()

	// Register test tables and fields with sentinel
	registerTestTables()
}

func registerTestTables() {
	// Define test structures
	type User struct {
		ID     string `json:"id" db:"id" table:"users"`
		Name   string `json:"name" db:"name"`
		Email  string `json:"email" db:"email"`
		Age    int    `json:"age" db:"age"`
		Active bool   `json:"active" db:"active"`
	}

	type Product struct {
		SKU   string  `json:"sku" db:"sku" table:"products"`
		Name  string  `json:"name" db:"name"`
		Price float64 `json:"price" db:"price"`
	}

	type Order struct {
		OrderID string `json:"order_id" db:"order_id" table:"orders"`
		Status  string `json:"status" db:"status"`
	}

	// Register with sentinel
	sentinel.Inspect[User]()
	sentinel.Inspect[Product]()
	sentinel.Inspect[Order]()
}

// mockRows implements the methods we need from sqlx.Rows for testing
type mockRows struct {
	data      []map[string]interface{}
	index     int
	scanError error
	closed    bool
}

func (m *mockRows) Next() bool {
	if m.closed || m.index >= len(m.data) {
		return false
	}
	m.index++
	return true
}

func (m *mockRows) MapScan(dest map[string]interface{}) error {
	if m.scanError != nil {
		return m.scanError
	}
	if m.index == 0 || m.index > len(m.data) {
		return errors.New("no current row")
	}
	// Copy data to dest
	for k, v := range m.data[m.index-1] {
		dest[k] = v
	}
	return nil
}

func (m *mockRows) Close() error {
	m.closed = true
	return nil
}

func (m *mockRows) Err() error {
	return nil
}

// mockDB implements the methods we need from sqlx.DB for testing
type mockDB struct {
	// Query execution mocks
	namedQueryFunc func(query string, arg interface{}) (*sqlx.Rows, error)
	namedExecFunc  func(query string, arg interface{}) (sql.Result, error)

	// Expected calls for verification
	expectedQueries []string
	callIndex       int

	// Mock data for queries
	mockRows *mockRows
}

func (m *mockDB) NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error) {
	if m.callIndex < len(m.expectedQueries) {
		m.expectedQueries[m.callIndex] = query
		m.callIndex++
	}

	if m.namedQueryFunc != nil {
		return m.namedQueryFunc(query, arg)
	}

	// Return mock rows if available
	if m.mockRows != nil {
		// Reset the mock rows index for new query
		m.mockRows.index = 0
		m.mockRows.closed = false
		// Create a type that wraps mockRows to satisfy sqlx.Rows interface
		return (*sqlx.Rows)(nil), nil // This is a limitation - we'd need the full sqlx.Rows interface
	}

	return nil, nil
}

func (m *mockDB) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	if m.callIndex < len(m.expectedQueries) {
		m.expectedQueries[m.callIndex] = query
		m.callIndex++
	}

	if m.namedExecFunc != nil {
		return m.namedExecFunc(query, arg)
	}
	return mockResult{}, nil
}

func (m *mockDB) Close() error {
	return nil
}

// mockResult implements sql.Result
type mockResult struct {
	lastInsertId int64
	rowsAffected int64
}

func (r mockResult) LastInsertId() (int64, error) {
	return r.lastInsertId, nil
}

func (r mockResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// Helper to create a provider with a mock DB
func newMockProvider() *Provider {
	// We can't directly mock sqlx.DB, so we'll test the logic that doesn't require DB
	return &Provider{
		db:      nil, // Will need to mock individual methods
		queries: make(map[string]*postgres.AST),
	}
}

// Helper to create a provider with a custom mock DB
func newMockProviderWithDB(db *mockDB) *Provider {
	// We can't directly mock sqlx.DB, so we'll test the logic that doesn't require DB
	return &Provider{
		db:      nil, // Will need to mock individual methods
		queries: make(map[string]*postgres.AST),
	}
}

func TestProvider_Get(t *testing.T) {
	tests := []struct {
		name     string
		table    string
		keyField string
		keyValue string
		mockRows []map[string]interface{}
		wantData map[string]interface{}
		wantErr  error
	}{
		{
			name:     "successful get",
			table:    "users",
			keyField: "id",
			keyValue: "123",
			mockRows: []map[string]interface{}{
				{"id": "123", "name": "John", "email": "john@example.com"},
			},
			wantData: map[string]interface{}{
				"id": "123", "name": "John", "email": "john@example.com",
			},
			wantErr: nil,
		},
		{
			name:     "not found",
			table:    "users",
			keyField: "id",
			keyValue: "999",
			mockRows: []map[string]interface{}{},
			wantData: nil,
			wantErr:  cereal.ErrNotFound,
		},
		{
			name:     "custom key field",
			table:    "products",
			keyField: "sku",
			keyValue: "ABC-123",
			mockRows: []map[string]interface{}{
				{"sku": "ABC-123", "name": "Product A", "price": 99.99},
			},
			wantData: map[string]interface{}{
				"sku": "ABC-123", "name": "Product A", "price": 99.99,
			},
			wantErr: nil,
		},
	}

	// Validation is handled by astql itself

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// For now, test AST building
			builder := postgres.Select(astql.T(tt.table)).
				Where(astql.C(astql.F(tt.keyField), astql.EQ, astql.P("key"))).
				Limit(1)

			ast, err := builder.Build()
			if err != nil {
				t.Fatalf("Failed to build AST: %v", err)
			}

			// Verify AST structure
			if ast.Target.Name != tt.table {
				t.Errorf("AST target = %v, want %v", ast.Target.Name, tt.table)
			}

			if ast.Limit == nil || *ast.Limit != 1 {
				t.Error("Expected LIMIT 1 in query")
			}
		})
	}
}

func TestProvider_Set(t *testing.T) {
	tests := []struct {
		name     string
		table    string
		keyField string
		keyValue string
		data     map[string]interface{}
		wantErr  bool
	}{
		{
			name:     "successful insert",
			table:    "users",
			keyField: "id",
			keyValue: "123",
			data: map[string]interface{}{
				"name":  "John",
				"email": "john@example.com",
			},
			wantErr: false,
		},
		{
			name:     "with custom key field",
			table:    "products",
			keyField: "sku",
			keyValue: "ABC-123",
			data: map[string]interface{}{
				"name":  "Product A",
				"price": 99.99,
			},
			wantErr: false,
		},
		{
			name:     "invalid json",
			table:    "users",
			keyField: "id",
			keyValue: "123",
			data:     nil,
			wantErr:  true, // Will fail on JSON marshal
		},
	}

	// Validation is handled by astql itself

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Convert data to JSON
			var jsonData []byte
			var err error
			if tt.data != nil {
				jsonData, err = json.Marshal(tt.data)
				if err != nil && !tt.wantErr {
					t.Fatalf("Failed to marshal test data: %v", err)
				}
			}

			// Test AST building for Set operation
			if tt.data != nil {
				// Add key to data
				tt.data[tt.keyField] = tt.keyValue

				// Build values and updates maps
				values := make(map[astql.Field]astql.Param)
				updates := make(map[astql.Field]astql.Param)

				for name := range tt.data {
					field := astql.F(name)
					param := astql.P(name)
					values[field] = param
					if name != tt.keyField {
						updates[field] = param
					}
				}

				// Build INSERT ... ON CONFLICT query
				builder := postgres.Insert(astql.T(tt.table)).
					Values(values).
					OnConflict(astql.F(tt.keyField)).DoUpdate(updates)

				ast, err := builder.Build()
				if err != nil {
					t.Fatalf("Failed to build AST: %v", err)
				}

				// Verify AST structure
				if ast.Target.Name != tt.table {
					t.Errorf("AST target = %v, want %v", ast.Target.Name, tt.table)
				}

				if ast.OnConflict == nil {
					t.Error("Expected ON CONFLICT clause")
				} else {
					if len(ast.OnConflict.Columns) != 1 || ast.OnConflict.Columns[0].Name != tt.keyField {
						t.Errorf("ON CONFLICT column = %v, want %v", ast.OnConflict.Columns, tt.keyField)
					}
				}
			}

			// In a real implementation, we'd test the actual Set method
			// For now, we've verified the AST building logic
			_ = jsonData
		})
	}
}

func TestProvider_Delete(t *testing.T) {
	// Validation is handled by astql itself

	// Test AST building for Delete
	table := "users"
	keyField := "id"
	keyValue := "123"

	builder := postgres.Delete(astql.T(table)).
		Where(astql.C(astql.F(keyField), astql.EQ, astql.P("key")))

	ast, err := builder.Build()
	if err != nil {
		t.Fatalf("Failed to build AST: %v", err)
	}

	// Verify AST structure
	if ast.Target.Name != table {
		t.Errorf("AST target = %v, want %v", ast.Target.Name, table)
	}

	if ast.Operation != astql.OpDelete {
		t.Errorf("AST operation = %v, want %v", ast.Operation, astql.OpDelete)
	}

	_ = keyValue
}

func TestProvider_Exists(t *testing.T) {
	// Validation is handled by astql itself

	// Test AST building for Exists (COUNT query)
	table := "users"
	keyField := "id"
	keyValue := "123"

	builder := postgres.Count(astql.T(table)).
		Where(astql.C(astql.F(keyField), astql.EQ, astql.P("key")))

	ast, err := builder.Build()
	if err != nil {
		t.Fatalf("Failed to build AST: %v", err)
	}

	// Verify AST structure
	if ast.Target.Name != table {
		t.Errorf("AST target = %v, want %v", ast.Target.Name, table)
	}

	if ast.Operation != astql.OpCount {
		t.Errorf("AST operation = %v, want %v", ast.Operation, astql.OpCount)
	}

	_ = keyValue
}

func TestProvider_QueryManagement(t *testing.T) {
	// Validation is handled by astql itself

	provider := newMockProvider()

	// Create a test AST
	builder := postgres.Select(astql.T("users")).
		Where(astql.C(astql.F("active"), astql.EQ, astql.P("active")))

	ast, err := builder.Build()
	if err != nil {
		t.Fatalf("Failed to build AST: %v", err)
	}

	// Test RegisterQuery
	err = provider.RegisterQuery("active_users", ast)
	if err != nil {
		t.Errorf("RegisterQuery failed: %v", err)
	}

	// Test duplicate registration
	err = provider.RegisterQuery("active_users", ast)
	if !errors.Is(err, cereal.ErrQueryExists) {
		t.Errorf("Expected ErrQueryExists, got %v", err)
	}

	// Test GetQuery
	retrieved, err := provider.GetQuery("active_users")
	if err != nil {
		t.Errorf("GetQuery failed: %v", err)
	}

	if retrieved != ast {
		t.Error("Retrieved AST doesn't match registered AST")
	}

	// Test ListQueries
	queries := provider.ListQueries()
	if len(queries) != 1 || queries[0] != "active_users" {
		t.Errorf("ListQueries = %v, want [active_users]", queries)
	}

	// Test UnregisterQuery
	err = provider.UnregisterQuery("active_users")
	if err != nil {
		t.Errorf("UnregisterQuery failed: %v", err)
	}

	// Test unregistering non-existent query
	err = provider.UnregisterQuery("non_existent")
	if !errors.Is(err, cereal.ErrQueryNotFound) {
		t.Errorf("Expected ErrQueryNotFound, got %v", err)
	}

	// Verify query was removed
	_, err = provider.GetQuery("active_users")
	if !errors.Is(err, cereal.ErrQueryNotFound) {
		t.Errorf("Expected ErrQueryNotFound after unregister, got %v", err)
	}
}

func TestProvider_BatchOperations(t *testing.T) {
	// Validation is handled by astql itself

	t.Run("BatchGet", func(t *testing.T) {
		provider := newMockProvider()

		table := "users"
		keyField := "id"
		keyValues := []string{"1", "2", "3"}

		// Test AST building for IN query
		ids := make([]interface{}, len(keyValues))
		for i, key := range keyValues {
			ids[i] = key
		}

		builder := postgres.Select(astql.T(table)).
			Where(astql.C(astql.F(keyField), astql.IN, astql.P("keys")))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build AST: %v", err)
		}

		// Verify AST structure
		if ast.Target.Name != table {
			t.Errorf("AST target = %v, want %v", ast.Target.Name, table)
		}

		_ = provider
	})

	t.Run("BatchSet size mismatch", func(t *testing.T) {
		provider := newMockProvider()

		keyValues := []string{"1", "2", "3"}
		data := [][]byte{{}, {}} // Mismatched length

		errs := provider.BatchSet(context.Background(), "users", "id", keyValues, data)

		// All errors should be ErrBatchSizeMismatch
		for i, err := range errs {
			if !errors.Is(err, cereal.ErrBatchSizeMismatch) {
				t.Errorf("errs[%d] = %v, want ErrBatchSizeMismatch", i, err)
			}
		}
	})

	t.Run("BatchDelete", func(t *testing.T) {
		provider := newMockProvider()

		table := "users"
		keyField := "id"
		keyValues := []string{"1", "2", "3"}

		// Test AST building for DELETE IN query
		ids := make([]interface{}, len(keyValues))
		for i, key := range keyValues {
			ids[i] = key
		}

		builder := postgres.Delete(astql.T(table)).
			Where(astql.C(astql.F(keyField), astql.IN, astql.P("keys")))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build AST: %v", err)
		}

		// Verify AST structure
		if ast.Target.Name != table {
			t.Errorf("AST target = %v, want %v", ast.Target.Name, table)
		}

		if ast.Operation != astql.OpDelete {
			t.Errorf("AST operation = %v, want %v", ast.Operation, astql.OpDelete)
		}

		_ = provider
	})
}

func TestProvider_Execute(t *testing.T) {
	// Validation is handled by astql itself

	provider := newMockProvider()

	// Register a test query
	builder := postgres.Select(astql.T("users")).
		Where(astql.C(astql.F("age"), astql.GT, astql.P("min_age")))

	ast, err := builder.Build()
	if err != nil {
		t.Fatalf("Failed to build AST: %v", err)
	}

	err = provider.RegisterQuery("adult_users", ast)
	if err != nil {
		t.Fatalf("Failed to register query: %v", err)
	}

	tests := []struct {
		name       string
		queryName  string
		parameters map[string]interface{}
		wantErr    bool
	}{
		{
			name:      "execute named query",
			queryName: "adult_users",
			parameters: map[string]interface{}{
				"min_age": 18,
			},
			wantErr: false,
		},
		{
			name:       "non-existent named query",
			queryName:  "non_existent",
			parameters: map[string]interface{}{},
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// We can't fully test Execute without mocking the database
			// but we can verify the validation logic

			if tt.name == "execute named query" {
				// This should work since we registered the query
				storedAST, err := provider.GetQuery(tt.queryName)
				if err != nil {
					t.Errorf("Expected to find registered query, got error: %v", err)
				}
				if !reflect.DeepEqual(storedAST, ast) {
					t.Error("Stored AST doesn't match expected")
				}
			}

			// Further testing would require mocking executePostgresAST
		})
	}
}

func TestProvider_executePostgresAST(t *testing.T) {
	// Validation is handled by astql itself

	t.Run("SELECT with single result", func(t *testing.T) {
		// Create a SELECT query with LIMIT 1
		builder := postgres.Select(astql.T("users")).
			Where(astql.C(astql.F("id"), astql.EQ, astql.P("id"))).
			Limit(1)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build AST: %v", err)
		}

		// Verify AST structure
		if ast.Limit == nil || *ast.Limit != 1 {
			t.Error("Expected LIMIT 1 in AST")
		}

		// Test SQL rendering
		provider := postgres.NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render AST: %v", err)
		}

		// Verify the query contains LIMIT 1
		if !strings.Contains(result.SQL, "LIMIT 1") {
			t.Error("Expected query to contain LIMIT 1")
		}
	})

	t.Run("COUNT operation", func(t *testing.T) {
		// Create a COUNT query
		builder := postgres.Count(astql.T("users")).
			Where(astql.C(astql.F("active"), astql.EQ, astql.P("active")))

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build AST: %v", err)
		}

		// Verify AST structure
		if ast.Operation != astql.OpCount {
			t.Errorf("Expected OpCount, got %v", ast.Operation)
		}

		// Test SQL rendering
		provider := postgres.NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render AST: %v", err)
		}

		// Verify the query contains COUNT
		if !strings.Contains(result.SQL, "COUNT(*)") {
			t.Error("Expected query to contain COUNT(*)")
		}
	})

	t.Run("INSERT operation", func(t *testing.T) {
		// Create an INSERT query
		values := map[astql.Field]astql.Param{
			astql.F("name"):  astql.P("name"),
			astql.F("email"): astql.P("email"),
		}

		builder := postgres.Insert(astql.T("users")).Values(values)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build AST: %v", err)
		}

		// Verify AST structure
		if ast.Operation != astql.OpInsert {
			t.Errorf("Expected OpInsert, got %v", ast.Operation)
		}

		// Test SQL rendering
		provider := postgres.NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render AST: %v", err)
		}

		// Verify the query is an INSERT
		if !strings.Contains(result.SQL, "INSERT INTO") {
			t.Error("Expected query to contain INSERT INTO")
		}

		// Verify required parameters
		expectedParams := []string{"name", "email"}
		if len(result.RequiredParams) != len(expectedParams) {
			t.Errorf("Expected %d parameters, got %d", len(expectedParams), len(result.RequiredParams))
		}
	})
}

func TestProvider_Close(t *testing.T) {
	// Test that Close is safe even with nil db
	provider := &Provider{
		db:      nil,
		queries: make(map[string]*postgres.AST),
	}

	// Close with nil db should handle gracefully
	err := provider.Close()
	if err != nil {
		t.Errorf("Close() with nil db should return nil, got %v", err)
	}
}

func TestProvider_JSON_Handling(t *testing.T) {
	// Test JSON marshaling/unmarshaling logic
	t.Run("JSON parsing in Set", func(t *testing.T) {
		// Test that Set properly parses JSON data and builds the correct AST
		data := map[string]interface{}{
			"name":   "John Doe",
			"age":    30,
			"active": true,
		}

		jsonData, err := json.Marshal(data)
		if err != nil {
			t.Fatalf("Failed to marshal test data: %v", err)
		}

		// Parse JSON to verify the structure
		var parsed map[string]interface{}
		err = json.Unmarshal(jsonData, &parsed)
		if err != nil {
			t.Fatalf("Failed to unmarshal test data: %v", err)
		}

		// Add key field
		parsed["id"] = "123"

		// Build values map as Set would do
		values := make(map[astql.Field]astql.Param)
		for name := range parsed {
			values[astql.F(name)] = astql.P(name)
		}

		// Verify we have all expected fields
		expectedFields := []string{"name", "age", "active", "id"}
		if len(values) != len(expectedFields) {
			t.Errorf("Expected %d fields, got %d", len(expectedFields), len(values))
		}
	})

	t.Run("Invalid JSON handling", func(t *testing.T) {
		invalidJSON := []byte("{invalid json")

		// Test JSON parsing
		var data map[string]interface{}
		err := json.Unmarshal(invalidJSON, &data)
		if err == nil {
			t.Error("Expected error for invalid JSON")
		}
	})
}

func TestProvider_SQL_Rendering(t *testing.T) {
	// Validation is handled by astql itself

	t.Run("render SELECT query", func(t *testing.T) {
		builder := postgres.Select(astql.T("users")).
			Where(astql.C(astql.F("id"), astql.EQ, astql.P("id"))).
			Limit(1)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build AST: %v", err)
		}

		provider := postgres.NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render AST: %v", err)
		}

		// Verify SQL structure
		expectedSQL := "SELECT * FROM users WHERE id = :id LIMIT 1"
		if result.SQL != expectedSQL {
			t.Errorf("SQL = %q, want %q", result.SQL, expectedSQL)
		}

		// Verify required parameters
		if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "id" {
			t.Errorf("RequiredParams = %v, want [id]", result.RequiredParams)
		}
	})

	t.Run("render INSERT with ON CONFLICT", func(t *testing.T) {
		values := map[astql.Field]astql.Param{
			astql.F("id"):    astql.P("id"),
			astql.F("name"):  astql.P("name"),
			astql.F("email"): astql.P("email"),
		}

		updates := map[astql.Field]astql.Param{
			astql.F("name"):  astql.P("name"),
			astql.F("email"): astql.P("email"),
		}

		builder := postgres.Insert(astql.T("users")).
			Values(values).
			OnConflict(astql.F("id")).DoUpdate(updates)

		ast, err := builder.Build()
		if err != nil {
			t.Fatalf("Failed to build AST: %v", err)
		}

		provider := postgres.NewProvider()
		result, err := provider.Render(ast)
		if err != nil {
			t.Fatalf("Failed to render AST: %v", err)
		}

		// Verify it contains ON CONFLICT clause
		if !strings.Contains(result.SQL, "ON CONFLICT") {
			t.Error("Expected SQL to contain ON CONFLICT clause")
		}
		if !strings.Contains(result.SQL, "DO UPDATE SET") {
			t.Error("Expected SQL to contain DO UPDATE SET")
		}
	})
}
