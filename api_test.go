package soy

import (
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql/pkg/postgres"
	"github.com/zoobzio/sentinel"
)

// Test model for soy tests.
type soyTestUser struct {
	ID    int    `db:"id" type:"integer" constraints:"primarykey"`
	Email string `db:"email" type:"text" constraints:"notnull,unique"`
	Name  string `db:"name" type:"text"`
	Age   *int   `db:"age" type:"integer"`
}

func TestNew_Success(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[soyTestUser](db, "users", postgres.New())

	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if soy == nil {
		t.Fatal("New() returned nil")
	}

	if soy.tableName != "users" {
		t.Errorf("tableName = %q, want %q", soy.tableName, "users")
	}

	if soy.db != db {
		t.Error("db connection not set correctly")
	}

	if soy.instance == nil {
		t.Error("ASTQL instance not initialized")
	}
}

func TestNew_EmptyTableName(t *testing.T) {
	db := &sqlx.DB{}
	_, err := New[soyTestUser](db, "", postgres.New())

	if err == nil {
		t.Error("New() should error with empty table name")
	}
}

func TestNew_NilRenderer(t *testing.T) {
	db := &sqlx.DB{}
	_, err := New[soyTestUser](db, "users", nil)

	if err == nil {
		t.Error("New() should error with nil renderer")
	}
}

func TestNew_NilDB(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	soy, err := New[soyTestUser](nil, "users", postgres.New())

	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if soy == nil {
		t.Fatal("New() should allow nil DB for query building only")
	}

	if soy.db != nil {
		t.Error("db should be nil")
	}
}

func TestSoy_TableName(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[soyTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tableName := soy.TableName()
	if tableName != "users" {
		t.Errorf("TableName() = %q, want %q", tableName, "users")
	}
}

func TestSoy_Metadata(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[soyTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	metadata := soy.Metadata()
	if len(metadata.Fields) == 0 {
		t.Error("Metadata() returned empty fields")
	}

	// Verify we have expected fields
	fieldNames := make(map[string]bool)
	for _, field := range metadata.Fields {
		dbTag := field.Tags["db"]
		if dbTag != "" {
			fieldNames[dbTag] = true
		}
	}

	expectedFields := []string{"id", "email", "name", "age"}
	for _, expected := range expectedFields {
		if !fieldNames[expected] {
			t.Errorf("Missing expected field: %s", expected)
		}
	}
}

func TestSoy_Instance(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[soyTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	instance := soy.Instance()
	if instance == nil {
		t.Fatal("Instance() returned nil")
	}

	// Verify instance can be used for ASTQL operations
	field := instance.F("email")
	if field.GetName() != "email" {
		t.Errorf("Field name = %s, want email", field.GetName())
	}
}

func TestSoy_Select(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[soyTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	sel := soy.Select()
	if sel == nil {
		t.Fatal("Select() returned nil")
	}

	if sel.instance != soy.instance {
		t.Error("Select() instance mismatch")
	}
}

func TestSoy_Query(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[soyTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	query := soy.Query()
	if query == nil {
		t.Fatal("Query() returned nil")
	}

	if query.instance != soy.instance {
		t.Error("Query() instance mismatch")
	}
}

func TestSoy_Insert(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[soyTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	insert := soy.Insert()
	if insert == nil {
		t.Fatal("Insert() returned nil")
	}

	if insert.instance != soy.instance {
		t.Error("Insert() instance mismatch")
	}

	// Verify it renders correctly
	result, err := insert.Render()
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if result.SQL == "" {
		t.Error("Render() returned empty SQL")
	}
}

func TestSoy_Modify(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[soyTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	modify := soy.Modify()
	if modify == nil {
		t.Fatal("Modify() returned nil")
	}

	if modify.instance != soy.instance {
		t.Error("Modify() instance mismatch")
	}
}

func TestSoy_Remove(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[soyTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	remove := soy.Remove()
	if remove == nil {
		t.Fatal("Remove() returned nil")
	}

	if remove.instance != soy.instance {
		t.Error("Remove() instance mismatch")
	}
}

func TestSoy_Aggregates(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[soyTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tests := []struct {
		name string
		agg  *Aggregate[soyTestUser]
	}{
		{"Count", soy.Count()},
		{"Sum", soy.Sum("age")},
		{"Avg", soy.Avg("age")},
		{"Min", soy.Min("age")},
		{"Max", soy.Max("age")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.agg == nil {
				t.Fatalf("%s() returned nil", tt.name)
			}

			if tt.agg.agg.instance != soy.instance {
				t.Errorf("%s() instance mismatch", tt.name)
			}

			// Verify it renders
			_, err := tt.agg.Render()
			if err != nil {
				t.Errorf("%s() Render() error = %v", tt.name, err)
			}
		})
	}
}

func TestSoy_Aggregate_InvalidField(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[soyTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	agg := soy.Sum("nonexistent_field")
	if agg == nil {
		t.Fatal("Sum() returned nil")
	}

	// Should have error stored
	_, err = agg.Render()
	if err == nil {
		t.Error("Sum() with invalid field should error on Render()")
	}
}

func TestSoy_Contains(t *testing.T) {
	tests := []struct {
		name   string
		s      string
		substr string
		want   bool
	}{
		{"exact match", "primarykey", "primarykey", true},
		{"case insensitive", "PrimaryKey", "primarykey", true},
		{"substring", "notnull,unique", "unique", true},
		{"not found", "notnull", "primarykey", false},
		{"empty substring", "test", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := contains(tt.s, tt.substr)
			if got != tt.want {
				t.Errorf("contains(%q, %q) = %v, want %v", tt.s, tt.substr, got, tt.want)
			}
		})
	}
}
