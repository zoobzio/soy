package cereal

import (
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/sentinel"
)

type createTestUser struct {
	ID    int    `db:"id" type:"integer" constraints:"primarykey"`
	Email string `db:"email" type:"text" constraints:"notnull,unique"`
	Name  string `db:"name" type:"text"`
	Age   *int   `db:"age" type:"integer"`
}

func TestCreate_Basic(t *testing.T) {
	// Register tags
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")

	db := &sqlx.DB{}
	cereal, err := New[createTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("simple INSERT with RETURNING", func(t *testing.T) {
		result, err := cereal.Insert().Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "INSERT INTO") {
			t.Errorf("SQL missing INSERT INTO: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"users"`) {
			t.Errorf("SQL missing table name: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "VALUES") {
			t.Errorf("SQL missing VALUES: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "RETURNING") {
			t.Errorf("SQL missing RETURNING: %s", result.SQL)
		}

		// Should have parameters for non-PK columns (email, name, age, created_at)
		if len(result.RequiredParams) < 2 {
			t.Errorf("Expected at least 2 params, got %d", len(result.RequiredParams))
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("INSERT with ON CONFLICT DO NOTHING", func(t *testing.T) {
		result, err := cereal.Insert().
			OnConflict("email").
			DoNothing().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "ON CONFLICT") {
			t.Errorf("SQL missing ON CONFLICT: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "DO NOTHING") {
			t.Errorf("SQL missing DO NOTHING: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"email"`) {
			t.Errorf("SQL missing conflict column: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("INSERT with ON CONFLICT DO UPDATE", func(t *testing.T) {
		result, err := cereal.Insert().
			OnConflict("email").
			DoUpdate().
			Set("name", "name").
			Set("age", "age").
			Build().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "ON CONFLICT") {
			t.Errorf("SQL missing ON CONFLICT: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "DO UPDATE") {
			t.Errorf("SQL missing DO UPDATE: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "SET") {
			t.Errorf("SQL missing SET: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"name"`) {
			t.Errorf("SQL missing updated field 'name': %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"age"`) {
			t.Errorf("SQL missing updated field 'age': %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("INSERT with multi-column conflict", func(t *testing.T) {
		result, err := cereal.Insert().
			OnConflict("email", "name").
			DoNothing().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, `"email"`) {
			t.Errorf("SQL missing first conflict column: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"name"`) {
			t.Errorf("SQL missing second conflict column: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})
}

func TestCreate_InstanceAccess(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[createTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	builder := cereal.Insert()
	instance := builder.Instance()

	if instance == nil {
		t.Fatal("Instance() returned nil")
	}

	// Verify we can use instance methods for advanced queries
	field := instance.F("email")
	if field.GetName() != "email" {
		t.Errorf("Field name = %s, want email", field.GetName())
	}
}

func TestCreate_MustRender(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[createTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("successful MustRender", func(t *testing.T) {
		result := cereal.Insert().MustRender()
		if result == nil {
			t.Fatal("MustRender() returned nil")
		}
		if result.SQL == "" {
			t.Error("MustRender() returned empty SQL")
		}
	})

	t.Run("MustRender panics on invalid conflict column", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("MustRender() did not panic with invalid column")
			}
		}()
		cereal.Insert().
			OnConflict("nonexistent_field").
			DoNothing().
			MustRender()
	})
}

func TestCreate_ConflictChaining(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[createTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("DoUpdate allows chaining Set calls", func(t *testing.T) {
		result, err := cereal.Insert().
			OnConflict("email").
			DoUpdate().
			Set("name", "name").
			Set("age", "age").
			Build().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		// Verify all SET clauses present
		setCount := strings.Count(result.SQL, "SET")
		if setCount != 1 {
			t.Errorf("Expected 1 SET keyword, got %d", setCount)
		}

		// Verify all fields in SET clause
		if !strings.Contains(result.SQL, `"name"`) {
			t.Error("Missing 'name' in SET clause")
		}
		if !strings.Contains(result.SQL, `"age"`) {
			t.Error("Missing 'age' in SET clause")
		}

		t.Logf("SQL: %s", result.SQL)
	})
}

func TestCreate_BatchOperations(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[createTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("ExecBatch renders query once for multiple records", func(t *testing.T) {
		// Just verify the query renders correctly - we can't execute without a real DB
		result, err := cereal.Insert().Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		// Query should be same for batch as for single insert
		if !strings.Contains(result.SQL, "INSERT INTO") {
			t.Errorf("SQL missing INSERT INTO: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "VALUES") {
			t.Errorf("SQL missing VALUES: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "RETURNING") {
			t.Errorf("SQL missing RETURNING: %s", result.SQL)
		}

		t.Logf("Batch query SQL: %s", result.SQL)
	})

	t.Run("ExecBatch with ON CONFLICT", func(t *testing.T) {
		result, err := cereal.Insert().
			OnConflict("email").
			DoNothing().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "ON CONFLICT") {
			t.Errorf("SQL missing ON CONFLICT: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "DO NOTHING") {
			t.Errorf("SQL missing DO NOTHING: %s", result.SQL)
		}

		t.Logf("Batch with conflict SQL: %s", result.SQL)
	})

	t.Run("ExecBatch with upsert", func(t *testing.T) {
		result, err := cereal.Insert().
			OnConflict("email").
			DoUpdate().
			Set("name", "name").
			Build().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "ON CONFLICT") {
			t.Errorf("SQL missing ON CONFLICT: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "DO UPDATE") {
			t.Errorf("SQL missing DO UPDATE: %s", result.SQL)
		}

		t.Logf("Batch upsert SQL: %s", result.SQL)
	})
}

func TestCreate_ErrorPaths(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[createTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("invalid Set field propagates error", func(t *testing.T) {
		_, err := cereal.Insert().
			OnConflict("email").
			DoUpdate().
			Set("nonexistent", "value").
			Build().
			Render()
		if err == nil {
			t.Error("expected error for invalid Set field")
		}
		if !strings.Contains(err.Error(), "nonexistent") {
			t.Errorf("error should mention invalid field: %v", err)
		}
	})

	t.Run("invalid Set param propagates error", func(t *testing.T) {
		_, err := cereal.Insert().
			OnConflict("email").
			DoUpdate().
			Set("name", "").
			Build().
			Render()
		if err == nil {
			t.Error("expected error for empty param")
		}
	})

	t.Run("error propagates through DoUpdate Set chain", func(t *testing.T) {
		builder := cereal.Insert().
			OnConflict("email").
			DoUpdate().
			Set("bad_field", "value").
			Set("name", "name"). // valid field shouldn't override error
			Build()
		_, err := builder.Render()
		if err == nil {
			t.Error("expected error to propagate through Set chain")
		}
	})

	t.Run("multiple valid sets work correctly", func(t *testing.T) {
		result, err := cereal.Insert().
			OnConflict("email").
			DoUpdate().
			Set("name", "name").
			Set("age", "age").
			Build().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, `"name"`) {
			t.Error("SQL missing name field")
		}
		if !strings.Contains(result.SQL, `"age"`) {
			t.Error("SQL missing age field")
		}
	})
}
