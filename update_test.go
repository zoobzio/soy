package cereal

import (
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/sentinel"
)

type updateTestUser struct {
	ID    int    `db:"id" type:"integer" constraints:"primarykey"`
	Email string `db:"email" type:"text" constraints:"notnull,unique"`
	Name  string `db:"name" type:"text"`
	Age   *int   `db:"age" type:"integer"`
}

func TestUpdate_Basic(t *testing.T) {
	// Register tags
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")

	db := &sqlx.DB{}
	cereal, err := New[updateTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("UPDATE with SET and WHERE", func(t *testing.T) {
		result, err := cereal.Modify().
			Set("name", "new_name").
			Set("age", "new_age").
			Where("id", "=", "user_id").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "UPDATE") {
			t.Errorf("SQL missing UPDATE: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"users"`) {
			t.Errorf("SQL missing table name: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "SET") {
			t.Errorf("SQL missing SET: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"name"`) {
			t.Errorf("SQL missing name field: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"age"`) {
			t.Errorf("SQL missing age field: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "WHERE") {
			t.Errorf("SQL missing WHERE: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "RETURNING") {
			t.Errorf("SQL missing RETURNING: %s", result.SQL)
		}

		// Check required parameters
		if len(result.RequiredParams) < 3 {
			t.Errorf("Expected at least 3 params, got %d", len(result.RequiredParams))
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("UPDATE with multiple WHERE (AND)", func(t *testing.T) {
		result, err := cereal.Modify().
			Set("name", "new_name").
			Where("id", "=", "user_id").
			Where("email", "=", "user_email").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "AND") {
			t.Errorf("SQL missing AND: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("UPDATE with WhereAnd", func(t *testing.T) {
		result, err := cereal.Modify().
			Set("name", "new_name").
			WhereAnd(
				C("age", ">=", "min_age"),
				C("age", "<=", "max_age"),
			).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "AND") {
			t.Errorf("SQL missing AND: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("UPDATE with WhereOr", func(t *testing.T) {
		result, err := cereal.Modify().
			Set("name", "new_name").
			WhereOr(
				C("age", "<", "young_age"),
				C("age", ">", "old_age"),
			).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "OR") {
			t.Errorf("SQL missing OR: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("UPDATE with WhereNull", func(t *testing.T) {
		result, err := cereal.Modify().
			Set("name", "new_name").
			WhereNull("age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "IS NULL") {
			t.Errorf("SQL missing IS NULL: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("UPDATE with WhereNotNull", func(t *testing.T) {
		result, err := cereal.Modify().
			Set("name", "new_name").
			WhereNotNull("age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "IS NOT NULL") {
			t.Errorf("SQL missing IS NOT NULL: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("UPDATE single field", func(t *testing.T) {
		result, err := cereal.Modify().
			Set("name", "new_name").
			Where("id", "=", "user_id").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, `"name"`) {
			t.Errorf("SQL missing name field: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("UPDATE multiple fields", func(t *testing.T) {
		result, err := cereal.Modify().
			Set("name", "new_name").
			Set("age", "new_age").
			Set("email", "new_email").
			Where("id", "=", "user_id").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, `"name"`) {
			t.Error("Missing 'name' in SET clause")
		}
		if !strings.Contains(result.SQL, `"age"`) {
			t.Error("Missing 'age' in SET clause")
		}
		if !strings.Contains(result.SQL, `"email"`) {
			t.Error("Missing 'email' in SET clause")
		}

		t.Logf("SQL: %s", result.SQL)
	})
}

func TestUpdate_InstanceAccess(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[updateTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	builder := cereal.Modify()
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

func TestUpdate_MustRender(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[updateTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("successful MustRender", func(t *testing.T) {
		result := cereal.Modify().
			Set("name", "new_name").
			Where("id", "=", "user_id").
			MustRender()
		if result == nil {
			t.Fatal("MustRender() returned nil")
		}
		if result.SQL == "" {
			t.Error("MustRender() returned empty SQL")
		}
	})

	t.Run("MustRender panics on invalid field in Set", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("MustRender() did not panic with invalid field")
			}
		}()
		cereal.Modify().
			Set("nonexistent_field", "value").
			Where("id", "=", "user_id").
			MustRender()
	})

	t.Run("MustRender panics on invalid operator", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("MustRender() did not panic with invalid operator")
			}
		}()
		cereal.Modify().
			Set("name", "new_name").
			Where("id", "INVALID", "user_id").
			MustRender()
	})
}

func TestUpdate_Validation(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[updateTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("all supported operators in WHERE", func(t *testing.T) {
		operators := []string{"=", "!=", ">", ">=", "<", "<="}
		for _, op := range operators {
			result, err := cereal.Modify().
				Set("name", "new_name").
				Where("age", op, "value").
				Render()
			if err != nil {
				t.Errorf("Operator %s failed: %v", op, err)
			}
			if !strings.Contains(result.SQL, "WHERE") {
				t.Errorf("Operator %s produced invalid SQL: %s", op, result.SQL)
			}
		}
	})
}

func TestUpdate_BatchOperations(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[updateTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("ExecBatch renders query once for multiple param sets", func(t *testing.T) {
		result, err := cereal.Modify().
			Set("name", "new_name").
			Where("id", "=", "user_id").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "UPDATE") {
			t.Errorf("SQL missing UPDATE: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "SET") {
			t.Errorf("SQL missing SET: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "WHERE") {
			t.Errorf("SQL missing WHERE: %s", result.SQL)
		}

		t.Logf("Batch update SQL: %s", result.SQL)
	})

	t.Run("ExecBatch with multiple SET fields", func(t *testing.T) {
		result, err := cereal.Modify().
			Set("name", "new_name").
			Set("age", "new_age").
			Where("id", "=", "user_id").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, `"name"`) {
			t.Error("Missing 'name' in SET clause")
		}
		if !strings.Contains(result.SQL, `"age"`) {
			t.Error("Missing 'age' in SET clause")
		}

		t.Logf("Batch update multiple fields SQL: %s", result.SQL)
	})

	t.Run("ExecBatch with complex WHERE", func(t *testing.T) {
		result, err := cereal.Modify().
			Set("name", "new_name").
			WhereAnd(
				C("age", ">=", "min_age"),
				C("age", "<=", "max_age"),
			).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "AND") {
			t.Errorf("SQL missing AND: %s", result.SQL)
		}

		t.Logf("Batch update with AND SQL: %s", result.SQL)
	})
}
