package cereal

import (
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/sentinel"
)

func TestCount_Basic(t *testing.T) {
	// Register tags
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")

	db := &sqlx.DB{}
	cereal, err := New[User](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("COUNT all records", func(t *testing.T) {
		result, err := cereal.Count().Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "SELECT") {
			t.Errorf("SQL missing SELECT: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "COUNT(*)") {
			t.Errorf("SQL missing COUNT(*): %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"users"`) {
			t.Errorf("SQL missing table name: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("COUNT with WHERE", func(t *testing.T) {
		result, err := cereal.Count().
			Where("age", ">=", "min_age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "COUNT(*)") {
			t.Errorf("SQL missing COUNT(*): %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "WHERE") {
			t.Errorf("SQL missing WHERE: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"age"`) {
			t.Errorf("SQL missing age field: %s", result.SQL)
		}

		// Check required parameters
		if len(result.RequiredParams) != 1 {
			t.Errorf("Expected 1 required param, got %d", len(result.RequiredParams))
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("COUNT with multiple WHERE (AND)", func(t *testing.T) {
		result, err := cereal.Count().
			Where("age", ">=", "min_age").
			Where("age", "<=", "max_age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "COUNT(*)") {
			t.Errorf("SQL missing COUNT(*): %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "AND") {
			t.Errorf("SQL missing AND: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("COUNT with WhereAnd", func(t *testing.T) {
		result, err := cereal.Count().
			WhereAnd(
				C("age", ">=", "min_age"),
				C("age", "<=", "max_age"),
			).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "COUNT(*)") {
			t.Errorf("SQL missing COUNT(*): %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "AND") {
			t.Errorf("SQL missing AND: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("COUNT with WhereOr", func(t *testing.T) {
		result, err := cereal.Count().
			WhereOr(
				C("age", "<", "young_age"),
				C("age", ">", "old_age"),
			).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "COUNT(*)") {
			t.Errorf("SQL missing COUNT(*): %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "OR") {
			t.Errorf("SQL missing OR: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("COUNT with WhereNull", func(t *testing.T) {
		result, err := cereal.Count().
			WhereNull("age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "COUNT(*)") {
			t.Errorf("SQL missing COUNT(*): %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "IS NULL") {
			t.Errorf("SQL missing IS NULL: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("COUNT with WhereNotNull", func(t *testing.T) {
		result, err := cereal.Count().
			WhereNotNull("age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "COUNT(*)") {
			t.Errorf("SQL missing COUNT(*): %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "IS NOT NULL") {
			t.Errorf("SQL missing IS NOT NULL: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})
}

func TestCount_ComplexConditions(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")

	db := &sqlx.DB{}
	cereal, err := New[User](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("COUNT with complex conditions", func(t *testing.T) {
		result, err := cereal.Count().
			WhereAnd(
				C("age", ">=", "min_age"),
				C("age", "<=", "max_age"),
			).
			WhereNotNull("email").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		// Verify all components present
		if !strings.Contains(result.SQL, "COUNT(*)") {
			t.Error("Missing COUNT(*)")
		}
		if !strings.Contains(result.SQL, "WHERE") {
			t.Error("Missing WHERE")
		}
		if !strings.Contains(result.SQL, "AND") {
			t.Error("Missing AND")
		}
		if !strings.Contains(result.SQL, "IS NOT NULL") {
			t.Error("Missing IS NOT NULL")
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})
}

func TestCount_InstanceAccess(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[User](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	builder := cereal.Count()
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

func TestCount_MustRender(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[User](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("successful MustRender", func(t *testing.T) {
		result := cereal.Count().
			Where("age", ">=", "min_age").
			MustRender()
		if result == nil {
			t.Fatal("MustRender() returned nil")
		}
		if result.SQL == "" {
			t.Error("MustRender() returned empty SQL")
		}
		if !strings.Contains(result.SQL, "COUNT(*)") {
			t.Error("MustRender() missing COUNT(*)")
		}
	})

	t.Run("MustRender panics on invalid field", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("MustRender() did not panic with invalid field")
			}
		}()
		cereal.Count().
			Where("nonexistent_field", "=", "value").
			MustRender()
	})

	t.Run("MustRender panics on invalid operator", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("MustRender() did not panic with invalid operator")
			}
		}()
		cereal.Count().
			Where("age", "INVALID", "value").
			MustRender()
	})
}

func TestCount_Validation(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[User](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("all supported operators in WHERE", func(t *testing.T) {
		operators := []string{"=", "!=", ">", ">=", "<", "<="}
		for _, op := range operators {
			result, err := cereal.Count().
				Where("age", op, "value").
				Render()
			if err != nil {
				t.Errorf("Operator %s failed: %v", op, err)
			}
			if !strings.Contains(result.SQL, "COUNT(*)") {
				t.Errorf("Operator %s produced invalid SQL: %s", op, result.SQL)
			}
		}
	})
}
