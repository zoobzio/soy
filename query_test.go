package cereal

import (
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/sentinel"
)

type queryTestUser struct {
	ID    int    `db:"id" type:"integer" constraints:"primarykey"`
	Email string `db:"email" type:"text" constraints:"notnull,unique"`
	Name  string `db:"name" type:"text"`
	Age   *int   `db:"age" type:"integer"`
}

func TestQuery_Basic(t *testing.T) {
	// Register tags
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")

	db := &sqlx.DB{}
	cereal, err := New[queryTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("SELECT all with WHERE", func(t *testing.T) {
		result, err := cereal.Query().
			Where("age", ">=", "min_age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "SELECT") {
			t.Errorf("SQL missing SELECT: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"users"`) {
			t.Errorf("SQL missing table name: %s", result.SQL)
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

	t.Run("SELECT specific fields", func(t *testing.T) {
		result, err := cereal.Query().
			Fields("id", "email", "name").
			Where("age", ">=", "min_age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, `"id"`) {
			t.Errorf("SQL missing id field: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"email"`) {
			t.Errorf("SQL missing email field: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"name"`) {
			t.Errorf("SQL missing name field: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SELECT with multiple WHERE (AND)", func(t *testing.T) {
		result, err := cereal.Query().
			Where("age", ">=", "min_age").
			Where("age", "<=", "max_age").
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

	t.Run("SELECT with WhereAnd", func(t *testing.T) {
		result, err := cereal.Query().
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

	t.Run("SELECT with WhereOr", func(t *testing.T) {
		result, err := cereal.Query().
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

	t.Run("SELECT with WhereNull", func(t *testing.T) {
		result, err := cereal.Query().
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

	t.Run("SELECT with WhereNotNull", func(t *testing.T) {
		result, err := cereal.Query().
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
}

func TestQuery_OrderBy(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")

	db := &sqlx.DB{}
	cereal, err := New[queryTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("ORDER BY single field ASC", func(t *testing.T) {
		result, err := cereal.Query().
			Where("age", ">=", "min_age").
			OrderBy("name", "ASC").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "ORDER BY") {
			t.Errorf("SQL missing ORDER BY: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"name"`) {
			t.Errorf("SQL missing name field in ORDER BY: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "ASC") {
			t.Errorf("SQL missing ASC: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("ORDER BY single field DESC", func(t *testing.T) {
		result, err := cereal.Query().
			Where("age", ">=", "min_age").
			OrderBy("age", "DESC").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "ORDER BY") {
			t.Errorf("SQL missing ORDER BY: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "DESC") {
			t.Errorf("SQL missing DESC: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("ORDER BY multiple fields", func(t *testing.T) {
		result, err := cereal.Query().
			Where("age", ">=", "min_age").
			OrderBy("age", "DESC").
			OrderBy("name", "ASC").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "ORDER BY") {
			t.Errorf("SQL missing ORDER BY: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"age"`) {
			t.Errorf("SQL missing age field: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"name"`) {
			t.Errorf("SQL missing name field: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})
}

func TestQuery_Pagination(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")

	db := &sqlx.DB{}
	cereal, err := New[queryTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("LIMIT only", func(t *testing.T) {
		result, err := cereal.Query().
			Where("age", ">=", "min_age").
			Limit(10).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "LIMIT") {
			t.Errorf("SQL missing LIMIT: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "10") {
			t.Errorf("SQL missing limit value: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("OFFSET only", func(t *testing.T) {
		result, err := cereal.Query().
			Where("age", ">=", "min_age").
			Offset(20).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "OFFSET") {
			t.Errorf("SQL missing OFFSET: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "20") {
			t.Errorf("SQL missing offset value: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("LIMIT and OFFSET (pagination)", func(t *testing.T) {
		result, err := cereal.Query().
			Where("age", ">=", "min_age").
			OrderBy("name", "ASC").
			Limit(10).
			Offset(20).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "LIMIT") {
			t.Errorf("SQL missing LIMIT: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "OFFSET") {
			t.Errorf("SQL missing OFFSET: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})
}

func TestQuery_ComplexQueries(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")

	db := &sqlx.DB{}
	cereal, err := New[queryTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("full featured query", func(t *testing.T) {
		result, err := cereal.Query().
			Fields("id", "email", "name", "age").
			WhereAnd(
				C("age", ">=", "min_age"),
				C("age", "<=", "max_age"),
			).
			WhereNotNull("email").
			OrderBy("age", "DESC").
			OrderBy("name", "ASC").
			Limit(10).
			Offset(20).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		// Verify all components present
		if !strings.Contains(result.SQL, "SELECT") {
			t.Error("Missing SELECT")
		}
		if !strings.Contains(result.SQL, `"id"`) {
			t.Error("Missing id field")
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
		if !strings.Contains(result.SQL, "ORDER BY") {
			t.Error("Missing ORDER BY")
		}
		if !strings.Contains(result.SQL, "LIMIT") {
			t.Error("Missing LIMIT")
		}
		if !strings.Contains(result.SQL, "OFFSET") {
			t.Error("Missing OFFSET")
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})
}

func TestQuery_InstanceAccess(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[queryTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	builder := cereal.Query()
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

func TestQuery_MustRender(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[queryTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("successful MustRender", func(t *testing.T) {
		result := cereal.Query().
			Where("age", ">=", "min_age").
			MustRender()
		if result == nil {
			t.Fatal("MustRender() returned nil")
		}
		if result.SQL == "" {
			t.Error("MustRender() returned empty SQL")
		}
	})

	t.Run("MustRender panics on invalid field in Where", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("MustRender() did not panic with invalid field")
			}
		}()
		cereal.Query().
			Where("nonexistent_field", "=", "value").
			MustRender()
	})

	t.Run("MustRender panics on invalid field in Fields", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("MustRender() did not panic with invalid field")
			}
		}()
		cereal.Query().
			Fields("nonexistent_field").
			Where("id", "=", "user_id").
			MustRender()
	})

	t.Run("MustRender panics on invalid operator", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("MustRender() did not panic with invalid operator")
			}
		}()
		cereal.Query().
			Where("id", "INVALID", "user_id").
			MustRender()
	})

	t.Run("MustRender panics on invalid direction", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("MustRender() did not panic with invalid direction")
			}
		}()
		cereal.Query().
			Where("age", ">=", "min_age").
			OrderBy("age", "INVALID").
			MustRender()
	})
}

func TestQuery_Validation(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[queryTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("all supported operators in WHERE", func(t *testing.T) {
		operators := []string{"=", "!=", ">", ">=", "<", "<="}
		for _, op := range operators {
			result, err := cereal.Query().
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

	t.Run("all supported directions in OrderBy", func(t *testing.T) {
		directions := []string{"ASC", "DESC", "asc", "desc"}
		for _, dir := range directions {
			result, err := cereal.Query().
				Where("age", ">=", "min_age").
				OrderBy("name", dir).
				Render()
			if err != nil {
				t.Errorf("Direction %s failed: %v", dir, err)
			}
			if !strings.Contains(result.SQL, "ORDER BY") {
				t.Errorf("Direction %s produced invalid SQL: %s", dir, result.SQL)
			}
		}
	})
}
