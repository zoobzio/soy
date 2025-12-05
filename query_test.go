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

type queryTestDocument struct {
	ID        int    `db:"id" type:"integer" constraints:"primarykey"`
	Content   string `db:"content" type:"text"`
	Embedding []byte `db:"embedding" type:"vector(1536)"`
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

func TestQuery_OrderByExpr(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")

	db := &sqlx.DB{}
	cereal, err := New[queryTestDocument](db, "documents")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("ORDER BY vector L2 distance", func(t *testing.T) {
		result, err := cereal.Query().
			WhereNotNull("embedding").
			OrderByExpr("embedding", "<->", "query_embedding", "ASC").
			Limit(10).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		expected := `SELECT * FROM "documents" WHERE "embedding" IS NOT NULL ORDER BY "embedding" <-> :query_embedding ASC LIMIT 10`
		if result.SQL != expected {
			t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
		}

		if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "query_embedding" {
			t.Errorf("Expected params [query_embedding], got %v", result.RequiredParams)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("ORDER BY vector cosine distance", func(t *testing.T) {
		result, err := cereal.Query().
			OrderByExpr("embedding", "<=>", "query", "ASC").
			Limit(5).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "<=>") {
			t.Errorf("SQL missing cosine distance operator: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("invalid operator", func(t *testing.T) {
		_, err := cereal.Query().
			OrderByExpr("embedding", "INVALID", "query", "ASC").
			Render()
		if err == nil {
			t.Fatal("Expected error for invalid operator")
		}
	})

	t.Run("invalid direction", func(t *testing.T) {
		_, err := cereal.Query().
			OrderByExpr("embedding", "<->", "query", "INVALID").
			Render()
		if err == nil {
			t.Fatal("Expected error for invalid direction")
		}
	})
}

func TestQuery_WhereIn(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")

	db := &sqlx.DB{}
	cereal, err := New[queryTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("WHERE IN", func(t *testing.T) {
		result, err := cereal.Query().
			Where("id", "IN", "ids").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		// PostgreSQL array syntax: = ANY(:param)
		expected := `SELECT * FROM "users" WHERE "id" = ANY(:ids)`
		if result.SQL != expected {
			t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("WHERE NOT IN", func(t *testing.T) {
		result, err := cereal.Query().
			Where("id", "NOT IN", "excluded_ids").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		// PostgreSQL array syntax: != ALL(:param)
		if !strings.Contains(result.SQL, "!= ALL") {
			t.Errorf("SQL missing != ALL: %s", result.SQL)
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

func TestQuery_ErrorPaths(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[queryTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("invalid field in Fields returns error", func(t *testing.T) {
		_, err := cereal.Query().
			Fields("nonexistent").
			Render()
		if err == nil {
			t.Error("expected error for invalid field")
		}
	})

	t.Run("invalid Where field returns error", func(t *testing.T) {
		_, err := cereal.Query().
			Where("nonexistent", "=", "value").
			Render()
		if err == nil {
			t.Error("expected error for invalid Where field")
		}
	})

	t.Run("invalid Where param returns error", func(t *testing.T) {
		_, err := cereal.Query().
			Where("id", "=", "").
			Render()
		if err == nil {
			t.Error("expected error for empty param")
		}
	})

	t.Run("invalid WhereAnd field returns error", func(t *testing.T) {
		_, err := cereal.Query().
			WhereAnd(C("nonexistent", "=", "value")).
			Render()
		if err == nil {
			t.Error("expected error for invalid WhereAnd field")
		}
	})

	t.Run("invalid WhereOr field returns error", func(t *testing.T) {
		_, err := cereal.Query().
			WhereOr(C("nonexistent", "=", "value")).
			Render()
		if err == nil {
			t.Error("expected error for invalid WhereOr field")
		}
	})

	t.Run("invalid WhereNull field returns error", func(t *testing.T) {
		_, err := cereal.Query().
			WhereNull("nonexistent").
			Render()
		if err == nil {
			t.Error("expected error for invalid WhereNull field")
		}
	})

	t.Run("invalid WhereNotNull field returns error", func(t *testing.T) {
		_, err := cereal.Query().
			WhereNotNull("nonexistent").
			Render()
		if err == nil {
			t.Error("expected error for invalid WhereNotNull field")
		}
	})

	t.Run("invalid OrderBy field returns error", func(t *testing.T) {
		_, err := cereal.Query().
			OrderBy("nonexistent", "asc").
			Render()
		if err == nil {
			t.Error("expected error for invalid OrderBy field")
		}
	})

	t.Run("invalid OrderByExpr field returns error", func(t *testing.T) {
		_, err := cereal.Query().
			OrderByExpr("nonexistent", "<->", "query_value", "asc").
			Render()
		if err == nil {
			t.Error("expected error for invalid OrderByExpr field")
		}
	})

	t.Run("invalid OrderByExpr param returns error", func(t *testing.T) {
		_, err := cereal.Query().
			OrderByExpr("id", "<->", "", "asc").
			Render()
		if err == nil {
			t.Error("expected error for empty param")
		}
	})

	t.Run("error propagates through chain", func(t *testing.T) {
		builder := cereal.Query().
			Fields("bad_field").
			Where("id", "=", "user_id")
		_, err := builder.Render()
		if err == nil {
			t.Error("expected error to propagate through chain")
		}
	})

	t.Run("empty WhereAnd is ignored", func(t *testing.T) {
		result, err := cereal.Query().
			WhereAnd().
			Where("id", "=", "user_id").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "WHERE") {
			t.Errorf("SQL missing WHERE: %s", result.SQL)
		}
	})

	t.Run("empty WhereOr is ignored", func(t *testing.T) {
		result, err := cereal.Query().
			WhereOr().
			Where("id", "=", "user_id").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "WHERE") {
			t.Errorf("SQL missing WHERE: %s", result.SQL)
		}
	})

	t.Run("Null condition in WhereAnd", func(t *testing.T) {
		result, err := cereal.Query().
			WhereAnd(Null("age")).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "IS NULL") {
			t.Errorf("SQL missing IS NULL: %s", result.SQL)
		}
	})

	t.Run("NotNull condition in WhereOr", func(t *testing.T) {
		result, err := cereal.Query().
			WhereOr(NotNull("age")).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "IS NOT NULL") {
			t.Errorf("SQL missing IS NOT NULL: %s", result.SQL)
		}
	})

	t.Run("invalid operator in condition returns error", func(t *testing.T) {
		_, err := cereal.Query().
			WhereAnd(C("age", "INVALID", "value")).
			Render()
		if err == nil {
			t.Error("expected error for invalid operator")
		}
	})

	t.Run("invalid param in condition returns error", func(t *testing.T) {
		_, err := cereal.Query().
			WhereAnd(C("age", "=", "")).
			Render()
		if err == nil {
			t.Error("expected error for empty param")
		}
	})
}
