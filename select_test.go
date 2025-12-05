package cereal

import (
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/sentinel"
)

type selectTestUser struct {
	ID    int    `db:"id" type:"integer" constraints:"primarykey"`
	Email string `db:"email" type:"text" constraints:"notnull,unique"`
	Name  string `db:"name" type:"text"`
	Age   *int   `db:"age" type:"integer"`
}

func TestSelect_Basic(t *testing.T) {
	// Register tags
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")

	db := &sqlx.DB{}
	cereal, err := New[selectTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("simple SELECT all fields", func(t *testing.T) {
		result, err := cereal.Select().Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "SELECT") {
			t.Errorf("SQL missing SELECT: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"users"`) {
			t.Errorf("SQL missing table name: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SELECT specific fields", func(t *testing.T) {
		result, err := cereal.Select().
			Fields("id", "email", "name").
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

	t.Run("SELECT with WHERE", func(t *testing.T) {
		result, err := cereal.Select().
			Fields("id", "email").
			Where("age", ">=", "min_age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "WHERE") {
			t.Errorf("SQL missing WHERE: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"age"`) {
			t.Errorf("SQL missing age field: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, ":min_age") {
			t.Errorf("SQL missing parameter: %s", result.SQL)
		}

		// Check required parameters
		if len(result.RequiredParams) != 1 {
			t.Errorf("Expected 1 required param, got %d", len(result.RequiredParams))
		}
		if result.RequiredParams[0] != "min_age" {
			t.Errorf("Expected min_age param, got %s", result.RequiredParams[0])
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("SELECT with multiple WHERE (AND)", func(t *testing.T) {
		result, err := cereal.Select().
			Where("age", ">=", "min_age").
			Where("email", "=", "user_email").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "AND") {
			t.Errorf("SQL missing AND: %s", result.SQL)
		}

		// Check required parameters
		if len(result.RequiredParams) != 2 {
			t.Errorf("Expected 2 required params, got %d", len(result.RequiredParams))
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("SELECT with WhereAnd", func(t *testing.T) {
		result, err := cereal.Select().
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

		// Check required parameters
		if len(result.RequiredParams) != 2 {
			t.Errorf("Expected 2 required params, got %d", len(result.RequiredParams))
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("SELECT with WhereOr", func(t *testing.T) {
		result, err := cereal.Select().
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

		// Check required parameters
		if len(result.RequiredParams) != 2 {
			t.Errorf("Expected 2 required params, got %d", len(result.RequiredParams))
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("SELECT with WhereNull", func(t *testing.T) {
		result, err := cereal.Select().
			WhereNull("email").
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
		result, err := cereal.Select().
			WhereNotNull("email").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "IS NOT NULL") {
			t.Errorf("SQL missing IS NOT NULL: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SELECT with ORDER BY", func(t *testing.T) {
		result, err := cereal.Select().
			Fields("name", "email").
			OrderBy("name", "asc").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "ORDER BY") {
			t.Errorf("SQL missing ORDER BY: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "ASC") {
			t.Errorf("SQL missing ASC: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SELECT with ORDER BY desc", func(t *testing.T) {
		result, err := cereal.Select().
			OrderBy("age", "DESC").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "DESC") {
			t.Errorf("SQL missing DESC: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SELECT with LIMIT and OFFSET", func(t *testing.T) {
		result, err := cereal.Select().
			Limit(10).
			Offset(20).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "LIMIT 10") {
			t.Errorf("SQL missing LIMIT: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "OFFSET 20") {
			t.Errorf("SQL missing OFFSET: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SELECT with DISTINCT", func(t *testing.T) {
		result, err := cereal.Select().
			Fields("email").
			Distinct().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "DISTINCT") {
			t.Errorf("SQL missing DISTINCT: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("complex SELECT query", func(t *testing.T) {
		result, err := cereal.Select().
			Fields("id", "email", "name").
			WhereAnd(
				C("age", ">=", "min_age"),
				C("age", "<=", "max_age"),
			).
			OrderBy("name", "asc").
			Limit(25).
			Offset(50).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		// Verify all components present
		if !strings.Contains(result.SQL, "WHERE") {
			t.Errorf("SQL missing WHERE: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "AND") {
			t.Errorf("SQL missing AND: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "ORDER BY") {
			t.Errorf("SQL missing ORDER BY: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "LIMIT") {
			t.Errorf("SQL missing LIMIT: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "OFFSET") {
			t.Errorf("SQL missing OFFSET: %s", result.SQL)
		}

		// Check parameters
		if len(result.RequiredParams) != 2 {
			t.Errorf("Expected 2 required params, got %d", len(result.RequiredParams))
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})
}

func TestSelect_ConditionHelpers(t *testing.T) {
	t.Run("C creates valid condition", func(t *testing.T) {
		cond := C("email", "=", "user_email")
		if cond.field != "email" {
			t.Errorf("field = %s, want email", cond.field)
		}
		if cond.operator != "=" {
			t.Errorf("operator = %s, want =", cond.operator)
		}
		if cond.param != "user_email" {
			t.Errorf("param = %s, want user_email", cond.param)
		}
	})

	t.Run("Null creates IS NULL condition", func(t *testing.T) {
		cond := Null("email")
		if cond.field != "email" {
			t.Errorf("field = %s, want email", cond.field)
		}
		if !cond.isNull {
			t.Error("isNull should be true")
		}
		if cond.operator != "IS NULL" {
			t.Errorf("operator = %s, want IS NULL", cond.operator)
		}
	})

	t.Run("NotNull creates IS NOT NULL condition", func(t *testing.T) {
		cond := NotNull("age")
		if cond.field != "age" {
			t.Errorf("field = %s, want age", cond.field)
		}
		if !cond.isNull {
			t.Error("isNull should be true")
		}
		if cond.operator != "IS NOT NULL" {
			t.Errorf("operator = %s, want IS NOT NULL", cond.operator)
		}
	})
}

func TestSelect_InstanceAccess(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[selectTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	builder := cereal.Select()
	instance := builder.Instance()

	if instance == nil {
		t.Fatal("Instance() returned nil")
	}

	// Verify we can use instance methods for advanced queries
	t.Run("use instance for advanced condition", func(t *testing.T) {
		// This demonstrates the escape hatch for advanced ASTQL features
		field := instance.F("email")
		if field.GetName() != "email" {
			t.Errorf("Field name = %s, want email", field.GetName())
		}
	})
}

func TestSelect_MustRender(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[selectTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("successful MustRender", func(t *testing.T) {
		result := cereal.Select().Fields("id").MustRender()
		if result == nil {
			t.Fatal("MustRender() returned nil")
		}
		if result.SQL == "" {
			t.Error("MustRender() returned empty SQL")
		}
	})

	t.Run("MustRender panics on invalid field", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("MustRender() did not panic with invalid field")
			}
		}()
		cereal.Select().Fields("nonexistent_field").MustRender()
	})

	t.Run("MustRender panics on invalid operator", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("MustRender() did not panic with invalid operator")
			}
		}()
		cereal.Select().Where("age", "INVALID", "min_age").MustRender()
	})

	t.Run("MustRender panics on invalid direction", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("MustRender() did not panic with invalid direction")
			}
		}()
		cereal.Select().OrderBy("name", "INVALID").MustRender()
	})
}

func TestSelect_Validation(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[selectTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("all supported operators", func(t *testing.T) {
		operators := []string{"=", "!=", ">", ">=", "<", "<=", "LIKE", "NOT LIKE", "<->", "<#>", "<=>", "<+>"}
		for _, op := range operators {
			result, err := cereal.Select().
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

	t.Run("case insensitive directions", func(t *testing.T) {
		directions := []string{"asc", "ASC", "Asc", "desc", "DESC", "Desc"}
		for _, dir := range directions {
			result, err := cereal.Select().
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

	t.Run("LIKE pattern matching", func(t *testing.T) {
		result, err := cereal.Select().
			Where("email", "LIKE", "email_pattern").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "LIKE") {
			t.Errorf("SQL missing LIKE operator: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, ":email_pattern") {
			t.Errorf("SQL missing pattern parameter: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("NOT LIKE pattern matching", func(t *testing.T) {
		result, err := cereal.Select().
			Where("email", "NOT LIKE", "spam_pattern").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "NOT LIKE") {
			t.Errorf("SQL missing NOT LIKE operator: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, ":spam_pattern") {
			t.Errorf("SQL missing pattern parameter: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})
}

func TestSelect_OrderByExpr(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[selectTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("OrderByExpr with vector distance operator", func(t *testing.T) {
		result, err := cereal.Select().
			OrderByExpr("id", "<->", "query_value", "asc").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "ORDER BY") {
			t.Errorf("SQL missing ORDER BY: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "<->") {
			t.Errorf("SQL missing <-> operator: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("OrderByExpr with invalid direction", func(t *testing.T) {
		_, err := cereal.Select().
			OrderByExpr("id", "<->", "query_value", "INVALID").
			Render()
		if err == nil {
			t.Error("expected error for invalid direction")
		}
	})

	t.Run("OrderByExpr with invalid operator", func(t *testing.T) {
		_, err := cereal.Select().
			OrderByExpr("id", "BADOP", "query_value", "asc").
			Render()
		if err == nil {
			t.Error("expected error for invalid operator")
		}
	})

	t.Run("OrderByExpr with invalid field", func(t *testing.T) {
		_, err := cereal.Select().
			OrderByExpr("nonexistent", "<->", "query_value", "asc").
			Render()
		if err == nil {
			t.Error("expected error for invalid field")
		}
	})

	t.Run("OrderByExpr with empty param", func(t *testing.T) {
		_, err := cereal.Select().
			OrderByExpr("id", "<->", "", "asc").
			Render()
		if err == nil {
			t.Error("expected error for empty param")
		}
	})
}

func TestSelect_ErrorPaths(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[selectTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("invalid field in Fields returns error", func(t *testing.T) {
		_, err := cereal.Select().
			Fields("nonexistent").
			Render()
		if err == nil {
			t.Error("expected error for invalid field")
		}
	})

	t.Run("empty Fields is ignored", func(t *testing.T) {
		result, err := cereal.Select().
			Fields().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "SELECT") {
			t.Errorf("SQL missing SELECT: %s", result.SQL)
		}
	})

	t.Run("invalid Where field returns error", func(t *testing.T) {
		_, err := cereal.Select().
			Where("nonexistent", "=", "value").
			Render()
		if err == nil {
			t.Error("expected error for invalid Where field")
		}
	})

	t.Run("invalid Where param returns error", func(t *testing.T) {
		_, err := cereal.Select().
			Where("id", "=", "").
			Render()
		if err == nil {
			t.Error("expected error for empty param")
		}
	})

	t.Run("invalid OrderBy field returns error", func(t *testing.T) {
		_, err := cereal.Select().
			OrderBy("nonexistent", "asc").
			Render()
		if err == nil {
			t.Error("expected error for invalid OrderBy field")
		}
	})

	t.Run("error propagates through chain", func(t *testing.T) {
		builder := cereal.Select().
			Fields("bad_field").
			Where("id", "=", "user_id")
		_, err := builder.Render()
		if err == nil {
			t.Error("expected error to propagate through chain")
		}
	})

	t.Run("empty WhereAnd is ignored", func(t *testing.T) {
		result, err := cereal.Select().
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
		result, err := cereal.Select().
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
		result, err := cereal.Select().
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
		result, err := cereal.Select().
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
		_, err := cereal.Select().
			WhereAnd(C("age", "INVALID", "value")).
			Render()
		if err == nil {
			t.Error("expected error for invalid operator")
		}
	})

	t.Run("invalid param in condition returns error", func(t *testing.T) {
		_, err := cereal.Select().
			WhereAnd(C("age", "=", "")).
			Render()
		if err == nil {
			t.Error("expected error for empty param")
		}
	})
}
