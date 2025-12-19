package cereal

import (
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql/pkg/postgres"
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
	cereal, err := New[selectTestUser](db, "users", postgres.New())
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
	cereal, err := New[selectTestUser](db, "users", postgres.New())
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
	cereal, err := New[selectTestUser](db, "users", postgres.New())
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
	cereal, err := New[selectTestUser](db, "users", postgres.New())
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
	cereal, err := New[selectTestUser](db, "users", postgres.New())
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
	cereal, err := New[selectTestUser](db, "users", postgres.New())
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

func TestSelect_HavingClauses(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[selectTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("simple Having", func(t *testing.T) {
		result, err := cereal.Select().
			Fields("age").
			GroupBy("age").
			Having("age", ">", "min_age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "HAVING") {
			t.Errorf("SQL missing HAVING: %s", result.SQL)
		}
	})

	t.Run("HavingAgg COUNT", func(t *testing.T) {
		result, err := cereal.Select().
			Fields("age").
			GroupBy("age").
			HavingAgg("COUNT", "", ">", "min_count").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "HAVING") {
			t.Errorf("SQL missing HAVING: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "COUNT") {
			t.Errorf("SQL missing COUNT: %s", result.SQL)
		}
	})

	t.Run("HavingAgg SUM", func(t *testing.T) {
		result, err := cereal.Select().
			Fields("name").
			GroupBy("name").
			HavingAgg("SUM", "age", ">=", "threshold").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "SUM") {
			t.Errorf("SQL missing SUM: %s", result.SQL)
		}
	})

	t.Run("Having with invalid field", func(t *testing.T) {
		_, err := cereal.Select().
			Fields("age").
			GroupBy("age").
			Having("invalid_field", ">", "value").
			Render()
		if err == nil {
			t.Error("expected error for invalid field in Having")
		}
	})

	t.Run("HavingAgg with invalid function", func(t *testing.T) {
		_, err := cereal.Select().
			Fields("age").
			GroupBy("age").
			HavingAgg("INVALID_FUNC", "*", ">", "value").
			Render()
		if err == nil {
			t.Error("expected error for invalid function in HavingAgg")
		}
	})
}

func TestSelect_WhereBetween(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[selectTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("WhereBetween", func(t *testing.T) {
		result, err := cereal.Select().
			WhereBetween("age", "min_age", "max_age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "BETWEEN") {
			t.Errorf("SQL missing BETWEEN: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, ":min_age") {
			t.Errorf("SQL missing min_age param: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, ":max_age") {
			t.Errorf("SQL missing max_age param: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("WhereNotBetween", func(t *testing.T) {
		result, err := cereal.Select().
			WhereNotBetween("age", "min_age", "max_age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "NOT BETWEEN") {
			t.Errorf("SQL missing NOT BETWEEN: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("Between condition helper", func(t *testing.T) {
		result, err := cereal.Select().
			WhereAnd(Between("age", "min_age", "max_age")).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "BETWEEN") {
			t.Errorf("SQL missing BETWEEN: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("NotBetween condition helper", func(t *testing.T) {
		result, err := cereal.Select().
			WhereAnd(NotBetween("age", "min_age", "max_age")).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "NOT BETWEEN") {
			t.Errorf("SQL missing NOT BETWEEN: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("WhereBetween invalid field", func(t *testing.T) {
		_, err := cereal.Select().
			WhereBetween("nonexistent", "min", "max").
			Render()
		if err == nil {
			t.Error("expected error for invalid field")
		}
	})

	t.Run("WhereBetween empty low param", func(t *testing.T) {
		_, err := cereal.Select().
			WhereBetween("age", "", "max").
			Render()
		if err == nil {
			t.Error("expected error for empty low param")
		}
	})

	t.Run("WhereBetween empty high param", func(t *testing.T) {
		_, err := cereal.Select().
			WhereBetween("age", "min", "").
			Render()
		if err == nil {
			t.Error("expected error for empty high param")
		}
	})
}

func TestSelect_StringExpressions(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[selectTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("SelectUpper", func(t *testing.T) {
		result, err := cereal.Select().
			SelectUpper("name", "upper_name").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "UPPER") {
			t.Errorf("SQL missing UPPER: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"upper_name"`) {
			t.Errorf("SQL missing alias: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SelectLower", func(t *testing.T) {
		result, err := cereal.Select().
			SelectLower("email", "lower_email").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "LOWER") {
			t.Errorf("SQL missing LOWER: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SelectLength", func(t *testing.T) {
		result, err := cereal.Select().
			SelectLength("name", "name_len").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "LENGTH") {
			t.Errorf("SQL missing LENGTH: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SelectTrim", func(t *testing.T) {
		result, err := cereal.Select().
			SelectTrim("name", "trimmed").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "TRIM") {
			t.Errorf("SQL missing TRIM: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SelectLTrim", func(t *testing.T) {
		result, err := cereal.Select().
			SelectLTrim("name", "ltrimmed").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "LTRIM") {
			t.Errorf("SQL missing LTRIM: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SelectRTrim", func(t *testing.T) {
		result, err := cereal.Select().
			SelectRTrim("name", "rtrimmed").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "RTRIM") {
			t.Errorf("SQL missing RTRIM: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SelectUpper invalid field", func(t *testing.T) {
		_, err := cereal.Select().
			SelectUpper("nonexistent", "alias").
			Render()
		if err == nil {
			t.Error("expected error for invalid field")
		}
	})
}

func TestSelect_MathExpressions(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[selectTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("SelectAbs", func(t *testing.T) {
		result, err := cereal.Select().
			SelectAbs("age", "abs_age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "ABS") {
			t.Errorf("SQL missing ABS: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SelectCeil", func(t *testing.T) {
		result, err := cereal.Select().
			SelectCeil("age", "ceil_age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "CEIL") {
			t.Errorf("SQL missing CEIL: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SelectFloor", func(t *testing.T) {
		result, err := cereal.Select().
			SelectFloor("age", "floor_age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "FLOOR") {
			t.Errorf("SQL missing FLOOR: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SelectRound", func(t *testing.T) {
		result, err := cereal.Select().
			SelectRound("age", "round_age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "ROUND") {
			t.Errorf("SQL missing ROUND: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SelectSqrt", func(t *testing.T) {
		result, err := cereal.Select().
			SelectSqrt("age", "sqrt_age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "SQRT") {
			t.Errorf("SQL missing SQRT: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SelectAbs invalid field", func(t *testing.T) {
		_, err := cereal.Select().
			SelectAbs("nonexistent", "alias").
			Render()
		if err == nil {
			t.Error("expected error for invalid field")
		}
	})
}

func TestSelect_CastExpressions(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[selectTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("SelectCast to TEXT", func(t *testing.T) {
		result, err := cereal.Select().
			SelectCast("age", CastText, "age_str").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "CAST") {
			t.Errorf("SQL missing CAST: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "TEXT") {
			t.Errorf("SQL missing TEXT type: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SelectCast to INTEGER", func(t *testing.T) {
		result, err := cereal.Select().
			SelectCast("id", CastInteger, "id_int").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "CAST") {
			t.Errorf("SQL missing CAST: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SelectCast invalid field", func(t *testing.T) {
		_, err := cereal.Select().
			SelectCast("nonexistent", CastText, "alias").
			Render()
		if err == nil {
			t.Error("expected error for invalid field")
		}
	})
}

func TestSelect_DateExpressions(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[selectTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("SelectNow", func(t *testing.T) {
		result, err := cereal.Select().
			SelectNow("current_ts").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "NOW") {
			t.Errorf("SQL missing NOW: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SelectCurrentDate", func(t *testing.T) {
		result, err := cereal.Select().
			SelectCurrentDate("today").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "CURRENT_DATE") {
			t.Errorf("SQL missing CURRENT_DATE: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SelectCurrentTime", func(t *testing.T) {
		result, err := cereal.Select().
			SelectCurrentTime("now_time").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "CURRENT_TIME") {
			t.Errorf("SQL missing CURRENT_TIME: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SelectCurrentTimestamp", func(t *testing.T) {
		result, err := cereal.Select().
			SelectCurrentTimestamp("ts").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "CURRENT_TIMESTAMP") {
			t.Errorf("SQL missing CURRENT_TIMESTAMP: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})
}

func TestSelect_AggregateExpressions(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[selectTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("SelectCountStar", func(t *testing.T) {
		result, err := cereal.Select().
			SelectCountStar("total").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "COUNT(*)") {
			t.Errorf("SQL missing COUNT(*): %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SelectCount", func(t *testing.T) {
		result, err := cereal.Select().
			SelectCount("id", "count_id").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "COUNT") {
			t.Errorf("SQL missing COUNT: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SelectCountDistinct", func(t *testing.T) {
		result, err := cereal.Select().
			SelectCountDistinct("email", "unique_emails").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "COUNT") {
			t.Errorf("SQL missing COUNT: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "DISTINCT") {
			t.Errorf("SQL missing DISTINCT: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SelectSum", func(t *testing.T) {
		result, err := cereal.Select().
			SelectSum("age", "age_sum").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "SUM") {
			t.Errorf("SQL missing SUM: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SelectAvg", func(t *testing.T) {
		result, err := cereal.Select().
			SelectAvg("age", "avg_age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "AVG") {
			t.Errorf("SQL missing AVG: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SelectMin", func(t *testing.T) {
		result, err := cereal.Select().
			SelectMin("age", "min_age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "MIN") {
			t.Errorf("SQL missing MIN: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SelectMax", func(t *testing.T) {
		result, err := cereal.Select().
			SelectMax("age", "max_age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "MAX") {
			t.Errorf("SQL missing MAX: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SelectCount invalid field", func(t *testing.T) {
		_, err := cereal.Select().
			SelectCount("nonexistent", "alias").
			Render()
		if err == nil {
			t.Error("expected error for invalid field")
		}
	})

	t.Run("combined expression query", func(t *testing.T) {
		result, err := cereal.Select().
			Fields("name").
			SelectUpper("name", "upper_name").
			SelectCount("id", "count").
			GroupBy("name").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "UPPER") {
			t.Errorf("SQL missing UPPER: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "COUNT") {
			t.Errorf("SQL missing COUNT: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "GROUP BY") {
			t.Errorf("SQL missing GROUP BY: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})
}

func TestSelect_CaseExpressions(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[selectTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("simple CASE with single WHEN", func(t *testing.T) {
		result, err := cereal.Select().
			SelectCase().
			When("age", ">=", "adult_age", "result_adult").
			Else("result_minor").
			As("age_group").
			End().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "CASE") {
			t.Errorf("SQL missing CASE: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "WHEN") {
			t.Errorf("SQL missing WHEN: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "THEN") {
			t.Errorf("SQL missing THEN: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "ELSE") {
			t.Errorf("SQL missing ELSE: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "END") {
			t.Errorf("SQL missing END: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"age_group"`) {
			t.Errorf("SQL missing alias: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("CASE with multiple WHEN clauses", func(t *testing.T) {
		result, err := cereal.Select().
			SelectCase().
			When("age", "<", "teen_age", "result_child").
			When("age", "<", "adult_age", "result_teen").
			When("age", "<", "senior_age", "result_adult").
			Else("result_senior").
			As("life_stage").
			End().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		// Count WHEN occurrences
		whenCount := strings.Count(result.SQL, "WHEN")
		if whenCount != 3 {
			t.Errorf("Expected 3 WHEN clauses, got %d: %s", whenCount, result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("CASE with WhenNull", func(t *testing.T) {
		result, err := cereal.Select().
			SelectCase().
			WhenNull("age", "result_unknown").
			Else("result_known").
			As("age_status").
			End().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "IS NULL") {
			t.Errorf("SQL missing IS NULL: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("CASE with WhenNotNull", func(t *testing.T) {
		result, err := cereal.Select().
			SelectCase().
			WhenNotNull("age", "result_has_age").
			Else("result_no_age").
			As("has_age").
			End().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "IS NOT NULL") {
			t.Errorf("SQL missing IS NOT NULL: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("CASE without ELSE", func(t *testing.T) {
		result, err := cereal.Select().
			SelectCase().
			When("age", ">=", "adult_age", "result_adult").
			As("is_adult").
			End().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if strings.Contains(result.SQL, "ELSE") {
			t.Errorf("SQL should not have ELSE: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("CASE chained with other operations", func(t *testing.T) {
		result, err := cereal.Select().
			Fields("id", "name").
			SelectCase().
			When("age", ">=", "adult_age", "result_adult").
			Else("result_minor").
			As("age_group").
			End().
			Where("id", "=", "user_id").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, `"id"`) {
			t.Errorf("SQL missing id field: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "CASE") {
			t.Errorf("SQL missing CASE: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "WHERE") {
			t.Errorf("SQL missing WHERE: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("CASE with invalid field returns error", func(t *testing.T) {
		_, err := cereal.Select().
			SelectCase().
			When("nonexistent", "=", "val", "result").
			As("alias").
			End().
			Render()
		if err == nil {
			t.Error("expected error for invalid field")
		}
	})

	t.Run("CASE with invalid operator returns error", func(t *testing.T) {
		_, err := cereal.Select().
			SelectCase().
			When("age", "INVALID", "val", "result").
			As("alias").
			End().
			Render()
		if err == nil {
			t.Error("expected error for invalid operator")
		}
	})

	t.Run("CASE with empty param returns error", func(t *testing.T) {
		_, err := cereal.Select().
			SelectCase().
			When("age", "=", "", "result").
			As("alias").
			End().
			Render()
		if err == nil {
			t.Error("expected error for empty param")
		}
	})

	t.Run("CASE with empty result param returns error", func(t *testing.T) {
		_, err := cereal.Select().
			SelectCase().
			When("age", "=", "val", "").
			As("alias").
			End().
			Render()
		if err == nil {
			t.Error("expected error for empty result param")
		}
	})

	t.Run("CASE Else with empty param returns error", func(t *testing.T) {
		_, err := cereal.Select().
			SelectCase().
			When("age", "=", "val", "result").
			Else("").
			As("alias").
			End().
			Render()
		if err == nil {
			t.Error("expected error for empty Else param")
		}
	})
}

// TestSelect_WhereFields tests field-to-field comparison conditions.
func TestSelect_WhereFields(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")

	db := &sqlx.DB{}
	cereal, err := New[selectTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("WhereFields equal", func(t *testing.T) {
		result, err := cereal.Select().
			WhereFields("id", "=", "age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, `"id" = "age"`) {
			t.Errorf("SQL missing field comparison: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("WhereFields less than", func(t *testing.T) {
		result, err := cereal.Select().
			WhereFields("id", "<", "age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, `"id" < "age"`) {
			t.Errorf("SQL missing field comparison: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("WhereFields invalid operator", func(t *testing.T) {
		_, err := cereal.Select().
			WhereFields("id", "INVALID", "age").
			Render()
		if err == nil {
			t.Error("expected error for invalid operator")
		}
	})
}

// TestSelect_AggregateFilter tests aggregate FILTER expressions.
func TestSelect_AggregateFilter(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")

	db := &sqlx.DB{}
	cereal, err := New[selectTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("SumFilter", func(t *testing.T) {
		result, err := cereal.Select().
			SelectSumFilter("age", "name", "=", "filter_val", "filtered_sum").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "SUM") && !strings.Contains(result.SQL, "FILTER") {
			t.Errorf("SQL missing SUM FILTER: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("AvgFilter", func(t *testing.T) {
		result, err := cereal.Select().
			SelectAvgFilter("age", "name", "=", "filter_val", "filtered_avg").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "AVG") {
			t.Errorf("SQL missing AVG: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("CountFilter", func(t *testing.T) {
		result, err := cereal.Select().
			SelectCountFilter("id", "name", "=", "filter_val", "filtered_count").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "COUNT") {
			t.Errorf("SQL missing COUNT: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})
}

// TestSelect_WindowFunctions tests window function expressions.
func TestSelect_WindowFunctions(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")

	db := &sqlx.DB{}
	cereal, err := New[selectTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("RowNumber with OrderBy", func(t *testing.T) {
		result, err := cereal.Select().
			SelectRowNumber().
			OrderBy("age", "DESC").
			As("row_num").
			End().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "ROW_NUMBER") {
			t.Errorf("SQL missing ROW_NUMBER: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "OVER") {
			t.Errorf("SQL missing OVER: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("Rank with PartitionBy and OrderBy", func(t *testing.T) {
		result, err := cereal.Select().
			SelectRank().
			PartitionBy("name").
			OrderBy("age", "DESC").
			As("rank").
			End().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "RANK") {
			t.Errorf("SQL missing RANK: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "PARTITION BY") {
			t.Errorf("SQL missing PARTITION BY: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("DenseRank", func(t *testing.T) {
		result, err := cereal.Select().
			SelectDenseRank().
			OrderBy("age", "ASC").
			As("dense_rank").
			End().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "DENSE_RANK") {
			t.Errorf("SQL missing DENSE_RANK: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("Lag", func(t *testing.T) {
		result, err := cereal.Select().
			SelectLag("age", "offset_val").
			OrderBy("id", "ASC").
			As("prev_age").
			End().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "LAG") {
			t.Errorf("SQL missing LAG: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("Lead", func(t *testing.T) {
		result, err := cereal.Select().
			SelectLead("age", "offset_val").
			OrderBy("id", "ASC").
			As("next_age").
			End().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "LEAD") {
			t.Errorf("SQL missing LEAD: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("FirstValue", func(t *testing.T) {
		result, err := cereal.Select().
			SelectFirstValue("age").
			OrderBy("id", "ASC").
			As("first_age").
			End().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "FIRST_VALUE") {
			t.Errorf("SQL missing FIRST_VALUE: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SumOver with PartitionBy", func(t *testing.T) {
		result, err := cereal.Select().
			SelectSumOver("age").
			PartitionBy("name").
			As("running_total").
			End().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "SUM") && !strings.Contains(result.SQL, "OVER") {
			t.Errorf("SQL missing SUM OVER: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("CountOver", func(t *testing.T) {
		result, err := cereal.Select().
			SelectCountOver().
			PartitionBy("name").
			As("category_count").
			End().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "COUNT") && !strings.Contains(result.SQL, "OVER") {
			t.Errorf("SQL missing COUNT OVER: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("Window with Frame", func(t *testing.T) {
		result, err := cereal.Select().
			SelectSumOver("age").
			OrderBy("id", "ASC").
			Frame("UNBOUNDED PRECEDING", "CURRENT ROW").
			As("running_sum").
			End().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "ROWS BETWEEN") || !strings.Contains(result.SQL, "UNBOUNDED PRECEDING") {
			t.Errorf("SQL missing frame clause: %s", result.SQL)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("Invalid frame bound", func(t *testing.T) {
		_, err := cereal.Select().
			SelectSumOver("age").
			Frame("INVALID", "CURRENT ROW").
			As("running_sum").
			End().
			Render()
		if err == nil {
			t.Error("expected error for invalid frame bound")
		}
	})
}
