package soy

import (
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql/mariadb"
	"github.com/zoobzio/astql/postgres"
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
	soy, err := New[updateTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("UPDATE with SET and WHERE", func(t *testing.T) {
		result, err := soy.Modify().
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
		result, err := soy.Modify().
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
		result, err := soy.Modify().
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
		result, err := soy.Modify().
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
		result, err := soy.Modify().
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
		result, err := soy.Modify().
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
		result, err := soy.Modify().
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
		result, err := soy.Modify().
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

func TestUpdate_SetExpr(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[updateTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("SetExpr atomic increment", func(t *testing.T) {
		result, err := soy.Modify().
			SetExpr("age", "+", "increment").
			Where("id", "=", "user_id").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "SET") {
			t.Errorf("SQL missing SET: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"age"`) {
			t.Errorf("SQL missing age field: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, ":increment") {
			t.Errorf("SQL missing increment param: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("SetExpr with regular Set", func(t *testing.T) {
		result, err := soy.Modify().
			Set("name", "new_name").
			SetExpr("age", "+", "increment").
			Where("id", "=", "user_id").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, `"name"`) {
			t.Errorf("SQL missing name field: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"age"`) {
			t.Errorf("SQL missing age field: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("SetExpr invalid field", func(t *testing.T) {
		_, err := soy.Modify().
			SetExpr("nonexistent", "+", "increment").
			Where("id", "=", "user_id").
			Render()
		if err == nil {
			t.Error("expected error for invalid field")
		}
	})

	t.Run("SetExpr invalid operator", func(t *testing.T) {
		_, err := soy.Modify().
			SetExpr("age", "INVALID", "increment").
			Where("id", "=", "user_id").
			Render()
		if err == nil {
			t.Error("expected error for invalid operator")
		}
	})

	t.Run("SetExpr invalid param", func(t *testing.T) {
		_, err := soy.Modify().
			SetExpr("age", "+", "").
			Where("id", "=", "user_id").
			Render()
		if err == nil {
			t.Error("expected error for empty param")
		}
	})

	t.Run("SetExpr error propagates", func(t *testing.T) {
		_, err := soy.Modify().
			SetExpr("nonexistent", "+", "increment").
			Set("name", "new_name").
			Where("id", "=", "user_id").
			Render()
		if err == nil {
			t.Error("expected error to propagate through chain")
		}
	})
}

func TestUpdate_InstanceAccess(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[updateTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	builder := soy.Modify()
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
	soy, err := New[updateTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("successful MustRender", func(t *testing.T) {
		result := soy.Modify().
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
		soy.Modify().
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
		soy.Modify().
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
	soy, err := New[updateTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("all supported operators in WHERE", func(t *testing.T) {
		operators := []string{"=", "!=", ">", ">=", "<", "<="}
		for _, op := range operators {
			result, err := soy.Modify().
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
	soy, err := New[updateTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("ExecBatch renders query once for multiple param sets", func(t *testing.T) {
		result, err := soy.Modify().
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
		result, err := soy.Modify().
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
		result, err := soy.Modify().
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

func TestUpdate_FallbackSelect(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[updateTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("buildFallbackSelect with single Where", func(t *testing.T) {
		builder := soy.Modify().
			Set("name", "new_name").
			Where("id", "=", "user_id")

		selectBuilder, err := builder.buildFallbackSelect()
		if err != nil {
			t.Fatalf("buildFallbackSelect() failed: %v", err)
		}

		result, err := selectBuilder.Render(postgres.New())
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "SELECT") {
			t.Errorf("SQL missing SELECT: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "WHERE") {
			t.Errorf("SQL missing WHERE: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, ":user_id") {
			t.Errorf("SQL missing user_id param: %s", result.SQL)
		}

		t.Logf("Fallback SELECT SQL: %s", result.SQL)
	})

	t.Run("buildFallbackSelect with multiple Where (AND)", func(t *testing.T) {
		builder := soy.Modify().
			Set("name", "new_name").
			Where("id", "=", "user_id").
			Where("email", "=", "user_email")

		selectBuilder, err := builder.buildFallbackSelect()
		if err != nil {
			t.Fatalf("buildFallbackSelect() failed: %v", err)
		}

		result, err := selectBuilder.Render(postgres.New())
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "AND") {
			t.Errorf("SQL missing AND: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, ":user_id") {
			t.Errorf("SQL missing user_id param: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, ":user_email") {
			t.Errorf("SQL missing user_email param: %s", result.SQL)
		}

		t.Logf("Fallback SELECT SQL: %s", result.SQL)
	})

	t.Run("buildFallbackSelect with WhereAnd", func(t *testing.T) {
		builder := soy.Modify().
			Set("name", "new_name").
			WhereAnd(
				C("age", ">=", "min_age"),
				C("age", "<=", "max_age"),
			)

		selectBuilder, err := builder.buildFallbackSelect()
		if err != nil {
			t.Fatalf("buildFallbackSelect() failed: %v", err)
		}

		result, err := selectBuilder.Render(postgres.New())
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "AND") {
			t.Errorf("SQL missing AND: %s", result.SQL)
		}

		t.Logf("Fallback SELECT SQL: %s", result.SQL)
	})

	t.Run("buildFallbackSelect with WhereOr", func(t *testing.T) {
		builder := soy.Modify().
			Set("name", "new_name").
			WhereOr(
				C("age", "<", "young_age"),
				C("age", ">", "old_age"),
			)

		selectBuilder, err := builder.buildFallbackSelect()
		if err != nil {
			t.Fatalf("buildFallbackSelect() failed: %v", err)
		}

		result, err := selectBuilder.Render(postgres.New())
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "OR") {
			t.Errorf("SQL missing OR: %s", result.SQL)
		}

		t.Logf("Fallback SELECT SQL: %s", result.SQL)
	})

	t.Run("buildFallbackSelect includes all fields", func(t *testing.T) {
		builder := soy.Modify().
			Set("name", "new_name").
			Where("id", "=", "user_id")

		selectBuilder, err := builder.buildFallbackSelect()
		if err != nil {
			t.Fatalf("buildFallbackSelect() failed: %v", err)
		}

		result, err := selectBuilder.Render(postgres.New())
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		// Should include all fields from the struct
		if !strings.Contains(result.SQL, `"id"`) {
			t.Errorf("SQL missing id field: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"email"`) {
			t.Errorf("SQL missing email field: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"name"`) {
			t.Errorf("SQL missing name field: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"age"`) {
			t.Errorf("SQL missing age field: %s", result.SQL)
		}

		t.Logf("Fallback SELECT SQL: %s", result.SQL)
	})
}

func TestUpdate_ErrorPaths(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[updateTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("invalid Set field returns error", func(t *testing.T) {
		_, err := soy.Modify().
			Set("nonexistent", "value").
			Where("id", "=", "user_id").
			Render()
		if err == nil {
			t.Error("expected error for invalid Set field")
		}
		if !strings.Contains(err.Error(), "nonexistent") {
			t.Errorf("error should mention invalid field: %v", err)
		}
	})

	t.Run("invalid Set param returns error", func(t *testing.T) {
		_, err := soy.Modify().
			Set("name", "").
			Where("id", "=", "user_id").
			Render()
		if err == nil {
			t.Error("expected error for empty param")
		}
	})

	t.Run("invalid Where field returns error", func(t *testing.T) {
		_, err := soy.Modify().
			Set("name", "new_name").
			Where("nonexistent", "=", "value").
			Render()
		if err == nil {
			t.Error("expected error for invalid Where field")
		}
	})

	t.Run("invalid WhereAnd field returns error", func(t *testing.T) {
		_, err := soy.Modify().
			Set("name", "new_name").
			WhereAnd(C("nonexistent", "=", "value")).
			Render()
		if err == nil {
			t.Error("expected error for invalid WhereAnd field")
		}
	})

	t.Run("invalid WhereOr field returns error", func(t *testing.T) {
		_, err := soy.Modify().
			Set("name", "new_name").
			WhereOr(C("nonexistent", "=", "value")).
			Render()
		if err == nil {
			t.Error("expected error for invalid WhereOr field")
		}
	})

	t.Run("invalid WhereNull field returns error", func(t *testing.T) {
		_, err := soy.Modify().
			Set("name", "new_name").
			WhereNull("nonexistent").
			Render()
		if err == nil {
			t.Error("expected error for invalid WhereNull field")
		}
	})

	t.Run("invalid WhereNotNull field returns error", func(t *testing.T) {
		_, err := soy.Modify().
			Set("name", "new_name").
			WhereNotNull("nonexistent").
			Render()
		if err == nil {
			t.Error("expected error for invalid WhereNotNull field")
		}
	})

	t.Run("error propagates through chain", func(t *testing.T) {
		builder := soy.Modify().
			Set("bad_field", "value").
			Set("name", "new_name"). // Should not override error
			Where("id", "=", "user_id")
		_, err := builder.Render()
		if err == nil {
			t.Error("expected error to propagate through chain")
		}
	})

	t.Run("empty WhereAnd is ignored", func(t *testing.T) {
		result, err := soy.Modify().
			Set("name", "new_name").
			WhereAnd().
			Where("id", "=", "user_id").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		// Should still have WHERE from the regular Where call
		if !strings.Contains(result.SQL, "WHERE") {
			t.Errorf("SQL missing WHERE: %s", result.SQL)
		}
	})

	t.Run("empty WhereOr is ignored", func(t *testing.T) {
		result, err := soy.Modify().
			Set("name", "new_name").
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
		result, err := soy.Modify().
			Set("name", "new_name").
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
		result, err := soy.Modify().
			Set("name", "new_name").
			WhereOr(NotNull("age")).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, "IS NOT NULL") {
			t.Errorf("SQL missing IS NOT NULL: %s", result.SQL)
		}
	})
}

func TestUpdate_WhereBetween(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[updateTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("WhereBetween basic", func(t *testing.T) {
		result, err := soy.Modify().
			Set("name", "new_name").
			WhereBetween("age", "min_age", "max_age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "BETWEEN") {
			t.Errorf("SQL missing BETWEEN: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "AND") {
			t.Errorf("SQL missing AND: %s", result.SQL)
		}

		if len(result.RequiredParams) < 3 { // set param + between params
			t.Errorf("Expected at least 3 required params, got %d", len(result.RequiredParams))
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("WhereBetween with invalid field", func(t *testing.T) {
		_, err := soy.Modify().
			Set("name", "new_name").
			WhereBetween("nonexistent", "min", "max").
			Render()
		if err == nil {
			t.Error("Expected error for invalid field")
		}
	})
}

func TestUpdate_WhereNotBetween(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[updateTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("WhereNotBetween basic", func(t *testing.T) {
		result, err := soy.Modify().
			Set("name", "new_name").
			WhereNotBetween("age", "min_age", "max_age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "NOT BETWEEN") {
			t.Errorf("SQL missing NOT BETWEEN: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "AND") {
			t.Errorf("SQL missing AND: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("WhereNotBetween with invalid field", func(t *testing.T) {
		_, err := soy.Modify().
			Set("name", "new_name").
			WhereNotBetween("nonexistent", "min", "max").
			Render()
		if err == nil {
			t.Error("Expected error for invalid field")
		}
	})
}

func TestUpdate_DialectCapabilities(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}

	t.Run("PostgreSQL UPDATE includes RETURNING", func(t *testing.T) {
		soy, err := New[updateTestUser](db, "users", postgres.New())
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		result, err := soy.Modify().
			Set("name", "new_name").
			Where("id", "=", "user_id").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "RETURNING") {
			t.Errorf("PostgreSQL UPDATE should include RETURNING: %s", result.SQL)
		}

		t.Logf("PostgreSQL SQL: %s", result.SQL)
	})

	t.Run("MariaDB UPDATE excludes RETURNING", func(t *testing.T) {
		soy, err := New[updateTestUser](db, "users", mariadb.New())
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		result, err := soy.Modify().
			Set("name", "new_name").
			Where("id", "=", "user_id").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		// MariaDB doesn't support RETURNING on UPDATE (MDEV-5092)
		if strings.Contains(result.SQL, "RETURNING") {
			t.Errorf("MariaDB UPDATE should NOT include RETURNING: %s", result.SQL)
		}

		t.Logf("MariaDB SQL: %s", result.SQL)
	})

	t.Run("capability check determines RETURNING inclusion", func(t *testing.T) {
		// PostgreSQL has ReturningOnUpdate = true
		pgCaps := postgres.New().Capabilities()
		if !pgCaps.ReturningOnUpdate {
			t.Error("PostgreSQL should support ReturningOnUpdate")
		}

		// MariaDB has ReturningOnUpdate = false
		mariaCaps := mariadb.New().Capabilities()
		if mariaCaps.ReturningOnUpdate {
			t.Error("MariaDB should NOT support ReturningOnUpdate")
		}
	})
}
