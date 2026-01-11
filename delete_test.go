package soy

import (
	"context"
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql/postgres"
	"github.com/zoobzio/sentinel"
)

type deleteTestUser struct {
	ID    int    `db:"id" type:"integer" constraints:"primarykey"`
	Email string `db:"email" type:"text" constraints:"notnull,unique"`
	Name  string `db:"name" type:"text"`
	Age   *int   `db:"age" type:"integer"`
}

func TestDelete_Basic(t *testing.T) {
	// Register tags
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")

	db := &sqlx.DB{}
	soy, err := New[deleteTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("DELETE with WHERE", func(t *testing.T) {
		result, err := soy.Remove().
			Where("id", "=", "user_id").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "DELETE FROM") {
			t.Errorf("SQL missing DELETE FROM: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"users"`) {
			t.Errorf("SQL missing table name: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "WHERE") {
			t.Errorf("SQL missing WHERE: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"id"`) {
			t.Errorf("SQL missing id field: %s", result.SQL)
		}

		// Check required parameters
		if len(result.RequiredParams) != 1 {
			t.Errorf("Expected 1 required param, got %d", len(result.RequiredParams))
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("DELETE with multiple WHERE (AND)", func(t *testing.T) {
		result, err := soy.Remove().
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

	t.Run("DELETE with WhereAnd", func(t *testing.T) {
		result, err := soy.Remove().
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

	t.Run("DELETE with WhereOr", func(t *testing.T) {
		result, err := soy.Remove().
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

	t.Run("DELETE with WhereNull", func(t *testing.T) {
		result, err := soy.Remove().
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

	t.Run("DELETE with WhereNotNull", func(t *testing.T) {
		result, err := soy.Remove().
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

func TestDelete_SafetyChecks(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[deleteTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("Exec fails without WHERE clause", func(t *testing.T) {
		ctx := context.Background()
		params := map[string]any{}

		_, err := soy.Remove().Exec(ctx, params)
		if err == nil {
			t.Error("Expected error when executing DELETE without WHERE")
		}
		if !strings.Contains(err.Error(), "WHERE condition") {
			t.Errorf("Error message should mention WHERE condition, got: %s", err.Error())
		}
	})

	t.Run("Render succeeds with WHERE clause", func(t *testing.T) {
		builder := soy.Remove().Where("id", "=", "user_id")

		// Render should work
		result, err := builder.Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if result.SQL == "" {
			t.Error("Render() returned empty SQL")
		}
		if !strings.Contains(result.SQL, "WHERE") {
			t.Errorf("Expected WHERE clause in SQL: %s", result.SQL)
		}
	})
}

func TestDelete_InstanceAccess(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[deleteTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	builder := soy.Remove()
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

func TestDelete_MustRender(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[deleteTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("successful MustRender", func(t *testing.T) {
		result := soy.Remove().
			Where("id", "=", "user_id").
			MustRender()
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
		soy.Remove().
			Where("nonexistent_field", "=", "value").
			MustRender()
	})

	t.Run("MustRender panics on invalid operator", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("MustRender() did not panic with invalid operator")
			}
		}()
		soy.Remove().
			Where("id", "INVALID", "user_id").
			MustRender()
	})
}

func TestDelete_Validation(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[deleteTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("all supported operators in WHERE", func(t *testing.T) {
		operators := []string{"=", "!=", ">", ">=", "<", "<="}
		for _, op := range operators {
			result, err := soy.Remove().
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

func TestDelete_BatchOperations(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[deleteTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("ExecBatch renders query once for multiple param sets", func(t *testing.T) {
		result, err := soy.Remove().
			Where("id", "=", "user_id").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "DELETE FROM") {
			t.Errorf("SQL missing DELETE FROM: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "WHERE") {
			t.Errorf("SQL missing WHERE: %s", result.SQL)
		}

		t.Logf("Batch delete SQL: %s", result.SQL)
	})

	t.Run("ExecBatch with complex WHERE", func(t *testing.T) {
		result, err := soy.Remove().
			WhereAnd(
				C("age", ">=", "min_age"),
				C("email", "=", "user_email"),
			).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "AND") {
			t.Errorf("SQL missing AND: %s", result.SQL)
		}

		t.Logf("Batch delete with AND SQL: %s", result.SQL)
	})

	t.Run("ExecBatch enforces WHERE requirement", func(t *testing.T) {
		ctx := context.Background()
		batchParams := []map[string]any{
			{"user_id": 1},
			{"user_id": 2},
		}

		_, err := soy.Remove().ExecBatch(ctx, batchParams)
		if err == nil {
			t.Error("Expected error when executing batch DELETE without WHERE")
		}
		if !strings.Contains(err.Error(), "WHERE condition") {
			t.Errorf("Error message should mention WHERE condition, got: %s", err.Error())
		}
	})
}

func TestDelete_ErrorPaths(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[deleteTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("invalid Where field returns error", func(t *testing.T) {
		_, err := soy.Remove().
			Where("nonexistent", "=", "value").
			Render()
		if err == nil {
			t.Error("expected error for invalid Where field")
		}
	})

	t.Run("invalid WhereAnd field returns error", func(t *testing.T) {
		_, err := soy.Remove().
			WhereAnd(C("nonexistent", "=", "value")).
			Render()
		if err == nil {
			t.Error("expected error for invalid WhereAnd field")
		}
	})

	t.Run("invalid WhereOr field returns error", func(t *testing.T) {
		_, err := soy.Remove().
			WhereOr(C("nonexistent", "=", "value")).
			Render()
		if err == nil {
			t.Error("expected error for invalid WhereOr field")
		}
	})

	t.Run("invalid WhereNull field returns error", func(t *testing.T) {
		_, err := soy.Remove().
			WhereNull("nonexistent").
			Render()
		if err == nil {
			t.Error("expected error for invalid WhereNull field")
		}
	})

	t.Run("invalid WhereNotNull field returns error", func(t *testing.T) {
		_, err := soy.Remove().
			WhereNotNull("nonexistent").
			Render()
		if err == nil {
			t.Error("expected error for invalid WhereNotNull field")
		}
	})

	t.Run("error propagates through chain", func(t *testing.T) {
		builder := soy.Remove().
			Where("bad_field", "=", "value").
			Where("id", "=", "user_id") // Should not override error
		_, err := builder.Render()
		if err == nil {
			t.Error("expected error to propagate through chain")
		}
	})

	t.Run("empty WhereAnd is ignored", func(t *testing.T) {
		result, err := soy.Remove().
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
		result, err := soy.Remove().
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
		result, err := soy.Remove().
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
		result, err := soy.Remove().
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
		_, err := soy.Remove().
			WhereAnd(C("age", "INVALID", "value")).
			Render()
		if err == nil {
			t.Error("expected error for invalid operator")
		}
	})
}

func TestDelete_WhereBetween(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[deleteTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("WhereBetween basic", func(t *testing.T) {
		result, err := soy.Remove().
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

		if len(result.RequiredParams) != 2 {
			t.Errorf("Expected 2 required params, got %d", len(result.RequiredParams))
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("WhereBetween with invalid field", func(t *testing.T) {
		_, err := soy.Remove().
			WhereBetween("nonexistent", "min", "max").
			Render()
		if err == nil {
			t.Error("Expected error for invalid field")
		}
	})
}

func TestDelete_WhereNotBetween(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[deleteTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("WhereNotBetween basic", func(t *testing.T) {
		result, err := soy.Remove().
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
		_, err := soy.Remove().
			WhereNotBetween("nonexistent", "min", "max").
			Render()
		if err == nil {
			t.Error("Expected error for invalid field")
		}
	})
}

func TestDelete_WhereFields(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[deleteTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("WhereFields basic", func(t *testing.T) {
		result, err := soy.Remove().
			WhereFields("email", "=", "name").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "WHERE") {
			t.Errorf("SQL missing WHERE: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"email"`) {
			t.Errorf("SQL missing email field: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"name"`) {
			t.Errorf("SQL missing name field: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("WhereFields with invalid left field", func(t *testing.T) {
		_, err := soy.Remove().
			WhereFields("nonexistent", "=", "name").
			Render()
		if err == nil {
			t.Error("Expected error for invalid field")
		}
	})

	t.Run("WhereFields with invalid right field", func(t *testing.T) {
		_, err := soy.Remove().
			WhereFields("email", "=", "nonexistent").
			Render()
		if err == nil {
			t.Error("Expected error for invalid field")
		}
	})

	t.Run("WhereFields with invalid operator", func(t *testing.T) {
		_, err := soy.Remove().
			WhereFields("email", "INVALID", "name").
			Render()
		if err == nil {
			t.Error("Expected error for invalid operator")
		}
	})
}
