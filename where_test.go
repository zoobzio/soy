package soy

import (
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/pkg/postgres"
	"github.com/zoobzio/sentinel"
)

// Test model for where tests.
type whereTestUser struct {
	ID    int    `db:"id" type:"integer" constraints:"primarykey"`
	Email string `db:"email" type:"text" constraints:"notnull,unique"`
	Name  string `db:"name" type:"text"`
	Age   *int   `db:"age" type:"integer"`
}

func TestWhereBuilder_AddWhere(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[whereTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tbl, _ := soy.instance.TryT("users")
	builder := astql.Select(tbl)

	wb := newWhereBuilder(soy.instance, builder)
	newBuilder, err := wb.addWhere("age", ">=", "min_age")

	if err != nil {
		t.Errorf("addWhere() error = %v", err)
	}

	if newBuilder == nil {
		t.Fatal("addWhere() returned nil builder")
	}

	result, err := newBuilder.Render(postgres.New())
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "WHERE") {
		t.Error("SQL missing WHERE clause")
	}

	if !strings.Contains(result.SQL, `"age"`) {
		t.Error("SQL missing age field")
	}
}

func TestWhereBuilder_AddWhere_InvalidField(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[whereTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tbl, _ := soy.instance.TryT("users")
	builder := astql.Select(tbl)

	wb := newWhereBuilder(soy.instance, builder)
	_, err = wb.addWhere("nonexistent_field", "=", "value")

	if err == nil {
		t.Error("addWhere() should error with invalid field")
	}
}

func TestWhereBuilder_AddWhere_InvalidOperator(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[whereTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tbl, _ := soy.instance.TryT("users")
	builder := astql.Select(tbl)

	wb := newWhereBuilder(soy.instance, builder)
	_, err = wb.addWhere("age", "INVALID", "value")

	if err == nil {
		t.Error("addWhere() should error with invalid operator")
	}
}

func TestWhereBuilder_AddWhereAnd(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[whereTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tbl, _ := soy.instance.TryT("users")
	builder := astql.Select(tbl)

	wb := newWhereBuilder(soy.instance, builder)
	conditions := []Condition{
		C("age", ">=", "min_age"),
		C("age", "<=", "max_age"),
	}

	newBuilder, err := wb.addWhereAnd(conditions...)

	if err != nil {
		t.Errorf("addWhereAnd() error = %v", err)
	}

	result, err := newBuilder.Render(postgres.New())
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "AND") {
		t.Error("SQL missing AND operator")
	}
}

func TestWhereBuilder_AddWhereAnd_Empty(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[whereTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tbl, _ := soy.instance.TryT("users")
	builder := astql.Select(tbl)

	wb := newWhereBuilder(soy.instance, builder)
	newBuilder, err := wb.addWhereAnd()

	if err != nil {
		t.Errorf("addWhereAnd() error = %v", err)
	}

	if newBuilder != builder {
		t.Error("addWhereAnd() with no conditions should return original builder")
	}
}

func TestWhereBuilder_AddWhereOr(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[whereTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tbl, _ := soy.instance.TryT("users")
	builder := astql.Select(tbl)

	wb := newWhereBuilder(soy.instance, builder)
	conditions := []Condition{
		C("age", "<", "young_age"),
		C("age", ">", "old_age"),
	}

	newBuilder, err := wb.addWhereOr(conditions...)

	if err != nil {
		t.Errorf("addWhereOr() error = %v", err)
	}

	result, err := newBuilder.Render(postgres.New())
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "OR") {
		t.Error("SQL missing OR operator")
	}
}

func TestWhereBuilder_AddWhereNull(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[whereTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tbl, _ := soy.instance.TryT("users")
	builder := astql.Select(tbl)

	wb := newWhereBuilder(soy.instance, builder)
	newBuilder, err := wb.addWhereNull("age")

	if err != nil {
		t.Errorf("addWhereNull() error = %v", err)
	}

	result, err := newBuilder.Render(postgres.New())
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "IS NULL") {
		t.Error("SQL missing IS NULL clause")
	}
}

func TestWhereBuilder_AddWhereNull_InvalidField(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[whereTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tbl, _ := soy.instance.TryT("users")
	builder := astql.Select(tbl)

	wb := newWhereBuilder(soy.instance, builder)
	_, err = wb.addWhereNull("nonexistent_field")

	if err == nil {
		t.Error("addWhereNull() should error with invalid field")
	}
}

func TestWhereBuilder_AddWhereNotNull(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[whereTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tbl, _ := soy.instance.TryT("users")
	builder := astql.Select(tbl)

	wb := newWhereBuilder(soy.instance, builder)
	newBuilder, err := wb.addWhereNotNull("age")

	if err != nil {
		t.Errorf("addWhereNotNull() error = %v", err)
	}

	result, err := newBuilder.Render(postgres.New())
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "IS NOT NULL") {
		t.Error("SQL missing IS NOT NULL clause")
	}
}

func TestWhereBuilder_BuildCondition(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[whereTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tbl, _ := soy.instance.TryT("users")
	builder := astql.Select(tbl)
	wb := newWhereBuilder(soy.instance, builder)

	t.Run("standard condition", func(t *testing.T) {
		cond := C("age", ">=", "min_age")
		condItem, err := wb.buildCondition(cond)

		if err != nil {
			t.Errorf("buildCondition() error = %v", err)
		}

		if condItem == nil {
			t.Fatal("buildCondition() returned nil")
		}
	})

	t.Run("IS NULL condition", func(t *testing.T) {
		cond := Null("age")
		condItem, err := wb.buildCondition(cond)

		if err != nil {
			t.Errorf("buildCondition() error = %v", err)
		}

		if condItem == nil {
			t.Fatal("buildCondition() returned nil")
		}
	})

	t.Run("IS NOT NULL condition", func(t *testing.T) {
		cond := NotNull("age")
		condItem, err := wb.buildCondition(cond)

		if err != nil {
			t.Errorf("buildCondition() error = %v", err)
		}

		if condItem == nil {
			t.Fatal("buildCondition() returned nil")
		}
	})

	t.Run("invalid field", func(t *testing.T) {
		cond := C("nonexistent_field", "=", "value")
		_, err := wb.buildCondition(cond)

		if err == nil {
			t.Error("buildCondition() should error with invalid field")
		}
	})

	t.Run("invalid operator", func(t *testing.T) {
		cond := C("age", "INVALID", "value")
		_, err := wb.buildCondition(cond)

		if err == nil {
			t.Error("buildCondition() should error with invalid operator")
		}
	})
}

func TestWhereBuilder_OperatorConstants(t *testing.T) {
	if opIsNull != "IS NULL" {
		t.Errorf("opIsNull = %q, want %q", opIsNull, "IS NULL")
	}

	if opIsNotNull != "IS NOT NULL" {
		t.Errorf("opIsNotNull = %q, want %q", opIsNotNull, "IS NOT NULL")
	}

	if opBetween != "BETWEEN" {
		t.Errorf("opBetween = %q, want %q", opBetween, "BETWEEN")
	}

	if opNotBetween != "NOT BETWEEN" {
		t.Errorf("opNotBetween = %q, want %q", opNotBetween, "NOT BETWEEN")
	}
}

func TestWhereBuilder_AddWhereBetween(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[whereTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tbl, _ := soy.instance.TryT("users")
	builder := astql.Select(tbl)

	wb := newWhereBuilder(soy.instance, builder)
	newBuilder, err := wb.addWhereBetween("age", "min_age", "max_age")

	if err != nil {
		t.Errorf("addWhereBetween() error = %v", err)
	}

	result, err := newBuilder.Render(postgres.New())
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "BETWEEN") {
		t.Error("SQL missing BETWEEN clause")
	}

	if !strings.Contains(result.SQL, ":min_age") {
		t.Error("SQL missing min_age param")
	}

	if !strings.Contains(result.SQL, ":max_age") {
		t.Error("SQL missing max_age param")
	}

	t.Logf("Generated SQL: %s", result.SQL)
}

func TestWhereBuilder_AddWhereBetween_InvalidField(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[whereTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tbl, _ := soy.instance.TryT("users")
	builder := astql.Select(tbl)

	wb := newWhereBuilder(soy.instance, builder)
	_, err = wb.addWhereBetween("nonexistent_field", "min", "max")

	if err == nil {
		t.Error("addWhereBetween() should error with invalid field")
	}
}

func TestWhereBuilder_AddWhereNotBetween(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[whereTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tbl, _ := soy.instance.TryT("users")
	builder := astql.Select(tbl)

	wb := newWhereBuilder(soy.instance, builder)
	newBuilder, err := wb.addWhereNotBetween("age", "min_age", "max_age")

	if err != nil {
		t.Errorf("addWhereNotBetween() error = %v", err)
	}

	result, err := newBuilder.Render(postgres.New())
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "NOT BETWEEN") {
		t.Error("SQL missing NOT BETWEEN clause")
	}

	t.Logf("Generated SQL: %s", result.SQL)
}

func TestWhereBuilder_BuildCondition_Between(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[whereTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tbl, _ := soy.instance.TryT("users")
	builder := astql.Select(tbl)
	wb := newWhereBuilder(soy.instance, builder)

	t.Run("BETWEEN condition", func(t *testing.T) {
		cond := Between("age", "min_age", "max_age")
		condItem, err := wb.buildCondition(cond)

		if err != nil {
			t.Errorf("buildCondition() error = %v", err)
		}

		if condItem == nil {
			t.Fatal("buildCondition() returned nil")
		}
	})

	t.Run("NOT BETWEEN condition", func(t *testing.T) {
		cond := NotBetween("age", "min_age", "max_age")
		condItem, err := wb.buildCondition(cond)

		if err != nil {
			t.Errorf("buildCondition() error = %v", err)
		}

		if condItem == nil {
			t.Fatal("buildCondition() returned nil")
		}
	})

	t.Run("BETWEEN with invalid field", func(t *testing.T) {
		cond := Between("nonexistent_field", "min", "max")
		_, err := wb.buildCondition(cond)

		if err == nil {
			t.Error("buildCondition() should error with invalid field")
		}
	})
}

func TestConditionHelpers_Between(t *testing.T) {
	t.Run("Between helper", func(t *testing.T) {
		cond := Between("age", "min_age", "max_age")

		if cond.field != "age" {
			t.Errorf("field = %q, want %q", cond.field, "age")
		}
		if cond.operator != opBetween {
			t.Errorf("operator = %q, want %q", cond.operator, opBetween)
		}
		if !cond.isBetween {
			t.Error("isBetween should be true")
		}
		if cond.lowParam != "min_age" {
			t.Errorf("lowParam = %q, want %q", cond.lowParam, "min_age")
		}
		if cond.highParam != "max_age" {
			t.Errorf("highParam = %q, want %q", cond.highParam, "max_age")
		}
	})

	t.Run("NotBetween helper", func(t *testing.T) {
		cond := NotBetween("age", "min_age", "max_age")

		if cond.field != "age" {
			t.Errorf("field = %q, want %q", cond.field, "age")
		}
		if cond.operator != opNotBetween {
			t.Errorf("operator = %q, want %q", cond.operator, opNotBetween)
		}
		if !cond.isBetween {
			t.Error("isBetween should be true")
		}
		if cond.lowParam != "min_age" {
			t.Errorf("lowParam = %q, want %q", cond.lowParam, "min_age")
		}
		if cond.highParam != "max_age" {
			t.Errorf("highParam = %q, want %q", cond.highParam, "max_age")
		}
	})
}
