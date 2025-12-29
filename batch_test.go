package soy

import (
	"context"
	"database/sql"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/pkg/postgres"
)

// Test model for batch tests.
type batchTestUser struct {
	ID    int    `db:"id" type:"integer" constraints:"primarykey"`
	Email string `db:"email" type:"text" constraints:"notnull,unique"`
	Name  string `db:"name" type:"text"`
	Age   *int   `db:"age" type:"integer"`
}

func TestExecuteBatch_RequiresWhere(t *testing.T) {
	db := &sqlx.DB{}

	soy, err := New[batchTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tbl, _ := soy.instance.TryT("users")
	builder := astql.Update(tbl)

	batchParams := []map[string]any{
		{"name": "Alice"},
	}

	ctx := context.Background()
	_, err = executeBatch(ctx, db, batchParams, builder, postgres.New(), "users", "UPDATE", false, nil)

	if err == nil {
		t.Error("executeBatch() should error without WHERE clause")
	}
}

func TestExecuteBatch_EmptyBatch(t *testing.T) {
	db := &sqlx.DB{}

	soy, err := New[batchTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tbl, _ := soy.instance.TryT("users")
	builder := astql.Update(tbl)

	var batchParams []map[string]any

	ctx := context.Background()
	affected, err := executeBatch(ctx, db, batchParams, builder, postgres.New(), "users", "UPDATE", true, nil)

	if err != nil {
		t.Errorf("executeBatch() error = %v", err)
	}

	if affected != 0 {
		t.Errorf("executeBatch() affected = %d, want 0", affected)
	}
}

func TestExecuteBatch_BuilderError(t *testing.T) {
	db := &sqlx.DB{}

	soy, err := New[batchTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tbl, _ := soy.instance.TryT("users")
	builder := astql.Update(tbl)

	batchParams := []map[string]any{
		{"name": "Alice"},
	}

	builderErr := sql.ErrNoRows

	ctx := context.Background()
	_, err = executeBatch(ctx, db, batchParams, builder, postgres.New(), "users", "UPDATE", true, builderErr)

	if err == nil {
		t.Error("executeBatch() should propagate builder error")
	}
}
