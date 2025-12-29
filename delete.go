package soy

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql"
	"github.com/zoobzio/capitan"
)

// Delete provides a focused API for building DELETE queries.
// It wraps ASTQL's DELETE functionality with a simple string-based interface.
// Use this for deleting records from the database.
type Delete[T any] struct {
	instance *astql.ASTQL
	builder  *astql.Builder
	soy      soyExecutor // interface for execution
	hasWhere bool        // tracks if WHERE was called
	err      error       // stores first error encountered during building
}

// Where adds a simple WHERE condition with field = param pattern.
// Multiple calls are combined with AND.
// IMPORTANT: At least one WHERE condition is required to prevent accidental full-table deletes.
//
// Example:
//
//	.Where("id", "=", "user_id")
func (db *Delete[T]) Where(field, operator, param string) *Delete[T] {
	if db.err != nil {
		return db
	}

	wb := newWhereBuilder(db.instance, db.builder)
	builder, err := wb.addWhere(field, operator, param)
	if err != nil {
		db.err = err
		return db
	}

	db.builder = builder
	db.hasWhere = true
	return db
}

// WhereAnd adds multiple conditions combined with AND.
//
// Example:
//
//	.WhereAnd(
//	    soy.C("age", ">=", "min_age"),
//	    soy.C("status", "=", "inactive"),
//	)
func (db *Delete[T]) WhereAnd(conditions ...Condition) *Delete[T] {
	if db.err != nil {
		return db
	}

	if len(conditions) == 0 {
		return db
	}

	conditionItems := db.instance.ConditionItems()
	for _, cond := range conditions {
		condItem, err := db.buildCondition(cond)
		if err != nil {
			db.err = err
			return db
		}
		conditionItems = append(conditionItems, condItem)
	}

	andGroup, err := db.instance.TryAnd(conditionItems...)
	if err != nil {
		db.err = fmt.Errorf("invalid AND condition: %w", err)
		return db
	}

	db.builder = db.builder.Where(andGroup)
	db.hasWhere = true
	return db
}

// WhereOr adds multiple conditions combined with OR.
//
// Example:
//
//	.WhereOr(
//	    soy.C("status", "=", "deleted"),
//	    soy.C("status", "=", "archived"),
//	)
func (db *Delete[T]) WhereOr(conditions ...Condition) *Delete[T] {
	if db.err != nil {
		return db
	}

	if len(conditions) == 0 {
		return db
	}

	conditionItems := db.instance.ConditionItems()
	for _, cond := range conditions {
		condItem, err := db.buildCondition(cond)
		if err != nil {
			db.err = err
			return db
		}
		conditionItems = append(conditionItems, condItem)
	}

	orGroup, err := db.instance.TryOr(conditionItems...)
	if err != nil {
		db.err = fmt.Errorf("invalid OR condition: %w", err)
		return db
	}

	db.builder = db.builder.Where(orGroup)
	db.hasWhere = true
	return db
}

// WhereNull adds a WHERE field IS NULL condition.
func (db *Delete[T]) WhereNull(field string) *Delete[T] {
	if db.err != nil {
		return db
	}

	f, err := db.instance.TryF(field)
	if err != nil {
		db.err = fmt.Errorf("invalid field %q: %w", field, err)
		return db
	}

	condition, err := db.instance.TryNull(f)
	if err != nil {
		db.err = fmt.Errorf("invalid NULL condition: %w", err)
		return db
	}

	db.builder = db.builder.Where(condition)
	db.hasWhere = true
	return db
}

// WhereNotNull adds a WHERE field IS NOT NULL condition.
func (db *Delete[T]) WhereNotNull(field string) *Delete[T] {
	if db.err != nil {
		return db
	}

	f, err := db.instance.TryF(field)
	if err != nil {
		db.err = fmt.Errorf("invalid field %q: %w", field, err)
		return db
	}

	condition, err := db.instance.TryNotNull(f)
	if err != nil {
		db.err = fmt.Errorf("invalid NOT NULL condition: %w", err)
		return db
	}

	db.builder = db.builder.Where(condition)
	db.hasWhere = true
	return db
}

// WhereBetween adds a WHERE field BETWEEN low AND high condition.
// Multiple calls are combined with AND.
//
// Example:
//
//	.WhereBetween("age", "min_age", "max_age")
//	// params: map[string]any{"min_age": 18, "max_age": 65}
func (db *Delete[T]) WhereBetween(field, lowParam, highParam string) *Delete[T] {
	if db.err != nil {
		return db
	}

	f, err := db.instance.TryF(field)
	if err != nil {
		db.err = fmt.Errorf("invalid field %q: %w", field, err)
		return db
	}

	lowP, err := db.instance.TryP(lowParam)
	if err != nil {
		db.err = fmt.Errorf("invalid low param %q: %w", lowParam, err)
		return db
	}

	highP, err := db.instance.TryP(highParam)
	if err != nil {
		db.err = fmt.Errorf("invalid high param %q: %w", highParam, err)
		return db
	}

	db.builder = db.builder.Where(astql.Between(f, lowP, highP))
	db.hasWhere = true
	return db
}

// WhereNotBetween adds a WHERE field NOT BETWEEN low AND high condition.
// Multiple calls are combined with AND.
//
// Example:
//
//	.WhereNotBetween("age", "min_age", "max_age")
//	// params: map[string]any{"min_age": 18, "max_age": 65}
func (db *Delete[T]) WhereNotBetween(field, lowParam, highParam string) *Delete[T] {
	if db.err != nil {
		return db
	}

	f, err := db.instance.TryF(field)
	if err != nil {
		db.err = fmt.Errorf("invalid field %q: %w", field, err)
		return db
	}

	lowP, err := db.instance.TryP(lowParam)
	if err != nil {
		db.err = fmt.Errorf("invalid low param %q: %w", lowParam, err)
		return db
	}

	highP, err := db.instance.TryP(highParam)
	if err != nil {
		db.err = fmt.Errorf("invalid high param %q: %w", highParam, err)
		return db
	}

	db.builder = db.builder.Where(astql.NotBetween(f, lowP, highP))
	db.hasWhere = true
	return db
}

// WhereFields adds a WHERE condition comparing two fields.
// Multiple calls are combined with AND.
//
// Example:
//
//	.WhereFields("created_at", "<", "updated_at")
//	// WHERE "created_at" < "updated_at"
func (db *Delete[T]) WhereFields(leftField, operator, rightField string) *Delete[T] {
	if db.err != nil {
		return db
	}

	astqlOp, err := validateOperator(operator)
	if err != nil {
		db.err = err
		return db
	}

	left, err := db.instance.TryF(leftField)
	if err != nil {
		db.err = fmt.Errorf("invalid left field %q: %w", leftField, err)
		return db
	}

	right, err := db.instance.TryF(rightField)
	if err != nil {
		db.err = fmt.Errorf("invalid right field %q: %w", rightField, err)
		return db
	}

	db.builder = db.builder.Where(astql.CF(left, astqlOp, right))
	db.hasWhere = true
	return db
}

// buildCondition converts a Condition to an ASTQL condition.
func (db *Delete[T]) buildCondition(cond Condition) (astql.ConditionItem, error) {
	return buildConditionWithInstance(db.instance, cond)
}

// Exec executes the DELETE query with values from the provided params map.
// Returns the number of rows deleted.
// Requires at least one WHERE condition to prevent accidental full-table deletes.
//
// Example:
//
//	params := map[string]any{"user_id": 123}
//	rowsDeleted, err := soy.Delete().
//	    Where("id", "=", "user_id").
//	    Exec(ctx, params)
func (db *Delete[T]) Exec(ctx context.Context, params map[string]any) (int64, error) {
	return db.exec(ctx, db.soy.execer(), params)
}

// ExecTx executes the DELETE query within a transaction.
func (db *Delete[T]) ExecTx(ctx context.Context, tx *sqlx.Tx, params map[string]any) (int64, error) {
	return db.exec(ctx, tx, params)
}

// ExecBatch executes the DELETE query for multiple parameter sets.
// Returns the total number of rows deleted.
// Each parameter set is executed separately with the same WHERE clause.
//
// Example:
//
//	batchParams := []map[string]any{
//	    {"user_id": 1},
//	    {"user_id": 2},
//	}
//	rowsDeleted, err := soy.Remove().
//	    Where("id", "=", "user_id").
//	    ExecBatch(ctx, batchParams)
func (db *Delete[T]) ExecBatch(ctx context.Context, batchParams []map[string]any) (int64, error) {
	return db.execBatch(ctx, db.soy.execer(), batchParams)
}

// ExecBatchTx executes the DELETE query for multiple parameter sets within a transaction.
// Returns the total number of rows deleted.
func (db *Delete[T]) ExecBatchTx(ctx context.Context, tx *sqlx.Tx, batchParams []map[string]any) (int64, error) {
	return db.execBatch(ctx, tx, batchParams)
}

// execBatch is the internal batch execution method.
func (db *Delete[T]) execBatch(ctx context.Context, execer sqlx.ExtContext, batchParams []map[string]any) (int64, error) {
	return executeBatch(ctx, execer, batchParams, db.builder, db.soy.renderer(), db.soy.getTableName(), "DELETE", db.hasWhere, db.err)
}

// exec is the internal execution method used by both Exec and ExecTx.
func (db *Delete[T]) exec(ctx context.Context, execer sqlx.ExtContext, params map[string]any) (int64, error) {
	// Check for  errors first
	if db.err != nil {
		return 0, fmt.Errorf("delete  has errors: %w", db.err)
	}

	// Safety check: require WHERE clause
	if !db.hasWhere {
		return 0, fmt.Errorf("DELETE requires at least one WHERE condition to prevent accidental full-table deletion")
	}

	// Render the query
	result, err := db.builder.Render(db.soy.renderer())
	if err != nil {
		return 0, fmt.Errorf("failed to render DELETE query: %w", err)
	}

	// Emit query started event
	tableName := db.soy.getTableName()
	capitan.Debug(ctx, QueryStarted,
		TableKey.Field(tableName),
		OperationKey.Field("DELETE"),
		SQLKey.Field(result.SQL),
	)

	startTime := time.Now()

	// Execute named query
	res, err := sqlx.NamedExecContext(ctx, execer, result.SQL, params)
	if err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field("DELETE"),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return 0, fmt.Errorf("DELETE failed: %w", err)
	}

	// Get affected rows
	affected, err := res.RowsAffected()
	if err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field("DELETE"),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	// Emit query completed event
	durationMs := time.Since(startTime).Milliseconds()
	capitan.Info(ctx, QueryCompleted,
		TableKey.Field(tableName),
		OperationKey.Field("DELETE"),
		DurationMsKey.Field(durationMs),
		RowsAffectedKey.Field(affected),
	)

	return affected, nil
}

// Render builds and renders the query to SQL with parameter placeholders.
// Returns the SQL string and list of required parameters.
// Useful for inspection/debugging before execution.
func (db *Delete[T]) Render() (*astql.QueryResult, error) {
	// Check for  errors first
	if db.err != nil {
		return nil, fmt.Errorf("delete  has errors: %w", db.err)
	}

	result, err := db.builder.Render(db.soy.renderer())
	if err != nil {
		return nil, fmt.Errorf("failed to render DELETE query: %w", err)
	}
	return result, nil
}

// MustRender is like Render but panics on error.
// This is intentionally preserved for cases where panicking is desired (e.g., tests, initialization).
func (db *Delete[T]) MustRender() *astql.QueryResult {
	result, err := db.Render()
	if err != nil {
		panic(err)
	}
	return result
}

// Instance returns the underlying ASTQL instance for advanced query building.
// Use this escape hatch when you need ASTQL features not exposed by Delete.
func (db *Delete[T]) Instance() *astql.ASTQL {
	return db.instance
}
