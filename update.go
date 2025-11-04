package cereal

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql"
	"github.com/zoobzio/capitan"
)

// Update provides a focused API for building UPDATE queries.
// It wraps ASTQL's UPDATE functionality with a simple string-based interface.
// Use this for updating existing records in the database.
type Update[T any] struct {
	instance *astql.ASTQL
	builder  *astql.Builder
	cereal   cerealExecutor // interface for execution
	hasWhere bool           // tracks if WHERE was called
	err      error          // stores first error encountered during building
}

// Set specifies a field to update with a parameter value.
// Multiple calls add more fields to the SET clause.
//
// Example:
//
//	cereal.Update().
//	    Set("name", "new_name").
//	    Set("age", "new_age").
//	    Where("id", "=", "user_id").
//	    Exec(ctx, params)
func (ub *Update[T]) Set(field, param string) *Update[T] {
	if ub.err != nil {
		return ub
	}

	f, err := ub.instance.TryF(field)
	if err != nil {
		ub.err = fmt.Errorf("invalid field %q: %w", field, err)
		return ub
	}

	p, err := ub.instance.TryP(param)
	if err != nil {
		ub.err = fmt.Errorf("invalid param %q: %w", param, err)
		return ub
	}

	ub.builder = ub.builder.Set(f, p)
	return ub
}

// Where adds a simple WHERE condition with field = param pattern.
// Multiple calls are combined with AND.
// IMPORTANT: At least one WHERE condition is required to prevent accidental full-table updates.
//
// Example:
//
//	.Where("id", "=", "user_id")
func (ub *Update[T]) Where(field, operator, param string) *Update[T] {
	if ub.err != nil {
		return ub
	}

	wb := newWhereBuilder(ub.instance, ub.builder)
	builder, err := wb.addWhere(field, operator, param)
	if err != nil {
		ub.err = err
		return ub
	}

	ub.builder = builder
	ub.hasWhere = true
	return ub
}

// WhereAnd adds multiple conditions combined with AND.
//
// Example:
//
//	.WhereAnd(
//	    cereal.C("age", ">=", "min_age"),
//	    cereal.C("status", "=", "active"),
//	)
func (ub *Update[T]) WhereAnd(conditions ...Condition) *Update[T] {
	if ub.err != nil {
		return ub
	}

	if len(conditions) == 0 {
		return ub
	}

	conditionItems := ub.instance.ConditionItems()
	for _, cond := range conditions {
		condItem, err := ub.buildCondition(cond)
		if err != nil {
			ub.err = err
			return ub
		}
		conditionItems = append(conditionItems, condItem)
	}

	andGroup, err := ub.instance.TryAnd(conditionItems...)
	if err != nil {
		ub.err = fmt.Errorf("invalid AND condition: %w", err)
		return ub
	}

	ub.builder = ub.builder.Where(andGroup)
	ub.hasWhere = true
	return ub
}

// WhereOr adds multiple conditions combined with OR.
//
// Example:
//
//	.WhereOr(
//	    cereal.C("status", "=", "active"),
//	    cereal.C("status", "=", "pending"),
//	)
func (ub *Update[T]) WhereOr(conditions ...Condition) *Update[T] {
	if ub.err != nil {
		return ub
	}

	if len(conditions) == 0 {
		return ub
	}

	conditionItems := ub.instance.ConditionItems()
	for _, cond := range conditions {
		condItem, err := ub.buildCondition(cond)
		if err != nil {
			ub.err = err
			return ub
		}
		conditionItems = append(conditionItems, condItem)
	}

	orGroup, err := ub.instance.TryOr(conditionItems...)
	if err != nil {
		ub.err = fmt.Errorf("invalid OR condition: %w", err)
		return ub
	}

	ub.builder = ub.builder.Where(orGroup)
	ub.hasWhere = true
	return ub
}

// WhereNull adds a WHERE field IS NULL condition.
func (ub *Update[T]) WhereNull(field string) *Update[T] {
	if ub.err != nil {
		return ub
	}

	f, err := ub.instance.TryF(field)
	if err != nil {
		ub.err = fmt.Errorf("invalid field %q: %w", field, err)
		return ub
	}

	condition, err := ub.instance.TryNull(f)
	if err != nil {
		ub.err = fmt.Errorf("invalid NULL condition: %w", err)
		return ub
	}

	ub.builder = ub.builder.Where(condition)
	ub.hasWhere = true
	return ub
}

// WhereNotNull adds a WHERE field IS NOT NULL condition.
func (ub *Update[T]) WhereNotNull(field string) *Update[T] {
	if ub.err != nil {
		return ub
	}

	f, err := ub.instance.TryF(field)
	if err != nil {
		ub.err = fmt.Errorf("invalid field %q: %w", field, err)
		return ub
	}

	condition, err := ub.instance.TryNotNull(f)
	if err != nil {
		ub.err = fmt.Errorf("invalid NOT NULL condition: %w", err)
		return ub
	}

	ub.builder = ub.builder.Where(condition)
	ub.hasWhere = true
	return ub
}

// buildCondition converts a Condition to an ASTQL condition.
func (ub *Update[T]) buildCondition(cond Condition) (astql.ConditionItem, error) {
	f, err := ub.instance.TryF(cond.field)
	if err != nil {
		return nil, fmt.Errorf("invalid field %q: %w", cond.field, err)
	}

	if cond.isNull {
		if cond.operator == "IS NULL" {
			return ub.instance.TryNull(f)
		}
		return ub.instance.TryNotNull(f)
	}

	astqlOp, err := validateOperator(cond.operator)
	if err != nil {
		return nil, err
	}

	p, err := ub.instance.TryP(cond.param)
	if err != nil {
		return nil, fmt.Errorf("invalid param %q: %w", cond.param, err)
	}

	return ub.instance.TryC(f, astqlOp, p)
}

// Exec executes the UPDATE query with values from the provided params map.
// Returns the updated record with all fields populated.
// Requires at least one WHERE condition to prevent accidental full-table updates.
//
// Example:
//
//	params := map[string]any{
//	    "new_name": "Updated Name",
//	    "new_age": 30,
//	    "user_id": 123,
//	}
//	updated, err := cereal.Update().
//	    Set("name", "new_name").
//	    Set("age", "new_age").
//	    Where("id", "=", "user_id").
//	    Exec(ctx, params)
func (ub *Update[T]) Exec(ctx context.Context, params map[string]any) (*T, error) {
	return ub.exec(ctx, ub.cereal.execer(), params)
}

// ExecTx executes the UPDATE query within a transaction.
func (ub *Update[T]) ExecTx(ctx context.Context, tx *sqlx.Tx, params map[string]any) (*T, error) {
	return ub.exec(ctx, tx, params)
}

// ExecBatch executes the UPDATE query for multiple parameter sets.
// Returns the total number of rows affected.
// Each parameter set is executed separately with the same WHERE clause.
//
// Example:
//
//	batchParams := []map[string]any{
//	    {"new_name": "Updated 1", "user_id": 1},
//	    {"new_name": "Updated 2", "user_id": 2},
//	}
//	rowsAffected, err := cereal.Modify().
//	    Set("name", "new_name").
//	    Where("id", "=", "user_id").
//	    ExecBatch(ctx, batchParams)
func (ub *Update[T]) ExecBatch(ctx context.Context, batchParams []map[string]any) (int64, error) {
	return ub.execBatch(ctx, ub.cereal.execer(), batchParams)
}

// ExecBatchTx executes the UPDATE query for multiple parameter sets within a transaction.
// Returns the total number of rows affected.
func (ub *Update[T]) ExecBatchTx(ctx context.Context, tx *sqlx.Tx, batchParams []map[string]any) (int64, error) {
	return ub.execBatch(ctx, tx, batchParams)
}

// execBatch is the internal batch execution method.
func (ub *Update[T]) execBatch(ctx context.Context, execer sqlx.ExtContext, batchParams []map[string]any) (int64, error) {
	return executeBatch(ctx, execer, batchParams, ub.builder, ub.cereal.getTableName(), "UPDATE", ub.hasWhere, ub.err)
}

// exec is the internal execution method used by both Exec and ExecTx.
func (ub *Update[T]) exec(ctx context.Context, execer sqlx.ExtContext, params map[string]any) (*T, error) {
	// Check for  errors first
	if ub.err != nil {
		return nil, fmt.Errorf("update  has errors: %w", ub.err)
	}

	// Safety check: require WHERE clause
	if !ub.hasWhere {
		return nil, fmt.Errorf("UPDATE requires at least one WHERE condition to prevent accidental full-table update")
	}

	// Render the query
	result, err := ub.builder.Render()
	if err != nil {
		return nil, fmt.Errorf("failed to render UPDATE query: %w", err)
	}

	// Emit query started event
	tableName := ub.cereal.getTableName()
	capitan.Debug(ctx, QueryStarted,
		TableKey.Field(tableName),
		OperationKey.Field("UPDATE"),
		SQLKey.Field(result.SQL),
	)

	startTime := time.Now()

	// Execute named query with RETURNING
	rows, err := sqlx.NamedQueryContext(ctx, execer, result.SQL, params)
	if err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field("UPDATE"),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return nil, fmt.Errorf("UPDATE failed: %w", err)
	}
	defer rows.Close()

	// Check for exactly one row
	if !rows.Next() {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field("UPDATE"),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field("no rows updated"),
		)
		return nil, fmt.Errorf("no rows updated")
	}

	// Scan the updated row
	var updated T
	if err := rows.StructScan(&updated); err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field("UPDATE"),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return nil, fmt.Errorf("failed to scan UPDATE result: %w", err)
	}

	// Ensure no additional rows (UPDATE should affect exactly one record)
	if rows.Next() {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field("UPDATE"),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field("expected exactly one row updated, found multiple"),
		)
		return nil, fmt.Errorf("expected exactly one row updated, found multiple")
	}

	// Emit query completed event
	durationMs := time.Since(startTime).Milliseconds()
	capitan.Info(ctx, QueryCompleted,
		TableKey.Field(tableName),
		OperationKey.Field("UPDATE"),
		DurationMsKey.Field(durationMs),
		RowsAffectedKey.Field(1),
	)

	return &updated, nil
}

// Render builds and renders the query to SQL with parameter placeholders.
// Returns the SQL string and list of required parameters.
// Useful for inspection/debugging before execution.
func (ub *Update[T]) Render() (*astql.QueryResult, error) {
	// Check for  errors first
	if ub.err != nil {
		return nil, fmt.Errorf("update  has errors: %w", ub.err)
	}

	result, err := ub.builder.Render()
	if err != nil {
		return nil, fmt.Errorf("failed to render UPDATE query: %w", err)
	}
	return result, nil
}

// MustRender is like Render but panics on error.
// This is intentionally preserved for cases where panicking is desired (e.g., tests, initialization).
func (ub *Update[T]) MustRender() *astql.QueryResult {
	result, err := ub.Render()
	if err != nil {
		panic(err)
	}
	return result
}

// Instance returns the underlying ASTQL instance for advanced query building.
// Use this escape hatch when you need ASTQL features not exposed by Update.
func (ub *Update[T]) Instance() *astql.ASTQL {
	return ub.instance
}
