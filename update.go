package soy

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
	instance   *astql.ASTQL
	builder    *astql.Builder
	soy        soyExecutor           // interface for execution
	hasWhere   bool                  // tracks if WHERE was called
	whereItems []astql.ConditionItem // tracks WHERE conditions for fallback SELECT
	err        error                 // stores first error encountered during building
}

// Set specifies a field to update with a parameter value.
// Multiple calls add more fields to the SET clause.
//
// Example:
//
//	soy.Update().
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
		ub.err = newFieldError(field, err)
		return ub
	}

	p, err := ub.instance.TryP(param)
	if err != nil {
		ub.err = newParamError(param, err)
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
	builder, cond, err := wb.addWhereWithCondition(field, operator, param)
	if err != nil {
		ub.err = err
		return ub
	}

	ub.builder = builder
	ub.whereItems = append(ub.whereItems, cond)
	ub.hasWhere = true
	return ub
}

// WhereAnd adds multiple conditions combined with AND.
//
// Example:
//
//	.WhereAnd(
//	    soy.C("age", ">=", "min_age"),
//	    soy.C("status", "=", "active"),
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
		ub.err = newConditionError(err)
		return ub
	}

	ub.builder = ub.builder.Where(andGroup)
	ub.whereItems = append(ub.whereItems, andGroup)
	ub.hasWhere = true
	return ub
}

// WhereOr adds multiple conditions combined with OR.
//
// Example:
//
//	.WhereOr(
//	    soy.C("status", "=", "active"),
//	    soy.C("status", "=", "pending"),
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
		ub.err = newConditionError(err)
		return ub
	}

	ub.builder = ub.builder.Where(orGroup)
	ub.whereItems = append(ub.whereItems, orGroup)
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
		ub.err = newFieldError(field, err)
		return ub
	}

	condition, err := ub.instance.TryNull(f)
	if err != nil {
		ub.err = newConditionError(err)
		return ub
	}

	ub.builder = ub.builder.Where(condition)
	ub.whereItems = append(ub.whereItems, condition)
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
		ub.err = newFieldError(field, err)
		return ub
	}

	condition, err := ub.instance.TryNotNull(f)
	if err != nil {
		ub.err = newConditionError(err)
		return ub
	}

	ub.builder = ub.builder.Where(condition)
	ub.whereItems = append(ub.whereItems, condition)
	ub.hasWhere = true
	return ub
}

// WhereBetween adds a WHERE field BETWEEN low AND high condition.
// Multiple calls are combined with AND.
//
// Example:
//
//	.WhereBetween("age", "min_age", "max_age")
//	// params: map[string]any{"min_age": 18, "max_age": 65}
func (ub *Update[T]) WhereBetween(field, lowParam, highParam string) *Update[T] {
	return ub.whereBetweenHelper(field, lowParam, highParam, false)
}

// WhereNotBetween adds a WHERE field NOT BETWEEN low AND high condition.
// Multiple calls are combined with AND.
//
// Example:
//
//	.WhereNotBetween("age", "min_age", "max_age")
//	// params: map[string]any{"min_age": 18, "max_age": 65}
func (ub *Update[T]) WhereNotBetween(field, lowParam, highParam string) *Update[T] {
	return ub.whereBetweenHelper(field, lowParam, highParam, true)
}

// whereBetweenHelper is a shared helper for WhereBetween and WhereNotBetween.
func (ub *Update[T]) whereBetweenHelper(field, lowParam, highParam string, negate bool) *Update[T] {
	if ub.err != nil {
		return ub
	}

	f, err := ub.instance.TryF(field)
	if err != nil {
		ub.err = newFieldError(field, err)
		return ub
	}

	lowP, err := ub.instance.TryP(lowParam)
	if err != nil {
		ub.err = fmt.Errorf("invalid low param %q: %w", lowParam, err)
		return ub
	}

	highP, err := ub.instance.TryP(highParam)
	if err != nil {
		ub.err = fmt.Errorf("invalid high param %q: %w", highParam, err)
		return ub
	}

	var condition astql.ConditionItem
	if negate {
		condition = astql.NotBetween(f, lowP, highP)
	} else {
		condition = astql.Between(f, lowP, highP)
	}
	ub.builder = ub.builder.Where(condition)
	ub.whereItems = append(ub.whereItems, condition)
	ub.hasWhere = true
	return ub
}

// buildCondition converts a Condition to an ASTQL condition.
func (ub *Update[T]) buildCondition(cond Condition) (astql.ConditionItem, error) {
	return buildConditionWithInstance(ub.instance, cond)
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
//	updated, err := soy.Update().
//	    Set("name", "new_name").
//	    Set("age", "new_age").
//	    Where("id", "=", "user_id").
//	    Exec(ctx, params)
func (ub *Update[T]) Exec(ctx context.Context, params map[string]any) (*T, error) {
	return ub.exec(ctx, ub.soy.execer(), params)
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
//	rowsAffected, err := soy.Modify().
//	    Set("name", "new_name").
//	    Where("id", "=", "user_id").
//	    ExecBatch(ctx, batchParams)
func (ub *Update[T]) ExecBatch(ctx context.Context, batchParams []map[string]any) (int64, error) {
	return ub.execBatch(ctx, ub.soy.execer(), batchParams)
}

// ExecBatchTx executes the UPDATE query for multiple parameter sets within a transaction.
// Returns the total number of rows affected.
func (ub *Update[T]) ExecBatchTx(ctx context.Context, tx *sqlx.Tx, batchParams []map[string]any) (int64, error) {
	return ub.execBatch(ctx, tx, batchParams)
}

// execBatch is the internal batch execution method.
func (ub *Update[T]) execBatch(ctx context.Context, execer sqlx.ExtContext, batchParams []map[string]any) (int64, error) {
	return executeBatch(ctx, execer, batchParams, ub.builder, ub.soy.renderer(), ub.soy.getTableName(), "UPDATE", ub.hasWhere, ub.err)
}

// exec is the internal execution method used by both Exec and ExecTx.
// It checks renderer capabilities and routes to the appropriate execution strategy.
func (ub *Update[T]) exec(ctx context.Context, execer sqlx.ExtContext, params map[string]any) (*T, error) {
	// Check for errors first
	if ub.err != nil {
		return nil, fmt.Errorf("update builder has errors: %w", ub.err)
	}

	// Safety check: require WHERE clause
	if !ub.hasWhere {
		return nil, fmt.Errorf("UPDATE requires at least one WHERE condition to prevent accidental full-table update")
	}

	// Check capabilities and route to appropriate execution strategy
	caps := ub.soy.renderer().Capabilities()
	if caps.ReturningOnUpdate {
		return ub.execWithReturning(ctx, execer, params)
	}
	return ub.execThenSelect(ctx, execer, params)
}

// execWithReturning executes UPDATE with RETURNING clause (PostgreSQL, SQLite, MSSQL).
func (ub *Update[T]) execWithReturning(ctx context.Context, execer sqlx.ExtContext, params map[string]any) (*T, error) {
	// Render the query
	result, err := ub.builder.Render(ub.soy.renderer())
	if err != nil {
		return nil, fmt.Errorf("failed to render UPDATE query: %w", err)
	}

	// Emit query started event
	tableName := ub.soy.getTableName()
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
	defer func() { _ = rows.Close() }()

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

	if err := ub.soy.callOnScan(ctx, &updated); err != nil {
		return nil, fmt.Errorf("onScan callback failed: %w", err)
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

// execThenSelect executes UPDATE without RETURNING, then SELECTs the updated row (MariaDB fallback).
func (ub *Update[T]) execThenSelect(ctx context.Context, execer sqlx.ExtContext, params map[string]any) (*T, error) {
	// Render the UPDATE query (RETURNING will be omitted by renderer)
	result, err := ub.builder.Render(ub.soy.renderer())
	if err != nil {
		return nil, fmt.Errorf("failed to render UPDATE query: %w", err)
	}

	tableName := ub.soy.getTableName()
	capitan.Debug(ctx, QueryStarted,
		TableKey.Field(tableName),
		OperationKey.Field("UPDATE"),
		SQLKey.Field(result.SQL),
	)

	startTime := time.Now()

	// Execute UPDATE without expecting rows back
	res, err := sqlx.NamedExecContext(ctx, execer, result.SQL, params)
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

	// Verify exactly one row was affected
	affected, err := res.RowsAffected()
	if err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field("UPDATE"),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return nil, fmt.Errorf("failed to get rows affected: %w", err)
	}

	if affected == 0 {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field("UPDATE"),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field("no rows updated"),
		)
		return nil, fmt.Errorf("no rows updated")
	}

	if affected > 1 {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field("UPDATE"),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field("expected exactly one row updated, found multiple"),
		)
		return nil, fmt.Errorf("expected exactly one row updated, found %d", affected)
	}

	// Build and execute SELECT to fetch the updated row
	selectBuilder, err := ub.buildFallbackSelect()
	if err != nil {
		return nil, fmt.Errorf("failed to build fallback SELECT: %w", err)
	}

	selectResult, err := selectBuilder.Render(ub.soy.renderer())
	if err != nil {
		return nil, fmt.Errorf("failed to render fallback SELECT: %w", err)
	}

	capitan.Debug(ctx, QueryStarted,
		TableKey.Field(tableName),
		OperationKey.Field("SELECT (UPDATE fallback)"),
		SQLKey.Field(selectResult.SQL),
	)

	rows, err := sqlx.NamedQueryContext(ctx, execer, selectResult.SQL, params)
	if err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field("SELECT (UPDATE fallback)"),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return nil, fmt.Errorf("fallback SELECT failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field("SELECT (UPDATE fallback)"),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field("fallback SELECT returned no rows"),
		)
		return nil, fmt.Errorf("fallback SELECT returned no rows")
	}

	var updated T
	if err := rows.StructScan(&updated); err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field("SELECT (UPDATE fallback)"),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return nil, fmt.Errorf("failed to scan fallback SELECT result: %w", err)
	}

	if err := ub.soy.callOnScan(ctx, &updated); err != nil {
		return nil, fmt.Errorf("onScan callback failed: %w", err)
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

// buildFallbackSelect builds a SELECT query using the same WHERE conditions as the UPDATE.
func (ub *Update[T]) buildFallbackSelect() (*astql.Builder, error) {
	tableName := ub.soy.getTableName()
	t, err := ub.instance.TryT(tableName)
	if err != nil {
		return nil, fmt.Errorf("invalid table %q: %w", tableName, err)
	}

	builder := astql.Select(t)

	// Collect all fields from metadata
	metadata := ub.soy.getMetadata()
	fieldSlice := ub.instance.Fields()
	for _, field := range metadata.Fields {
		dbCol := field.Tags["db"]
		if dbCol == "" || dbCol == "-" {
			continue
		}
		f, err := ub.instance.TryF(dbCol)
		if err != nil {
			return nil, fmt.Errorf("invalid field %q: %w", dbCol, err)
		}
		fieldSlice = append(fieldSlice, f)
	}
	builder = builder.Fields(fieldSlice...)

	// Add stored WHERE conditions
	for _, cond := range ub.whereItems {
		builder = builder.Where(cond)
	}

	return builder, nil
}

// Render builds and renders the query to SQL with parameter placeholders.
// Returns the SQL string and list of required parameters.
// Useful for inspection/debugging before execution.
func (ub *Update[T]) Render() (*astql.QueryResult, error) {
	// Check for  errors first
	if ub.err != nil {
		return nil, fmt.Errorf("update  has errors: %w", ub.err)
	}

	result, err := ub.builder.Render(ub.soy.renderer())
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
