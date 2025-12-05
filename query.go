package cereal

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql"
	"github.com/zoobzio/capitan"
)

// Query provides a focused API for building SELECT queries that return multiple records.
// It wraps ASTQL's SELECT functionality with a simple string-based interface.
// Use this for querying multiple records from the database.
type Query[T any] struct {
	instance *astql.ASTQL
	builder  *astql.Builder
	cereal   cerealExecutor // interface for execution
	err      error          // stores first error encountered during building
}

// Fields specifies which fields to select. If not called, selects all fields (*).
// Multiple calls overwrite previous field selections.
//
// Example:
//
//	cereal.Query().
//	    Fields("id", "email", "name").
//	    Where("age", ">=", "min_age").
//	    Exec(ctx, params)
func (qb *Query[T]) Fields(fields ...string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	astqlFields := qb.instance.Fields()
	for _, field := range fields {
		f, err := qb.instance.TryF(field)
		if err != nil {
			qb.err = fmt.Errorf("invalid field %q: %w", field, err)
			return qb
		}
		astqlFields = append(astqlFields, f)
	}
	qb.builder = qb.builder.Fields(astqlFields...)
	return qb
}

// Where adds a simple WHERE condition with field operator param pattern.
// Multiple calls are combined with AND.
//
// Example:
//
//	.Where("age", ">=", "min_age")
func (qb *Query[T]) Where(field, operator, param string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	astqlOp, err := validateOperator(operator)
	if err != nil {
		qb.err = err
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	p, err := qb.instance.TryP(param)
	if err != nil {
		qb.err = fmt.Errorf("invalid param %q: %w", param, err)
		return qb
	}

	condition, err := qb.instance.TryC(f, astqlOp, p)
	if err != nil {
		qb.err = fmt.Errorf("invalid condition: %w", err)
		return qb
	}

	qb.builder = qb.builder.Where(condition)
	return qb
}

// WhereAnd adds multiple conditions combined with AND.
//
// Example:
//
//	.WhereAnd(
//	    cereal.C("age", ">=", "min_age"),
//	    cereal.C("status", "=", "active"),
//	)
func (qb *Query[T]) WhereAnd(conditions ...Condition) *Query[T] {
	if qb.err != nil {
		return qb
	}

	if len(conditions) == 0 {
		return qb
	}

	conditionItems := qb.instance.ConditionItems()
	for _, cond := range conditions {
		condItem, err := qb.buildCondition(cond)
		if err != nil {
			qb.err = err
			return qb
		}
		conditionItems = append(conditionItems, condItem)
	}

	andGroup, err := qb.instance.TryAnd(conditionItems...)
	if err != nil {
		qb.err = fmt.Errorf("invalid AND condition: %w", err)
		return qb
	}

	qb.builder = qb.builder.Where(andGroup)
	return qb
}

// WhereOr adds multiple conditions combined with OR.
//
// Example:
//
//	.WhereOr(
//	    cereal.C("status", "=", "active"),
//	    cereal.C("status", "=", "pending"),
//	)
func (qb *Query[T]) WhereOr(conditions ...Condition) *Query[T] {
	if qb.err != nil {
		return qb
	}

	if len(conditions) == 0 {
		return qb
	}

	conditionItems := qb.instance.ConditionItems()
	for _, cond := range conditions {
		condItem, err := qb.buildCondition(cond)
		if err != nil {
			qb.err = err
			return qb
		}
		conditionItems = append(conditionItems, condItem)
	}

	orGroup, err := qb.instance.TryOr(conditionItems...)
	if err != nil {
		qb.err = fmt.Errorf("invalid OR condition: %w", err)
		return qb
	}

	qb.builder = qb.builder.Where(orGroup)
	return qb
}

// WhereNull adds a WHERE field IS NULL condition.
func (qb *Query[T]) WhereNull(field string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	condition, err := qb.instance.TryNull(f)
	if err != nil {
		qb.err = fmt.Errorf("invalid NULL condition: %w", err)
		return qb
	}

	qb.builder = qb.builder.Where(condition)
	return qb
}

// WhereNotNull adds a WHERE field IS NOT NULL condition.
func (qb *Query[T]) WhereNotNull(field string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	condition, err := qb.instance.TryNotNull(f)
	if err != nil {
		qb.err = fmt.Errorf("invalid NOT NULL condition: %w", err)
		return qb
	}

	qb.builder = qb.builder.Where(condition)
	return qb
}

// OrderBy adds an ORDER BY clause.
// Direction must be "ASC" or "DESC" (case-insensitive).
// Multiple calls add additional sort fields.
//
// Example:
//
//	.OrderBy("age", "DESC").OrderBy("name", "ASC")
func (qb *Query[T]) OrderBy(field, direction string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	astqlDir, err := validateDirection(direction)
	if err != nil {
		qb.err = err
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	qb.builder = qb.builder.OrderBy(f, astqlDir)
	return qb
}

// OrderByExpr adds an ORDER BY clause with an expression (field <op> param).
// Useful for vector distance ordering with pgvector.
// Direction must be "ASC" or "DESC" (case-insensitive).
//
// Example:
//
//	.OrderByExpr("embedding", "<->", "query_embedding", "ASC")
func (qb *Query[T]) OrderByExpr(field, operator, param, direction string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	astqlDir, err := validateDirection(direction)
	if err != nil {
		qb.err = err
		return qb
	}

	astqlOp, err := validateOperator(operator)
	if err != nil {
		qb.err = err
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	p, err := qb.instance.TryP(param)
	if err != nil {
		qb.err = fmt.Errorf("invalid param %q: %w", param, err)
		return qb
	}

	qb.builder = qb.builder.OrderByExpr(f, astqlOp, p, astqlDir)
	return qb
}

// Limit adds a LIMIT clause to restrict the number of rows returned.
//
// Example:
//
//	.Limit(10)
func (qb *Query[T]) Limit(limit int) *Query[T] {
	qb.builder = qb.builder.Limit(limit)
	return qb
}

// Offset adds an OFFSET clause to skip rows.
//
// Example:
//
//	.Limit(10).Offset(20) // page 3 of results
func (qb *Query[T]) Offset(offset int) *Query[T] {
	qb.builder = qb.builder.Offset(offset)
	return qb
}

// buildCondition converts a Condition to an ASTQL condition.
func (qb *Query[T]) buildCondition(cond Condition) (astql.ConditionItem, error) {
	f, err := qb.instance.TryF(cond.field)
	if err != nil {
		return nil, fmt.Errorf("invalid field %q: %w", cond.field, err)
	}

	if cond.isNull {
		if cond.operator == opIsNull {
			return qb.instance.TryNull(f)
		}
		return qb.instance.TryNotNull(f)
	}

	astqlOp, err := validateOperator(cond.operator)
	if err != nil {
		return nil, err
	}

	p, err := qb.instance.TryP(cond.param)
	if err != nil {
		return nil, fmt.Errorf("invalid param %q: %w", cond.param, err)
	}

	return qb.instance.TryC(f, astqlOp, p)
}

// Exec executes the SELECT query with values from the provided params map.
// Returns a slice of pointers to all matching records.
//
// Example:
//
//	params := map[string]any{"min_age": 18, "status": "active"}
//	users, err := cereal.Query().
//	    Where("age", ">=", "min_age").
//	    Where("status", "=", "status").
//	    OrderBy("name", "ASC").
//	    Exec(ctx, params)
func (qb *Query[T]) Exec(ctx context.Context, params map[string]any) ([]*T, error) {
	return qb.exec(ctx, qb.cereal.execer(), params)
}

// ExecTx executes the SELECT query within a transaction.
// Returns a slice of pointers to all matching records.
//
// Example:
//
//	tx, _ := db.BeginTxx(ctx, nil)
//	defer tx.Rollback()
//	users, err := cereal.Query().
//	    Where("age", ">=", "min_age").
//	    ExecTx(ctx, tx, params)
//	tx.Commit()
func (qb *Query[T]) ExecTx(ctx context.Context, tx *sqlx.Tx, params map[string]any) ([]*T, error) {
	return qb.exec(ctx, tx, params)
}

// exec is the internal execution method used by both Exec and ExecTx.
func (qb *Query[T]) exec(ctx context.Context, execer sqlx.ExtContext, params map[string]any) ([]*T, error) {
	// Check for  errors first
	if qb.err != nil {
		return nil, fmt.Errorf("query  has errors: %w", qb.err)
	}

	// Render the query
	result, err := qb.builder.Render()
	if err != nil {
		return nil, fmt.Errorf("failed to render SELECT query: %w", err)
	}

	// Emit query started event
	tableName := qb.cereal.getTableName()
	capitan.Debug(ctx, QueryStarted,
		TableKey.Field(tableName),
		OperationKey.Field("QUERY"),
		SQLKey.Field(result.SQL),
	)

	startTime := time.Now()

	// Execute named query
	rows, err := sqlx.NamedQueryContext(ctx, execer, result.SQL, params)
	if err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field("QUERY"),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return nil, fmt.Errorf("SELECT query failed: %w", err)
	}
	defer rows.Close()

	// Scan all rows
	var records []*T
	for rows.Next() {
		var record T
		if err := rows.StructScan(&record); err != nil {
			durationMs := time.Since(startTime).Milliseconds()
			capitan.Error(ctx, QueryFailed,
				TableKey.Field(tableName),
				OperationKey.Field("QUERY"),
				DurationMsKey.Field(durationMs),
				ErrorKey.Field(err.Error()),
			)
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		records = append(records, &record)
	}

	// Check for errors during iteration
	if err := rows.Err(); err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field("QUERY"),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	// Emit query completed event
	durationMs := time.Since(startTime).Milliseconds()
	capitan.Info(ctx, QueryCompleted,
		TableKey.Field(tableName),
		OperationKey.Field("QUERY"),
		DurationMsKey.Field(durationMs),
		RowsReturnedKey.Field(len(records)),
	)

	return records, nil
}

// Render builds and renders the query to SQL with parameter placeholders.
// Returns the SQL string and list of required parameters.
// Useful for inspection/debugging before execution.
func (qb *Query[T]) Render() (*astql.QueryResult, error) {
	// Check for  errors first
	if qb.err != nil {
		return nil, fmt.Errorf("query  has errors: %w", qb.err)
	}

	result, err := qb.builder.Render()
	if err != nil {
		return nil, fmt.Errorf("failed to render SELECT query: %w", err)
	}
	return result, nil
}

// MustRender is like Render but panics on error.
// This is intentionally preserved for cases where panicking is desired (e.g., tests, initialization).
func (qb *Query[T]) MustRender() *astql.QueryResult {
	result, err := qb.Render()
	if err != nil {
		panic(err)
	}
	return result
}

// Instance returns the underlying ASTQL instance for advanced query building.
// Use this escape hatch when you need ASTQL features not exposed by Query.
func (qb *Query[T]) Instance() *astql.ASTQL {
	return qb.instance
}
