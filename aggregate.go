package cereal

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql"
	"github.com/zoobzio/capitan"
)

// aggregateBuilder provides shared logic for all aggregate query builders.
// This eliminates duplication across Avg, Min, Max, Sum, and Count builders.
type aggregateBuilder[T any] struct {
	instance *astql.ASTQL
	builder  *astql.Builder
	cereal   cerealExecutor
	field    string // field to aggregate (empty for COUNT(*))
	funcName string // aggregate function name (AVG, MIN, MAX, SUM, COUNT)
	err      error
}

// newAggregateBuilder creates a new aggregate builder helper.
func newAggregateBuilder[T any](instance *astql.ASTQL, builder *astql.Builder, cereal cerealExecutor, field, funcName string) *aggregateBuilder[T] {
	return &aggregateBuilder[T]{
		instance: instance,
		builder:  builder,
		cereal:   cereal,
		field:    field,
		funcName: funcName,
	}
}

// addWhere adds a simple WHERE condition.
func (ab *aggregateBuilder[T]) addWhere(field, operator, param string) error {
	wb := newWhereBuilder(ab.instance, ab.builder)
	builder, err := wb.addWhere(field, operator, param)
	if err != nil {
		return err
	}
	ab.builder = builder
	return nil
}

// addWhereAnd adds multiple conditions combined with AND.
func (ab *aggregateBuilder[T]) addWhereAnd(conditions ...Condition) error {
	wb := newWhereBuilder(ab.instance, ab.builder)
	builder, err := wb.addWhereAnd(conditions...)
	if err != nil {
		return err
	}
	ab.builder = builder
	return nil
}

// addWhereOr adds multiple conditions combined with OR.
func (ab *aggregateBuilder[T]) addWhereOr(conditions ...Condition) error {
	wb := newWhereBuilder(ab.instance, ab.builder)
	builder, err := wb.addWhereOr(conditions...)
	if err != nil {
		return err
	}
	ab.builder = builder
	return nil
}

// addWhereNull adds a WHERE field IS NULL condition.
func (ab *aggregateBuilder[T]) addWhereNull(field string) error {
	wb := newWhereBuilder(ab.instance, ab.builder)
	builder, err := wb.addWhereNull(field)
	if err != nil {
		return err
	}
	ab.builder = builder
	return nil
}

// addWhereNotNull adds a WHERE field IS NOT NULL condition.
func (ab *aggregateBuilder[T]) addWhereNotNull(field string) error {
	wb := newWhereBuilder(ab.instance, ab.builder)
	builder, err := wb.addWhereNotNull(field)
	if err != nil {
		return err
	}
	ab.builder = builder
	return nil
}

// addWhereBetween adds a WHERE field BETWEEN low AND high condition.
func (ab *aggregateBuilder[T]) addWhereBetween(field, lowParam, highParam string) error {
	wb := newWhereBuilder(ab.instance, ab.builder)
	builder, err := wb.addWhereBetween(field, lowParam, highParam)
	if err != nil {
		return err
	}
	ab.builder = builder
	return nil
}

// addWhereNotBetween adds a WHERE field NOT BETWEEN low AND high condition.
func (ab *aggregateBuilder[T]) addWhereNotBetween(field, lowParam, highParam string) error {
	wb := newWhereBuilder(ab.instance, ab.builder)
	builder, err := wb.addWhereNotBetween(field, lowParam, highParam)
	if err != nil {
		return err
	}
	ab.builder = builder
	return nil
}

// addWhereFields adds a WHERE condition comparing two fields.
func (ab *aggregateBuilder[T]) addWhereFields(leftField, operator, rightField string) error {
	astqlOp, err := validateOperator(operator)
	if err != nil {
		return err
	}

	left, err := ab.instance.TryF(leftField)
	if err != nil {
		return fmt.Errorf("invalid left field %q: %w", leftField, err)
	}

	right, err := ab.instance.TryF(rightField)
	if err != nil {
		return fmt.Errorf("invalid right field %q: %w", rightField, err)
	}

	ab.builder = ab.builder.Where(astql.CF(left, astqlOp, right))
	return nil
}

// exec executes the aggregate query and returns the result as float64.
// Handles both regular execution and transaction execution.
func (ab *aggregateBuilder[T]) exec(ctx context.Context, execer sqlx.ExtContext, params map[string]any) (float64, error) {
	// Check for builder errors first
	if ab.err != nil {
		return 0, fmt.Errorf("%s builder has errors: %w", ab.funcName, ab.err)
	}

	// Render the query
	result, err := ab.builder.Render(ab.cereal.renderer())
	if err != nil {
		return 0, fmt.Errorf("failed to render %s query: %w", ab.funcName, err)
	}

	// Emit query started event
	tableName := ab.cereal.getTableName()
	capitan.Debug(ctx, QueryStarted,
		TableKey.Field(tableName),
		OperationKey.Field(ab.funcName),
		SQLKey.Field(result.SQL),
		FieldKey.Field(ab.field),
	)

	startTime := time.Now()

	// Execute named query
	rows, err := sqlx.NamedQueryContext(ctx, execer, result.SQL, params)
	if err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field(ab.funcName),
			DurationMsKey.Field(durationMs),
			FieldKey.Field(ab.field),
			ErrorKey.Field(err.Error()),
		)
		return 0, fmt.Errorf("%s query failed: %w", ab.funcName, err)
	}
	defer func() { _ = rows.Close() }()

	// Scan the result
	if !rows.Next() {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field(ab.funcName),
			DurationMsKey.Field(durationMs),
			FieldKey.Field(ab.field),
			ErrorKey.Field(fmt.Sprintf("%s query returned no rows", ab.funcName)),
		)
		return 0, fmt.Errorf("%s query returned no rows", ab.funcName)
	}

	var resultPtr *float64
	if err := rows.Scan(&resultPtr); err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field(ab.funcName),
			DurationMsKey.Field(durationMs),
			FieldKey.Field(ab.field),
			ErrorKey.Field(err.Error()),
		)
		return 0, fmt.Errorf("failed to scan %s result: %w", ab.funcName, err)
	}

	// Handle NULL (no matching rows)
	resultValue := 0.0
	if resultPtr != nil {
		resultValue = *resultPtr
	}

	// Emit query completed event
	durationMs := time.Since(startTime).Milliseconds()
	capitan.Info(ctx, QueryCompleted,
		TableKey.Field(tableName),
		OperationKey.Field(ab.funcName),
		DurationMsKey.Field(durationMs),
		FieldKey.Field(ab.field),
		ResultValueKey.Field(resultValue),
	)

	return resultValue, nil
}

// render builds and renders the query to SQL with parameter placeholders.
func (ab *aggregateBuilder[T]) render() (*astql.QueryResult, error) {
	if ab.err != nil {
		return nil, fmt.Errorf("%s builder has errors: %w", ab.funcName, ab.err)
	}

	result, err := ab.builder.Render(ab.cereal.renderer())
	if err != nil {
		return nil, fmt.Errorf("failed to render %s query: %w", ab.funcName, err)
	}
	return result, nil
}

// Aggregate provides a unified API for all aggregate query builders (AVG, MIN, MAX, SUM, COUNT).
// This single type replaces the separate Avg, Min, Max, Sum, Count types, eliminating duplication
// while maintaining a clean, type-safe API through factory methods.
type Aggregate[T any] struct {
	agg *aggregateBuilder[T]
}

// Where adds a simple WHERE condition with field operator param pattern.
// Multiple calls are combined with AND.
//
// Example:
//
//	.Where("status", "=", "active")
func (ab *Aggregate[T]) Where(field, operator, param string) *Aggregate[T] {
	if ab.agg.err != nil {
		return ab
	}

	if err := ab.agg.addWhere(field, operator, param); err != nil {
		ab.agg.err = err
	}
	return ab
}

// WhereAnd adds multiple conditions combined with AND.
//
// Example:
//
//	.WhereAnd(
//	    cereal.C("status", "=", "active"),
//	    cereal.C("age", ">", "min_age"),
//	)
func (ab *Aggregate[T]) WhereAnd(conditions ...Condition) *Aggregate[T] {
	if ab.agg.err != nil {
		return ab
	}

	if err := ab.agg.addWhereAnd(conditions...); err != nil {
		ab.agg.err = err
	}
	return ab
}

// WhereOr adds multiple conditions combined with OR.
//
// Example:
//
//	.WhereOr(
//	    cereal.C("status", "=", "active"),
//	    cereal.C("status", "=", "pending"),
//	)
func (ab *Aggregate[T]) WhereOr(conditions ...Condition) *Aggregate[T] {
	if ab.agg.err != nil {
		return ab
	}

	if err := ab.agg.addWhereOr(conditions...); err != nil {
		ab.agg.err = err
	}
	return ab
}

// WhereNull adds a WHERE field IS NULL condition.
func (ab *Aggregate[T]) WhereNull(field string) *Aggregate[T] {
	if ab.agg.err != nil {
		return ab
	}

	if err := ab.agg.addWhereNull(field); err != nil {
		ab.agg.err = err
	}
	return ab
}

// WhereNotNull adds a WHERE field IS NOT NULL condition.
func (ab *Aggregate[T]) WhereNotNull(field string) *Aggregate[T] {
	if ab.agg.err != nil {
		return ab
	}

	if err := ab.agg.addWhereNotNull(field); err != nil {
		ab.agg.err = err
	}
	return ab
}

// WhereBetween adds a WHERE field BETWEEN low AND high condition.
// Multiple calls are combined with AND.
//
// Example:
//
//	.WhereBetween("age", "min_age", "max_age")
//	// params: map[string]any{"min_age": 18, "max_age": 65}
func (ab *Aggregate[T]) WhereBetween(field, lowParam, highParam string) *Aggregate[T] {
	if ab.agg.err != nil {
		return ab
	}

	if err := ab.agg.addWhereBetween(field, lowParam, highParam); err != nil {
		ab.agg.err = err
	}
	return ab
}

// WhereNotBetween adds a WHERE field NOT BETWEEN low AND high condition.
// Multiple calls are combined with AND.
//
// Example:
//
//	.WhereNotBetween("age", "min_age", "max_age")
//	// params: map[string]any{"min_age": 18, "max_age": 65}
func (ab *Aggregate[T]) WhereNotBetween(field, lowParam, highParam string) *Aggregate[T] {
	if ab.agg.err != nil {
		return ab
	}

	if err := ab.agg.addWhereNotBetween(field, lowParam, highParam); err != nil {
		ab.agg.err = err
	}
	return ab
}

// WhereFields adds a WHERE condition comparing two fields.
// Multiple calls are combined with AND.
//
// Example:
//
//	.WhereFields("created_at", "<", "updated_at")
//	// WHERE "created_at" < "updated_at"
func (ab *Aggregate[T]) WhereFields(leftField, operator, rightField string) *Aggregate[T] {
	if ab.agg.err != nil {
		return ab
	}

	if err := ab.agg.addWhereFields(leftField, operator, rightField); err != nil {
		ab.agg.err = err
	}
	return ab
}

// Exec executes the aggregate query with values from the provided params map.
// Returns the result as float64.
//
// Example:
//
//	params := map[string]any{"status": "active"}
//	result, err := cereal.Avg("age").
//	    Where("status", "=", "status").
//	    Exec(ctx, params)
func (ab *Aggregate[T]) Exec(ctx context.Context, params map[string]any) (float64, error) {
	return ab.agg.exec(ctx, ab.agg.cereal.execer(), params)
}

// ExecTx executes the aggregate query within a transaction.
// Returns the result as float64.
//
// Example:
//
//	tx, _ := db.BeginTxx(ctx, nil)
//	defer tx.Rollback()
//	result, err := cereal.Avg("age").
//	    Where("status", "=", "status").
//	    ExecTx(ctx, tx, params)
//	tx.Commit()
func (ab *Aggregate[T]) ExecTx(ctx context.Context, tx *sqlx.Tx, params map[string]any) (float64, error) {
	return ab.agg.exec(ctx, tx, params)
}

// Render builds and renders the query to SQL with parameter placeholders.
// Returns the SQL string and list of required parameters.
// Useful for inspection/debugging before execution.
func (ab *Aggregate[T]) Render() (*astql.QueryResult, error) {
	return ab.agg.render()
}

// MustRender is like Render but panics on error.
// This is intentionally preserved for cases where panicking is desired (e.g., tests, initialization).
func (ab *Aggregate[T]) MustRender() *astql.QueryResult {
	result, err := ab.Render()
	if err != nil {
		panic(err)
	}
	return result
}

// Instance returns the underlying ASTQL instance for advanced query building.
// Use this escape hatch when you need ASTQL features not exposed by Aggregate.
func (ab *Aggregate[T]) Instance() *astql.ASTQL {
	return ab.agg.instance
}
