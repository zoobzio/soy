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

// exec executes the aggregate query and returns the result as float64.
// Handles both regular execution and transaction execution.
func (ab *aggregateBuilder[T]) exec(ctx context.Context, execer sqlx.ExtContext, params map[string]any) (float64, error) {
	// Check for builder errors first
	if ab.err != nil {
		return 0, fmt.Errorf("%s builder has errors: %w", ab.funcName, ab.err)
	}

	// Render the query
	result, err := ab.builder.Render()
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
	defer rows.Close()

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

	result, err := ab.builder.Render()
	if err != nil {
		return nil, fmt.Errorf("failed to render %s query: %w", ab.funcName, err)
	}
	return result, nil
}
