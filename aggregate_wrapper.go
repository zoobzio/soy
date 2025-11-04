package cereal

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql"
)

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
