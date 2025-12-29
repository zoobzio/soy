package soy

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql"
)

// Compound provides a focused API for building compound queries with set operations
// (UNION, INTERSECT, EXCEPT). It wraps ASTQL's CompoundBuilder functionality.
type Compound[T any] struct {
	instance *astql.ASTQL
	builder  *astql.CompoundBuilder
	soy      soyExecutor
	err      error
}

// Union adds a UNION operation with another query.
// UNION removes duplicate rows from the combined result set.
//
// Example:
//
//	compound := soy.Query().
//	    Where("status", "=", "active").
//	    Union(soy.Query().Where("status", "=", "pending"))
func (cb *Compound[T]) Union(other *Query[T]) *Compound[T] {
	if cb.err != nil {
		return cb
	}
	if other.err != nil {
		cb.err = other.err
		return cb
	}

	cb.builder = cb.builder.Union(other.builder)
	return cb
}

// UnionAll adds a UNION ALL operation with another query.
// UNION ALL keeps all rows including duplicates.
//
// Example:
//
//	compound := soy.Query().
//	    Where("status", "=", "active").
//	    UnionAll(soy.Query().Where("status", "=", "pending"))
func (cb *Compound[T]) UnionAll(other *Query[T]) *Compound[T] {
	if cb.err != nil {
		return cb
	}
	if other.err != nil {
		cb.err = other.err
		return cb
	}

	cb.builder = cb.builder.UnionAll(other.builder)
	return cb
}

// Intersect adds an INTERSECT operation with another query.
// INTERSECT returns only rows that appear in both result sets.
//
// Example:
//
//	compound := soy.Query().
//	    Where("role", "=", "admin").
//	    Intersect(soy.Query().Where("status", "=", "active"))
func (cb *Compound[T]) Intersect(other *Query[T]) *Compound[T] {
	if cb.err != nil {
		return cb
	}
	if other.err != nil {
		cb.err = other.err
		return cb
	}

	cb.builder = cb.builder.Intersect(other.builder)
	return cb
}

// IntersectAll adds an INTERSECT ALL operation with another query.
// INTERSECT ALL keeps duplicates in the intersection.
func (cb *Compound[T]) IntersectAll(other *Query[T]) *Compound[T] {
	if cb.err != nil {
		return cb
	}
	if other.err != nil {
		cb.err = other.err
		return cb
	}

	cb.builder = cb.builder.IntersectAll(other.builder)
	return cb
}

// Except adds an EXCEPT operation with another query.
// EXCEPT returns rows from the first query that don't appear in the second.
//
// Example:
//
//	compound := soy.Query().
//	    Where("status", "=", "active").
//	    Except(soy.Query().Where("role", "=", "admin"))
func (cb *Compound[T]) Except(other *Query[T]) *Compound[T] {
	if cb.err != nil {
		return cb
	}
	if other.err != nil {
		cb.err = other.err
		return cb
	}

	cb.builder = cb.builder.Except(other.builder)
	return cb
}

// ExceptAll adds an EXCEPT ALL operation with another query.
// EXCEPT ALL keeps duplicates when computing the difference.
func (cb *Compound[T]) ExceptAll(other *Query[T]) *Compound[T] {
	if cb.err != nil {
		return cb
	}
	if other.err != nil {
		cb.err = other.err
		return cb
	}

	cb.builder = cb.builder.ExceptAll(other.builder)
	return cb
}

// OrderBy adds an ORDER BY clause to the compound query result.
// This orders the final combined result set.
//
// Example:
//
//	compound.OrderBy("name", "asc")
func (cb *Compound[T]) OrderBy(field, direction string) *Compound[T] {
	if cb.err != nil {
		return cb
	}

	astqlDir, err := validateDirection(direction)
	if err != nil {
		cb.err = err
		return cb
	}

	f, err := cb.instance.TryF(field)
	if err != nil {
		cb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return cb
	}

	cb.builder = cb.builder.OrderBy(f, astqlDir)
	return cb
}

// Limit adds a LIMIT clause to the compound query.
func (cb *Compound[T]) Limit(limit int) *Compound[T] {
	cb.builder = cb.builder.Limit(limit)
	return cb
}

// LimitParam sets the LIMIT clause to a parameterized value.
// Useful for API pagination where limit comes from request parameters.
//
// Example:
//
//	.LimitParam("page_size")
//	// params: map[string]any{"page_size": 10}
func (cb *Compound[T]) LimitParam(param string) *Compound[T] {
	if cb.err != nil {
		return cb
	}

	p, err := cb.instance.TryP(param)
	if err != nil {
		cb.err = fmt.Errorf("invalid limit param %q: %w", param, err)
		return cb
	}

	cb.builder = cb.builder.LimitParam(p)
	return cb
}

// Offset adds an OFFSET clause to the compound query.
func (cb *Compound[T]) Offset(offset int) *Compound[T] {
	cb.builder = cb.builder.Offset(offset)
	return cb
}

// OffsetParam sets the OFFSET clause to a parameterized value.
// Useful for API pagination where offset comes from request parameters.
//
// Example:
//
//	.OffsetParam("page_offset")
//	// params: map[string]any{"page_offset": 20}
func (cb *Compound[T]) OffsetParam(param string) *Compound[T] {
	if cb.err != nil {
		return cb
	}

	p, err := cb.instance.TryP(param)
	if err != nil {
		cb.err = fmt.Errorf("invalid offset param %q: %w", param, err)
		return cb
	}

	cb.builder = cb.builder.OffsetParam(p)
	return cb
}

// Render builds and renders the compound query to SQL.
func (cb *Compound[T]) Render() (*astql.QueryResult, error) {
	if cb.err != nil {
		return nil, fmt.Errorf("compound query has errors: %w", cb.err)
	}

	result, err := cb.builder.Render(cb.soy.renderer())
	if err != nil {
		return nil, fmt.Errorf("failed to render compound query: %w", err)
	}
	return result, nil
}

// MustRender is like Render but panics on error.
func (cb *Compound[T]) MustRender() *astql.QueryResult {
	result, err := cb.Render()
	if err != nil {
		panic(err)
	}
	return result
}

// Exec executes the compound query and returns all matching records.
func (cb *Compound[T]) Exec(ctx context.Context, params map[string]any) ([]*T, error) {
	return cb.exec(ctx, cb.soy.execer(), params)
}

// ExecTx executes the compound query within a transaction.
func (cb *Compound[T]) ExecTx(ctx context.Context, tx *sqlx.Tx, params map[string]any) ([]*T, error) {
	return cb.exec(ctx, tx, params)
}

// exec is the internal execution method.
func (cb *Compound[T]) exec(ctx context.Context, execer sqlx.ExtContext, params map[string]any) ([]*T, error) {
	if cb.err != nil {
		return nil, fmt.Errorf("compound query has errors: %w", cb.err)
	}

	result, err := cb.builder.Render(cb.soy.renderer())
	if err != nil {
		return nil, fmt.Errorf("failed to render compound query: %w", err)
	}

	return execMultipleRows[T](ctx, execer, result.SQL, params, cb.soy.getTableName(), "COMPOUND")
}

// Instance returns the underlying ASTQL instance.
func (cb *Compound[T]) Instance() *astql.ASTQL {
	return cb.instance
}
