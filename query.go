package cereal

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql"
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

// OrderByNulls adds an ORDER BY clause with NULLS FIRST or NULLS LAST.
// Direction must be "asc" or "desc" (case insensitive).
// Nulls must be "first" or "last" (case insensitive).
//
// Example:
//
//	.OrderByNulls("created_at", "desc", "last")  // ORDER BY "created_at" DESC NULLS LAST
func (qb *Query[T]) OrderByNulls(field, direction, nulls string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	astqlDir, err := validateDirection(direction)
	if err != nil {
		qb.err = err
		return qb
	}

	astqlNulls, err := validateNulls(nulls)
	if err != nil {
		qb.err = err
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	qb.builder = qb.builder.OrderByNulls(f, astqlDir, astqlNulls)
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

// Distinct adds DISTINCT to the SELECT query.
func (qb *Query[T]) Distinct() *Query[T] {
	qb.builder = qb.builder.Distinct()
	return qb
}

// DistinctOn adds DISTINCT ON (PostgreSQL-specific) to the SELECT query.
// Returns only the first row for each distinct combination of the specified fields.
//
// Example:
//
//	.DistinctOn("user_id").OrderBy("created_at", "desc")  // First row per user_id
func (qb *Query[T]) DistinctOn(fields ...string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	if len(fields) == 0 {
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

	qb.builder = qb.builder.DistinctOn(astqlFields...)
	return qb
}

// GroupBy adds a GROUP BY clause.
// Multiple calls add additional grouping fields.
//
// Example:
//
//	.GroupBy("status").GroupBy("category")
func (qb *Query[T]) GroupBy(fields ...string) *Query[T] {
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
	qb.builder = qb.builder.GroupBy(astqlFields...)
	return qb
}

// Having adds a HAVING condition. Must be used after GroupBy.
// Multiple calls are combined with AND.
//
// Example:
//
//	.GroupBy("status").Having("age", ">", "min_age")
func (qb *Query[T]) Having(field, operator, param string) *Query[T] {
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

	qb.builder = qb.builder.Having(condition)
	return qb
}

// HavingAgg adds an aggregate HAVING condition. Must be used after GroupBy.
// Supports COUNT(*), SUM, AVG, MIN, MAX aggregate functions.
//
// Example:
//
//	.GroupBy("status").HavingAgg("count", "", ">", "min_count")  // COUNT(*) > :min_count
//	.GroupBy("status").HavingAgg("sum", "amount", ">=", "min_total")  // SUM("amount") >= :min_total
func (qb *Query[T]) HavingAgg(aggFunc, field, operator, param string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	astqlOp, err := validateOperator(operator)
	if err != nil {
		qb.err = err
		return qb
	}

	aggCond, err := buildAggregateCondition(qb.instance, aggFunc, field, param, astqlOp)
	if err != nil {
		qb.err = err
		return qb
	}

	qb.builder = qb.builder.HavingAgg(aggCond)
	return qb
}

// ForUpdate adds FOR UPDATE row locking to the SELECT query.
// Locks selected rows for update, blocking other transactions from modifying them.
func (qb *Query[T]) ForUpdate() *Query[T] {
	qb.builder = qb.builder.ForUpdate()
	return qb
}

// ForNoKeyUpdate adds FOR NO KEY UPDATE row locking to the SELECT query.
// Similar to FOR UPDATE but allows SELECT FOR KEY SHARE on the same rows.
func (qb *Query[T]) ForNoKeyUpdate() *Query[T] {
	qb.builder = qb.builder.ForNoKeyUpdate()
	return qb
}

// ForShare adds FOR SHARE row locking to the SELECT query.
// Locks selected rows in share mode, allowing other SELECT FOR SHARE but blocking updates.
func (qb *Query[T]) ForShare() *Query[T] {
	qb.builder = qb.builder.ForShare()
	return qb
}

// ForKeyShare adds FOR KEY SHARE row locking to the SELECT query.
// The weakest lock level, blocks only FOR UPDATE but allows other locks.
func (qb *Query[T]) ForKeyShare() *Query[T] {
	qb.builder = qb.builder.ForKeyShare()
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
	if qb.err != nil {
		return nil, fmt.Errorf("query has errors: %w", qb.err)
	}

	result, err := qb.builder.Render()
	if err != nil {
		return nil, fmt.Errorf("failed to render SELECT query: %w", err)
	}

	return execMultipleRows[T](ctx, execer, result.SQL, params, qb.cereal.getTableName(), "QUERY")
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

// Union creates a compound query with UNION.
// UNION removes duplicate rows from the combined result set.
//
// Example:
//
//	results, err := cereal.Query().
//	    Where("status", "=", "active").
//	    Union(cereal.Query().Where("status", "=", "pending")).
//	    OrderBy("name", "asc").
//	    Exec(ctx, params)
func (qb *Query[T]) Union(other *Query[T]) *Compound[T] {
	if qb.err != nil {
		return &Compound[T]{
			instance: qb.instance,
			cereal:   qb.cereal,
			err:      qb.err,
		}
	}
	if other.err != nil {
		return &Compound[T]{
			instance: qb.instance,
			cereal:   qb.cereal,
			err:      other.err,
		}
	}

	return &Compound[T]{
		instance: qb.instance,
		builder:  qb.builder.Union(other.builder),
		cereal:   qb.cereal,
	}
}

// UnionAll creates a compound query with UNION ALL.
// UNION ALL keeps all rows including duplicates.
func (qb *Query[T]) UnionAll(other *Query[T]) *Compound[T] {
	if qb.err != nil {
		return &Compound[T]{
			instance: qb.instance,
			cereal:   qb.cereal,
			err:      qb.err,
		}
	}
	if other.err != nil {
		return &Compound[T]{
			instance: qb.instance,
			cereal:   qb.cereal,
			err:      other.err,
		}
	}

	return &Compound[T]{
		instance: qb.instance,
		builder:  qb.builder.UnionAll(other.builder),
		cereal:   qb.cereal,
	}
}

// Intersect creates a compound query with INTERSECT.
// INTERSECT returns only rows that appear in both result sets.
func (qb *Query[T]) Intersect(other *Query[T]) *Compound[T] {
	if qb.err != nil {
		return &Compound[T]{
			instance: qb.instance,
			cereal:   qb.cereal,
			err:      qb.err,
		}
	}
	if other.err != nil {
		return &Compound[T]{
			instance: qb.instance,
			cereal:   qb.cereal,
			err:      other.err,
		}
	}

	return &Compound[T]{
		instance: qb.instance,
		builder:  qb.builder.Intersect(other.builder),
		cereal:   qb.cereal,
	}
}

// IntersectAll creates a compound query with INTERSECT ALL.
// INTERSECT ALL keeps duplicates in the intersection.
func (qb *Query[T]) IntersectAll(other *Query[T]) *Compound[T] {
	if qb.err != nil {
		return &Compound[T]{
			instance: qb.instance,
			cereal:   qb.cereal,
			err:      qb.err,
		}
	}
	if other.err != nil {
		return &Compound[T]{
			instance: qb.instance,
			cereal:   qb.cereal,
			err:      other.err,
		}
	}

	return &Compound[T]{
		instance: qb.instance,
		builder:  qb.builder.IntersectAll(other.builder),
		cereal:   qb.cereal,
	}
}

// Except creates a compound query with EXCEPT.
// EXCEPT returns rows from the first query that don't appear in the second.
func (qb *Query[T]) Except(other *Query[T]) *Compound[T] {
	if qb.err != nil {
		return &Compound[T]{
			instance: qb.instance,
			cereal:   qb.cereal,
			err:      qb.err,
		}
	}
	if other.err != nil {
		return &Compound[T]{
			instance: qb.instance,
			cereal:   qb.cereal,
			err:      other.err,
		}
	}

	return &Compound[T]{
		instance: qb.instance,
		builder:  qb.builder.Except(other.builder),
		cereal:   qb.cereal,
	}
}

// ExceptAll creates a compound query with EXCEPT ALL.
// EXCEPT ALL keeps duplicates when computing the difference.
func (qb *Query[T]) ExceptAll(other *Query[T]) *Compound[T] {
	if qb.err != nil {
		return &Compound[T]{
			instance: qb.instance,
			cereal:   qb.cereal,
			err:      qb.err,
		}
	}
	if other.err != nil {
		return &Compound[T]{
			instance: qb.instance,
			cereal:   qb.cereal,
			err:      other.err,
		}
	}

	return &Compound[T]{
		instance: qb.instance,
		builder:  qb.builder.ExceptAll(other.builder),
		cereal:   qb.cereal,
	}
}
