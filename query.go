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

// WhereBetween adds a WHERE field BETWEEN low AND high condition.
// Multiple calls are combined with AND.
//
// Example:
//
//	.WhereBetween("age", "min_age", "max_age")
//	// params: map[string]any{"min_age": 18, "max_age": 65}
func (qb *Query[T]) WhereBetween(field, lowParam, highParam string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	lowP, err := qb.instance.TryP(lowParam)
	if err != nil {
		qb.err = fmt.Errorf("invalid low param %q: %w", lowParam, err)
		return qb
	}

	highP, err := qb.instance.TryP(highParam)
	if err != nil {
		qb.err = fmt.Errorf("invalid high param %q: %w", highParam, err)
		return qb
	}

	qb.builder = qb.builder.Where(astql.Between(f, lowP, highP))
	return qb
}

// WhereNotBetween adds a WHERE field NOT BETWEEN low AND high condition.
// Multiple calls are combined with AND.
//
// Example:
//
//	.WhereNotBetween("age", "min_age", "max_age")
//	// params: map[string]any{"min_age": 18, "max_age": 65}
func (qb *Query[T]) WhereNotBetween(field, lowParam, highParam string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	lowP, err := qb.instance.TryP(lowParam)
	if err != nil {
		qb.err = fmt.Errorf("invalid low param %q: %w", lowParam, err)
		return qb
	}

	highP, err := qb.instance.TryP(highParam)
	if err != nil {
		qb.err = fmt.Errorf("invalid high param %q: %w", highParam, err)
		return qb
	}

	qb.builder = qb.builder.Where(astql.NotBetween(f, lowP, highP))
	return qb
}

// WhereFields adds a WHERE condition comparing two fields.
// Multiple calls are combined with AND.
//
// Example:
//
//	.WhereFields("created_at", "<", "updated_at")
//	// WHERE "created_at" < "updated_at"
func (qb *Query[T]) WhereFields(leftField, operator, rightField string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	astqlOp, err := validateOperator(operator)
	if err != nil {
		qb.err = err
		return qb
	}

	left, err := qb.instance.TryF(leftField)
	if err != nil {
		qb.err = fmt.Errorf("invalid left field %q: %w", leftField, err)
		return qb
	}

	right, err := qb.instance.TryF(rightField)
	if err != nil {
		qb.err = fmt.Errorf("invalid right field %q: %w", rightField, err)
		return qb
	}

	qb.builder = qb.builder.Where(astql.CF(left, astqlOp, right))
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

// LimitParam sets the LIMIT clause to a parameterized value.
// Useful for API pagination where limit comes from request parameters.
//
// Example:
//
//	.LimitParam("page_size")
//	// params: map[string]any{"page_size": 10}
func (qb *Query[T]) LimitParam(param string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	p, err := qb.instance.TryP(param)
	if err != nil {
		qb.err = fmt.Errorf("invalid limit param %q: %w", param, err)
		return qb
	}

	qb.builder = qb.builder.LimitParam(p)
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

// OffsetParam sets the OFFSET clause to a parameterized value.
// Useful for API pagination where offset comes from request parameters.
//
// Example:
//
//	.OffsetParam("page_offset")
//	// params: map[string]any{"page_offset": 20}
func (qb *Query[T]) OffsetParam(param string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	p, err := qb.instance.TryP(param)
	if err != nil {
		qb.err = fmt.Errorf("invalid offset param %q: %w", param, err)
		return qb
	}

	qb.builder = qb.builder.OffsetParam(p)
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

// --- String Expression Methods ---

// SelectUpper adds UPPER(field) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectUpper("name", "upper_name")  // SELECT UPPER("name") AS "upper_name"
func (qb *Query[T]) SelectUpper(field, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.Upper(f), alias))
	return qb
}

// SelectLower adds LOWER(field) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectLower("email", "lower_email")  // SELECT LOWER("email") AS "lower_email"
func (qb *Query[T]) SelectLower(field, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.Lower(f), alias))
	return qb
}

// SelectLength adds LENGTH(field) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectLength("name", "name_length")  // SELECT LENGTH("name") AS "name_length"
func (qb *Query[T]) SelectLength(field, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.Length(f), alias))
	return qb
}

// SelectTrim adds TRIM(field) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectTrim("name", "trimmed_name")  // SELECT TRIM("name") AS "trimmed_name"
func (qb *Query[T]) SelectTrim(field, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.Trim(f), alias))
	return qb
}

// SelectLTrim adds LTRIM(field) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectLTrim("name", "ltrimmed")  // SELECT LTRIM("name") AS "ltrimmed"
func (qb *Query[T]) SelectLTrim(field, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.LTrim(f), alias))
	return qb
}

// SelectRTrim adds RTRIM(field) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectRTrim("name", "rtrimmed")  // SELECT RTRIM("name") AS "rtrimmed"
func (qb *Query[T]) SelectRTrim(field, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.RTrim(f), alias))
	return qb
}

// --- Math Expression Methods ---

// SelectAbs adds ABS(field) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectAbs("balance", "abs_balance")  // SELECT ABS("balance") AS "abs_balance"
func (qb *Query[T]) SelectAbs(field, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.Abs(f), alias))
	return qb
}

// SelectCeil adds CEIL(field) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectCeil("price", "rounded_up")  // SELECT CEIL("price") AS "rounded_up"
func (qb *Query[T]) SelectCeil(field, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.Ceil(f), alias))
	return qb
}

// SelectFloor adds FLOOR(field) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectFloor("price", "rounded_down")  // SELECT FLOOR("price") AS "rounded_down"
func (qb *Query[T]) SelectFloor(field, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.Floor(f), alias))
	return qb
}

// SelectRound adds ROUND(field) AS alias to the SELECT clause (no precision).
//
// Example:
//
//	.SelectRound("price", "rounded")  // SELECT ROUND("price") AS "rounded"
func (qb *Query[T]) SelectRound(field, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.Round(f), alias))
	return qb
}

// SelectSqrt adds SQRT(field) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectSqrt("variance", "std_dev")  // SELECT SQRT("variance") AS "std_dev"
func (qb *Query[T]) SelectSqrt(field, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.Sqrt(f), alias))
	return qb
}

// --- Cast Expression Methods ---

// SelectCast adds CAST(field AS type) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectCast("age", cereal.CastText, "age_str")  // SELECT CAST("age" AS TEXT) AS "age_str"
func (qb *Query[T]) SelectCast(field string, castType CastType, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.Cast(f, castType), alias))
	return qb
}

// --- Date Expression Methods (functions that don't need DatePart) ---

// SelectNow adds NOW() AS alias to the SELECT clause.
//
// Example:
//
//	.SelectNow("current_time")  // SELECT NOW() AS "current_time"
func (qb *Query[T]) SelectNow(alias string) *Query[T] {
	qb.builder = qb.builder.SelectExpr(astql.As(astql.Now(), alias))
	return qb
}

// SelectCurrentDate adds CURRENT_DATE AS alias to the SELECT clause.
//
// Example:
//
//	.SelectCurrentDate("today")  // SELECT CURRENT_DATE AS "today"
func (qb *Query[T]) SelectCurrentDate(alias string) *Query[T] {
	qb.builder = qb.builder.SelectExpr(astql.As(astql.CurrentDate(), alias))
	return qb
}

// SelectCurrentTime adds CURRENT_TIME AS alias to the SELECT clause.
//
// Example:
//
//	.SelectCurrentTime("now_time")  // SELECT CURRENT_TIME AS "now_time"
func (qb *Query[T]) SelectCurrentTime(alias string) *Query[T] {
	qb.builder = qb.builder.SelectExpr(astql.As(astql.CurrentTime(), alias))
	return qb
}

// SelectCurrentTimestamp adds CURRENT_TIMESTAMP AS alias to the SELECT clause.
//
// Example:
//
//	.SelectCurrentTimestamp("ts")  // SELECT CURRENT_TIMESTAMP AS "ts"
func (qb *Query[T]) SelectCurrentTimestamp(alias string) *Query[T] {
	qb.builder = qb.builder.SelectExpr(astql.As(astql.CurrentTimestamp(), alias))
	return qb
}

// --- Aggregate Expression Methods ---

// SelectCountStar adds COUNT(*) AS alias to the SELECT clause.
//
// Example:
//
//	.GroupBy("status").SelectCountStar("count")  // SELECT COUNT(*) AS "count"
func (qb *Query[T]) SelectCountStar(alias string) *Query[T] {
	qb.builder = qb.builder.SelectExpr(astql.As(astql.CountStar(), alias))
	return qb
}

// SelectCount adds COUNT(field) AS alias to the SELECT clause.
//
// Example:
//
//	.GroupBy("status").SelectCount("id", "count")  // SELECT COUNT("id") AS "count"
func (qb *Query[T]) SelectCount(field, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.CountField(f), alias))
	return qb
}

// SelectCountDistinct adds COUNT(DISTINCT field) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectCountDistinct("email", "unique_emails")  // SELECT COUNT(DISTINCT "email") AS "unique_emails"
func (qb *Query[T]) SelectCountDistinct(field, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.CountDistinct(f), alias))
	return qb
}

// SelectSum adds SUM(field) AS alias to the SELECT clause.
//
// Example:
//
//	.GroupBy("status").SelectSum("amount", "total")  // SELECT SUM("amount") AS "total"
func (qb *Query[T]) SelectSum(field, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.Sum(f), alias))
	return qb
}

// SelectAvg adds AVG(field) AS alias to the SELECT clause.
//
// Example:
//
//	.GroupBy("status").SelectAvg("age", "avg_age")  // SELECT AVG("age") AS "avg_age"
func (qb *Query[T]) SelectAvg(field, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.Avg(f), alias))
	return qb
}

// SelectMin adds MIN(field) AS alias to the SELECT clause.
//
// Example:
//
//	.GroupBy("status").SelectMin("age", "youngest")  // SELECT MIN("age") AS "youngest"
func (qb *Query[T]) SelectMin(field, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.Min(f), alias))
	return qb
}

// SelectMax adds MAX(field) AS alias to the SELECT clause.
//
// Example:
//
//	.GroupBy("status").SelectMax("age", "oldest")  // SELECT MAX("age") AS "oldest"
func (qb *Query[T]) SelectMax(field, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.Max(f), alias))
	return qb
}

// --- Aggregate FILTER Expression Methods ---

// SelectSumFilter adds SUM(field) FILTER (WHERE condition) AS alias to the SELECT clause.
// The filter condition is specified as field operator param.
//
// Example:
//
//	.SelectSumFilter("amount", "status", "=", "active", "active_total")
//	// SELECT SUM("amount") FILTER (WHERE "status" = :active) AS "active_total"
func (qb *Query[T]) SelectSumFilter(field, condField, condOp, condParam, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	filter, err := qb.buildSimpleCondition(condField, condOp, condParam)
	if err != nil {
		qb.err = err
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.SumFilter(f, filter), alias))
	return qb
}

// SelectAvgFilter adds AVG(field) FILTER (WHERE condition) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectAvgFilter("age", "active", "=", "true", "active_avg_age")
//	// SELECT AVG("age") FILTER (WHERE "active" = :true) AS "active_avg_age"
func (qb *Query[T]) SelectAvgFilter(field, condField, condOp, condParam, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	filter, err := qb.buildSimpleCondition(condField, condOp, condParam)
	if err != nil {
		qb.err = err
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.AvgFilter(f, filter), alias))
	return qb
}

// SelectMinFilter adds MIN(field) FILTER (WHERE condition) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectMinFilter("price", "category", "=", "electronics", "min_electronics")
func (qb *Query[T]) SelectMinFilter(field, condField, condOp, condParam, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	filter, err := qb.buildSimpleCondition(condField, condOp, condParam)
	if err != nil {
		qb.err = err
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.MinFilter(f, filter), alias))
	return qb
}

// SelectMaxFilter adds MAX(field) FILTER (WHERE condition) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectMaxFilter("score", "passed", "=", "true", "max_passed")
func (qb *Query[T]) SelectMaxFilter(field, condField, condOp, condParam, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	filter, err := qb.buildSimpleCondition(condField, condOp, condParam)
	if err != nil {
		qb.err = err
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.MaxFilter(f, filter), alias))
	return qb
}

// SelectCountFilter adds COUNT(field) FILTER (WHERE condition) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectCountFilter("id", "status", "=", "active", "active_count")
//	// SELECT COUNT("id") FILTER (WHERE "status" = :active) AS "active_count"
func (qb *Query[T]) SelectCountFilter(field, condField, condOp, condParam, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	filter, err := qb.buildSimpleCondition(condField, condOp, condParam)
	if err != nil {
		qb.err = err
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.CountFieldFilter(f, filter), alias))
	return qb
}

// SelectCountDistinctFilter adds COUNT(DISTINCT field) FILTER (WHERE condition) AS alias.
//
// Example:
//
//	.SelectCountDistinctFilter("email", "verified", "=", "true", "verified_users")
func (qb *Query[T]) SelectCountDistinctFilter(field, condField, condOp, condParam, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	filter, err := qb.buildSimpleCondition(condField, condOp, condParam)
	if err != nil {
		qb.err = err
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.CountDistinctFilter(f, filter), alias))
	return qb
}

// buildSimpleCondition creates a simple condition (field op param) for FILTER clauses.
func (qb *Query[T]) buildSimpleCondition(field, operator, param string) (astql.ConditionItem, error) {
	astqlOp, err := validateOperator(operator)
	if err != nil {
		return nil, err
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		return nil, fmt.Errorf("invalid field %q: %w", field, err)
	}

	p, err := qb.instance.TryP(param)
	if err != nil {
		return nil, fmt.Errorf("invalid param %q: %w", param, err)
	}

	return qb.instance.TryC(f, astqlOp, p)
}

// --- Additional String Expression Methods ---

// SelectSubstring adds SUBSTRING(field, start, length) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectSubstring("name", "start_pos", "substr_len", "short_name")
//	// SELECT SUBSTRING("name", :start_pos, :substr_len) AS "short_name"
func (qb *Query[T]) SelectSubstring(field, startParam, lengthParam, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	start, err := qb.instance.TryP(startParam)
	if err != nil {
		qb.err = fmt.Errorf("invalid start param %q: %w", startParam, err)
		return qb
	}

	length, err := qb.instance.TryP(lengthParam)
	if err != nil {
		qb.err = fmt.Errorf("invalid length param %q: %w", lengthParam, err)
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.Substring(f, start, length), alias))
	return qb
}

// SelectReplace adds REPLACE(field, search, replacement) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectReplace("name", "search_str", "replace_str", "replaced_name")
//	// SELECT REPLACE("name", :search_str, :replace_str) AS "replaced_name"
func (qb *Query[T]) SelectReplace(field, searchParam, replacementParam, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	search, err := qb.instance.TryP(searchParam)
	if err != nil {
		qb.err = fmt.Errorf("invalid search param %q: %w", searchParam, err)
		return qb
	}

	replacement, err := qb.instance.TryP(replacementParam)
	if err != nil {
		qb.err = fmt.Errorf("invalid replacement param %q: %w", replacementParam, err)
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.Replace(f, search, replacement), alias))
	return qb
}

// SelectConcat adds CONCAT(field1, field2, ...) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectConcat("full_name", "name", "email")  // SELECT CONCAT("name", "email") AS "full_name"
func (qb *Query[T]) SelectConcat(alias string, fields ...string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	if len(fields) < 2 {
		qb.err = fmt.Errorf("CONCAT requires at least 2 fields")
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

	qb.builder = qb.builder.SelectExpr(astql.As(astql.Concat(astqlFields...), alias))
	return qb
}

// --- Additional Math Expression Methods ---

// SelectPower adds POWER(field, exponent) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectPower("value", "exponent", "powered")  // SELECT POWER("value", :exponent) AS "powered"
func (qb *Query[T]) SelectPower(field, exponentParam, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		qb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qb
	}

	exp, err := qb.instance.TryP(exponentParam)
	if err != nil {
		qb.err = fmt.Errorf("invalid exponent param %q: %w", exponentParam, err)
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.Power(f, exp), alias))
	return qb
}

// --- Null Handling Expression Methods ---

// SelectCoalesce adds COALESCE(param1, param2, ...) AS alias to the SELECT clause.
// Returns the first non-null value from the parameters.
// Requires at least 2 parameters.
//
// Example:
//
//	.SelectCoalesce("display_name", "nickname", "name", "default_name")
//	// SELECT COALESCE(:nickname, :name, :default_name) AS "display_name"
func (qb *Query[T]) SelectCoalesce(alias string, params ...string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	if len(params) < 2 {
		qb.err = fmt.Errorf("COALESCE requires at least 2 parameters, got %d", len(params))
		return qb
	}

	// Build params slice using Params() factory
	astqlParams := qb.instance.Params()
	for _, param := range params {
		p, err := qb.instance.TryP(param)
		if err != nil {
			qb.err = fmt.Errorf("invalid param %q: %w", param, err)
			return qb
		}
		astqlParams = append(astqlParams, p)
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.Coalesce(astqlParams...), alias))
	return qb
}

// SelectNullIf adds NULLIF(param1, param2) AS alias to the SELECT clause.
// Returns NULL if param1 equals param2, otherwise returns param1.
//
// Example:
//
//	.SelectNullIf("value", "empty_val", "result")
//	// SELECT NULLIF(:value, :empty_val) AS "result"
func (qb *Query[T]) SelectNullIf(param1, param2, alias string) *Query[T] {
	if qb.err != nil {
		return qb
	}

	p1, err := qb.instance.TryP(param1)
	if err != nil {
		qb.err = fmt.Errorf("invalid param1 %q: %w", param1, err)
		return qb
	}

	p2, err := qb.instance.TryP(param2)
	if err != nil {
		qb.err = fmt.Errorf("invalid param2 %q: %w", param2, err)
		return qb
	}

	qb.builder = qb.builder.SelectExpr(astql.As(astql.NullIf(p1, p2), alias))
	return qb
}

// --- CASE Expression Methods ---

// SelectCase starts building a CASE expression for the SELECT clause.
// Returns a QueryCaseBuilder that allows chaining When/Else/As clauses.
// Call End() to complete the CASE expression and return to the Query builder.
//
// Example:
//
//	cereal.Query().
//	    SelectCase().
//	        When("status", "=", "status_active", "result_active").
//	        When("status", "=", "status_pending", "result_pending").
//	        Else("result_default").
//	        As("status_label").
//	    End().
//	    Exec(ctx, params)
func (qb *Query[T]) SelectCase() *QueryCaseBuilder[T] {
	return &QueryCaseBuilder[T]{
		queryBuilder: qb,
		instance:     qb.instance,
		caseExpr:     astql.Case(),
	}
}

// QueryCaseBuilder provides a fluent API for building CASE expressions
// with string-based field and parameter names.
type QueryCaseBuilder[T any] struct {
	queryBuilder *Query[T]
	instance     *astql.ASTQL
	caseExpr     *astql.CaseBuilder
	err          error
}

// When adds a WHEN...THEN clause to the CASE expression.
// The condition is specified as field operator param, and resultParam is the THEN value.
//
// Example:
//
//	.When("status", "=", "status_val", "result_val")  // WHEN "status" = :status_val THEN :result_val
func (qcb *QueryCaseBuilder[T]) When(field, operator, param, resultParam string) *QueryCaseBuilder[T] {
	if qcb.err != nil {
		return qcb
	}

	condition, err := buildCaseWhenCondition(qcb.instance, field, operator, param)
	if err != nil {
		qcb.err = err
		return qcb
	}

	result, err := qcb.instance.TryP(resultParam)
	if err != nil {
		qcb.err = fmt.Errorf("invalid result param %q: %w", resultParam, err)
		return qcb
	}

	qcb.caseExpr.When(condition, result)
	return qcb
}

// WhenNull adds a WHEN field IS NULL THEN resultParam clause.
//
// Example:
//
//	.WhenNull("status", "result_null")  // WHEN "status" IS NULL THEN :result_null
func (qcb *QueryCaseBuilder[T]) WhenNull(field, resultParam string) *QueryCaseBuilder[T] {
	if qcb.err != nil {
		return qcb
	}

	f, err := qcb.instance.TryF(field)
	if err != nil {
		qcb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qcb
	}

	condition, err := qcb.instance.TryNull(f)
	if err != nil {
		qcb.err = fmt.Errorf("invalid NULL condition: %w", err)
		return qcb
	}

	result, err := qcb.instance.TryP(resultParam)
	if err != nil {
		qcb.err = fmt.Errorf("invalid result param %q: %w", resultParam, err)
		return qcb
	}

	qcb.caseExpr.When(condition, result)
	return qcb
}

// WhenNotNull adds a WHEN field IS NOT NULL THEN resultParam clause.
//
// Example:
//
//	.WhenNotNull("status", "result_not_null")  // WHEN "status" IS NOT NULL THEN :result_not_null
func (qcb *QueryCaseBuilder[T]) WhenNotNull(field, resultParam string) *QueryCaseBuilder[T] {
	if qcb.err != nil {
		return qcb
	}

	f, err := qcb.instance.TryF(field)
	if err != nil {
		qcb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return qcb
	}

	condition, err := qcb.instance.TryNotNull(f)
	if err != nil {
		qcb.err = fmt.Errorf("invalid NOT NULL condition: %w", err)
		return qcb
	}

	result, err := qcb.instance.TryP(resultParam)
	if err != nil {
		qcb.err = fmt.Errorf("invalid result param %q: %w", resultParam, err)
		return qcb
	}

	qcb.caseExpr.When(condition, result)
	return qcb
}

// Else sets the ELSE clause of the CASE expression.
//
// Example:
//
//	.Else("default_result")  // ELSE :default_result
func (qcb *QueryCaseBuilder[T]) Else(resultParam string) *QueryCaseBuilder[T] {
	if qcb.err != nil {
		return qcb
	}

	result, err := qcb.instance.TryP(resultParam)
	if err != nil {
		qcb.err = fmt.Errorf("invalid result param %q: %w", resultParam, err)
		return qcb
	}

	qcb.caseExpr.Else(result)
	return qcb
}

// As sets the alias for the CASE expression.
//
// Example:
//
//	.As("status_label")  // AS "status_label"
func (qcb *QueryCaseBuilder[T]) As(alias string) *QueryCaseBuilder[T] {
	if qcb.err != nil {
		return qcb
	}
	qcb.caseExpr.As(alias)
	return qcb
}

// End completes the CASE expression and adds it to the SELECT clause.
// Returns the parent Query builder to continue chaining.
func (qcb *QueryCaseBuilder[T]) End() *Query[T] {
	if qcb.err != nil {
		qcb.queryBuilder.err = qcb.err
		return qcb.queryBuilder
	}

	qcb.queryBuilder.builder = qcb.queryBuilder.builder.SelectExpr(qcb.caseExpr.Build())
	return qcb.queryBuilder
}

// --- Window Function Methods ---

// SelectRowNumber starts building a ROW_NUMBER() window function.
// Returns a QueryWindowBuilder for configuring the window specification.
//
// Example:
//
//	cereal.Query().
//	    SelectRowNumber().
//	    OrderBy("created_at", "DESC").
//	    As("row_num").
//	    End()
func (qb *Query[T]) SelectRowNumber() *QueryWindowBuilder[T] {
	return &QueryWindowBuilder[T]{
		queryBuilder: qb,
		instance:     qb.instance,
		windowExpr:   astql.RowNumber(),
	}
}

// SelectRank starts building a RANK() window function.
//
// Example:
//
//	.SelectRank().OrderBy("score", "DESC").As("rank").End()
func (qb *Query[T]) SelectRank() *QueryWindowBuilder[T] {
	return &QueryWindowBuilder[T]{
		queryBuilder: qb,
		instance:     qb.instance,
		windowExpr:   astql.Rank(),
	}
}

// SelectDenseRank starts building a DENSE_RANK() window function.
//
// Example:
//
//	.SelectDenseRank().OrderBy("score", "DESC").As("dense_rank").End()
func (qb *Query[T]) SelectDenseRank() *QueryWindowBuilder[T] {
	return &QueryWindowBuilder[T]{
		queryBuilder: qb,
		instance:     qb.instance,
		windowExpr:   astql.DenseRank(),
	}
}

// SelectNtile starts building an NTILE(n) window function.
//
// Example:
//
//	.SelectNtile("num_buckets").OrderBy("value", "ASC").As("quartile").End()
func (qb *Query[T]) SelectNtile(nParam string) *QueryWindowBuilder[T] {
	if qb.err != nil {
		return &QueryWindowBuilder[T]{
			queryBuilder: qb,
			instance:     qb.instance,
			err:          qb.err,
		}
	}

	n, err := qb.instance.TryP(nParam)
	if err != nil {
		return &QueryWindowBuilder[T]{
			queryBuilder: qb,
			instance:     qb.instance,
			err:          fmt.Errorf("invalid ntile param %q: %w", nParam, err),
		}
	}

	return &QueryWindowBuilder[T]{
		queryBuilder: qb,
		instance:     qb.instance,
		windowExpr:   astql.Ntile(n),
	}
}

// SelectLag starts building a LAG(field, offset) window function.
//
// Example:
//
//	.SelectLag("price", "offset").OrderBy("date", "ASC").As("prev_price").End()
func (qb *Query[T]) SelectLag(field, offsetParam string) *QueryWindowBuilder[T] {
	if qb.err != nil {
		return &QueryWindowBuilder[T]{
			queryBuilder: qb,
			instance:     qb.instance,
			err:          qb.err,
		}
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		return &QueryWindowBuilder[T]{
			queryBuilder: qb,
			instance:     qb.instance,
			err:          fmt.Errorf("invalid field %q: %w", field, err),
		}
	}

	offset, err := qb.instance.TryP(offsetParam)
	if err != nil {
		return &QueryWindowBuilder[T]{
			queryBuilder: qb,
			instance:     qb.instance,
			err:          fmt.Errorf("invalid offset param %q: %w", offsetParam, err),
		}
	}

	return &QueryWindowBuilder[T]{
		queryBuilder: qb,
		instance:     qb.instance,
		windowExpr:   astql.Lag(f, offset),
	}
}

// SelectLead starts building a LEAD(field, offset) window function.
//
// Example:
//
//	.SelectLead("price", "offset").OrderBy("date", "ASC").As("next_price").End()
func (qb *Query[T]) SelectLead(field, offsetParam string) *QueryWindowBuilder[T] {
	if qb.err != nil {
		return &QueryWindowBuilder[T]{
			queryBuilder: qb,
			instance:     qb.instance,
			err:          qb.err,
		}
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		return &QueryWindowBuilder[T]{
			queryBuilder: qb,
			instance:     qb.instance,
			err:          fmt.Errorf("invalid field %q: %w", field, err),
		}
	}

	offset, err := qb.instance.TryP(offsetParam)
	if err != nil {
		return &QueryWindowBuilder[T]{
			queryBuilder: qb,
			instance:     qb.instance,
			err:          fmt.Errorf("invalid offset param %q: %w", offsetParam, err),
		}
	}

	return &QueryWindowBuilder[T]{
		queryBuilder: qb,
		instance:     qb.instance,
		windowExpr:   astql.Lead(f, offset),
	}
}

// SelectFirstValue starts building a FIRST_VALUE(field) window function.
//
// Example:
//
//	.SelectFirstValue("price").OrderBy("date", "ASC").As("first_price").End()
func (qb *Query[T]) SelectFirstValue(field string) *QueryWindowBuilder[T] {
	if qb.err != nil {
		return &QueryWindowBuilder[T]{
			queryBuilder: qb,
			instance:     qb.instance,
			err:          qb.err,
		}
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		return &QueryWindowBuilder[T]{
			queryBuilder: qb,
			instance:     qb.instance,
			err:          fmt.Errorf("invalid field %q: %w", field, err),
		}
	}

	return &QueryWindowBuilder[T]{
		queryBuilder: qb,
		instance:     qb.instance,
		windowExpr:   astql.FirstValue(f),
	}
}

// SelectLastValue starts building a LAST_VALUE(field) window function.
//
// Example:
//
//	.SelectLastValue("price").OrderBy("date", "ASC").As("last_price").End()
func (qb *Query[T]) SelectLastValue(field string) *QueryWindowBuilder[T] {
	if qb.err != nil {
		return &QueryWindowBuilder[T]{
			queryBuilder: qb,
			instance:     qb.instance,
			err:          qb.err,
		}
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		return &QueryWindowBuilder[T]{
			queryBuilder: qb,
			instance:     qb.instance,
			err:          fmt.Errorf("invalid field %q: %w", field, err),
		}
	}

	return &QueryWindowBuilder[T]{
		queryBuilder: qb,
		instance:     qb.instance,
		windowExpr:   astql.LastValue(f),
	}
}

// SelectSumOver starts building a SUM(field) OVER window function.
//
// Example:
//
//	.SelectSumOver("amount").PartitionBy("category").As("running_total").End()
func (qb *Query[T]) SelectSumOver(field string) *QueryWindowBuilder[T] {
	if qb.err != nil {
		return &QueryWindowBuilder[T]{
			queryBuilder: qb,
			instance:     qb.instance,
			err:          qb.err,
		}
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		return &QueryWindowBuilder[T]{
			queryBuilder: qb,
			instance:     qb.instance,
			err:          fmt.Errorf("invalid field %q: %w", field, err),
		}
	}

	return &QueryWindowBuilder[T]{
		queryBuilder: qb,
		instance:     qb.instance,
		windowExpr:   astql.SumOver(f),
	}
}

// SelectAvgOver starts building an AVG(field) OVER window function.
//
// Example:
//
//	.SelectAvgOver("score").PartitionBy("category").As("avg_score").End()
func (qb *Query[T]) SelectAvgOver(field string) *QueryWindowBuilder[T] {
	if qb.err != nil {
		return &QueryWindowBuilder[T]{
			queryBuilder: qb,
			instance:     qb.instance,
			err:          qb.err,
		}
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		return &QueryWindowBuilder[T]{
			queryBuilder: qb,
			instance:     qb.instance,
			err:          fmt.Errorf("invalid field %q: %w", field, err),
		}
	}

	return &QueryWindowBuilder[T]{
		queryBuilder: qb,
		instance:     qb.instance,
		windowExpr:   astql.AvgOver(f),
	}
}

// SelectCountOver starts building a COUNT(*) OVER window function.
//
// Example:
//
//	.SelectCountOver().PartitionBy("category").As("category_count").End()
func (qb *Query[T]) SelectCountOver() *QueryWindowBuilder[T] {
	return &QueryWindowBuilder[T]{
		queryBuilder: qb,
		instance:     qb.instance,
		windowExpr:   astql.CountOver(),
	}
}

// SelectMinOver starts building a MIN(field) OVER window function.
//
// Example:
//
//	.SelectMinOver("price").PartitionBy("category").As("min_price").End()
func (qb *Query[T]) SelectMinOver(field string) *QueryWindowBuilder[T] {
	if qb.err != nil {
		return &QueryWindowBuilder[T]{
			queryBuilder: qb,
			instance:     qb.instance,
			err:          qb.err,
		}
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		return &QueryWindowBuilder[T]{
			queryBuilder: qb,
			instance:     qb.instance,
			err:          fmt.Errorf("invalid field %q: %w", field, err),
		}
	}

	return &QueryWindowBuilder[T]{
		queryBuilder: qb,
		instance:     qb.instance,
		windowExpr:   astql.MinOver(f),
	}
}

// SelectMaxOver starts building a MAX(field) OVER window function.
//
// Example:
//
//	.SelectMaxOver("price").PartitionBy("category").As("max_price").End()
func (qb *Query[T]) SelectMaxOver(field string) *QueryWindowBuilder[T] {
	if qb.err != nil {
		return &QueryWindowBuilder[T]{
			queryBuilder: qb,
			instance:     qb.instance,
			err:          qb.err,
		}
	}

	f, err := qb.instance.TryF(field)
	if err != nil {
		return &QueryWindowBuilder[T]{
			queryBuilder: qb,
			instance:     qb.instance,
			err:          fmt.Errorf("invalid field %q: %w", field, err),
		}
	}

	return &QueryWindowBuilder[T]{
		queryBuilder: qb,
		instance:     qb.instance,
		windowExpr:   astql.MaxOver(f),
	}
}

// QueryWindowBuilder provides a fluent API for building window function expressions.
type QueryWindowBuilder[T any] struct {
	queryBuilder *Query[T]
	instance     *astql.ASTQL
	windowExpr   *astql.WindowBuilder
	err          error
	alias        string
}

// PartitionBy adds PARTITION BY fields to the window specification.
//
// Example:
//
//	.PartitionBy("department", "location")
func (qwb *QueryWindowBuilder[T]) PartitionBy(fields ...string) *QueryWindowBuilder[T] {
	if qwb.err != nil {
		return qwb
	}

	astqlFields := qwb.instance.Fields()
	for _, field := range fields {
		f, err := qwb.instance.TryF(field)
		if err != nil {
			qwb.err = fmt.Errorf("invalid partition field %q: %w", field, err)
			return qwb
		}
		astqlFields = append(astqlFields, f)
	}

	qwb.windowExpr.PartitionBy(astqlFields...)
	return qwb
}

// OrderBy adds ORDER BY to the window specification.
// Direction must be "ASC" or "DESC".
//
// Example:
//
//	.OrderBy("created_at", "DESC")
func (qwb *QueryWindowBuilder[T]) OrderBy(field, direction string) *QueryWindowBuilder[T] {
	if qwb.err != nil {
		return qwb
	}

	astqlDir, err := validateDirection(direction)
	if err != nil {
		qwb.err = err
		return qwb
	}

	f, err := qwb.instance.TryF(field)
	if err != nil {
		qwb.err = fmt.Errorf("invalid order field %q: %w", field, err)
		return qwb
	}

	qwb.windowExpr.OrderBy(f, astqlDir)
	return qwb
}

// Frame sets the frame clause with ROWS BETWEEN start AND end.
// Valid bounds: "UNBOUNDED PRECEDING", "CURRENT ROW", "UNBOUNDED FOLLOWING"
//
// Example:
//
//	.Frame("UNBOUNDED PRECEDING", "CURRENT ROW")
func (qwb *QueryWindowBuilder[T]) Frame(start, end string) *QueryWindowBuilder[T] {
	if qwb.err != nil {
		return qwb
	}

	startBound, err := validateFrameBound(start)
	if err != nil {
		qwb.err = fmt.Errorf("invalid frame start: %w", err)
		return qwb
	}

	endBound, err := validateFrameBound(end)
	if err != nil {
		qwb.err = fmt.Errorf("invalid frame end: %w", err)
		return qwb
	}

	qwb.windowExpr.Frame(startBound, endBound)
	return qwb
}

// As sets the alias for the window function result.
//
// Example:
//
//	.As("row_num")
func (qwb *QueryWindowBuilder[T]) As(alias string) *QueryWindowBuilder[T] {
	if qwb.err != nil {
		return qwb
	}
	qwb.alias = alias
	return qwb
}

// End completes the window function and adds it to the SELECT clause.
// Returns the parent Query builder to continue chaining.
func (qwb *QueryWindowBuilder[T]) End() *Query[T] {
	if qwb.err != nil {
		qwb.queryBuilder.err = qwb.err
		return qwb.queryBuilder
	}

	if qwb.alias != "" {
		qwb.queryBuilder.builder = qwb.queryBuilder.builder.SelectExpr(qwb.windowExpr.As(qwb.alias))
	} else {
		qwb.queryBuilder.builder = qwb.queryBuilder.builder.SelectExpr(qwb.windowExpr.Build())
	}
	return qwb.queryBuilder
}

// buildCondition converts a Condition to an ASTQL condition.
func (qb *Query[T]) buildCondition(cond Condition) (astql.ConditionItem, error) {
	return buildConditionWithInstance(qb.instance, cond)
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

	result, err := qb.builder.Render(qb.cereal.renderer())
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

	result, err := qb.builder.Render(qb.cereal.renderer())
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
