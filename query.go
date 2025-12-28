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
	qb.builder, qb.err = fieldsImpl(qb.instance, qb.builder, fields...)
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
	qb.builder, qb.err = whereImpl(qb.instance, qb.builder, field, operator, param)
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
	qb.builder, qb.err = whereAndImpl(qb.instance, qb.builder, conditions...)
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
	qb.builder, qb.err = whereOrImpl(qb.instance, qb.builder, conditions...)
	return qb
}

// WhereNull adds a WHERE field IS NULL condition.
func (qb *Query[T]) WhereNull(field string) *Query[T] {
	if qb.err != nil {
		return qb
	}
	qb.builder, qb.err = whereNullImpl(qb.instance, qb.builder, field)
	return qb
}

// WhereNotNull adds a WHERE field IS NOT NULL condition.
func (qb *Query[T]) WhereNotNull(field string) *Query[T] {
	if qb.err != nil {
		return qb
	}
	qb.builder, qb.err = whereNotNullImpl(qb.instance, qb.builder, field)
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
	qb.builder, qb.err = whereBetweenImpl(qb.instance, qb.builder, field, lowParam, highParam)
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
	qb.builder, qb.err = whereNotBetweenImpl(qb.instance, qb.builder, field, lowParam, highParam)
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
	qb.builder, qb.err = whereFieldsImpl(qb.instance, qb.builder, leftField, operator, rightField)
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
	qb.builder, qb.err = orderByImpl(qb.instance, qb.builder, field, direction)
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
	qb.builder, qb.err = orderByNullsImpl(qb.instance, qb.builder, field, direction, nulls)
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
	qb.builder, qb.err = orderByExprImpl(qb.instance, qb.builder, field, operator, param, direction)
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
	qb.builder, qb.err = limitParamImpl(qb.instance, qb.builder, param)
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
	qb.builder, qb.err = offsetParamImpl(qb.instance, qb.builder, param)
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
	qb.builder, qb.err = distinctOnImpl(qb.instance, qb.builder, fields...)
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
	qb.builder, qb.err = groupByImpl(qb.instance, qb.builder, fields...)
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
	qb.builder, qb.err = havingImpl(qb.instance, qb.builder, field, operator, param)
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
	qb.builder, qb.err = havingAggImpl(qb.instance, qb.builder, aggFunc, field, operator, param)
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
	qb.builder, qb.err = selectUpperImpl(qb.instance, qb.builder, field, alias)
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
	qb.builder, qb.err = selectLowerImpl(qb.instance, qb.builder, field, alias)
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
	qb.builder, qb.err = selectLengthImpl(qb.instance, qb.builder, field, alias)
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
	qb.builder, qb.err = selectTrimImpl(qb.instance, qb.builder, field, alias)
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
	qb.builder, qb.err = selectLTrimImpl(qb.instance, qb.builder, field, alias)
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
	qb.builder, qb.err = selectRTrimImpl(qb.instance, qb.builder, field, alias)
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
	qb.builder, qb.err = selectAbsImpl(qb.instance, qb.builder, field, alias)
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
	qb.builder, qb.err = selectCeilImpl(qb.instance, qb.builder, field, alias)
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
	qb.builder, qb.err = selectFloorImpl(qb.instance, qb.builder, field, alias)
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
	qb.builder, qb.err = selectRoundImpl(qb.instance, qb.builder, field, alias)
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
	qb.builder, qb.err = selectSqrtImpl(qb.instance, qb.builder, field, alias)
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
	qb.builder, qb.err = selectCastImpl(qb.instance, qb.builder, field, castType, alias)
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
	qb.builder, qb.err = selectCountImpl(qb.instance, qb.builder, field, alias)
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
	qb.builder, qb.err = selectCountDistinctImpl(qb.instance, qb.builder, field, alias)
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
	qb.builder, qb.err = selectSumImpl(qb.instance, qb.builder, field, alias)
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
	qb.builder, qb.err = selectAvgImpl(qb.instance, qb.builder, field, alias)
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
	qb.builder, qb.err = selectMinImpl(qb.instance, qb.builder, field, alias)
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
	qb.builder, qb.err = selectMaxImpl(qb.instance, qb.builder, field, alias)
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
	qb.builder, qb.err = selectSumFilterImpl(qb.instance, qb.builder, field, condField, condOp, condParam, alias)
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
	qb.builder, qb.err = selectAvgFilterImpl(qb.instance, qb.builder, field, condField, condOp, condParam, alias)
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
	qb.builder, qb.err = selectMinFilterImpl(qb.instance, qb.builder, field, condField, condOp, condParam, alias)
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
	qb.builder, qb.err = selectMaxFilterImpl(qb.instance, qb.builder, field, condField, condOp, condParam, alias)
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
	qb.builder, qb.err = selectCountFilterImpl(qb.instance, qb.builder, field, condField, condOp, condParam, alias)
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
	qb.builder, qb.err = selectCountDistinctFilterImpl(qb.instance, qb.builder, field, condField, condOp, condParam, alias)
	return qb
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
	qb.builder, qb.err = selectSubstringImpl(qb.instance, qb.builder, field, startParam, lengthParam, alias)
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
	qb.builder, qb.err = selectReplaceImpl(qb.instance, qb.builder, field, searchParam, replacementParam, alias)
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
	qb.builder, qb.err = selectConcatImpl(qb.instance, qb.builder, alias, fields...)
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
	qb.builder, qb.err = selectPowerImpl(qb.instance, qb.builder, field, exponentParam, alias)
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
	qb.builder, qb.err = selectCoalesceImpl(qb.instance, qb.builder, alias, params...)
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
	qb.builder, qb.err = selectNullIfImpl(qb.instance, qb.builder, param1, param2, alias)
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
	return newQueryCaseBuilder(qb)
}

// QueryCaseBuilder is now defined in case.go

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
	return newQueryWindowBuilder(qb, createRowNumberWindow(qb.instance))
}

// SelectRank starts building a RANK() window function.
//
// Example:
//
//	.SelectRank().OrderBy("score", "DESC").As("rank").End()
func (qb *Query[T]) SelectRank() *QueryWindowBuilder[T] {
	return newQueryWindowBuilder(qb, createRankWindow(qb.instance))
}

// SelectDenseRank starts building a DENSE_RANK() window function.
//
// Example:
//
//	.SelectDenseRank().OrderBy("score", "DESC").As("dense_rank").End()
func (qb *Query[T]) SelectDenseRank() *QueryWindowBuilder[T] {
	return newQueryWindowBuilder(qb, createDenseRankWindow(qb.instance))
}

// SelectNtile starts building an NTILE(n) window function.
//
// Example:
//
//	.SelectNtile("num_buckets").OrderBy("value", "ASC").As("quartile").End()
func (qb *Query[T]) SelectNtile(nParam string) *QueryWindowBuilder[T] {
	if qb.err != nil {
		return newQueryWindowBuilder(qb, newWindowStateWithError(qb.instance, qb.err))
	}
	return newQueryWindowBuilder(qb, createNtileWindow(qb.instance, nParam))
}

// SelectLag starts building a LAG(field, offset) window function.
//
// Example:
//
//	.SelectLag("price", "offset").OrderBy("date", "ASC").As("prev_price").End()
func (qb *Query[T]) SelectLag(field, offsetParam string) *QueryWindowBuilder[T] {
	if qb.err != nil {
		return newQueryWindowBuilder(qb, newWindowStateWithError(qb.instance, qb.err))
	}
	return newQueryWindowBuilder(qb, createLagWindow(qb.instance, field, offsetParam))
}

// SelectLead starts building a LEAD(field, offset) window function.
//
// Example:
//
//	.SelectLead("price", "offset").OrderBy("date", "ASC").As("next_price").End()
func (qb *Query[T]) SelectLead(field, offsetParam string) *QueryWindowBuilder[T] {
	if qb.err != nil {
		return newQueryWindowBuilder(qb, newWindowStateWithError(qb.instance, qb.err))
	}
	return newQueryWindowBuilder(qb, createLeadWindow(qb.instance, field, offsetParam))
}

// SelectFirstValue starts building a FIRST_VALUE(field) window function.
//
// Example:
//
//	.SelectFirstValue("price").OrderBy("date", "ASC").As("first_price").End()
func (qb *Query[T]) SelectFirstValue(field string) *QueryWindowBuilder[T] {
	if qb.err != nil {
		return newQueryWindowBuilder(qb, newWindowStateWithError(qb.instance, qb.err))
	}
	return newQueryWindowBuilder(qb, createFirstValueWindow(qb.instance, field))
}

// SelectLastValue starts building a LAST_VALUE(field) window function.
//
// Example:
//
//	.SelectLastValue("price").OrderBy("date", "ASC").As("last_price").End()
func (qb *Query[T]) SelectLastValue(field string) *QueryWindowBuilder[T] {
	if qb.err != nil {
		return newQueryWindowBuilder(qb, newWindowStateWithError(qb.instance, qb.err))
	}
	return newQueryWindowBuilder(qb, createLastValueWindow(qb.instance, field))
}

// SelectSumOver starts building a SUM(field) OVER window function.
//
// Example:
//
//	.SelectSumOver("amount").PartitionBy("category").As("running_total").End()
func (qb *Query[T]) SelectSumOver(field string) *QueryWindowBuilder[T] {
	if qb.err != nil {
		return newQueryWindowBuilder(qb, newWindowStateWithError(qb.instance, qb.err))
	}
	return newQueryWindowBuilder(qb, createSumOverWindow(qb.instance, field))
}

// SelectAvgOver starts building an AVG(field) OVER window function.
//
// Example:
//
//	.SelectAvgOver("score").PartitionBy("category").As("avg_score").End()
func (qb *Query[T]) SelectAvgOver(field string) *QueryWindowBuilder[T] {
	if qb.err != nil {
		return newQueryWindowBuilder(qb, newWindowStateWithError(qb.instance, qb.err))
	}
	return newQueryWindowBuilder(qb, createAvgOverWindow(qb.instance, field))
}

// SelectCountOver starts building a COUNT(*) OVER window function.
//
// Example:
//
//	.SelectCountOver().PartitionBy("category").As("category_count").End()
func (qb *Query[T]) SelectCountOver() *QueryWindowBuilder[T] {
	return newQueryWindowBuilder(qb, createCountOverWindow(qb.instance))
}

// SelectMinOver starts building a MIN(field) OVER window function.
//
// Example:
//
//	.SelectMinOver("price").PartitionBy("category").As("min_price").End()
func (qb *Query[T]) SelectMinOver(field string) *QueryWindowBuilder[T] {
	if qb.err != nil {
		return newQueryWindowBuilder(qb, newWindowStateWithError(qb.instance, qb.err))
	}
	return newQueryWindowBuilder(qb, createMinOverWindow(qb.instance, field))
}

// SelectMaxOver starts building a MAX(field) OVER window function.
//
// Example:
//
//	.SelectMaxOver("price").PartitionBy("category").As("max_price").End()
func (qb *Query[T]) SelectMaxOver(field string) *QueryWindowBuilder[T] {
	if qb.err != nil {
		return newQueryWindowBuilder(qb, newWindowStateWithError(qb.instance, qb.err))
	}
	return newQueryWindowBuilder(qb, createMaxOverWindow(qb.instance, field))
}

// QueryWindowBuilder is now defined in window.go

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
