package soy

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql"
	"github.com/zoobzio/atom"
	"github.com/zoobzio/capitan"
	"github.com/zoobzio/sentinel"
	"github.com/zoobzio/soy/internal/scanner"
)

// soyExecutor provides the interface for executing queries.
// This allows s to access DB and table info without circular dependencies.
type soyExecutor interface {
	execer() sqlx.ExtContext
	getTableName() string
	renderer() astql.Renderer
	atomScanner() *scanner.Scanner
	getMetadata() sentinel.Metadata
	getInstance() *astql.ASTQL
}

// Select provides a focused API for building SELECT queries that return a single record.
// It wraps ASTQL's  functionality with a simple string-based interface.
// Use this for queries expected to return exactly one row (e.g., GET by ID, fetch one record).
type Select[T any] struct {
	instance *astql.ASTQL
	builder  *astql.Builder
	soy      soyExecutor // interface for execution
	err      error       // stores first error encountered during building
}

// Condition represents a WHERE condition with string-based components.
type Condition struct {
	field     string
	operator  string
	param     string
	isNull    bool
	isBetween bool   // true for BETWEEN/NOT BETWEEN
	lowParam  string // low value param for BETWEEN
	highParam string // high value param for BETWEEN
}

// Note: operatorMap, directionMap, nullsMap, and validate* functions
// are defined in builder.go to avoid duplication.

// Fields specifies which fields to select. Field names must exist in the schema.
// If not called, SELECT * is used by default.
func (sb *Select[T]) Fields(fields ...string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = fieldsImpl(sb.instance, sb.builder, fields...)
	return sb
}

// Where adds a simple WHERE condition with field = param pattern.
// Multiple calls are combined with AND.
//
// Example:
//
//	.Where("email", "=", "user_email")
func (sb *Select[T]) Where(field, operator, param string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = whereImpl(sb.instance, sb.builder, field, operator, param)
	return sb
}

// WhereAnd adds multiple conditions combined with AND.
//
// Example:
//
//	.WhereAnd(
//	    soy.C("age", ">=", "min_age"),
//	    soy.C("age", "<=", "max_age"),
//	)
func (sb *Select[T]) WhereAnd(conditions ...Condition) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = whereAndImpl(sb.instance, sb.builder, conditions...)
	return sb
}

// WhereOr adds multiple conditions combined with OR.
//
// Example:
//
//	.WhereOr(
//	    soy.C("status", "=", "active"),
//	    soy.C("status", "=", "pending"),
//	)
func (sb *Select[T]) WhereOr(conditions ...Condition) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = whereOrImpl(sb.instance, sb.builder, conditions...)
	return sb
}

// WhereNull adds a WHERE field IS NULL condition.
func (sb *Select[T]) WhereNull(field string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = whereNullImpl(sb.instance, sb.builder, field)
	return sb
}

// WhereNotNull adds a WHERE field IS NOT NULL condition.
func (sb *Select[T]) WhereNotNull(field string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = whereNotNullImpl(sb.instance, sb.builder, field)
	return sb
}

// WhereBetween adds a WHERE field BETWEEN low AND high condition.
// Multiple calls are combined with AND.
//
// Example:
//
//	.WhereBetween("age", "min_age", "max_age")
//	// params: map[string]any{"min_age": 18, "max_age": 65}
func (sb *Select[T]) WhereBetween(field, lowParam, highParam string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = whereBetweenImpl(sb.instance, sb.builder, field, lowParam, highParam)
	return sb
}

// WhereNotBetween adds a WHERE field NOT BETWEEN low AND high condition.
// Multiple calls are combined with AND.
//
// Example:
//
//	.WhereNotBetween("age", "min_age", "max_age")
//	// params: map[string]any{"min_age": 18, "max_age": 65}
func (sb *Select[T]) WhereNotBetween(field, lowParam, highParam string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = whereNotBetweenImpl(sb.instance, sb.builder, field, lowParam, highParam)
	return sb
}

// WhereFields adds a WHERE condition comparing two fields.
// Multiple calls are combined with AND.
//
// Example:
//
//	.WhereFields("created_at", "<", "updated_at")
//	// WHERE "created_at" < "updated_at"
func (sb *Select[T]) WhereFields(leftField, operator, rightField string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = whereFieldsImpl(sb.instance, sb.builder, leftField, operator, rightField)
	return sb
}

// OrderBy adds an ORDER BY clause.
// Direction must be "asc" or "desc" (case insensitive).
func (sb *Select[T]) OrderBy(field string, direction string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = orderByImpl(sb.instance, sb.builder, field, direction)
	return sb
}

// OrderByNulls adds an ORDER BY clause with NULLS FIRST or NULLS LAST.
// Direction must be "asc" or "desc" (case insensitive).
// Nulls must be "first" or "last" (case insensitive).
//
// Example:
//
//	.OrderByNulls("created_at", "desc", "last")  // ORDER BY "created_at" DESC NULLS LAST
func (sb *Select[T]) OrderByNulls(field, direction, nulls string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = orderByNullsImpl(sb.instance, sb.builder, field, direction, nulls)
	return sb
}

// OrderByExpr adds an ORDER BY clause with an expression (field <op> param).
// Useful for vector distance ordering with pgvector.
// Direction must be "asc" or "desc" (case insensitive).
//
// Example:
//
//	.OrderByExpr("embedding", "<->", "query_embedding", "asc")
func (sb *Select[T]) OrderByExpr(field, operator, param, direction string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = orderByExprImpl(sb.instance, sb.builder, field, operator, param, direction)
	return sb
}

// SelectExpr adds a binary expression (field <op> param) AS alias to the SELECT clause.
// Useful for retrieving vector distance scores alongside results with pgvector.
//
// Example:
//
//	.SelectExpr("embedding", "<=>", "query_vec", "score")
//	// SELECT *, "embedding" <=> :query_vec AS "score" FROM ...
func (sb *Select[T]) SelectExpr(field, operator, param, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectExprImpl(sb.instance, sb.builder, field, operator, param, alias)
	return sb
}

// Limit sets the LIMIT clause to a static integer value.
func (sb *Select[T]) Limit(n int) *Select[T] {
	sb.builder = sb.builder.Limit(n)
	return sb
}

// LimitParam sets the LIMIT clause to a parameterized value.
// Useful for API pagination where limit comes from request parameters.
//
// Example:
//
//	.LimitParam("page_size")
//	// params: map[string]any{"page_size": 10}
func (sb *Select[T]) LimitParam(param string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = limitParamImpl(sb.instance, sb.builder, param)
	return sb
}

// Offset sets the OFFSET clause to a static integer value.
func (sb *Select[T]) Offset(n int) *Select[T] {
	sb.builder = sb.builder.Offset(n)
	return sb
}

// OffsetParam sets the OFFSET clause to a parameterized value.
// Useful for API pagination where offset comes from request parameters.
//
// Example:
//
//	.OffsetParam("page_offset")
//	// params: map[string]any{"page_offset": 20}
func (sb *Select[T]) OffsetParam(param string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = offsetParamImpl(sb.instance, sb.builder, param)
	return sb
}

// Distinct adds DISTINCT to the SELECT query.
func (sb *Select[T]) Distinct() *Select[T] {
	sb.builder = sb.builder.Distinct()
	return sb
}

// DistinctOn adds DISTINCT ON (PostgreSQL-specific) to the SELECT query.
// Returns only the first row for each distinct combination of the specified fields.
//
// Example:
//
//	.DistinctOn("user_id").OrderBy("created_at", "desc")  // First row per user_id
func (sb *Select[T]) DistinctOn(fields ...string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = distinctOnImpl(sb.instance, sb.builder, fields...)
	return sb
}

// GroupBy adds a GROUP BY clause.
// Multiple calls add additional grouping fields.
//
// Example:
//
//	.GroupBy("status").GroupBy("category")
func (sb *Select[T]) GroupBy(fields ...string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = groupByImpl(sb.instance, sb.builder, fields...)
	return sb
}

// Having adds a HAVING condition. Must be used after GroupBy.
// Multiple calls are combined with AND.
//
// Example:
//
//	.GroupBy("status").Having("age", ">", "min_age")
func (sb *Select[T]) Having(field, operator, param string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = havingImpl(sb.instance, sb.builder, field, operator, param)
	return sb
}

// HavingAgg adds an aggregate HAVING condition. Must be used after GroupBy.
// Supports COUNT(*), SUM, AVG, MIN, MAX aggregate functions.
//
// Example:
//
//	.GroupBy("status").HavingAgg("count", "", ">", "min_count")  // COUNT(*) > :min_count
//	.GroupBy("status").HavingAgg("sum", "amount", ">=", "min_total")  // SUM("amount") >= :min_total
func (sb *Select[T]) HavingAgg(aggFunc, field, operator, param string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = havingAggImpl(sb.instance, sb.builder, aggFunc, field, operator, param)
	return sb
}

// ForUpdate adds FOR UPDATE row locking to the SELECT query.
// Locks selected rows for update, blocking other transactions from modifying them.
func (sb *Select[T]) ForUpdate() *Select[T] {
	sb.builder = sb.builder.ForUpdate()
	return sb
}

// ForNoKeyUpdate adds FOR NO KEY UPDATE row locking to the SELECT query.
// Similar to FOR UPDATE but allows SELECT FOR KEY SHARE on the same rows.
func (sb *Select[T]) ForNoKeyUpdate() *Select[T] {
	sb.builder = sb.builder.ForNoKeyUpdate()
	return sb
}

// ForShare adds FOR SHARE row locking to the SELECT query.
// Locks selected rows in share mode, allowing other SELECT FOR SHARE but blocking updates.
func (sb *Select[T]) ForShare() *Select[T] {
	sb.builder = sb.builder.ForShare()
	return sb
}

// ForKeyShare adds FOR KEY SHARE row locking to the SELECT query.
// The weakest lock level, blocks only FOR UPDATE but allows other locks.
func (sb *Select[T]) ForKeyShare() *Select[T] {
	sb.builder = sb.builder.ForKeyShare()
	return sb
}

// --- String Expression Methods ---

// SelectUpper adds UPPER(field) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectUpper("name", "upper_name")  // SELECT UPPER("name") AS "upper_name"
func (sb *Select[T]) SelectUpper(field, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectUpperImpl(sb.instance, sb.builder, field, alias)
	return sb
}

// SelectLower adds LOWER(field) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectLower("email", "lower_email")  // SELECT LOWER("email") AS "lower_email"
func (sb *Select[T]) SelectLower(field, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectLowerImpl(sb.instance, sb.builder, field, alias)
	return sb
}

// SelectLength adds LENGTH(field) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectLength("name", "name_length")  // SELECT LENGTH("name") AS "name_length"
func (sb *Select[T]) SelectLength(field, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectLengthImpl(sb.instance, sb.builder, field, alias)
	return sb
}

// SelectTrim adds TRIM(field) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectTrim("name", "trimmed_name")  // SELECT TRIM("name") AS "trimmed_name"
func (sb *Select[T]) SelectTrim(field, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectTrimImpl(sb.instance, sb.builder, field, alias)
	return sb
}

// SelectLTrim adds LTRIM(field) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectLTrim("name", "ltrimmed")  // SELECT LTRIM("name") AS "ltrimmed"
func (sb *Select[T]) SelectLTrim(field, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectLTrimImpl(sb.instance, sb.builder, field, alias)
	return sb
}

// SelectRTrim adds RTRIM(field) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectRTrim("name", "rtrimmed")  // SELECT RTRIM("name") AS "rtrimmed"
func (sb *Select[T]) SelectRTrim(field, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectRTrimImpl(sb.instance, sb.builder, field, alias)
	return sb
}

// --- Math Expression Methods ---

// SelectAbs adds ABS(field) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectAbs("balance", "abs_balance")  // SELECT ABS("balance") AS "abs_balance"
func (sb *Select[T]) SelectAbs(field, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectAbsImpl(sb.instance, sb.builder, field, alias)
	return sb
}

// SelectCeil adds CEIL(field) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectCeil("price", "rounded_up")  // SELECT CEIL("price") AS "rounded_up"
func (sb *Select[T]) SelectCeil(field, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectCeilImpl(sb.instance, sb.builder, field, alias)
	return sb
}

// SelectFloor adds FLOOR(field) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectFloor("price", "rounded_down")  // SELECT FLOOR("price") AS "rounded_down"
func (sb *Select[T]) SelectFloor(field, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectFloorImpl(sb.instance, sb.builder, field, alias)
	return sb
}

// SelectRound adds ROUND(field) AS alias to the SELECT clause (no precision).
//
// Example:
//
//	.SelectRound("price", "rounded")  // SELECT ROUND("price") AS "rounded"
func (sb *Select[T]) SelectRound(field, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectRoundImpl(sb.instance, sb.builder, field, alias)
	return sb
}

// SelectSqrt adds SQRT(field) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectSqrt("variance", "std_dev")  // SELECT SQRT("variance") AS "std_dev"
func (sb *Select[T]) SelectSqrt(field, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectSqrtImpl(sb.instance, sb.builder, field, alias)
	return sb
}

// --- Cast Expression Methods ---

// SelectCast adds CAST(field AS type) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectCast("age", soy.CastText, "age_str")  // SELECT CAST("age" AS TEXT) AS "age_str"
func (sb *Select[T]) SelectCast(field string, castType CastType, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectCastImpl(sb.instance, sb.builder, field, castType, alias)
	return sb
}

// --- Date Expression Methods (functions that don't need DatePart) ---

// SelectNow adds NOW() AS alias to the SELECT clause.
//
// Example:
//
//	.SelectNow("current_time")  // SELECT NOW() AS "current_time"
func (sb *Select[T]) SelectNow(alias string) *Select[T] {
	sb.builder = sb.builder.SelectExpr(astql.As(astql.Now(), alias))
	return sb
}

// SelectCurrentDate adds CURRENT_DATE AS alias to the SELECT clause.
//
// Example:
//
//	.SelectCurrentDate("today")  // SELECT CURRENT_DATE AS "today"
func (sb *Select[T]) SelectCurrentDate(alias string) *Select[T] {
	sb.builder = sb.builder.SelectExpr(astql.As(astql.CurrentDate(), alias))
	return sb
}

// SelectCurrentTime adds CURRENT_TIME AS alias to the SELECT clause.
//
// Example:
//
//	.SelectCurrentTime("now_time")  // SELECT CURRENT_TIME AS "now_time"
func (sb *Select[T]) SelectCurrentTime(alias string) *Select[T] {
	sb.builder = sb.builder.SelectExpr(astql.As(astql.CurrentTime(), alias))
	return sb
}

// SelectCurrentTimestamp adds CURRENT_TIMESTAMP AS alias to the SELECT clause.
//
// Example:
//
//	.SelectCurrentTimestamp("ts")  // SELECT CURRENT_TIMESTAMP AS "ts"
func (sb *Select[T]) SelectCurrentTimestamp(alias string) *Select[T] {
	sb.builder = sb.builder.SelectExpr(astql.As(astql.CurrentTimestamp(), alias))
	return sb
}

// --- Aggregate Expression Methods ---

// SelectCountStar adds COUNT(*) AS alias to the SELECT clause.
//
// Example:
//
//	.GroupBy("status").SelectCountStar("count")  // SELECT COUNT(*) AS "count"
func (sb *Select[T]) SelectCountStar(alias string) *Select[T] {
	sb.builder = sb.builder.SelectExpr(astql.As(astql.CountStar(), alias))
	return sb
}

// SelectCount adds COUNT(field) AS alias to the SELECT clause.
//
// Example:
//
//	.GroupBy("status").SelectCount("id", "count")  // SELECT COUNT("id") AS "count"
func (sb *Select[T]) SelectCount(field, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectCountImpl(sb.instance, sb.builder, field, alias)
	return sb
}

// SelectCountDistinct adds COUNT(DISTINCT field) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectCountDistinct("email", "unique_emails")  // SELECT COUNT(DISTINCT "email") AS "unique_emails"
func (sb *Select[T]) SelectCountDistinct(field, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectCountDistinctImpl(sb.instance, sb.builder, field, alias)
	return sb
}

// SelectSum adds SUM(field) AS alias to the SELECT clause.
//
// Example:
//
//	.GroupBy("status").SelectSum("amount", "total")  // SELECT SUM("amount") AS "total"
func (sb *Select[T]) SelectSum(field, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectSumImpl(sb.instance, sb.builder, field, alias)
	return sb
}

// SelectAvg adds AVG(field) AS alias to the SELECT clause.
//
// Example:
//
//	.GroupBy("status").SelectAvg("age", "avg_age")  // SELECT AVG("age") AS "avg_age"
func (sb *Select[T]) SelectAvg(field, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectAvgImpl(sb.instance, sb.builder, field, alias)
	return sb
}

// SelectMin adds MIN(field) AS alias to the SELECT clause.
//
// Example:
//
//	.GroupBy("status").SelectMin("age", "youngest")  // SELECT MIN("age") AS "youngest"
func (sb *Select[T]) SelectMin(field, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectMinImpl(sb.instance, sb.builder, field, alias)
	return sb
}

// SelectMax adds MAX(field) AS alias to the SELECT clause.
//
// Example:
//
//	.GroupBy("status").SelectMax("age", "oldest")  // SELECT MAX("age") AS "oldest"
func (sb *Select[T]) SelectMax(field, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectMaxImpl(sb.instance, sb.builder, field, alias)
	return sb
}

// --- Aggregate FILTER Expression Methods ---

// SelectSumFilter adds SUM(field) FILTER (WHERE condition) AS alias to the SELECT clause.
// The filter condition is specified as field operator param.
//
// Example:
//
//	.SelectSumFilter("amount", "status", "=", "active", "active_total")
//	// SELECT SUM("amount") FILTER (WHERE "status" = :active) AS "active_total"
func (sb *Select[T]) SelectSumFilter(field, condField, condOp, condParam, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectSumFilterImpl(sb.instance, sb.builder, field, condField, condOp, condParam, alias)
	return sb
}

// SelectAvgFilter adds AVG(field) FILTER (WHERE condition) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectAvgFilter("age", "active", "=", "true", "active_avg_age")
//	// SELECT AVG("age") FILTER (WHERE "active" = :true) AS "active_avg_age"
func (sb *Select[T]) SelectAvgFilter(field, condField, condOp, condParam, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectAvgFilterImpl(sb.instance, sb.builder, field, condField, condOp, condParam, alias)
	return sb
}

// SelectMinFilter adds MIN(field) FILTER (WHERE condition) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectMinFilter("price", "category", "=", "electronics", "min_electronics")
func (sb *Select[T]) SelectMinFilter(field, condField, condOp, condParam, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectMinFilterImpl(sb.instance, sb.builder, field, condField, condOp, condParam, alias)
	return sb
}

// SelectMaxFilter adds MAX(field) FILTER (WHERE condition) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectMaxFilter("score", "passed", "=", "true", "max_passed")
func (sb *Select[T]) SelectMaxFilter(field, condField, condOp, condParam, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectMaxFilterImpl(sb.instance, sb.builder, field, condField, condOp, condParam, alias)
	return sb
}

// SelectCountFilter adds COUNT(field) FILTER (WHERE condition) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectCountFilter("id", "status", "=", "active", "active_count")
//	// SELECT COUNT("id") FILTER (WHERE "status" = :active) AS "active_count"
func (sb *Select[T]) SelectCountFilter(field, condField, condOp, condParam, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectCountFilterImpl(sb.instance, sb.builder, field, condField, condOp, condParam, alias)
	return sb
}

// SelectCountDistinctFilter adds COUNT(DISTINCT field) FILTER (WHERE condition) AS alias.
//
// Example:
//
//	.SelectCountDistinctFilter("email", "verified", "=", "true", "verified_users")
func (sb *Select[T]) SelectCountDistinctFilter(field, condField, condOp, condParam, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectCountDistinctFilterImpl(sb.instance, sb.builder, field, condField, condOp, condParam, alias)
	return sb
}

// --- Additional String Expression Methods ---

// SelectSubstring adds SUBSTRING(field, start, length) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectSubstring("name", "start_pos", "substr_len", "short_name")
//	// SELECT SUBSTRING("name", :start_pos, :substr_len) AS "short_name"
func (sb *Select[T]) SelectSubstring(field, startParam, lengthParam, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectSubstringImpl(sb.instance, sb.builder, field, startParam, lengthParam, alias)
	return sb
}

// SelectReplace adds REPLACE(field, search, replacement) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectReplace("name", "search_str", "replace_str", "replaced_name")
//	// SELECT REPLACE("name", :search_str, :replace_str) AS "replaced_name"
func (sb *Select[T]) SelectReplace(field, searchParam, replacementParam, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectReplaceImpl(sb.instance, sb.builder, field, searchParam, replacementParam, alias)
	return sb
}

// SelectConcat adds CONCAT(field1, field2, ...) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectConcat("full_name", "name", "email")  // SELECT CONCAT("name", "email") AS "full_name"
func (sb *Select[T]) SelectConcat(alias string, fields ...string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectConcatImpl(sb.instance, sb.builder, alias, fields...)
	return sb
}

// --- Additional Math Expression Methods ---

// SelectPower adds POWER(field, exponent) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectPower("value", "exponent", "powered")  // SELECT POWER("value", :exponent) AS "powered"
func (sb *Select[T]) SelectPower(field, exponentParam, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectPowerImpl(sb.instance, sb.builder, field, exponentParam, alias)
	return sb
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
func (sb *Select[T]) SelectCoalesce(alias string, params ...string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectCoalesceImpl(sb.instance, sb.builder, alias, params...)
	return sb
}

// SelectNullIf adds NULLIF(param1, param2) AS alias to the SELECT clause.
// Returns NULL if param1 equals param2, otherwise returns param1.
//
// Example:
//
//	.SelectNullIf("value", "empty_val", "result")
//	// SELECT NULLIF(:value, :empty_val) AS "result"
func (sb *Select[T]) SelectNullIf(param1, param2, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}
	sb.builder, sb.err = selectNullIfImpl(sb.instance, sb.builder, param1, param2, alias)
	return sb
}

// --- CASE Expression Methods ---

// SelectCase starts building a CASE expression for the SELECT clause.
// Returns a SelectCaseBuilder that allows chaining When/Else/As clauses.
// Call End() to complete the CASE expression and return to the Select builder.
//
// Example:
//
//	soy.Select().
//	    SelectCase().
//	        When("status", "=", "status_active", "result_active").
//	        When("status", "=", "status_pending", "result_pending").
//	        Else("result_default").
//	        As("status_label").
//	    End().
//	    Exec(ctx, params)
func (sb *Select[T]) SelectCase() *SelectCaseBuilder[T] {
	return newSelectCaseBuilder(sb)
}

// SelectCaseBuilder is now defined in case.go

// --- Window Function Methods ---

// SelectRowNumber starts building a ROW_NUMBER() window function.
// Returns a SelectWindowBuilder for configuring the window specification.
//
// Example:
//
//	soy.Select().
//	    SelectRowNumber().
//	    OrderBy("created_at", "DESC").
//	    As("row_num").
//	    End()
func (sb *Select[T]) SelectRowNumber() *SelectWindowBuilder[T] {
	return newSelectWindowBuilder(sb, createRowNumberWindow(sb.instance))
}

// SelectRank starts building a RANK() window function.
//
// Example:
//
//	.SelectRank().OrderBy("score", "DESC").As("rank").End()
func (sb *Select[T]) SelectRank() *SelectWindowBuilder[T] {
	return newSelectWindowBuilder(sb, createRankWindow(sb.instance))
}

// SelectDenseRank starts building a DENSE_RANK() window function.
//
// Example:
//
//	.SelectDenseRank().OrderBy("score", "DESC").As("dense_rank").End()
func (sb *Select[T]) SelectDenseRank() *SelectWindowBuilder[T] {
	return newSelectWindowBuilder(sb, createDenseRankWindow(sb.instance))
}

// SelectNtile starts building an NTILE(n) window function.
//
// Example:
//
//	.SelectNtile("num_buckets").OrderBy("value", "ASC").As("quartile").End()
func (sb *Select[T]) SelectNtile(nParam string) *SelectWindowBuilder[T] {
	if sb.err != nil {
		return newSelectWindowBuilder(sb, newWindowStateWithError(sb.instance, sb.err))
	}
	return newSelectWindowBuilder(sb, createNtileWindow(sb.instance, nParam))
}

// SelectLag starts building a LAG(field, offset) window function.
//
// Example:
//
//	.SelectLag("price", "offset").OrderBy("date", "ASC").As("prev_price").End()
func (sb *Select[T]) SelectLag(field, offsetParam string) *SelectWindowBuilder[T] {
	if sb.err != nil {
		return newSelectWindowBuilder(sb, newWindowStateWithError(sb.instance, sb.err))
	}
	return newSelectWindowBuilder(sb, createLagWindow(sb.instance, field, offsetParam))
}

// SelectLead starts building a LEAD(field, offset) window function.
//
// Example:
//
//	.SelectLead("price", "offset").OrderBy("date", "ASC").As("next_price").End()
func (sb *Select[T]) SelectLead(field, offsetParam string) *SelectWindowBuilder[T] {
	if sb.err != nil {
		return newSelectWindowBuilder(sb, newWindowStateWithError(sb.instance, sb.err))
	}
	return newSelectWindowBuilder(sb, createLeadWindow(sb.instance, field, offsetParam))
}

// SelectFirstValue starts building a FIRST_VALUE(field) window function.
//
// Example:
//
//	.SelectFirstValue("price").OrderBy("date", "ASC").As("first_price").End()
func (sb *Select[T]) SelectFirstValue(field string) *SelectWindowBuilder[T] {
	if sb.err != nil {
		return newSelectWindowBuilder(sb, newWindowStateWithError(sb.instance, sb.err))
	}
	return newSelectWindowBuilder(sb, createFirstValueWindow(sb.instance, field))
}

// SelectLastValue starts building a LAST_VALUE(field) window function.
//
// Example:
//
//	.SelectLastValue("price").OrderBy("date", "ASC").As("last_price").End()
func (sb *Select[T]) SelectLastValue(field string) *SelectWindowBuilder[T] {
	if sb.err != nil {
		return newSelectWindowBuilder(sb, newWindowStateWithError(sb.instance, sb.err))
	}
	return newSelectWindowBuilder(sb, createLastValueWindow(sb.instance, field))
}

// SelectSumOver starts building a SUM(field) OVER window function.
//
// Example:
//
//	.SelectSumOver("amount").PartitionBy("category").As("running_total").End()
func (sb *Select[T]) SelectSumOver(field string) *SelectWindowBuilder[T] {
	if sb.err != nil {
		return newSelectWindowBuilder(sb, newWindowStateWithError(sb.instance, sb.err))
	}
	return newSelectWindowBuilder(sb, createSumOverWindow(sb.instance, field))
}

// SelectAvgOver starts building an AVG(field) OVER window function.
//
// Example:
//
//	.SelectAvgOver("score").PartitionBy("category").As("avg_score").End()
func (sb *Select[T]) SelectAvgOver(field string) *SelectWindowBuilder[T] {
	if sb.err != nil {
		return newSelectWindowBuilder(sb, newWindowStateWithError(sb.instance, sb.err))
	}
	return newSelectWindowBuilder(sb, createAvgOverWindow(sb.instance, field))
}

// SelectCountOver starts building a COUNT(*) OVER window function.
//
// Example:
//
//	.SelectCountOver().PartitionBy("category").As("category_count").End()
func (sb *Select[T]) SelectCountOver() *SelectWindowBuilder[T] {
	return newSelectWindowBuilder(sb, createCountOverWindow(sb.instance))
}

// SelectMinOver starts building a MIN(field) OVER window function.
//
// Example:
//
//	.SelectMinOver("price").PartitionBy("category").As("min_price").End()
func (sb *Select[T]) SelectMinOver(field string) *SelectWindowBuilder[T] {
	if sb.err != nil {
		return newSelectWindowBuilder(sb, newWindowStateWithError(sb.instance, sb.err))
	}
	return newSelectWindowBuilder(sb, createMinOverWindow(sb.instance, field))
}

// SelectMaxOver starts building a MAX(field) OVER window function.
//
// Example:
//
//	.SelectMaxOver("price").PartitionBy("category").As("max_price").End()
func (sb *Select[T]) SelectMaxOver(field string) *SelectWindowBuilder[T] {
	if sb.err != nil {
		return newSelectWindowBuilder(sb, newWindowStateWithError(sb.instance, sb.err))
	}
	return newSelectWindowBuilder(sb, createMaxOverWindow(sb.instance, field))
}

// SelectWindowBuilder is now defined in window.go

// Render builds and renders the query to SQL with parameter placeholders.
// Returns the SQL string and list of required parameters for sqlx execution.
func (sb *Select[T]) Render() (*astql.QueryResult, error) {
	// Check for  errors first
	if sb.err != nil {
		return nil, newBuilderError("select", sb.err)
	}

	result, err := sb.builder.Render(sb.soy.renderer())
	if err != nil {
		return nil, newRenderError("SELECT", err)
	}
	return result, nil
}

// MustRender is like Render but panics on error.
// This is intentionally preserved for cases where panicking is desired (e.g., tests, initialization).
func (sb *Select[T]) MustRender() *astql.QueryResult {
	result, err := sb.Render()
	if err != nil {
		panic(err)
	}
	return result
}

// Instance returns the underlying ASTQL instance for advanced query building.
// Use this escape hatch when you need ASTQL features not exposed by Select.
func (sb *Select[T]) Instance() *astql.ASTQL {
	return sb.instance
}

// Exec executes the SELECT query and returns a single record of type T.
// Returns an error if zero rows or more than one row is found.
//
// Example:
//
//	user, err := soy.Select().
//	    Where("email", "=", "user_email").
//	    Exec(ctx, map[string]any{"user_email": "test@example.com"})
func (sb *Select[T]) Exec(ctx context.Context, params map[string]any) (*T, error) {
	return sb.exec(ctx, sb.soy.execer(), params)
}

// ExecTx executes the SELECT query within a transaction and returns a single record of type T.
// Returns an error if zero rows or more than one row is found.
//
// Example:
//
//	tx, _ := db.BeginTxx(ctx, nil)
//	defer tx.Rollback()
//	user, err := soy.Select().
//	    Where("email", "=", "user_email").
//	    ExecTx(ctx, tx, map[string]any{"user_email": "test@example.com"})
//	tx.Commit()
func (sb *Select[T]) ExecTx(ctx context.Context, tx *sqlx.Tx, params map[string]any) (*T, error) {
	return sb.exec(ctx, tx, params)
}

// ExecAtom executes the SELECT query and returns a single result as an Atom.
// This method enables type-erased execution where T is not known at consumption time.
// Returns an error if zero rows or more than one row is found.
//
// Example:
//
//	atom, err := soy.Select().
//	    Where("email", "=", "user_email").
//	    ExecAtom(ctx, map[string]any{"user_email": "test@example.com"})
func (sb *Select[T]) ExecAtom(ctx context.Context, params map[string]any) (*atom.Atom, error) {
	return sb.execAtom(ctx, sb.soy.execer(), params)
}

// ExecTxAtom executes the SELECT query within a transaction and returns a single result as an Atom.
// This method enables type-erased execution where T is not known at consumption time.
// Returns an error if zero rows or more than one row is found.
//
// Example:
//
//	tx, _ := db.BeginTxx(ctx, nil)
//	defer tx.Rollback()
//	atom, err := soy.Select().
//	    Where("email", "=", "user_email").
//	    ExecTxAtom(ctx, tx, map[string]any{"user_email": "test@example.com"})
//	tx.Commit()
func (sb *Select[T]) ExecTxAtom(ctx context.Context, tx *sqlx.Tx, params map[string]any) (*atom.Atom, error) {
	return sb.execAtom(ctx, tx, params)
}

// execAtom is the internal atom execution method used by both ExecAtom and ExecTxAtom.
func (sb *Select[T]) execAtom(ctx context.Context, execer sqlx.ExtContext, params map[string]any) (*atom.Atom, error) {
	if sb.err != nil {
		return nil, newBuilderError("select", sb.err)
	}

	result, err := sb.Render()
	if err != nil {
		return nil, err // already wrapped by Render
	}

	return execAtomSingleRow(ctx, execer, sb.soy.atomScanner(), result.SQL, params, sb.soy.getTableName(), "SELECT")
}

// exec is the internal execution method used by both Exec and ExecTx.
func (sb *Select[T]) exec(ctx context.Context, execer sqlx.ExtContext, params map[string]any) (*T, error) {
	// Check for  errors first
	if sb.err != nil {
		return nil, newBuilderError("select", sb.err)
	}

	// Render the query
	result, err := sb.Render()
	if err != nil {
		return nil, err // already wrapped by Render
	}

	// Emit query started event
	tableName := sb.soy.getTableName()
	capitan.Debug(ctx, QueryStarted,
		TableKey.Field(tableName),
		OperationKey.Field("SELECT"),
		SQLKey.Field(result.SQL),
	)

	startTime := time.Now()

	// Execute named query
	rows, err := sqlx.NamedQueryContext(ctx, execer, result.SQL, params)
	if err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field("SELECT"),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return nil, newQueryError("SELECT", err)
	}
	defer func() { _ = rows.Close() }()

	// Check for exactly one row
	if !rows.Next() {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field("SELECT"),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field("no rows found"),
		)
		return nil, ErrNotFound
	}

	// Scan the single row
	var record T
	if err := rows.StructScan(&record); err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field("SELECT"),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return nil, newScanError("SELECT", err)
	}

	// Ensure no additional rows
	if rows.Next() {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field("SELECT"),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field("expected exactly one row, found multiple"),
		)
		return nil, ErrMultipleRows
	}

	// Emit query completed event
	durationMs := time.Since(startTime).Milliseconds()
	capitan.Info(ctx, QueryCompleted,
		TableKey.Field(tableName),
		OperationKey.Field("SELECT"),
		DurationMsKey.Field(durationMs),
		RowsReturnedKey.Field(1),
	)

	return &record, nil
}

// C creates a simple condition for use with WhereAnd/WhereOr.
func C(field, operator, param string) Condition {
	return Condition{
		field:    field,
		operator: operator,
		param:    param,
	}
}

// Null creates an IS NULL condition for use with WhereAnd/WhereOr.
func Null(field string) Condition {
	return Condition{
		field:    field,
		operator: "IS NULL",
		isNull:   true,
	}
}

// NotNull creates an IS NOT NULL condition for use with WhereAnd/WhereOr.
func NotNull(field string) Condition {
	return Condition{
		field:    field,
		operator: "IS NOT NULL",
		isNull:   true,
	}
}

// Between creates a BETWEEN condition for use with WhereAnd/WhereOr.
// Matches values where field >= lowParam AND field <= highParam.
//
// Example:
//
//	soy.Between("age", "min_age", "max_age")  // age BETWEEN :min_age AND :max_age
func Between(field, lowParam, highParam string) Condition {
	return Condition{
		field:     field,
		operator:  opBetween,
		isBetween: true,
		lowParam:  lowParam,
		highParam: highParam,
	}
}

// NotBetween creates a NOT BETWEEN condition for use with WhereAnd/WhereOr.
// Matches values where field < lowParam OR field > highParam.
//
// Example:
//
//	soy.NotBetween("age", "min_age", "max_age")  // age NOT BETWEEN :min_age AND :max_age
func NotBetween(field, lowParam, highParam string) Condition {
	return Condition{
		field:     field,
		operator:  opNotBetween,
		isBetween: true,
		lowParam:  lowParam,
		highParam: highParam,
	}
}

// Note: buildAggregateCondition is defined in builder.go to avoid duplication.
