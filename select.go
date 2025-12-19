package cereal

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql"
	"github.com/zoobzio/capitan"
)

// cerealExecutor provides the interface for executing queries.
// This allows s to access DB and table info without circular dependencies.
type cerealExecutor interface {
	execer() sqlx.ExtContext
	getTableName() string
	renderer() astql.Renderer
}

// Select provides a focused API for building SELECT queries that return a single record.
// It wraps ASTQL's  functionality with a simple string-based interface.
// Use this for queries expected to return exactly one row (e.g., GET by ID, fetch one record).
type Select[T any] struct {
	instance *astql.ASTQL
	builder  *astql.Builder
	cereal   cerealExecutor // interface for execution
	err      error          // stores first error encountered during building
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

// operatorMap translates string operators to ASTQL operators.
var operatorMap = map[string]astql.Operator{
	// Basic comparison operators.
	"=":  astql.EQ,
	"!=": astql.NE,
	">":  astql.GT,
	">=": astql.GE,
	"<":  astql.LT,
	"<=": astql.LE,

	// Pattern matching operators.
	"LIKE":      astql.LIKE,
	"NOT LIKE":  astql.NotLike,
	"ILIKE":     astql.ILIKE,
	"NOT ILIKE": astql.NotILike,

	// Set membership operators.
	"IN":     astql.IN,
	"NOT IN": astql.NotIn,

	// Regex operators (PostgreSQL).
	"~":   astql.RegexMatch,
	"~*":  astql.RegexIMatch,
	"!~":  astql.NotRegexMatch,
	"!~*": astql.NotRegexIMatch,

	// Array operators (PostgreSQL).
	"@>": astql.ArrayContains,
	"<@": astql.ArrayContainedBy,
	"&&": astql.ArrayOverlap,

	// Vector operators (pgvector).
	"<->": astql.VectorL2Distance,
	"<#>": astql.VectorInnerProduct,
	"<=>": astql.VectorCosineDistance,
	"<+>": astql.VectorL1Distance,
}

// directionMap translates string directions to ASTQL directions.
var directionMap = map[string]astql.Direction{
	"asc":  astql.ASC,
	"desc": astql.DESC,
}

// nullsMap translates string nulls ordering to ASTQL nulls ordering.
var nullsMap = map[string]astql.NullsOrdering{
	"first": astql.NullsFirst,
	"last":  astql.NullsLast,
}

// validateOperator converts a string operator to ASTQL operator.
func validateOperator(op string) (astql.Operator, error) {
	astqlOp, ok := operatorMap[op]
	if !ok {
		return "", fmt.Errorf("invalid operator %q, supported: =, !=, >, >=, <, <=, LIKE, NOT LIKE, ILIKE, NOT ILIKE, IN, NOT IN, ~, ~*, !~, !~*, @>, <@, &&, <->, <#>, <=>, <+>", op)
	}
	return astqlOp, nil
}

// validateDirection converts a string direction to ASTQL direction.
func validateDirection(dir string) (astql.Direction, error) {
	lower := strings.ToLower(dir)
	astqlDir, ok := directionMap[lower]
	if !ok {
		return "", fmt.Errorf("invalid direction %q, must be 'asc' or 'desc'", dir)
	}
	return astqlDir, nil
}

// validateNulls converts a string nulls ordering to ASTQL nulls ordering.
func validateNulls(nulls string) (astql.NullsOrdering, error) {
	lower := strings.ToLower(nulls)
	astqlNulls, ok := nullsMap[lower]
	if !ok {
		return "", fmt.Errorf("invalid nulls ordering %q, must be 'first' or 'last'", nulls)
	}
	return astqlNulls, nil
}

// Fields specifies which fields to select. Field names must exist in the schema.
// If not called, SELECT * is used by default.
func (sb *Select[T]) Fields(fields ...string) *Select[T] {
	if sb.err != nil {
		return sb
	}

	if len(fields) == 0 {
		return sb
	}

	// Build fields slice using factory
	fieldSlice := sb.instance.Fields()
	for _, fieldName := range fields {
		f, err := sb.instance.TryF(fieldName)
		if err != nil {
			sb.err = fmt.Errorf("invalid field %q: %w", fieldName, err)
			return sb
		}
		fieldSlice = append(fieldSlice, f)
	}

	sb.builder = sb.builder.Fields(fieldSlice...)
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

	astqlOp, err := validateOperator(operator)
	if err != nil {
		sb.err = err
		return sb
	}

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	p, err := sb.instance.TryP(param)
	if err != nil {
		sb.err = fmt.Errorf("invalid param %q: %w", param, err)
		return sb
	}

	condition, err := sb.instance.TryC(f, astqlOp, p)
	if err != nil {
		sb.err = fmt.Errorf("invalid condition: %w", err)
		return sb
	}

	sb.builder = sb.builder.Where(condition)
	return sb
}

// WhereAnd adds multiple conditions combined with AND.
//
// Example:
//
//	.WhereAnd(
//	    cereal.C("age", ">=", "min_age"),
//	    cereal.C("age", "<=", "max_age"),
//	)
func (sb *Select[T]) WhereAnd(conditions ...Condition) *Select[T] {
	if sb.err != nil {
		return sb
	}

	if len(conditions) == 0 {
		return sb
	}

	conditionItems := sb.instance.ConditionItems()
	for _, cond := range conditions {
		condItem, err := sb.buildCondition(cond)
		if err != nil {
			sb.err = err
			return sb
		}
		conditionItems = append(conditionItems, condItem)
	}

	andGroup, err := sb.instance.TryAnd(conditionItems...)
	if err != nil {
		sb.err = fmt.Errorf("invalid AND condition: %w", err)
		return sb
	}

	sb.builder = sb.builder.Where(andGroup)
	return sb
}

// WhereOr adds multiple conditions combined with OR.
//
// Example:
//
//	.WhereOr(
//	    cereal.C("status", "=", "active"),
//	    cereal.C("status", "=", "pending"),
//	)
func (sb *Select[T]) WhereOr(conditions ...Condition) *Select[T] {
	if sb.err != nil {
		return sb
	}

	if len(conditions) == 0 {
		return sb
	}

	conditionItems := sb.instance.ConditionItems()
	for _, cond := range conditions {
		condItem, err := sb.buildCondition(cond)
		if err != nil {
			sb.err = err
			return sb
		}
		conditionItems = append(conditionItems, condItem)
	}

	orGroup, err := sb.instance.TryOr(conditionItems...)
	if err != nil {
		sb.err = fmt.Errorf("invalid OR condition: %w", err)
		return sb
	}

	sb.builder = sb.builder.Where(orGroup)
	return sb
}

// WhereNull adds a WHERE field IS NULL condition.
func (sb *Select[T]) WhereNull(field string) *Select[T] {
	if sb.err != nil {
		return sb
	}

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	condition, err := sb.instance.TryNull(f)
	if err != nil {
		sb.err = fmt.Errorf("invalid NULL condition: %w", err)
		return sb
	}

	sb.builder = sb.builder.Where(condition)
	return sb
}

// WhereNotNull adds a WHERE field IS NOT NULL condition.
func (sb *Select[T]) WhereNotNull(field string) *Select[T] {
	if sb.err != nil {
		return sb
	}

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	condition, err := sb.instance.TryNotNull(f)
	if err != nil {
		sb.err = fmt.Errorf("invalid NOT NULL condition: %w", err)
		return sb
	}

	sb.builder = sb.builder.Where(condition)
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	lowP, err := sb.instance.TryP(lowParam)
	if err != nil {
		sb.err = fmt.Errorf("invalid low param %q: %w", lowParam, err)
		return sb
	}

	highP, err := sb.instance.TryP(highParam)
	if err != nil {
		sb.err = fmt.Errorf("invalid high param %q: %w", highParam, err)
		return sb
	}

	sb.builder = sb.builder.Where(astql.Between(f, lowP, highP))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	lowP, err := sb.instance.TryP(lowParam)
	if err != nil {
		sb.err = fmt.Errorf("invalid low param %q: %w", lowParam, err)
		return sb
	}

	highP, err := sb.instance.TryP(highParam)
	if err != nil {
		sb.err = fmt.Errorf("invalid high param %q: %w", highParam, err)
		return sb
	}

	sb.builder = sb.builder.Where(astql.NotBetween(f, lowP, highP))
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

	astqlOp, err := validateOperator(operator)
	if err != nil {
		sb.err = err
		return sb
	}

	left, err := sb.instance.TryF(leftField)
	if err != nil {
		sb.err = fmt.Errorf("invalid left field %q: %w", leftField, err)
		return sb
	}

	right, err := sb.instance.TryF(rightField)
	if err != nil {
		sb.err = fmt.Errorf("invalid right field %q: %w", rightField, err)
		return sb
	}

	sb.builder = sb.builder.Where(astql.CF(left, astqlOp, right))
	return sb
}

// buildCondition converts a Condition to an ASTQL condition.
func (sb *Select[T]) buildCondition(cond Condition) (astql.ConditionItem, error) {
	return buildConditionWithInstance(sb.instance, cond)
}

// OrderBy adds an ORDER BY clause.
// Direction must be "asc" or "desc" (case insensitive).
func (sb *Select[T]) OrderBy(field string, direction string) *Select[T] {
	if sb.err != nil {
		return sb
	}

	astqlDir, err := validateDirection(direction)
	if err != nil {
		sb.err = err
		return sb
	}

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	sb.builder = sb.builder.OrderBy(f, astqlDir)
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

	astqlDir, err := validateDirection(direction)
	if err != nil {
		sb.err = err
		return sb
	}

	astqlNulls, err := validateNulls(nulls)
	if err != nil {
		sb.err = err
		return sb
	}

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	sb.builder = sb.builder.OrderByNulls(f, astqlDir, astqlNulls)
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

	astqlDir, err := validateDirection(direction)
	if err != nil {
		sb.err = err
		return sb
	}

	astqlOp, err := validateOperator(operator)
	if err != nil {
		sb.err = err
		return sb
	}

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	p, err := sb.instance.TryP(param)
	if err != nil {
		sb.err = fmt.Errorf("invalid param %q: %w", param, err)
		return sb
	}

	sb.builder = sb.builder.OrderByExpr(f, astqlOp, p, astqlDir)
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

	p, err := sb.instance.TryP(param)
	if err != nil {
		sb.err = fmt.Errorf("invalid limit param %q: %w", param, err)
		return sb
	}

	sb.builder = sb.builder.LimitParam(p)
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

	p, err := sb.instance.TryP(param)
	if err != nil {
		sb.err = fmt.Errorf("invalid offset param %q: %w", param, err)
		return sb
	}

	sb.builder = sb.builder.OffsetParam(p)
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

	if len(fields) == 0 {
		return sb
	}

	astqlFields := sb.instance.Fields()
	for _, field := range fields {
		f, err := sb.instance.TryF(field)
		if err != nil {
			sb.err = fmt.Errorf("invalid field %q: %w", field, err)
			return sb
		}
		astqlFields = append(astqlFields, f)
	}

	sb.builder = sb.builder.DistinctOn(astqlFields...)
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

	astqlFields := sb.instance.Fields()
	for _, field := range fields {
		f, err := sb.instance.TryF(field)
		if err != nil {
			sb.err = fmt.Errorf("invalid field %q: %w", field, err)
			return sb
		}
		astqlFields = append(astqlFields, f)
	}
	sb.builder = sb.builder.GroupBy(astqlFields...)
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

	astqlOp, err := validateOperator(operator)
	if err != nil {
		sb.err = err
		return sb
	}

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	p, err := sb.instance.TryP(param)
	if err != nil {
		sb.err = fmt.Errorf("invalid param %q: %w", param, err)
		return sb
	}

	condition, err := sb.instance.TryC(f, astqlOp, p)
	if err != nil {
		sb.err = fmt.Errorf("invalid condition: %w", err)
		return sb
	}

	sb.builder = sb.builder.Having(condition)
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

	astqlOp, err := validateOperator(operator)
	if err != nil {
		sb.err = err
		return sb
	}

	aggCond, err := buildAggregateCondition(sb.instance, aggFunc, field, param, astqlOp)
	if err != nil {
		sb.err = err
		return sb
	}

	sb.builder = sb.builder.HavingAgg(aggCond)
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.Upper(f), alias))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.Lower(f), alias))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.Length(f), alias))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.Trim(f), alias))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.LTrim(f), alias))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.RTrim(f), alias))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.Abs(f), alias))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.Ceil(f), alias))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.Floor(f), alias))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.Round(f), alias))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.Sqrt(f), alias))
	return sb
}

// --- Cast Expression Methods ---

// SelectCast adds CAST(field AS type) AS alias to the SELECT clause.
//
// Example:
//
//	.SelectCast("age", cereal.CastText, "age_str")  // SELECT CAST("age" AS TEXT) AS "age_str"
func (sb *Select[T]) SelectCast(field string, castType CastType, alias string) *Select[T] {
	if sb.err != nil {
		return sb
	}

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.Cast(f, castType), alias))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.CountField(f), alias))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.CountDistinct(f), alias))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.Sum(f), alias))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.Avg(f), alias))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.Min(f), alias))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.Max(f), alias))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	filter, err := sb.buildSimpleCondition(condField, condOp, condParam)
	if err != nil {
		sb.err = err
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.SumFilter(f, filter), alias))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	filter, err := sb.buildSimpleCondition(condField, condOp, condParam)
	if err != nil {
		sb.err = err
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.AvgFilter(f, filter), alias))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	filter, err := sb.buildSimpleCondition(condField, condOp, condParam)
	if err != nil {
		sb.err = err
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.MinFilter(f, filter), alias))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	filter, err := sb.buildSimpleCondition(condField, condOp, condParam)
	if err != nil {
		sb.err = err
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.MaxFilter(f, filter), alias))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	filter, err := sb.buildSimpleCondition(condField, condOp, condParam)
	if err != nil {
		sb.err = err
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.CountFieldFilter(f, filter), alias))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	filter, err := sb.buildSimpleCondition(condField, condOp, condParam)
	if err != nil {
		sb.err = err
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.CountDistinctFilter(f, filter), alias))
	return sb
}

// buildSimpleCondition creates a simple condition (field op param) for FILTER clauses.
func (sb *Select[T]) buildSimpleCondition(field, operator, param string) (astql.ConditionItem, error) {
	astqlOp, err := validateOperator(operator)
	if err != nil {
		return nil, err
	}

	f, err := sb.instance.TryF(field)
	if err != nil {
		return nil, fmt.Errorf("invalid field %q: %w", field, err)
	}

	p, err := sb.instance.TryP(param)
	if err != nil {
		return nil, fmt.Errorf("invalid param %q: %w", param, err)
	}

	return sb.instance.TryC(f, astqlOp, p)
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	start, err := sb.instance.TryP(startParam)
	if err != nil {
		sb.err = fmt.Errorf("invalid start param %q: %w", startParam, err)
		return sb
	}

	length, err := sb.instance.TryP(lengthParam)
	if err != nil {
		sb.err = fmt.Errorf("invalid length param %q: %w", lengthParam, err)
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.Substring(f, start, length), alias))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	search, err := sb.instance.TryP(searchParam)
	if err != nil {
		sb.err = fmt.Errorf("invalid search param %q: %w", searchParam, err)
		return sb
	}

	replacement, err := sb.instance.TryP(replacementParam)
	if err != nil {
		sb.err = fmt.Errorf("invalid replacement param %q: %w", replacementParam, err)
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.Replace(f, search, replacement), alias))
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

	if len(fields) < 2 {
		sb.err = fmt.Errorf("CONCAT requires at least 2 fields")
		return sb
	}

	astqlFields := sb.instance.Fields()
	for _, field := range fields {
		f, err := sb.instance.TryF(field)
		if err != nil {
			sb.err = fmt.Errorf("invalid field %q: %w", field, err)
			return sb
		}
		astqlFields = append(astqlFields, f)
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.Concat(astqlFields...), alias))
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

	f, err := sb.instance.TryF(field)
	if err != nil {
		sb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return sb
	}

	exp, err := sb.instance.TryP(exponentParam)
	if err != nil {
		sb.err = fmt.Errorf("invalid exponent param %q: %w", exponentParam, err)
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.Power(f, exp), alias))
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

	if len(params) < 2 {
		sb.err = fmt.Errorf("COALESCE requires at least 2 parameters, got %d", len(params))
		return sb
	}

	// Build params slice using Params() factory
	astqlParams := sb.instance.Params()
	for _, param := range params {
		p, err := sb.instance.TryP(param)
		if err != nil {
			sb.err = fmt.Errorf("invalid param %q: %w", param, err)
			return sb
		}
		astqlParams = append(astqlParams, p)
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.Coalesce(astqlParams...), alias))
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

	p1, err := sb.instance.TryP(param1)
	if err != nil {
		sb.err = fmt.Errorf("invalid param1 %q: %w", param1, err)
		return sb
	}

	p2, err := sb.instance.TryP(param2)
	if err != nil {
		sb.err = fmt.Errorf("invalid param2 %q: %w", param2, err)
		return sb
	}

	sb.builder = sb.builder.SelectExpr(astql.As(astql.NullIf(p1, p2), alias))
	return sb
}

// --- CASE Expression Methods ---

// SelectCase starts building a CASE expression for the SELECT clause.
// Returns a SelectCaseBuilder that allows chaining When/Else/As clauses.
// Call End() to complete the CASE expression and return to the Select builder.
//
// Example:
//
//	cereal.Select().
//	    SelectCase().
//	        When("status", "=", "status_active", "result_active").
//	        When("status", "=", "status_pending", "result_pending").
//	        Else("result_default").
//	        As("status_label").
//	    End().
//	    Exec(ctx, params)
func (sb *Select[T]) SelectCase() *SelectCaseBuilder[T] {
	return &SelectCaseBuilder[T]{
		selectBuilder: sb,
		instance:      sb.instance,
		caseExpr:      astql.Case(),
	}
}

// SelectCaseBuilder provides a fluent API for building CASE expressions
// with string-based field and parameter names.
type SelectCaseBuilder[T any] struct {
	selectBuilder *Select[T]
	instance      *astql.ASTQL
	caseExpr      *astql.CaseBuilder
	err           error
}

// When adds a WHEN...THEN clause to the CASE expression.
// The condition is specified as field operator param, and resultParam is the THEN value.
//
// Example:
//
//	.When("status", "=", "status_val", "result_val")  // WHEN "status" = :status_val THEN :result_val
func (scb *SelectCaseBuilder[T]) When(field, operator, param, resultParam string) *SelectCaseBuilder[T] {
	if scb.err != nil {
		return scb
	}

	condition, err := buildCaseWhenCondition(scb.instance, field, operator, param)
	if err != nil {
		scb.err = err
		return scb
	}

	result, err := scb.instance.TryP(resultParam)
	if err != nil {
		scb.err = fmt.Errorf("invalid result param %q: %w", resultParam, err)
		return scb
	}

	scb.caseExpr.When(condition, result)
	return scb
}

// WhenNull adds a WHEN field IS NULL THEN resultParam clause.
//
// Example:
//
//	.WhenNull("status", "result_null")  // WHEN "status" IS NULL THEN :result_null
func (scb *SelectCaseBuilder[T]) WhenNull(field, resultParam string) *SelectCaseBuilder[T] {
	if scb.err != nil {
		return scb
	}

	f, err := scb.instance.TryF(field)
	if err != nil {
		scb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return scb
	}

	condition, err := scb.instance.TryNull(f)
	if err != nil {
		scb.err = fmt.Errorf("invalid NULL condition: %w", err)
		return scb
	}

	result, err := scb.instance.TryP(resultParam)
	if err != nil {
		scb.err = fmt.Errorf("invalid result param %q: %w", resultParam, err)
		return scb
	}

	scb.caseExpr.When(condition, result)
	return scb
}

// WhenNotNull adds a WHEN field IS NOT NULL THEN resultParam clause.
//
// Example:
//
//	.WhenNotNull("status", "result_not_null")  // WHEN "status" IS NOT NULL THEN :result_not_null
func (scb *SelectCaseBuilder[T]) WhenNotNull(field, resultParam string) *SelectCaseBuilder[T] {
	if scb.err != nil {
		return scb
	}

	f, err := scb.instance.TryF(field)
	if err != nil {
		scb.err = fmt.Errorf("invalid field %q: %w", field, err)
		return scb
	}

	condition, err := scb.instance.TryNotNull(f)
	if err != nil {
		scb.err = fmt.Errorf("invalid NOT NULL condition: %w", err)
		return scb
	}

	result, err := scb.instance.TryP(resultParam)
	if err != nil {
		scb.err = fmt.Errorf("invalid result param %q: %w", resultParam, err)
		return scb
	}

	scb.caseExpr.When(condition, result)
	return scb
}

// Else sets the ELSE clause of the CASE expression.
//
// Example:
//
//	.Else("default_result")  // ELSE :default_result
func (scb *SelectCaseBuilder[T]) Else(resultParam string) *SelectCaseBuilder[T] {
	if scb.err != nil {
		return scb
	}

	result, err := scb.instance.TryP(resultParam)
	if err != nil {
		scb.err = fmt.Errorf("invalid result param %q: %w", resultParam, err)
		return scb
	}

	scb.caseExpr.Else(result)
	return scb
}

// As sets the alias for the CASE expression.
//
// Example:
//
//	.As("status_label")  // AS "status_label"
func (scb *SelectCaseBuilder[T]) As(alias string) *SelectCaseBuilder[T] {
	if scb.err != nil {
		return scb
	}
	scb.caseExpr.As(alias)
	return scb
}

// End completes the CASE expression and adds it to the SELECT clause.
// Returns the parent Select builder to continue chaining.
func (scb *SelectCaseBuilder[T]) End() *Select[T] {
	if scb.err != nil {
		scb.selectBuilder.err = scb.err
		return scb.selectBuilder
	}

	scb.selectBuilder.builder = scb.selectBuilder.builder.SelectExpr(scb.caseExpr.Build())
	return scb.selectBuilder
}

// --- Window Function Methods ---

// SelectRowNumber starts building a ROW_NUMBER() window function.
// Returns a SelectWindowBuilder for configuring the window specification.
//
// Example:
//
//	cereal.Select().
//	    SelectRowNumber().
//	    OrderBy("created_at", "DESC").
//	    As("row_num").
//	    End()
func (sb *Select[T]) SelectRowNumber() *SelectWindowBuilder[T] {
	return &SelectWindowBuilder[T]{
		selectBuilder: sb,
		instance:      sb.instance,
		windowExpr:    astql.RowNumber(),
	}
}

// SelectRank starts building a RANK() window function.
//
// Example:
//
//	.SelectRank().OrderBy("score", "DESC").As("rank").End()
func (sb *Select[T]) SelectRank() *SelectWindowBuilder[T] {
	return &SelectWindowBuilder[T]{
		selectBuilder: sb,
		instance:      sb.instance,
		windowExpr:    astql.Rank(),
	}
}

// SelectDenseRank starts building a DENSE_RANK() window function.
//
// Example:
//
//	.SelectDenseRank().OrderBy("score", "DESC").As("dense_rank").End()
func (sb *Select[T]) SelectDenseRank() *SelectWindowBuilder[T] {
	return &SelectWindowBuilder[T]{
		selectBuilder: sb,
		instance:      sb.instance,
		windowExpr:    astql.DenseRank(),
	}
}

// SelectNtile starts building an NTILE(n) window function.
//
// Example:
//
//	.SelectNtile("num_buckets").OrderBy("value", "ASC").As("quartile").End()
func (sb *Select[T]) SelectNtile(nParam string) *SelectWindowBuilder[T] {
	if sb.err != nil {
		return &SelectWindowBuilder[T]{
			selectBuilder: sb,
			instance:      sb.instance,
			err:           sb.err,
		}
	}

	n, err := sb.instance.TryP(nParam)
	if err != nil {
		return &SelectWindowBuilder[T]{
			selectBuilder: sb,
			instance:      sb.instance,
			err:           fmt.Errorf("invalid ntile param %q: %w", nParam, err),
		}
	}

	return &SelectWindowBuilder[T]{
		selectBuilder: sb,
		instance:      sb.instance,
		windowExpr:    astql.Ntile(n),
	}
}

// SelectLag starts building a LAG(field, offset) window function.
//
// Example:
//
//	.SelectLag("price", "offset").OrderBy("date", "ASC").As("prev_price").End()
func (sb *Select[T]) SelectLag(field, offsetParam string) *SelectWindowBuilder[T] {
	if sb.err != nil {
		return &SelectWindowBuilder[T]{
			selectBuilder: sb,
			instance:      sb.instance,
			err:           sb.err,
		}
	}

	f, err := sb.instance.TryF(field)
	if err != nil {
		return &SelectWindowBuilder[T]{
			selectBuilder: sb,
			instance:      sb.instance,
			err:           fmt.Errorf("invalid field %q: %w", field, err),
		}
	}

	offset, err := sb.instance.TryP(offsetParam)
	if err != nil {
		return &SelectWindowBuilder[T]{
			selectBuilder: sb,
			instance:      sb.instance,
			err:           fmt.Errorf("invalid offset param %q: %w", offsetParam, err),
		}
	}

	return &SelectWindowBuilder[T]{
		selectBuilder: sb,
		instance:      sb.instance,
		windowExpr:    astql.Lag(f, offset),
	}
}

// SelectLead starts building a LEAD(field, offset) window function.
//
// Example:
//
//	.SelectLead("price", "offset").OrderBy("date", "ASC").As("next_price").End()
func (sb *Select[T]) SelectLead(field, offsetParam string) *SelectWindowBuilder[T] {
	if sb.err != nil {
		return &SelectWindowBuilder[T]{
			selectBuilder: sb,
			instance:      sb.instance,
			err:           sb.err,
		}
	}

	f, err := sb.instance.TryF(field)
	if err != nil {
		return &SelectWindowBuilder[T]{
			selectBuilder: sb,
			instance:      sb.instance,
			err:           fmt.Errorf("invalid field %q: %w", field, err),
		}
	}

	offset, err := sb.instance.TryP(offsetParam)
	if err != nil {
		return &SelectWindowBuilder[T]{
			selectBuilder: sb,
			instance:      sb.instance,
			err:           fmt.Errorf("invalid offset param %q: %w", offsetParam, err),
		}
	}

	return &SelectWindowBuilder[T]{
		selectBuilder: sb,
		instance:      sb.instance,
		windowExpr:    astql.Lead(f, offset),
	}
}

// SelectFirstValue starts building a FIRST_VALUE(field) window function.
//
// Example:
//
//	.SelectFirstValue("price").OrderBy("date", "ASC").As("first_price").End()
func (sb *Select[T]) SelectFirstValue(field string) *SelectWindowBuilder[T] {
	if sb.err != nil {
		return &SelectWindowBuilder[T]{
			selectBuilder: sb,
			instance:      sb.instance,
			err:           sb.err,
		}
	}

	f, err := sb.instance.TryF(field)
	if err != nil {
		return &SelectWindowBuilder[T]{
			selectBuilder: sb,
			instance:      sb.instance,
			err:           fmt.Errorf("invalid field %q: %w", field, err),
		}
	}

	return &SelectWindowBuilder[T]{
		selectBuilder: sb,
		instance:      sb.instance,
		windowExpr:    astql.FirstValue(f),
	}
}

// SelectLastValue starts building a LAST_VALUE(field) window function.
//
// Example:
//
//	.SelectLastValue("price").OrderBy("date", "ASC").As("last_price").End()
func (sb *Select[T]) SelectLastValue(field string) *SelectWindowBuilder[T] {
	if sb.err != nil {
		return &SelectWindowBuilder[T]{
			selectBuilder: sb,
			instance:      sb.instance,
			err:           sb.err,
		}
	}

	f, err := sb.instance.TryF(field)
	if err != nil {
		return &SelectWindowBuilder[T]{
			selectBuilder: sb,
			instance:      sb.instance,
			err:           fmt.Errorf("invalid field %q: %w", field, err),
		}
	}

	return &SelectWindowBuilder[T]{
		selectBuilder: sb,
		instance:      sb.instance,
		windowExpr:    astql.LastValue(f),
	}
}

// SelectSumOver starts building a SUM(field) OVER window function.
//
// Example:
//
//	.SelectSumOver("amount").PartitionBy("category").As("running_total").End()
func (sb *Select[T]) SelectSumOver(field string) *SelectWindowBuilder[T] {
	if sb.err != nil {
		return &SelectWindowBuilder[T]{
			selectBuilder: sb,
			instance:      sb.instance,
			err:           sb.err,
		}
	}

	f, err := sb.instance.TryF(field)
	if err != nil {
		return &SelectWindowBuilder[T]{
			selectBuilder: sb,
			instance:      sb.instance,
			err:           fmt.Errorf("invalid field %q: %w", field, err),
		}
	}

	return &SelectWindowBuilder[T]{
		selectBuilder: sb,
		instance:      sb.instance,
		windowExpr:    astql.SumOver(f),
	}
}

// SelectAvgOver starts building an AVG(field) OVER window function.
//
// Example:
//
//	.SelectAvgOver("score").PartitionBy("category").As("avg_score").End()
func (sb *Select[T]) SelectAvgOver(field string) *SelectWindowBuilder[T] {
	if sb.err != nil {
		return &SelectWindowBuilder[T]{
			selectBuilder: sb,
			instance:      sb.instance,
			err:           sb.err,
		}
	}

	f, err := sb.instance.TryF(field)
	if err != nil {
		return &SelectWindowBuilder[T]{
			selectBuilder: sb,
			instance:      sb.instance,
			err:           fmt.Errorf("invalid field %q: %w", field, err),
		}
	}

	return &SelectWindowBuilder[T]{
		selectBuilder: sb,
		instance:      sb.instance,
		windowExpr:    astql.AvgOver(f),
	}
}

// SelectCountOver starts building a COUNT(*) OVER window function.
//
// Example:
//
//	.SelectCountOver().PartitionBy("category").As("category_count").End()
func (sb *Select[T]) SelectCountOver() *SelectWindowBuilder[T] {
	return &SelectWindowBuilder[T]{
		selectBuilder: sb,
		instance:      sb.instance,
		windowExpr:    astql.CountOver(),
	}
}

// SelectMinOver starts building a MIN(field) OVER window function.
//
// Example:
//
//	.SelectMinOver("price").PartitionBy("category").As("min_price").End()
func (sb *Select[T]) SelectMinOver(field string) *SelectWindowBuilder[T] {
	if sb.err != nil {
		return &SelectWindowBuilder[T]{
			selectBuilder: sb,
			instance:      sb.instance,
			err:           sb.err,
		}
	}

	f, err := sb.instance.TryF(field)
	if err != nil {
		return &SelectWindowBuilder[T]{
			selectBuilder: sb,
			instance:      sb.instance,
			err:           fmt.Errorf("invalid field %q: %w", field, err),
		}
	}

	return &SelectWindowBuilder[T]{
		selectBuilder: sb,
		instance:      sb.instance,
		windowExpr:    astql.MinOver(f),
	}
}

// SelectMaxOver starts building a MAX(field) OVER window function.
//
// Example:
//
//	.SelectMaxOver("price").PartitionBy("category").As("max_price").End()
func (sb *Select[T]) SelectMaxOver(field string) *SelectWindowBuilder[T] {
	if sb.err != nil {
		return &SelectWindowBuilder[T]{
			selectBuilder: sb,
			instance:      sb.instance,
			err:           sb.err,
		}
	}

	f, err := sb.instance.TryF(field)
	if err != nil {
		return &SelectWindowBuilder[T]{
			selectBuilder: sb,
			instance:      sb.instance,
			err:           fmt.Errorf("invalid field %q: %w", field, err),
		}
	}

	return &SelectWindowBuilder[T]{
		selectBuilder: sb,
		instance:      sb.instance,
		windowExpr:    astql.MaxOver(f),
	}
}

// SelectWindowBuilder provides a fluent API for building window function expressions.
type SelectWindowBuilder[T any] struct {
	selectBuilder *Select[T]
	instance      *astql.ASTQL
	windowExpr    *astql.WindowBuilder
	err           error
	alias         string
}

// PartitionBy adds PARTITION BY fields to the window specification.
//
// Example:
//
//	.PartitionBy("department", "location")
func (swb *SelectWindowBuilder[T]) PartitionBy(fields ...string) *SelectWindowBuilder[T] {
	if swb.err != nil {
		return swb
	}

	astqlFields := swb.instance.Fields()
	for _, field := range fields {
		f, err := swb.instance.TryF(field)
		if err != nil {
			swb.err = fmt.Errorf("invalid partition field %q: %w", field, err)
			return swb
		}
		astqlFields = append(astqlFields, f)
	}

	swb.windowExpr.PartitionBy(astqlFields...)
	return swb
}

// OrderBy adds ORDER BY to the window specification.
// Direction must be "ASC" or "DESC".
//
// Example:
//
//	.OrderBy("created_at", "DESC")
func (swb *SelectWindowBuilder[T]) OrderBy(field, direction string) *SelectWindowBuilder[T] {
	if swb.err != nil {
		return swb
	}

	astqlDir, err := validateDirection(direction)
	if err != nil {
		swb.err = err
		return swb
	}

	f, err := swb.instance.TryF(field)
	if err != nil {
		swb.err = fmt.Errorf("invalid order field %q: %w", field, err)
		return swb
	}

	swb.windowExpr.OrderBy(f, astqlDir)
	return swb
}

// Frame sets the frame clause with ROWS BETWEEN start AND end.
// Valid bounds: "UNBOUNDED PRECEDING", "CURRENT ROW", "UNBOUNDED FOLLOWING"
//
// Example:
//
//	.Frame("UNBOUNDED PRECEDING", "CURRENT ROW")
func (swb *SelectWindowBuilder[T]) Frame(start, end string) *SelectWindowBuilder[T] {
	if swb.err != nil {
		return swb
	}

	startBound, err := validateFrameBound(start)
	if err != nil {
		swb.err = fmt.Errorf("invalid frame start: %w", err)
		return swb
	}

	endBound, err := validateFrameBound(end)
	if err != nil {
		swb.err = fmt.Errorf("invalid frame end: %w", err)
		return swb
	}

	swb.windowExpr.Frame(startBound, endBound)
	return swb
}

// As sets the alias for the window function result.
//
// Example:
//
//	.As("row_num")
func (swb *SelectWindowBuilder[T]) As(alias string) *SelectWindowBuilder[T] {
	if swb.err != nil {
		return swb
	}
	swb.alias = alias
	return swb
}

// End completes the window function and adds it to the SELECT clause.
// Returns the parent Select builder to continue chaining.
func (swb *SelectWindowBuilder[T]) End() *Select[T] {
	if swb.err != nil {
		swb.selectBuilder.err = swb.err
		return swb.selectBuilder
	}

	if swb.alias != "" {
		swb.selectBuilder.builder = swb.selectBuilder.builder.SelectExpr(swb.windowExpr.As(swb.alias))
	} else {
		swb.selectBuilder.builder = swb.selectBuilder.builder.SelectExpr(swb.windowExpr.Build())
	}
	return swb.selectBuilder
}

// validateFrameBound converts a string frame bound to astql.FrameBound.
func validateFrameBound(bound string) (astql.FrameBound, error) {
	switch strings.ToUpper(bound) {
	case "UNBOUNDED PRECEDING":
		return astql.FrameUnboundedPreceding, nil
	case "CURRENT ROW":
		return astql.FrameCurrentRow, nil
	case "UNBOUNDED FOLLOWING":
		return astql.FrameUnboundedFollowing, nil
	default:
		return astql.FrameUnboundedPreceding, fmt.Errorf("invalid frame bound %q: must be UNBOUNDED PRECEDING, CURRENT ROW, or UNBOUNDED FOLLOWING", bound)
	}
}

// Render builds and renders the query to SQL with parameter placeholders.
// Returns the SQL string and list of required parameters for sqlx execution.
func (sb *Select[T]) Render() (*astql.QueryResult, error) {
	// Check for  errors first
	if sb.err != nil {
		return nil, fmt.Errorf("select  has errors: %w", sb.err)
	}

	result, err := sb.builder.Render(sb.cereal.renderer())
	if err != nil {
		return nil, fmt.Errorf("failed to render SELECT query: %w", err)
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
//	user, err := cereal.Select().
//	    Where("email", "=", "user_email").
//	    Exec(ctx, map[string]any{"user_email": "test@example.com"})
func (sb *Select[T]) Exec(ctx context.Context, params map[string]any) (*T, error) {
	return sb.exec(ctx, sb.cereal.execer(), params)
}

// ExecTx executes the SELECT query within a transaction and returns a single record of type T.
// Returns an error if zero rows or more than one row is found.
//
// Example:
//
//	tx, _ := db.BeginTxx(ctx, nil)
//	defer tx.Rollback()
//	user, err := cereal.Select().
//	    Where("email", "=", "user_email").
//	    ExecTx(ctx, tx, map[string]any{"user_email": "test@example.com"})
//	tx.Commit()
func (sb *Select[T]) ExecTx(ctx context.Context, tx *sqlx.Tx, params map[string]any) (*T, error) {
	return sb.exec(ctx, tx, params)
}

// exec is the internal execution method used by both Exec and ExecTx.
func (sb *Select[T]) exec(ctx context.Context, execer sqlx.ExtContext, params map[string]any) (*T, error) {
	// Check for  errors first
	if sb.err != nil {
		return nil, fmt.Errorf("select  has errors: %w", sb.err)
	}

	// Render the query
	result, err := sb.Render()
	if err != nil {
		return nil, fmt.Errorf("failed to render query: %w", err)
	}

	// Emit query started event
	tableName := sb.cereal.getTableName()
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
		return nil, fmt.Errorf("query execution failed: %w", err)
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
		return nil, fmt.Errorf("no rows found")
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
		return nil, fmt.Errorf("failed to scan row: %w", err)
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
		return nil, fmt.Errorf("expected exactly one row, found multiple")
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
//	cereal.Between("age", "min_age", "max_age")  // age BETWEEN :min_age AND :max_age
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
//	cereal.NotBetween("age", "min_age", "max_age")  // age NOT BETWEEN :min_age AND :max_age
func NotBetween(field, lowParam, highParam string) Condition {
	return Condition{
		field:     field,
		operator:  opNotBetween,
		isBetween: true,
		lowParam:  lowParam,
		highParam: highParam,
	}
}

// buildAggregateCondition creates an ASTQL AggregateCondition from string parameters.
// aggFunc: "count", "sum", "avg", "min", "max", "count_distinct"
// field: field name (empty string for COUNT(*))
// op: the ASTQL operator
// param: parameter name
func buildAggregateCondition(instance *astql.ASTQL, aggFunc, field, param string, op astql.Operator) (astql.AggregateCondition, error) {
	var aggType astql.AggregateFunc
	switch strings.ToLower(aggFunc) {
	case "count":
		aggType = astql.AggCountField
	case "count_distinct":
		aggType = astql.AggCountDistinct
	case "sum":
		aggType = astql.AggSum
	case "avg":
		aggType = astql.AggAvg
	case "min":
		aggType = astql.AggMin
	case "max":
		aggType = astql.AggMax
	default:
		return astql.AggregateCondition{}, fmt.Errorf("invalid aggregate function %q, must be one of: count, sum, avg, min, max, count_distinct", aggFunc)
	}

	// Validate and create param
	p, err := instance.TryP(param)
	if err != nil {
		return astql.AggregateCondition{}, fmt.Errorf("invalid param %q: %w", param, err)
	}

	// Handle field (nil for COUNT(*))
	if field == "" {
		return instance.TryAggC(aggType, nil, op, p)
	}

	f, err := instance.TryF(field)
	if err != nil {
		return astql.AggregateCondition{}, fmt.Errorf("invalid field %q: %w", field, err)
	}

	return instance.TryAggC(aggType, &f, op, p)
}
