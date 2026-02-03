package soy

import (
	"fmt"
	"strings"

	"github.com/zoobzio/astql"
)

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

	// Arithmetic operators.
	"+": "+",
	"-": "-",
	"*": "*",
	"/": "/",
	"%": "%",
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
		return "", newOperatorError(op)
	}
	return astqlOp, nil
}

// validateDirection converts a string direction to ASTQL direction.
func validateDirection(dir string) (astql.Direction, error) {
	lower := strings.ToLower(dir)
	astqlDir, ok := directionMap[lower]
	if !ok {
		return "", newDirectionError(dir)
	}
	return astqlDir, nil
}

// validateNulls converts a string nulls ordering to ASTQL nulls ordering.
func validateNulls(nulls string) (astql.NullsOrdering, error) {
	lower := strings.ToLower(nulls)
	astqlNulls, ok := nullsMap[lower]
	if !ok {
		return "", newNullsOrderingError(nulls)
	}
	return astqlNulls, nil
}

// buildAggregateCondition creates an ASTQL AggregateCondition from string parameters.
// This is shared by Select and Query for HAVING clauses with aggregates.
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
		return astql.AggregateCondition{}, newAggregateFuncError(aggFunc)
	}

	p, err := instance.TryP(param)
	if err != nil {
		return astql.AggregateCondition{}, newParamError(param, err)
	}

	if field == "" {
		return instance.TryAggC(aggType, nil, op, p)
	}

	f, err := instance.TryF(field)
	if err != nil {
		return astql.AggregateCondition{}, newFieldError(field, err)
	}

	return instance.TryAggC(aggType, &f, op, p)
}

// --- Shared Implementation Functions ---
// These functions contain the shared logic for Select and Query builders.
// Each builder wraps these with thin 3-line methods that handle error state.

// fieldsImpl adds fields to the SELECT clause.
func fieldsImpl(instance *astql.ASTQL, builder *astql.Builder, fields ...string) (*astql.Builder, error) {
	if len(fields) == 0 {
		return builder, nil
	}

	fieldSlice := instance.Fields()
	for _, fieldName := range fields {
		f, err := instance.TryF(fieldName)
		if err != nil {
			return builder, newFieldError(fieldName, err)
		}
		fieldSlice = append(fieldSlice, f)
	}

	return builder.Fields(fieldSlice...), nil
}

// whereImpl adds a WHERE condition.
func whereImpl(instance *astql.ASTQL, builder *astql.Builder, field, operator, param string) (*astql.Builder, error) {
	astqlOp, err := validateOperator(operator)
	if err != nil {
		return builder, err
	}

	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}

	p, err := instance.TryP(param)
	if err != nil {
		return builder, newParamError(param, err)
	}

	condition, err := instance.TryC(f, astqlOp, p)
	if err != nil {
		return builder, newConditionError(err)
	}

	return builder.Where(condition), nil
}

// whereAndImpl adds multiple conditions combined with AND.
func whereAndImpl(instance *astql.ASTQL, builder *astql.Builder, conditions ...Condition) (*astql.Builder, error) {
	if len(conditions) == 0 {
		return builder, nil
	}

	conditionItems := instance.ConditionItems()
	for _, cond := range conditions {
		condItem, err := buildConditionWithInstance(instance, cond)
		if err != nil {
			return builder, err
		}
		conditionItems = append(conditionItems, condItem)
	}

	andGroup, err := instance.TryAnd(conditionItems...)
	if err != nil {
		return builder, newConditionError(err)
	}

	return builder.Where(andGroup), nil
}

// whereOrImpl adds multiple conditions combined with OR.
func whereOrImpl(instance *astql.ASTQL, builder *astql.Builder, conditions ...Condition) (*astql.Builder, error) {
	if len(conditions) == 0 {
		return builder, nil
	}

	conditionItems := instance.ConditionItems()
	for _, cond := range conditions {
		condItem, err := buildConditionWithInstance(instance, cond)
		if err != nil {
			return builder, err
		}
		conditionItems = append(conditionItems, condItem)
	}

	orGroup, err := instance.TryOr(conditionItems...)
	if err != nil {
		return builder, newConditionError(err)
	}

	return builder.Where(orGroup), nil
}

// whereNullImpl adds a WHERE field IS NULL condition.
func whereNullImpl(instance *astql.ASTQL, builder *astql.Builder, field string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}

	condition, err := instance.TryNull(f)
	if err != nil {
		return builder, newConditionError(err)
	}

	return builder.Where(condition), nil
}

// whereNotNullImpl adds a WHERE field IS NOT NULL condition.
func whereNotNullImpl(instance *astql.ASTQL, builder *astql.Builder, field string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}

	condition, err := instance.TryNotNull(f)
	if err != nil {
		return builder, newConditionError(err)
	}

	return builder.Where(condition), nil
}

// whereBetweenImpl adds a WHERE field BETWEEN low AND high condition.
func whereBetweenImpl(instance *astql.ASTQL, builder *astql.Builder, field, lowParam, highParam string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}

	lowP, err := instance.TryP(lowParam)
	if err != nil {
		return builder, newParamError(lowParam, err)
	}

	highP, err := instance.TryP(highParam)
	if err != nil {
		return builder, newParamError(highParam, err)
	}

	return builder.Where(astql.Between(f, lowP, highP)), nil
}

// whereNotBetweenImpl adds a WHERE field NOT BETWEEN low AND high condition.
func whereNotBetweenImpl(instance *astql.ASTQL, builder *astql.Builder, field, lowParam, highParam string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}

	lowP, err := instance.TryP(lowParam)
	if err != nil {
		return builder, newParamError(lowParam, err)
	}

	highP, err := instance.TryP(highParam)
	if err != nil {
		return builder, newParamError(highParam, err)
	}

	return builder.Where(astql.NotBetween(f, lowP, highP)), nil
}

// whereFieldsImpl adds a WHERE condition comparing two fields.
func whereFieldsImpl(instance *astql.ASTQL, builder *astql.Builder, leftField, operator, rightField string) (*astql.Builder, error) {
	astqlOp, err := validateOperator(operator)
	if err != nil {
		return builder, err
	}

	left, err := instance.TryF(leftField)
	if err != nil {
		return builder, newFieldError(leftField, err)
	}

	right, err := instance.TryF(rightField)
	if err != nil {
		return builder, newFieldError(rightField, err)
	}

	return builder.Where(astql.CF(left, astqlOp, right)), nil
}

// orderByImpl adds an ORDER BY clause.
func orderByImpl(instance *astql.ASTQL, builder *astql.Builder, field, direction string) (*astql.Builder, error) {
	astqlDir, err := validateDirection(direction)
	if err != nil {
		return builder, err
	}

	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}

	return builder.OrderBy(f, astqlDir), nil
}

// orderByNullsImpl adds an ORDER BY clause with NULLS FIRST or NULLS LAST.
func orderByNullsImpl(instance *astql.ASTQL, builder *astql.Builder, field, direction, nulls string) (*astql.Builder, error) {
	astqlDir, err := validateDirection(direction)
	if err != nil {
		return builder, err
	}

	astqlNulls, err := validateNulls(nulls)
	if err != nil {
		return builder, err
	}

	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}

	return builder.OrderByNulls(f, astqlDir, astqlNulls), nil
}

// orderByExprImpl adds an ORDER BY clause with an expression.
func orderByExprImpl(instance *astql.ASTQL, builder *astql.Builder, field, operator, param, direction string) (*astql.Builder, error) {
	astqlDir, err := validateDirection(direction)
	if err != nil {
		return builder, err
	}

	astqlOp, err := validateOperator(operator)
	if err != nil {
		return builder, err
	}

	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}

	p, err := instance.TryP(param)
	if err != nil {
		return builder, newParamError(param, err)
	}

	return builder.OrderByExpr(f, astqlOp, p, astqlDir), nil
}

// limitParamImpl sets the LIMIT clause to a parameterized value.
func limitParamImpl(instance *astql.ASTQL, builder *astql.Builder, param string) (*astql.Builder, error) {
	p, err := instance.TryP(param)
	if err != nil {
		return builder, newParamError(param, err)
	}

	return builder.LimitParam(p), nil
}

// offsetParamImpl sets the OFFSET clause to a parameterized value.
func offsetParamImpl(instance *astql.ASTQL, builder *astql.Builder, param string) (*astql.Builder, error) {
	p, err := instance.TryP(param)
	if err != nil {
		return builder, newParamError(param, err)
	}

	return builder.OffsetParam(p), nil
}

// distinctOnImpl adds DISTINCT ON to the SELECT query.
func distinctOnImpl(instance *astql.ASTQL, builder *astql.Builder, fields ...string) (*astql.Builder, error) {
	if len(fields) == 0 {
		return builder, nil
	}

	astqlFields := instance.Fields()
	for _, field := range fields {
		f, err := instance.TryF(field)
		if err != nil {
			return builder, newFieldError(field, err)
		}
		astqlFields = append(astqlFields, f)
	}

	return builder.DistinctOn(astqlFields...), nil
}

// groupByImpl adds a GROUP BY clause.
func groupByImpl(instance *astql.ASTQL, builder *astql.Builder, fields ...string) (*astql.Builder, error) {
	astqlFields := instance.Fields()
	for _, field := range fields {
		f, err := instance.TryF(field)
		if err != nil {
			return builder, newFieldError(field, err)
		}
		astqlFields = append(astqlFields, f)
	}
	return builder.GroupBy(astqlFields...), nil
}

// havingImpl adds a HAVING condition.
func havingImpl(instance *astql.ASTQL, builder *astql.Builder, field, operator, param string) (*astql.Builder, error) {
	astqlOp, err := validateOperator(operator)
	if err != nil {
		return builder, err
	}

	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}

	p, err := instance.TryP(param)
	if err != nil {
		return builder, newParamError(param, err)
	}

	condition, err := instance.TryC(f, astqlOp, p)
	if err != nil {
		return builder, newConditionError(err)
	}

	return builder.Having(condition), nil
}

// havingAggImpl adds an aggregate HAVING condition.
func havingAggImpl(instance *astql.ASTQL, builder *astql.Builder, aggFunc, field, operator, param string) (*astql.Builder, error) {
	astqlOp, err := validateOperator(operator)
	if err != nil {
		return builder, err
	}

	aggCond, err := buildAggregateCondition(instance, aggFunc, field, param, astqlOp)
	if err != nil {
		return builder, err
	}

	return builder.HavingAgg(aggCond), nil
}

// --- String Expression Implementations ---

// selectUpperImpl adds UPPER(field) AS alias to the SELECT clause.
func selectUpperImpl(instance *astql.ASTQL, builder *astql.Builder, field, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}
	return builder.SelectExpr(astql.As(astql.Upper(f), alias)), nil
}

// selectLowerImpl adds LOWER(field) AS alias to the SELECT clause.
func selectLowerImpl(instance *astql.ASTQL, builder *astql.Builder, field, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}
	return builder.SelectExpr(astql.As(astql.Lower(f), alias)), nil
}

// selectLengthImpl adds LENGTH(field) AS alias to the SELECT clause.
func selectLengthImpl(instance *astql.ASTQL, builder *astql.Builder, field, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}
	return builder.SelectExpr(astql.As(astql.Length(f), alias)), nil
}

// selectTrimImpl adds TRIM(field) AS alias to the SELECT clause.
func selectTrimImpl(instance *astql.ASTQL, builder *astql.Builder, field, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}
	return builder.SelectExpr(astql.As(astql.Trim(f), alias)), nil
}

// selectLTrimImpl adds LTRIM(field) AS alias to the SELECT clause.
func selectLTrimImpl(instance *astql.ASTQL, builder *astql.Builder, field, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}
	return builder.SelectExpr(astql.As(astql.LTrim(f), alias)), nil
}

// selectRTrimImpl adds RTRIM(field) AS alias to the SELECT clause.
func selectRTrimImpl(instance *astql.ASTQL, builder *astql.Builder, field, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}
	return builder.SelectExpr(astql.As(astql.RTrim(f), alias)), nil
}

// --- Math Expression Implementations ---

// selectAbsImpl adds ABS(field) AS alias to the SELECT clause.
func selectAbsImpl(instance *astql.ASTQL, builder *astql.Builder, field, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}
	return builder.SelectExpr(astql.As(astql.Abs(f), alias)), nil
}

// selectCeilImpl adds CEIL(field) AS alias to the SELECT clause.
func selectCeilImpl(instance *astql.ASTQL, builder *astql.Builder, field, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}
	return builder.SelectExpr(astql.As(astql.Ceil(f), alias)), nil
}

// selectFloorImpl adds FLOOR(field) AS alias to the SELECT clause.
func selectFloorImpl(instance *astql.ASTQL, builder *astql.Builder, field, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}
	return builder.SelectExpr(astql.As(astql.Floor(f), alias)), nil
}

// selectRoundImpl adds ROUND(field) AS alias to the SELECT clause.
func selectRoundImpl(instance *astql.ASTQL, builder *astql.Builder, field, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}
	return builder.SelectExpr(astql.As(astql.Round(f), alias)), nil
}

// selectSqrtImpl adds SQRT(field) AS alias to the SELECT clause.
func selectSqrtImpl(instance *astql.ASTQL, builder *astql.Builder, field, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}
	return builder.SelectExpr(astql.As(astql.Sqrt(f), alias)), nil
}

// --- Cast Expression Implementation ---

// selectCastImpl adds CAST(field AS type) AS alias to the SELECT clause.
func selectCastImpl(instance *astql.ASTQL, builder *astql.Builder, field string, castType CastType, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}
	return builder.SelectExpr(astql.As(astql.Cast(f, castType), alias)), nil
}

// --- Aggregate Expression Implementations ---

// selectCountImpl adds COUNT(field) AS alias to the SELECT clause.
func selectCountImpl(instance *astql.ASTQL, builder *astql.Builder, field, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}
	return builder.SelectExpr(astql.As(astql.CountField(f), alias)), nil
}

// selectCountDistinctImpl adds COUNT(DISTINCT field) AS alias to the SELECT clause.
func selectCountDistinctImpl(instance *astql.ASTQL, builder *astql.Builder, field, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}
	return builder.SelectExpr(astql.As(astql.CountDistinct(f), alias)), nil
}

// selectSumImpl adds SUM(field) AS alias to the SELECT clause.
func selectSumImpl(instance *astql.ASTQL, builder *astql.Builder, field, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}
	return builder.SelectExpr(astql.As(astql.Sum(f), alias)), nil
}

// selectAvgImpl adds AVG(field) AS alias to the SELECT clause.
func selectAvgImpl(instance *astql.ASTQL, builder *astql.Builder, field, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}
	return builder.SelectExpr(astql.As(astql.Avg(f), alias)), nil
}

// selectMinImpl adds MIN(field) AS alias to the SELECT clause.
func selectMinImpl(instance *astql.ASTQL, builder *astql.Builder, field, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}
	return builder.SelectExpr(astql.As(astql.Min(f), alias)), nil
}

// selectMaxImpl adds MAX(field) AS alias to the SELECT clause.
func selectMaxImpl(instance *astql.ASTQL, builder *astql.Builder, field, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}
	return builder.SelectExpr(astql.As(astql.Max(f), alias)), nil
}

// --- Multi-param Expression Implementations ---

// selectConcatImpl adds CONCAT(fields...) AS alias to the SELECT clause.
func selectConcatImpl(instance *astql.ASTQL, builder *astql.Builder, alias string, fields ...string) (*astql.Builder, error) {
	if len(fields) < 2 {
		return builder, fmt.Errorf("CONCAT requires at least 2 fields")
	}

	astqlFields := instance.Fields()
	for _, field := range fields {
		f, err := instance.TryF(field)
		if err != nil {
			return builder, newFieldError(field, err)
		}
		astqlFields = append(astqlFields, f)
	}

	return builder.SelectExpr(astql.As(astql.Concat(astqlFields...), alias)), nil
}

// selectCoalesceImpl adds COALESCE(params...) AS alias to the SELECT clause.
func selectCoalesceImpl(instance *astql.ASTQL, builder *astql.Builder, alias string, params ...string) (*astql.Builder, error) {
	if len(params) < 2 {
		return builder, fmt.Errorf("COALESCE requires at least 2 parameters, got %d", len(params))
	}

	astqlParams := instance.Params()
	for _, param := range params {
		p, err := instance.TryP(param)
		if err != nil {
			return builder, newParamError(param, err)
		}
		astqlParams = append(astqlParams, p)
	}

	return builder.SelectExpr(astql.As(astql.Coalesce(astqlParams...), alias)), nil
}

// selectSubstringImpl adds SUBSTRING(field, start, length) AS alias to the SELECT clause.
func selectSubstringImpl(instance *astql.ASTQL, builder *astql.Builder, field, startParam, lengthParam, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}

	start, err := instance.TryP(startParam)
	if err != nil {
		return builder, newParamError(startParam, err)
	}

	length, err := instance.TryP(lengthParam)
	if err != nil {
		return builder, newParamError(lengthParam, err)
	}

	return builder.SelectExpr(astql.As(astql.Substring(f, start, length), alias)), nil
}

// selectReplaceImpl adds REPLACE(field, search, replacement) AS alias to the SELECT clause.
func selectReplaceImpl(instance *astql.ASTQL, builder *astql.Builder, field, searchParam, replacementParam, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}

	search, err := instance.TryP(searchParam)
	if err != nil {
		return builder, newParamError(searchParam, err)
	}

	replacement, err := instance.TryP(replacementParam)
	if err != nil {
		return builder, newParamError(replacementParam, err)
	}

	return builder.SelectExpr(astql.As(astql.Replace(f, search, replacement), alias)), nil
}

// selectPowerImpl adds POWER(field, exponent) AS alias to the SELECT clause.
func selectPowerImpl(instance *astql.ASTQL, builder *astql.Builder, field, exponentParam, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}

	exp, err := instance.TryP(exponentParam)
	if err != nil {
		return builder, newParamError(exponentParam, err)
	}

	return builder.SelectExpr(astql.As(astql.Power(f, exp), alias)), nil
}

// selectNullIfImpl adds NULLIF(param1, param2) AS alias to the SELECT clause.
func selectNullIfImpl(instance *astql.ASTQL, builder *astql.Builder, param1, param2, alias string) (*astql.Builder, error) {
	p1, err := instance.TryP(param1)
	if err != nil {
		return builder, newParamError(param1, err)
	}

	p2, err := instance.TryP(param2)
	if err != nil {
		return builder, newParamError(param2, err)
	}

	return builder.SelectExpr(astql.As(astql.NullIf(p1, p2), alias)), nil
}

// --- Set Expression Implementations ---

// setExprImpl adds a field update with a binary expression value for UPDATE queries.
// Use this for computed assignments like `age = age + :increment`.
func setExprImpl(instance *astql.ASTQL, builder *astql.Builder, field, operator, param string) (*astql.Builder, error) {
	astqlOp, err := validateOperator(operator)
	if err != nil {
		return builder, err
	}

	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}

	p, err := instance.TryP(param)
	if err != nil {
		return builder, newParamError(param, err)
	}

	return builder.SetExpr(f, astql.BinaryExpr(f, astqlOp, p)), nil
}

// --- Binary Expression Implementations ---

// selectExprImpl adds a binary expression (field <op> param) AS alias to the SELECT clause.
// Useful for vector distance calculations with pgvector.
func selectExprImpl(instance *astql.ASTQL, builder *astql.Builder, field, operator, param, alias string) (*astql.Builder, error) {
	astqlOp, err := validateOperator(operator)
	if err != nil {
		return builder, err
	}

	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}

	p, err := instance.TryP(param)
	if err != nil {
		return builder, newParamError(param, err)
	}

	return builder.SelectBinaryExpr(f, astqlOp, p, alias), nil
}

// --- Aggregate Filter Expression Implementations ---

// buildSimpleConditionImpl creates a simple condition (field op param) for FILTER clauses.
func buildSimpleConditionImpl(instance *astql.ASTQL, field, operator, param string) (astql.ConditionItem, error) {
	astqlOp, err := validateOperator(operator)
	if err != nil {
		return nil, err
	}

	f, err := instance.TryF(field)
	if err != nil {
		return nil, newFieldError(field, err)
	}

	p, err := instance.TryP(param)
	if err != nil {
		return nil, newParamError(param, err)
	}

	return instance.TryC(f, astqlOp, p)
}

// selectSumFilterImpl adds SUM(field) FILTER (WHERE condition) AS alias to the SELECT clause.
func selectSumFilterImpl(instance *astql.ASTQL, builder *astql.Builder, field, condField, condOp, condParam, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}

	filter, err := buildSimpleConditionImpl(instance, condField, condOp, condParam)
	if err != nil {
		return builder, err
	}

	return builder.SelectExpr(astql.As(astql.SumFilter(f, filter), alias)), nil
}

// selectAvgFilterImpl adds AVG(field) FILTER (WHERE condition) AS alias to the SELECT clause.
func selectAvgFilterImpl(instance *astql.ASTQL, builder *astql.Builder, field, condField, condOp, condParam, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}

	filter, err := buildSimpleConditionImpl(instance, condField, condOp, condParam)
	if err != nil {
		return builder, err
	}

	return builder.SelectExpr(astql.As(astql.AvgFilter(f, filter), alias)), nil
}

// selectMinFilterImpl adds MIN(field) FILTER (WHERE condition) AS alias to the SELECT clause.
func selectMinFilterImpl(instance *astql.ASTQL, builder *astql.Builder, field, condField, condOp, condParam, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}

	filter, err := buildSimpleConditionImpl(instance, condField, condOp, condParam)
	if err != nil {
		return builder, err
	}

	return builder.SelectExpr(astql.As(astql.MinFilter(f, filter), alias)), nil
}

// selectMaxFilterImpl adds MAX(field) FILTER (WHERE condition) AS alias to the SELECT clause.
func selectMaxFilterImpl(instance *astql.ASTQL, builder *astql.Builder, field, condField, condOp, condParam, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}

	filter, err := buildSimpleConditionImpl(instance, condField, condOp, condParam)
	if err != nil {
		return builder, err
	}

	return builder.SelectExpr(astql.As(astql.MaxFilter(f, filter), alias)), nil
}

// selectCountFilterImpl adds COUNT(field) FILTER (WHERE condition) AS alias to the SELECT clause.
func selectCountFilterImpl(instance *astql.ASTQL, builder *astql.Builder, field, condField, condOp, condParam, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}

	filter, err := buildSimpleConditionImpl(instance, condField, condOp, condParam)
	if err != nil {
		return builder, err
	}

	return builder.SelectExpr(astql.As(astql.CountFieldFilter(f, filter), alias)), nil
}

// selectCountDistinctFilterImpl adds COUNT(DISTINCT field) FILTER (WHERE condition) AS alias to the SELECT clause.
func selectCountDistinctFilterImpl(instance *astql.ASTQL, builder *astql.Builder, field, condField, condOp, condParam, alias string) (*astql.Builder, error) {
	f, err := instance.TryF(field)
	if err != nil {
		return builder, newFieldError(field, err)
	}

	filter, err := buildSimpleConditionImpl(instance, condField, condOp, condParam)
	if err != nil {
		return builder, err
	}

	return builder.SelectExpr(astql.As(astql.CountDistinctFilter(f, filter), alias)), nil
}
