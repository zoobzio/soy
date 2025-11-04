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
	field    string
	operator string
	param    string
	isNull   bool
}

// operatorMap translates string operators to ASTQL operators.
var operatorMap = map[string]astql.Operator{
	"=":        astql.EQ,
	"!=":       astql.NE,
	">":        astql.GT,
	">=":       astql.GE,
	"<":        astql.LT,
	"<=":       astql.LE,
	"LIKE":     astql.LIKE,
	"NOT LIKE": astql.NotLike,
}

// directionMap translates string directions to ASTQL directions.
var directionMap = map[string]astql.Direction{
	"asc":  astql.ASC,
	"desc": astql.DESC,
}

// validateOperator converts a string operator to ASTQL operator.
func validateOperator(op string) (astql.Operator, error) {
	astqlOp, ok := operatorMap[op]
	if !ok {
		return "", fmt.Errorf("invalid operator %q, must be one of: =, !=, >, >=, <, <=, LIKE, NOT LIKE", op)
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

// buildCondition converts a Condition to an ASTQL condition.
func (sb *Select[T]) buildCondition(cond Condition) (astql.ConditionItem, error) {
	f, err := sb.instance.TryF(cond.field)
	if err != nil {
		return nil, fmt.Errorf("invalid field %q: %w", cond.field, err)
	}

	if cond.isNull {
		if cond.operator == opIsNull {
			return sb.instance.TryNull(f)
		}
		return sb.instance.TryNotNull(f)
	}

	astqlOp, err := validateOperator(cond.operator)
	if err != nil {
		return nil, err
	}

	p, err := sb.instance.TryP(cond.param)
	if err != nil {
		return nil, fmt.Errorf("invalid param %q: %w", cond.param, err)
	}

	return sb.instance.TryC(f, astqlOp, p)
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

// Limit sets the LIMIT clause.
func (sb *Select[T]) Limit(n int) *Select[T] {
	sb.builder = sb.builder.Limit(n)
	return sb
}

// Offset sets the OFFSET clause.
func (sb *Select[T]) Offset(n int) *Select[T] {
	sb.builder = sb.builder.Offset(n)
	return sb
}

// Distinct adds DISTINCT to the SELECT query.
func (sb *Select[T]) Distinct() *Select[T] {
	sb.builder = sb.builder.Distinct()
	return sb
}

// Render builds and renders the query to SQL with parameter placeholders.
// Returns the SQL string and list of required parameters for sqlx execution.
func (sb *Select[T]) Render() (*astql.QueryResult, error) {
	// Check for  errors first
	if sb.err != nil {
		return nil, fmt.Errorf("select  has errors: %w", sb.err)
	}

	result, err := sb.builder.Render()
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
	defer rows.Close()

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
