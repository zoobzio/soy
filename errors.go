package soy

import (
	"errors"
	"fmt"
)

// Sentinel errors for soy package.
// These errors can be checked using errors.Is() for precise error handling.

// Initialization errors.
var (
	// ErrEmptyTableName is returned when a table name is empty.
	ErrEmptyTableName = errors.New("soy: table name cannot be empty")

	// ErrNilRenderer is returned when a renderer is nil.
	ErrNilRenderer = errors.New("soy: renderer cannot be nil")
)

// Data errors.
var (
	// ErrNotFound is returned when a query expects at least one row but finds none.
	ErrNotFound = errors.New("no rows found")

	// ErrMultipleRows is returned when a query expects exactly one row but finds multiple.
	ErrMultipleRows = errors.New("expected exactly one row, found multiple")

	// ErrNoRowsAffected is returned when an operation expects to affect rows but affects none.
	ErrNoRowsAffected = errors.New("no rows affected")
)

// Safety errors.
var (
	// ErrUnsafeUpdate is returned when an UPDATE is attempted without a WHERE clause.
	ErrUnsafeUpdate = errors.New("UPDATE requires at least one WHERE condition to prevent accidental full-table update")

	// ErrUnsafeDelete is returned when a DELETE is attempted without a WHERE clause.
	ErrUnsafeDelete = errors.New("DELETE requires at least one WHERE condition to prevent accidental full-table deletion")
)

// ValidationError represents a validation error with context about what was invalid.
type ValidationError struct {
	Kind    string // Type of invalid item: "field", "param", "operator", "direction", "table", "condition", etc.
	Name    string // The invalid value
	Message string // Additional context message
	Err     error  // Underlying error, if any
}

func (e *ValidationError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	if e.Err != nil {
		return fmt.Sprintf("invalid %s %q: %v", e.Kind, e.Name, e.Err)
	}
	return fmt.Sprintf("invalid %s %q", e.Kind, e.Name)
}

func (e *ValidationError) Unwrap() error {
	return e.Err
}

// Is implements errors.Is for ValidationError.
// It matches if the target is a ValidationError with the same Kind.
func (e *ValidationError) Is(target error) bool {
	t, ok := target.(*ValidationError)
	if !ok {
		return false
	}
	// Match if target has same Kind (or target Kind is empty for any match)
	return t.Kind == "" || e.Kind == t.Kind
}

// Sentinel ValidationError instances for use with errors.Is().
var (
	// ErrInvalidField is returned when a field name is not found in the schema.
	ErrInvalidField = &ValidationError{Kind: "field"}

	// ErrInvalidParam is returned when a parameter name is not found in the schema.
	ErrInvalidParam = &ValidationError{Kind: "param"}

	// ErrInvalidOperator is returned when an operator is not supported.
	ErrInvalidOperator = &ValidationError{Kind: "operator"}

	// ErrInvalidDirection is returned when a sort direction is not valid.
	ErrInvalidDirection = &ValidationError{Kind: "direction"}

	// ErrInvalidNullsOrdering is returned when a nulls ordering is not valid.
	ErrInvalidNullsOrdering = &ValidationError{Kind: "nulls ordering"}

	// ErrInvalidTable is returned when a table name is not found in the schema.
	ErrInvalidTable = &ValidationError{Kind: "table"}

	// ErrInvalidCondition is returned when a condition cannot be constructed.
	ErrInvalidCondition = &ValidationError{Kind: "condition"}

	// ErrInvalidAggregateFunc is returned when an aggregate function is not supported.
	ErrInvalidAggregateFunc = &ValidationError{Kind: "aggregate function"}
)

// newFieldError creates a ValidationError for an invalid field.
func newFieldError(name string, err error) error {
	return &ValidationError{Kind: "field", Name: name, Err: err}
}

// newParamError creates a ValidationError for an invalid param.
func newParamError(name string, err error) error {
	return &ValidationError{Kind: "param", Name: name, Err: err}
}

// newOperatorError creates a ValidationError for an invalid operator.
func newOperatorError(op string) error {
	return &ValidationError{
		Kind:    "operator",
		Name:    op,
		Message: fmt.Sprintf("invalid operator %q, supported: =, !=, >, >=, <, <=, LIKE, NOT LIKE, ILIKE, NOT ILIKE, IN, NOT IN, ~, ~*, !~, !~*, @>, <@, &&, <->, <#>, <=>, <+>", op),
	}
}

// newDirectionError creates a ValidationError for an invalid direction.
func newDirectionError(dir string) error {
	return &ValidationError{
		Kind:    "direction",
		Name:    dir,
		Message: fmt.Sprintf("invalid direction %q, must be 'asc' or 'desc'", dir),
	}
}

// newNullsOrderingError creates a ValidationError for an invalid nulls ordering.
func newNullsOrderingError(nulls string) error {
	return &ValidationError{
		Kind:    "nulls ordering",
		Name:    nulls,
		Message: fmt.Sprintf("invalid nulls ordering %q, must be 'first' or 'last'", nulls),
	}
}

// newTableError creates a ValidationError for an invalid table.
func newTableError(name string, err error) error {
	return &ValidationError{Kind: "table", Name: name, Err: err}
}

// newConditionError creates a ValidationError for an invalid condition.
func newConditionError(err error) error {
	return &ValidationError{Kind: "condition", Err: err}
}

// newAggregateFuncError creates a ValidationError for an invalid aggregate function.
func newAggregateFuncError(funcName string) error {
	return &ValidationError{
		Kind:    "aggregate function",
		Name:    funcName,
		Message: fmt.Sprintf("invalid aggregate function %q, must be one of: count, sum, avg, min, max, count_distinct", funcName),
	}
}

// QueryError represents an error during query execution.
type QueryError struct {
	Operation string // The operation that failed: "SELECT", "INSERT", "UPDATE", "DELETE", etc.
	Phase     string // The phase that failed: "execution", "scan", "iteration", "render"
	Err       error  // Underlying error
}

func (e *QueryError) Error() string {
	switch e.Phase {
	case "execution":
		return fmt.Sprintf("%s query failed: %v", e.Operation, e.Err)
	case "scan":
		return fmt.Sprintf("failed to scan %s result: %v", e.Operation, e.Err)
	case "iteration":
		return fmt.Sprintf("error iterating rows: %v", e.Err)
	case "render":
		return fmt.Sprintf("failed to render %s query: %v", e.Operation, e.Err)
	default:
		return fmt.Sprintf("%s failed: %v", e.Operation, e.Err)
	}
}

func (e *QueryError) Unwrap() error {
	return e.Err
}

// Is implements errors.Is for QueryError.
func (e *QueryError) Is(target error) bool {
	t, ok := target.(*QueryError)
	if !ok {
		return false
	}
	// Match if target has same Operation and Phase (or empty for any match)
	opMatch := t.Operation == "" || e.Operation == t.Operation
	phaseMatch := t.Phase == "" || e.Phase == t.Phase
	return opMatch && phaseMatch
}

// Sentinel QueryError instances for use with errors.Is().
var (
	// ErrQueryFailed is returned when query execution fails.
	ErrQueryFailed = &QueryError{Phase: "execution"}

	// ErrScanFailed is returned when scanning query results fails.
	ErrScanFailed = &QueryError{Phase: "scan"}

	// ErrIterationFailed is returned when iterating query results fails.
	ErrIterationFailed = &QueryError{Phase: "iteration"}

	// ErrRenderFailed is returned when rendering a query fails.
	ErrRenderFailed = &QueryError{Phase: "render"}
)

// newQueryError creates a QueryError for a failed query execution.
func newQueryError(operation string, err error) error {
	return &QueryError{Operation: operation, Phase: "execution", Err: err}
}

// newScanError creates a QueryError for a failed scan.
func newScanError(operation string, err error) error {
	return &QueryError{Operation: operation, Phase: "scan", Err: err}
}

// newIterationError creates a QueryError for failed iteration.
func newIterationError(err error) error {
	return &QueryError{Phase: "iteration", Err: err}
}

// newRenderError creates a QueryError for a failed render.
func newRenderError(operation string, err error) error {
	return &QueryError{Operation: operation, Phase: "render", Err: err}
}

// BuilderError represents an error accumulated during query building.
type BuilderError struct {
	Builder string // The builder type: "select", "create", "update", "delete", "aggregate", etc.
	Err     error  // Underlying error
}

func (e *BuilderError) Error() string {
	return fmt.Sprintf("%s builder has errors: %v", e.Builder, e.Err)
}

func (e *BuilderError) Unwrap() error {
	return e.Err
}

// Is implements errors.Is for BuilderError.
func (e *BuilderError) Is(target error) bool {
	t, ok := target.(*BuilderError)
	if !ok {
		return false
	}
	return t.Builder == "" || e.Builder == t.Builder
}

// ErrBuilderHasErrors is returned when a builder has accumulated errors.
var ErrBuilderHasErrors = &BuilderError{}

// newBuilderError creates a BuilderError.
func newBuilderError(builder string, err error) error {
	return &BuilderError{Builder: builder, Err: err}
}

// UnsafeOperationError represents an unsafe operation error with operation context.
type UnsafeOperationError struct {
	Operation string // The operation: "UPDATE", "DELETE"
}

func (e *UnsafeOperationError) Error() string {
	return fmt.Sprintf("%s requires at least one WHERE condition to prevent accidental full-table operation", e.Operation)
}

// Is implements errors.Is for UnsafeOperationError.
func (e *UnsafeOperationError) Is(target error) bool {
	// Check against specific sentinel errors
	if target == ErrUnsafeUpdate && e.Operation == "UPDATE" {
		return true
	}
	if target == ErrUnsafeDelete && e.Operation == "DELETE" {
		return true
	}
	// Check against other UnsafeOperationError
	t, ok := target.(*UnsafeOperationError)
	if !ok {
		return false
	}
	return t.Operation == "" || e.Operation == t.Operation
}

// ErrUnsafeOperation is returned when any unsafe operation is attempted.
var ErrUnsafeOperation = &UnsafeOperationError{}
