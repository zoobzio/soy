package cereal

import (
	"fmt"
	"strings"

	"github.com/zoobzio/astql"
)

// windowState holds the shared state for window function builders.
// This is embedded in SelectWindowBuilder and QueryWindowBuilder.
type windowState struct {
	instance   *astql.ASTQL
	windowExpr *astql.WindowBuilder
	err        error
	alias      string
}

// newWindowState creates a new windowState with the given instance and window expression.
func newWindowState(instance *astql.ASTQL, windowExpr *astql.WindowBuilder) *windowState {
	return &windowState{
		instance:   instance,
		windowExpr: windowExpr,
	}
}

// newWindowStateWithError creates a new windowState with a pre-existing error.
func newWindowStateWithError(instance *astql.ASTQL, err error) *windowState {
	return &windowState{
		instance: instance,
		err:      err,
	}
}

// partitionByImpl adds PARTITION BY fields to the window specification.
func (ws *windowState) partitionByImpl(fields ...string) error {
	if ws.err != nil {
		return ws.err
	}

	astqlFields := ws.instance.Fields()
	for _, field := range fields {
		f, err := ws.instance.TryF(field)
		if err != nil {
			return fmt.Errorf("invalid partition field %q: %w", field, err)
		}
		astqlFields = append(astqlFields, f)
	}

	ws.windowExpr.PartitionBy(astqlFields...)
	return nil
}

// orderByImpl adds ORDER BY to the window specification.
func (ws *windowState) orderByImpl(field, direction string) error {
	if ws.err != nil {
		return ws.err
	}

	astqlDir, err := validateDirection(direction)
	if err != nil {
		return err
	}

	f, err := ws.instance.TryF(field)
	if err != nil {
		return fmt.Errorf("invalid order field %q: %w", field, err)
	}

	ws.windowExpr.OrderBy(f, astqlDir)
	return nil
}

// frameImpl sets the frame clause with ROWS BETWEEN start AND end.
func (ws *windowState) frameImpl(start, end string) error {
	if ws.err != nil {
		return ws.err
	}

	startBound, err := validateFrameBound(start)
	if err != nil {
		return fmt.Errorf("invalid frame start: %w", err)
	}

	endBound, err := validateFrameBound(end)
	if err != nil {
		return fmt.Errorf("invalid frame end: %w", err)
	}

	ws.windowExpr.Frame(startBound, endBound)
	return nil
}

// asImpl sets the alias for the window function result.
func (ws *windowState) asImpl(alias string) {
	if ws.err != nil {
		return
	}
	ws.alias = alias
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

// Window function creation helpers (shared implementations)

// createRowNumberWindow creates a ROW_NUMBER() window expression.
func createRowNumberWindow(instance *astql.ASTQL) *windowState {
	return newWindowState(instance, astql.RowNumber())
}

// createRankWindow creates a RANK() window expression.
func createRankWindow(instance *astql.ASTQL) *windowState {
	return newWindowState(instance, astql.Rank())
}

// createDenseRankWindow creates a DENSE_RANK() window expression.
func createDenseRankWindow(instance *astql.ASTQL) *windowState {
	return newWindowState(instance, astql.DenseRank())
}

// createNtileWindow creates an NTILE(n) window expression.
func createNtileWindow(instance *astql.ASTQL, nParam string) *windowState {
	n, err := instance.TryP(nParam)
	if err != nil {
		return newWindowStateWithError(instance, fmt.Errorf("invalid ntile param %q: %w", nParam, err))
	}
	return newWindowState(instance, astql.Ntile(n))
}

// createLagWindow creates a LAG(field, offset) window expression.
func createLagWindow(instance *astql.ASTQL, field, offsetParam string) *windowState {
	f, err := instance.TryF(field)
	if err != nil {
		return newWindowStateWithError(instance, fmt.Errorf("invalid field %q: %w", field, err))
	}

	offset, err := instance.TryP(offsetParam)
	if err != nil {
		return newWindowStateWithError(instance, fmt.Errorf("invalid offset param %q: %w", offsetParam, err))
	}

	return newWindowState(instance, astql.Lag(f, offset))
}

// createLeadWindow creates a LEAD(field, offset) window expression.
func createLeadWindow(instance *astql.ASTQL, field, offsetParam string) *windowState {
	f, err := instance.TryF(field)
	if err != nil {
		return newWindowStateWithError(instance, fmt.Errorf("invalid field %q: %w", field, err))
	}

	offset, err := instance.TryP(offsetParam)
	if err != nil {
		return newWindowStateWithError(instance, fmt.Errorf("invalid offset param %q: %w", offsetParam, err))
	}

	return newWindowState(instance, astql.Lead(f, offset))
}

// createFirstValueWindow creates a FIRST_VALUE(field) window expression.
func createFirstValueWindow(instance *astql.ASTQL, field string) *windowState {
	f, err := instance.TryF(field)
	if err != nil {
		return newWindowStateWithError(instance, fmt.Errorf("invalid field %q: %w", field, err))
	}
	return newWindowState(instance, astql.FirstValue(f))
}

// createLastValueWindow creates a LAST_VALUE(field) window expression.
func createLastValueWindow(instance *astql.ASTQL, field string) *windowState {
	f, err := instance.TryF(field)
	if err != nil {
		return newWindowStateWithError(instance, fmt.Errorf("invalid field %q: %w", field, err))
	}
	return newWindowState(instance, astql.LastValue(f))
}

// createSumOverWindow creates a SUM(field) OVER window expression.
func createSumOverWindow(instance *astql.ASTQL, field string) *windowState {
	f, err := instance.TryF(field)
	if err != nil {
		return newWindowStateWithError(instance, fmt.Errorf("invalid field %q: %w", field, err))
	}
	return newWindowState(instance, astql.SumOver(f))
}

// createAvgOverWindow creates an AVG(field) OVER window expression.
func createAvgOverWindow(instance *astql.ASTQL, field string) *windowState {
	f, err := instance.TryF(field)
	if err != nil {
		return newWindowStateWithError(instance, fmt.Errorf("invalid field %q: %w", field, err))
	}
	return newWindowState(instance, astql.AvgOver(f))
}

// createCountOverWindow creates a COUNT(*) OVER window expression.
func createCountOverWindow(instance *astql.ASTQL) *windowState {
	return newWindowState(instance, astql.CountOver())
}

// createMinOverWindow creates a MIN(field) OVER window expression.
func createMinOverWindow(instance *astql.ASTQL, field string) *windowState {
	f, err := instance.TryF(field)
	if err != nil {
		return newWindowStateWithError(instance, fmt.Errorf("invalid field %q: %w", field, err))
	}
	return newWindowState(instance, astql.MinOver(f))
}

// createMaxOverWindow creates a MAX(field) OVER window expression.
func createMaxOverWindow(instance *astql.ASTQL, field string) *windowState {
	f, err := instance.TryF(field)
	if err != nil {
		return newWindowStateWithError(instance, fmt.Errorf("invalid field %q: %w", field, err))
	}
	return newWindowState(instance, astql.MaxOver(f))
}

// SelectWindowBuilder is a builder for window functions on Select queries.
// It wraps windowState and provides type-safe method chaining that returns *Select[T].
type SelectWindowBuilder[T any] struct {
	*windowState
	selectBuilder *Select[T]
}

// newSelectWindowBuilder creates a new SelectWindowBuilder from a windowState.
func newSelectWindowBuilder[T any](sb *Select[T], ws *windowState) *SelectWindowBuilder[T] {
	return &SelectWindowBuilder[T]{
		windowState:   ws,
		selectBuilder: sb,
	}
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
	if err := swb.partitionByImpl(fields...); err != nil {
		swb.err = err
	}
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
	if err := swb.orderByImpl(field, direction); err != nil {
		swb.err = err
	}
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
	if err := swb.frameImpl(start, end); err != nil {
		swb.err = err
	}
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
	swb.asImpl(alias)
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

// QueryWindowBuilder is a builder for window functions on Query queries.
// It wraps windowState and provides type-safe method chaining that returns *Query[T].
type QueryWindowBuilder[T any] struct {
	*windowState
	queryBuilder *Query[T]
}

// newQueryWindowBuilder creates a new QueryWindowBuilder from a windowState.
func newQueryWindowBuilder[T any](qb *Query[T], ws *windowState) *QueryWindowBuilder[T] {
	return &QueryWindowBuilder[T]{
		windowState:  ws,
		queryBuilder: qb,
	}
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
	if err := qwb.partitionByImpl(fields...); err != nil {
		qwb.err = err
	}
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
	if err := qwb.orderByImpl(field, direction); err != nil {
		qwb.err = err
	}
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
	if err := qwb.frameImpl(start, end); err != nil {
		qwb.err = err
	}
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
	qwb.asImpl(alias)
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
