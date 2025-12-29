package soy

import (
	"fmt"

	"github.com/zoobzio/astql"
)

// caseState holds the shared state for CASE expression builders.
// This is embedded in SelectCaseBuilder and QueryCaseBuilder.
type caseState struct {
	instance *astql.ASTQL
	caseExpr *astql.CaseBuilder
	err      error
}

// newCaseState creates a new caseState.
func newCaseState(instance *astql.ASTQL) *caseState {
	return &caseState{
		instance: instance,
		caseExpr: astql.Case(),
	}
}

// whenImpl adds a WHEN...THEN clause to the CASE expression.
func (cs *caseState) whenImpl(field, operator, param, resultParam string) error {
	if cs.err != nil {
		return cs.err
	}

	condition, err := buildCaseWhenCondition(cs.instance, field, operator, param)
	if err != nil {
		return err
	}

	result, err := cs.instance.TryP(resultParam)
	if err != nil {
		return fmt.Errorf("invalid result param %q: %w", resultParam, err)
	}

	cs.caseExpr.When(condition, result)
	return nil
}

// whenNullImpl adds a WHEN field IS NULL THEN resultParam clause.
func (cs *caseState) whenNullImpl(field, resultParam string) error {
	if cs.err != nil {
		return cs.err
	}

	f, err := cs.instance.TryF(field)
	if err != nil {
		return fmt.Errorf("invalid field %q: %w", field, err)
	}

	condition, err := cs.instance.TryNull(f)
	if err != nil {
		return fmt.Errorf("invalid NULL condition: %w", err)
	}

	result, err := cs.instance.TryP(resultParam)
	if err != nil {
		return fmt.Errorf("invalid result param %q: %w", resultParam, err)
	}

	cs.caseExpr.When(condition, result)
	return nil
}

// whenNotNullImpl adds a WHEN field IS NOT NULL THEN resultParam clause.
func (cs *caseState) whenNotNullImpl(field, resultParam string) error {
	if cs.err != nil {
		return cs.err
	}

	f, err := cs.instance.TryF(field)
	if err != nil {
		return fmt.Errorf("invalid field %q: %w", field, err)
	}

	condition, err := cs.instance.TryNotNull(f)
	if err != nil {
		return fmt.Errorf("invalid NOT NULL condition: %w", err)
	}

	result, err := cs.instance.TryP(resultParam)
	if err != nil {
		return fmt.Errorf("invalid result param %q: %w", resultParam, err)
	}

	cs.caseExpr.When(condition, result)
	return nil
}

// elseImpl sets the ELSE clause of the CASE expression.
func (cs *caseState) elseImpl(resultParam string) error {
	if cs.err != nil {
		return cs.err
	}

	result, err := cs.instance.TryP(resultParam)
	if err != nil {
		return fmt.Errorf("invalid result param %q: %w", resultParam, err)
	}

	cs.caseExpr.Else(result)
	return nil
}

// asImpl sets the alias for the CASE expression.
func (cs *caseState) asImpl(alias string) {
	if cs.err != nil {
		return
	}
	cs.caseExpr.As(alias)
}

// SelectCaseBuilder is a builder for CASE expressions on Select queries.
// It wraps caseState and provides type-safe method chaining that returns *Select[T].
type SelectCaseBuilder[T any] struct {
	*caseState
	selectBuilder *Select[T]
}

// newSelectCaseBuilder creates a new SelectCaseBuilder.
func newSelectCaseBuilder[T any](sb *Select[T]) *SelectCaseBuilder[T] {
	return &SelectCaseBuilder[T]{
		caseState:     newCaseState(sb.instance),
		selectBuilder: sb,
	}
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
	if err := scb.whenImpl(field, operator, param, resultParam); err != nil {
		scb.err = err
	}
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
	if err := scb.whenNullImpl(field, resultParam); err != nil {
		scb.err = err
	}
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
	if err := scb.whenNotNullImpl(field, resultParam); err != nil {
		scb.err = err
	}
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
	if err := scb.elseImpl(resultParam); err != nil {
		scb.err = err
	}
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
	scb.asImpl(alias)
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

// QueryCaseBuilder is a builder for CASE expressions on Query queries.
// It wraps caseState and provides type-safe method chaining that returns *Query[T].
type QueryCaseBuilder[T any] struct {
	*caseState
	queryBuilder *Query[T]
}

// newQueryCaseBuilder creates a new QueryCaseBuilder.
func newQueryCaseBuilder[T any](qb *Query[T]) *QueryCaseBuilder[T] {
	return &QueryCaseBuilder[T]{
		caseState:    newCaseState(qb.instance),
		queryBuilder: qb,
	}
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
	if err := qcb.whenImpl(field, operator, param, resultParam); err != nil {
		qcb.err = err
	}
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
	if err := qcb.whenNullImpl(field, resultParam); err != nil {
		qcb.err = err
	}
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
	if err := qcb.whenNotNullImpl(field, resultParam); err != nil {
		qcb.err = err
	}
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
	if err := qcb.elseImpl(resultParam); err != nil {
		qcb.err = err
	}
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
	qcb.asImpl(alias)
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
