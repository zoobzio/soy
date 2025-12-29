package soy

import (
	"fmt"

	"github.com/zoobzio/astql"
)

// whereBuilder provides shared WHERE clause building logic for query builders.
// This helper eliminates code duplication across Select, Update, Delete, and aggregate builders.
type whereBuilder struct {
	instance *astql.ASTQL
	builder  *astql.Builder
}

// newWhereBuilder creates a new WHERE clause builder helper.
func newWhereBuilder(instance *astql.ASTQL, builder *astql.Builder) *whereBuilder {
	return &whereBuilder{
		instance: instance,
		builder:  builder,
	}
}

// addWhere adds a simple WHERE condition with field operator param pattern.
// Returns the updated builder and any error encountered.
func (w *whereBuilder) addWhere(field, operator, param string) (*astql.Builder, error) {
	astqlOp, err := validateOperator(operator)
	if err != nil {
		return w.builder, err
	}

	f, err := w.instance.TryF(field)
	if err != nil {
		return w.builder, fmt.Errorf("invalid field %q: %w", field, err)
	}

	p, err := w.instance.TryP(param)
	if err != nil {
		return w.builder, fmt.Errorf("invalid param %q: %w", param, err)
	}

	condition, err := w.instance.TryC(f, astqlOp, p)
	if err != nil {
		return w.builder, fmt.Errorf("invalid condition: %w", err)
	}

	return w.builder.Where(condition), nil
}

// addWhereAnd adds multiple conditions combined with AND.
func (w *whereBuilder) addWhereAnd(conditions ...Condition) (*astql.Builder, error) {
	if len(conditions) == 0 {
		return w.builder, nil
	}

	conditionItems := w.instance.ConditionItems()
	for _, cond := range conditions {
		condItem, err := w.buildCondition(cond)
		if err != nil {
			return w.builder, err
		}
		conditionItems = append(conditionItems, condItem)
	}

	andGroup, err := w.instance.TryAnd(conditionItems...)
	if err != nil {
		return w.builder, fmt.Errorf("invalid AND condition: %w", err)
	}

	return w.builder.Where(andGroup), nil
}

// addWhereOr adds multiple conditions combined with OR.
func (w *whereBuilder) addWhereOr(conditions ...Condition) (*astql.Builder, error) {
	if len(conditions) == 0 {
		return w.builder, nil
	}

	conditionItems := w.instance.ConditionItems()
	for _, cond := range conditions {
		condItem, err := w.buildCondition(cond)
		if err != nil {
			return w.builder, err
		}
		conditionItems = append(conditionItems, condItem)
	}

	orGroup, err := w.instance.TryOr(conditionItems...)
	if err != nil {
		return w.builder, fmt.Errorf("invalid OR condition: %w", err)
	}

	return w.builder.Where(orGroup), nil
}

// addWhereNull adds a WHERE field IS NULL condition.
func (w *whereBuilder) addWhereNull(field string) (*astql.Builder, error) {
	f, err := w.instance.TryF(field)
	if err != nil {
		return w.builder, fmt.Errorf("invalid field %q: %w", field, err)
	}

	condition, err := w.instance.TryNull(f)
	if err != nil {
		return w.builder, fmt.Errorf("invalid NULL condition: %w", err)
	}

	return w.builder.Where(condition), nil
}

// addWhereNotNull adds a WHERE field IS NOT NULL condition.
func (w *whereBuilder) addWhereNotNull(field string) (*astql.Builder, error) {
	f, err := w.instance.TryF(field)
	if err != nil {
		return w.builder, fmt.Errorf("invalid field %q: %w", field, err)
	}

	condition, err := w.instance.TryNotNull(f)
	if err != nil {
		return w.builder, fmt.Errorf("invalid NOT NULL condition: %w", err)
	}

	return w.builder.Where(condition), nil
}

// addWhereBetween adds a WHERE field BETWEEN low AND high condition.
func (w *whereBuilder) addWhereBetween(field, lowParam, highParam string) (*astql.Builder, error) {
	f, err := w.instance.TryF(field)
	if err != nil {
		return w.builder, fmt.Errorf("invalid field %q: %w", field, err)
	}

	lowP, err := w.instance.TryP(lowParam)
	if err != nil {
		return w.builder, fmt.Errorf("invalid low param %q: %w", lowParam, err)
	}

	highP, err := w.instance.TryP(highParam)
	if err != nil {
		return w.builder, fmt.Errorf("invalid high param %q: %w", highParam, err)
	}

	return w.builder.Where(astql.Between(f, lowP, highP)), nil
}

// addWhereNotBetween adds a WHERE field NOT BETWEEN low AND high condition.
func (w *whereBuilder) addWhereNotBetween(field, lowParam, highParam string) (*astql.Builder, error) {
	f, err := w.instance.TryF(field)
	if err != nil {
		return w.builder, fmt.Errorf("invalid field %q: %w", field, err)
	}

	lowP, err := w.instance.TryP(lowParam)
	if err != nil {
		return w.builder, fmt.Errorf("invalid low param %q: %w", lowParam, err)
	}

	highP, err := w.instance.TryP(highParam)
	if err != nil {
		return w.builder, fmt.Errorf("invalid high param %q: %w", highParam, err)
	}

	return w.builder.Where(astql.NotBetween(f, lowP, highP)), nil
}

// buildCondition converts a Condition to an ASTQL condition.
func (w *whereBuilder) buildCondition(cond Condition) (astql.ConditionItem, error) {
	return buildConditionWithInstance(w.instance, cond)
}

// buildConditionWithInstance is a shared helper that converts a Condition to an ASTQL condition.
// This is extracted to avoid code duplication across Select, Query, Update, Delete builders.
func buildConditionWithInstance(instance *astql.ASTQL, cond Condition) (astql.ConditionItem, error) {
	f, err := instance.TryF(cond.field)
	if err != nil {
		return nil, fmt.Errorf("invalid field %q: %w", cond.field, err)
	}

	if cond.isNull {
		if cond.operator == opIsNull {
			return instance.TryNull(f)
		}
		return instance.TryNotNull(f)
	}

	if cond.isBetween {
		lowP, err := instance.TryP(cond.lowParam)
		if err != nil {
			return nil, fmt.Errorf("invalid low param %q: %w", cond.lowParam, err)
		}
		highP, err := instance.TryP(cond.highParam)
		if err != nil {
			return nil, fmt.Errorf("invalid high param %q: %w", cond.highParam, err)
		}
		if cond.operator == opBetween {
			return astql.Between(f, lowP, highP), nil
		}
		return astql.NotBetween(f, lowP, highP), nil
	}

	astqlOp, err := validateOperator(cond.operator)
	if err != nil {
		return nil, err
	}

	p, err := instance.TryP(cond.param)
	if err != nil {
		return nil, fmt.Errorf("invalid param %q: %w", cond.param, err)
	}

	return instance.TryC(f, astqlOp, p)
}

// buildCaseWhenCondition builds the condition for a CASE WHEN clause.
// This is extracted to avoid code duplication across SelectCaseBuilder and QueryCaseBuilder.
// The caller is responsible for resolving the result param.
func buildCaseWhenCondition(instance *astql.ASTQL, field, operator, param string) (astql.ConditionItem, error) {
	astqlOp, err := validateOperator(operator)
	if err != nil {
		return nil, err
	}

	f, err := instance.TryF(field)
	if err != nil {
		return nil, fmt.Errorf("invalid field %q: %w", field, err)
	}

	p, err := instance.TryP(param)
	if err != nil {
		return nil, fmt.Errorf("invalid param %q: %w", param, err)
	}

	condition, err := instance.TryC(f, astqlOp, p)
	if err != nil {
		return nil, fmt.Errorf("invalid condition: %w", err)
	}

	return condition, nil
}

// Operator constants to avoid duplication.
const (
	opIsNull     = "IS NULL"
	opIsNotNull  = "IS NOT NULL"
	opBetween    = "BETWEEN"
	opNotBetween = "NOT BETWEEN"
)
