# Feature Request: SelectExpr for Binary Expressions

## Summary

Add `SelectExpr()` method to include binary expressions (field \<op\> param) in the SELECT clause with an alias.

## Motivation

pgvector queries need to return distance scores alongside results:

```sql
SELECT *, embedding <=> :query_vec AS score
FROM documents
ORDER BY embedding <=> :query_vec ASC
LIMIT 10
```

Currently soy supports `OrderByExpr()` for the ORDER BY clause but has no equivalent for SELECT. Users cannot retrieve computed distance values.

## Proposed API

```go
func (sb *Select[T]) SelectExpr(field, operator, param, alias string) *Select[T]
```

## Usage

```go
docs.Query().
    SelectExpr("embedding", "<=>", "query_vec", "score").
    OrderByExpr("embedding", "<=>", "query_vec", "asc").
    Limit(10).
    Exec(ctx, map[string]any{"query_vec": vector})
```

Generates:

```sql
SELECT *, "embedding" <=> :query_vec AS "score"
FROM "documents"
ORDER BY "embedding" <=> :query_vec ASC
LIMIT 10
```

## Implementation

### select.go

```go
// SelectExpr adds a binary expression (field <op> param) AS alias to the SELECT clause.
// Useful for vector distance calculations with pgvector or similar operations.
func (sb *Select[T]) SelectExpr(field, operator, param, alias string) *Select[T] {
    if sb.err != nil {
        return sb
    }
    sb.builder, sb.err = selectExprImpl(sb.instance, sb.builder, field, operator, param, alias)
    return sb
}
```

### builder.go

```go
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
    return builder.SelectExpr(astql.As(astql.BinaryExpr(f, astqlOp, p), alias)), nil
}
```

## Existing Infrastructure

All required components exist:

- `OrderByExpr()` follows identical pattern
- `astql.BinaryExpr()` creates field-operator-param expressions
- `astql.As()` handles aliasing
- `validateOperator()` supports vector operators (`<->`, `<=>`, `<#>`, `<+>`)
- Error handling via `newFieldError()`, `newParamError()`

## Scope

- ~12 lines in select.go
- ~20 lines in builder.go
- ~40 lines in tests

## Use Case

Enables grub's pgvector provider to use `Database[T]` with typed columnar storage instead of JSONB metadata blobs. See `grub/FEATURE_REQUEST_PGVECTOR_COLUMNAR.md`.
