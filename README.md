# cereal

[![CI Status](https://github.com/zoobzio/cereal/workflows/CI/badge.svg)](https://github.com/zoobzio/cereal/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/zoobzio/cereal/graph/badge.svg?branch=main)](https://codecov.io/gh/zoobzio/cereal)
[![Go Report Card](https://goreportcard.com/badge/github.com/zoobzio/cereal)](https://goreportcard.com/report/github.com/zoobzio/cereal)
[![CodeQL](https://github.com/zoobzio/cereal/workflows/CodeQL/badge.svg)](https://github.com/zoobzio/cereal/security/code-scanning)
[![Go Reference](https://pkg.go.dev/badge/github.com/zoobzio/cereal.svg)](https://pkg.go.dev/github.com/zoobzio/cereal)
[![License](https://img.shields.io/github/license/zoobzio/cereal)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/zoobzio/cereal)](go.mod)
[![Release](https://img.shields.io/github/v/release/zoobzio/cereal)](https://github.com/zoobzio/cereal/releases)

Type-safe, query-validated ORM for Go with compile-time schema validation.

Build database queries with full type safety and SQL validation at initialization time, not runtime.

## Features

- **Type-safe queries**: Full compile-time type checking with Go generics
- **SQL validation**: Queries are validated against your schema at initialization using [ASTQL](https://github.com/zoobzio/astql)
- **Schema from structs**: Generate DBML schema from struct tags using [Sentinel](https://github.com/zoobzio/sentinel)
- **Fluent API**: Simple, chainable query builders for SELECT, INSERT, UPDATE, DELETE
- **Aggregate functions**: Built-in support for COUNT, SUM, AVG, MIN, MAX
- **Zero reflection on hot path**: All reflection happens once at initialization
- **Built on sqlx**: Leverages the battle-tested [sqlx](https://github.com/jmoiron/sqlx) library
- **PostgreSQL support**: Native support for PostgreSQL via pgx driver

## Installation

```bash
go get github.com/zoobzio/cereal
```

Requirements: Go 1.23.2+

## Quick Start

```go
package main

import (
    "context"
    "github.com/jmoiron/sqlx"
    _ "github.com/jackc/pgx/v5/stdlib"
    "github.com/zoobzio/cereal"
)

type User struct {
    ID    int64  `db:"id" type:"bigserial primary key"`
    Email string `db:"email" type:"text unique not null"`
    Name  string `db:"name" type:"text"`
}

func main() {
    // Connect to database
    db, err := sqlx.Connect("pgx", "postgres://localhost/mydb?sslmode=disable")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Create Cereal instance (validates schema at initialization)
    c, err := cereal.New[User](db, "users")
    if err != nil {
        panic(err)
    }

    ctx := context.Background()

    // Simple SELECT query - define query structure with parameter names
    selectQuery := c.Select("id", "email", "name").
        Where("email", "=", "user_email")

    // Execute with actual values
    user, err := selectQuery.Exec(ctx, map[string]any{
        "user_email": "user@example.com",
    })
    if err != nil {
        panic(err)
    }

    // INSERT query
    newUser := User{Email: "new@example.com", Name: "New User"}
    createQuery := c.Create().Values(newUser)

    err = createQuery.Exec(ctx)
    if err != nil {
        panic(err)
    }

    // UPDATE query - define structure with parameter placeholders
    updateQuery := c.Update().
        Set("name", "new_name").
        Where("id", "=", "user_id")

    // Execute with actual values
    updated, err := updateQuery.Exec(ctx, map[string]any{
        "new_name": "Updated Name",
        "user_id":  user.ID,
    })
    if err != nil {
        panic(err)
    }

    // DELETE query - define structure
    deleteQuery := c.Delete().
        Where("id", "=", "user_id")

    // Execute with actual values
    err = deleteQuery.Exec(ctx, map[string]any{
        "user_id": updated.ID,
    })
    if err != nil {
        panic(err)
    }

    // Aggregate queries - define structure
    countQuery := c.Count().
        Where("email", "LIKE", "email_pattern")

    // Execute with actual values
    count, err := countQuery.Exec(ctx, map[string]any{
        "email_pattern": "%@example.com",
    })
    if err != nil {
        panic(err)
    }
}
```

## Core Concepts

### Cereal Instance

The `Cereal[T]` type is the main entry point. It holds:
- Database connection (sqlx)
- Table name
- Type metadata (via Sentinel)
- ASTQL schema for validation

All schema inspection and validation happens once at initialization via `cereal.New[T]()`.

### Query Builders

Cereal provides simple query builders that hide ASTQL complexity:

- **Select**: Build SELECT queries that return `[]T` or single `T`
- **Create**: Build INSERT queries from struct values
- **Update**: Build UPDATE queries with SET and WHERE clauses
- **Delete**: Build DELETE queries with WHERE clauses
- **Count, Sum, Avg, Min, Max**: Aggregate function builders

### Schema from Structs

Define your schema using struct tags:

```go
type Product struct {
    ID          int64     `db:"id" type:"bigserial primary key"`
    Name        string    `db:"name" type:"text not null"`
    Price       float64   `db:"price" type:"numeric(10,2) not null"`
    Stock       int       `db:"stock" type:"integer default 0"`
    CategoryID  int64     `db:"category_id" type:"bigint references categories(id)"`
    CreatedAt   time.Time `db:"created_at" type:"timestamp default now()"`
}
```

Tags:
- `db`: Column name
- `type`: PostgreSQL column type with constraints
- `constraints`: Additional constraints
- `default`: Default value
- `references`: Foreign key reference
- `index`: Index definition
- `check`: Check constraint

### DBML Generation

You can generate DBML schema from your structs:

```go
project, err := cereal.GenerateDBML(metadata, "products")
```

This allows you to:
- Document your schema
- Generate database migrations
- Visualize table relationships

## Query Examples

### SELECT Queries

```go
// Select all columns
query := c.Select("*")
users, err := query.All(ctx, nil)

// Select specific columns with filtering
query := c.Select("id", "email").
    Where("status", "=", "status_value").
    OrderBy("created_at", "desc").
    Limit(10)

users, err := query.All(ctx, map[string]any{
    "status_value": "active",
})

// Select one record
query := c.Select("*").
    Where("id", "=", "user_id")

user, err := query.Exec(ctx, map[string]any{
    "user_id": 123,
})

// Complex conditions with AND
query := c.Select("id", "email", "created_at").
    WhereAnd(
        cereal.C("age", ">=", "min_age"),
        cereal.C("age", "<=", "max_age"),
        cereal.C("status", "=", "status"),
    ).
    OrderBy("created_at", "desc")

users, err := query.All(ctx, map[string]any{
    "min_age": 18,
    "max_age": 65,
    "status":  "active",
})

// Complex conditions with OR
query := c.Select("*").
    WhereOr(
        cereal.C("status", "=", "active"),
        cereal.C("status", "=", "pending"),
    )

users, err := query.All(ctx, nil)
```

### INSERT Queries

```go
// Insert single record
user := User{Email: "test@example.com", Name: "Test"}
query := c.Create().Values(user)

err := query.Exec(ctx)

// Insert with RETURNING
user := User{Email: "test@example.com", Name: "Test"}
query := c.Create().
    Values(user).
    Returning("id")

var id int64
err := query.Scan(ctx, &id)

// Insert multiple records
users := []User{
    {Email: "user1@example.com", Name: "User 1"},
    {Email: "user2@example.com", Name: "User 2"},
    {Email: "user3@example.com", Name: "User 3"},
}

for _, user := range users {
    query := c.Create().Values(user)
    err := query.Exec(ctx)
    if err != nil {
        // handle error
    }
}
```

### UPDATE Queries

```go
// Update single field
query := c.Update().
    Set("status", "new_status").
    Where("id", "=", "user_id")

updated, err := query.Exec(ctx, map[string]any{
    "new_status": "inactive",
    "user_id":    123,
})

// Update multiple fields
query := c.Update().
    Set("status", "new_status").
    Set("updated_at", "now").
    Set("name", "new_name").
    Where("id", "=", "user_id")

updated, err := query.Exec(ctx, map[string]any{
    "new_status": "active",
    "now":        time.Now(),
    "new_name":   "Updated Name",
    "user_id":    123,
})

// Update with complex WHERE conditions
query := c.Update().
    Set("status", "archived").
    WhereAnd(
        cereal.C("last_login", "<", "cutoff_date"),
        cereal.C("status", "=", "inactive"),
    )

updated, err := query.Exec(ctx, map[string]any{
    "cutoff_date": time.Now().AddDate(0, -6, 0),
})

// Batch update
query := c.Update().
    Set("status", "new_status").
    Where("id", "=", "user_id")

batchParams := []map[string]any{
    {"new_status": "active", "user_id": 1},
    {"new_status": "inactive", "user_id": 2},
    {"new_status": "pending", "user_id": 3},
}

rowsAffected, err := query.ExecBatch(ctx, batchParams)
```

### DELETE Queries

```go
// Delete single record
query := c.Delete().
    Where("id", "=", "user_id")

err := query.Exec(ctx, map[string]any{
    "user_id": 123,
})

// Delete with complex conditions
query := c.Delete().
    Where("status", "=", "status_value")

err := query.Exec(ctx, map[string]any{
    "status_value": "inactive",
})

// Delete with AND conditions
query := c.Delete().
    WhereAnd(
        cereal.C("status", "=", "deleted"),
        cereal.C("updated_at", "<", "cutoff_date"),
    )

err := query.Exec(ctx, map[string]any{
    "cutoff_date": time.Now().AddDate(-1, 0, 0),
})

// Batch delete
query := c.Delete().
    Where("id", "=", "user_id")

batchParams := []map[string]any{
    {"user_id": 1},
    {"user_id": 2},
    {"user_id": 3},
}

rowsAffected, err := query.ExecBatch(ctx, batchParams)
```

### Aggregate Queries

```go
// Count all records
query := c.Count()
count, err := query.Exec(ctx, nil)

// Count with WHERE
query := c.Count().
    Where("status", "=", "status_value")

count, err := query.Exec(ctx, map[string]any{
    "status_value": "active",
})

// Sum values
query := c.Sum("amount").
    Where("paid", "=", "paid_status")

total, err := query.Exec(ctx, map[string]any{
    "paid_status": true,
})

// Average
query := c.Avg("rating")
avg, err := query.Exec(ctx, nil)

// Average with conditions
query := c.Avg("rating").
    Where("created_at", ">", "start_date")

avg, err := query.Exec(ctx, map[string]any{
    "start_date": time.Now().AddDate(0, -1, 0),
})

// Min/Max
minQuery := c.Min("price")
min, err := minQuery.Exec(ctx, nil)

maxQuery := c.Max("price").
    Where("status", "=", "available")

max, err := maxQuery.Exec(ctx, nil)
```

## Advanced Usage

### Direct ASTQL Access

For complex queries beyond the simple API:

```go
instance := c.Instance()
query := astql.Select(instance.T("users")).
    Fields(instance.F("id"), instance.F("email")).
    Where(instance.C(instance.F("age"), ">=", instance.P("min_age")))

// Execute with parameters
result, err := query.Render()
rows, err := sqlx.NamedQueryContext(ctx, db, result.SQL, map[string]any{"min_age": 18})
```

### Custom Query Execution

```go
// Access underlying sqlx.DB
db := c.execer()
```

## Observability

Cereal emits structured events via [Capitan](https://github.com/zoobzio/capitan) for logging and monitoring.

### Signals

**QueryStarted** (`db.query.started`)
- Emitted when a database query begins execution
- Fields: `table`, `operation`, `sql`

**QueryCompleted** (`db.query.completed`)
- Emitted when a query completes successfully
- Fields: `table`, `operation`, `duration_ms`, `rows_affected` or `rows_returned`, `result_value` (for aggregates)

**QueryFailed** (`db.query.failed`)
- Emitted when a query fails with an error
- Fields: `table`, `operation`, `duration_ms`, `error`

### Event Fields

- `table` (string): Database table being operated on
- `operation` (string): Type of operation (SELECT, INSERT, UPDATE, DELETE, COUNT, SUM, AVG, MIN, MAX)
- `sql` (string): Rendered SQL query string
- `duration_ms` (int64): Query execution duration in milliseconds
- `rows_affected` (int64): Number of rows affected (INSERT/UPDATE/DELETE)
- `rows_returned` (int): Number of rows returned (SELECT)
- `error` (string): Error message when query fails
- `field` (string): Field being aggregated (SUM/AVG/MIN/MAX)
- `result_value` (float64): Result value for aggregates

### Example Integration

```go
import (
    "github.com/zoobzio/capitan"
    "github.com/zoobzio/cereal"
)

// Subscribe to query events
capitan.Subscribe(cereal.QueryCompleted, func(signal capitan.Signal, fields ...capitan.Field) {
    // Extract fields
    var table, operation string
    var duration int64

    for _, field := range fields {
        switch field.Key() {
        case "table":
            table = field.String()
        case "operation":
            operation = field.String()
        case "duration_ms":
            duration = field.Int64()
        }
    }

    log.Printf("Query %s on %s completed in %dms", operation, table, duration)
})
```

## Architecture

Cereal is built on three core libraries:

1. **[Sentinel](https://github.com/zoobzio/sentinel)**: Type inspection and metadata extraction
2. **[ASTQL](https://github.com/zoobzio/astql)**: SQL validation and query building
3. **[sqlx](https://github.com/jmoiron/sqlx)**: Database operations and scanning

The stack:
```
┌──────────────────────┐
│   Cereal (this)      │  Simple query API
├──────────────────────┤
│   ASTQL              │  SQL validation
├──────────────────────┤
│   Sentinel           │  Type metadata
├──────────────────────┤
│   sqlx               │  Database ops
└──────────────────────┘
```

## Performance

- **Zero reflection on hot path**: All type inspection happens once at initialization
- **Prepared statements**: sqlx handles statement preparation and caching
- **Efficient scanning**: Direct struct scanning via sqlx
- **Minimal allocations**: Query builders reuse buffers where possible

## Contributing

Contributions welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

```bash
# Run tests
make test

# Run linter
make lint

# Generate coverage
make coverage
```

## License

MIT License - see [LICENSE](LICENSE) file for details.
