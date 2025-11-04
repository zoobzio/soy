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

    // Simple SELECT query
    user, err := c.Select("id", "email", "name").
        Where("email", "=", "user@example.com").
        One(ctx)
    if err != nil {
        panic(err)
    }

    // INSERT query
    newUser := User{Email: "new@example.com", Name: "New User"}
    err = c.Create().
        Values(newUser).
        Exec(ctx)
    if err != nil {
        panic(err)
    }

    // UPDATE query
    err = c.Update().
        Set("name", "Updated Name").
        Where("id", "=", user.ID).
        Exec(ctx)
    if err != nil {
        panic(err)
    }

    // DELETE query
    err = c.Delete().
        Where("id", "=", user.ID).
        Exec(ctx)
    if err != nil {
        panic(err)
    }

    // Aggregate queries
    count, err := c.Count().
        Where("email", "LIKE", "%@example.com").
        Exec(ctx)
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
users, err := c.Select("*").All(ctx)

// Select specific columns
users, err := c.Select("id", "email").
    Where("status", "=", "active").
    OrderBy("created_at", false). // false = DESC
    Limit(10).
    All(ctx)

// Select one record
user, err := c.Select("*").
    Where("id", "=", 123).
    One(ctx)
```

### INSERT Queries

```go
// Insert single record
user := User{Email: "test@example.com", Name: "Test"}
err := c.Create().Values(user).Exec(ctx)

// Insert with RETURNING
var id int64
err := c.Create().
    Values(user).
    Returning("id").
    Scan(ctx, &id)
```

### UPDATE Queries

```go
// Update with WHERE
err := c.Update().
    Set("status", "inactive").
    Set("updated_at", time.Now()).
    Where("id", "=", 123).
    Exec(ctx)
```

### DELETE Queries

```go
// Delete with WHERE
err := c.Delete().
    Where("status", "=", "inactive").
    Exec(ctx)
```

### Aggregate Queries

```go
// Count records
count, err := c.Count().
    Where("status", "=", "active").
    Exec(ctx)

// Sum values
total, err := c.Sum("amount").
    Where("paid", "=", true).
    Exec(ctx)

// Average
avg, err := c.Avg("rating").Exec(ctx)

// Min/Max
min, err := c.Min("price").Exec(ctx)
max, err := c.Max("price").Exec(ctx)
```

## Advanced Usage

### Direct ASTQL Access

For complex queries beyond the simple API:

```go
instance := c.Instance()
query := astql.Select(instance.T("users")).
    Fields(instance.F("id"), instance.F("email")).
    Where(instance.C(instance.F("age"), ">=", instance.P("min_age")))
```

### Custom Query Execution

```go
// Access underlying sqlx.DB
db := c.execer()
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
