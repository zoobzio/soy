# cereal

[![CI Status](https://github.com/zoobzio/cereal/workflows/CI/badge.svg)](https://github.com/zoobzio/cereal/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/zoobzio/cereal/graph/badge.svg?branch=main)](https://codecov.io/gh/zoobzio/cereal)
[![Go Report Card](https://goreportcard.com/badge/github.com/zoobzio/cereal)](https://goreportcard.com/report/github.com/zoobzio/cereal)
[![CodeQL](https://github.com/zoobzio/cereal/workflows/CodeQL/badge.svg)](https://github.com/zoobzio/cereal/security/code-scanning)
[![Go Reference](https://pkg.go.dev/badge/github.com/zoobzio/cereal.svg)](https://pkg.go.dev/github.com/zoobzio/cereal)
[![License](https://img.shields.io/github/license/zoobzio/cereal)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/zoobzio/cereal)](go.mod)
[![Release](https://img.shields.io/github/v/release/zoobzio/cereal)](https://github.com/zoobzio/cereal/releases)

Type-safe SQL query builder for Go with schema validation and multi-database support.

## The Problem

ORMs in Go typically use string-based field names and runtime reflection:

```go
db.Where("emial = ?", email).First(&user)  // Typo: "emial" — fails at runtime
db.Model(&user).Update("status", value)    // Is "status" a valid column?
```

Field names are validated at runtime, not compile time. Reflection happens on every query. And when you need to support multiple databases, you're back to string interpolation.

## The Solution

Cereal validates your schema at initialization, not runtime:

```go
import "github.com/zoobzio/astql/pkg/postgres"

type User struct {
    ID    int64  `db:"id" type:"bigserial primary key"`
    Email string `db:"email" type:"text unique not null"`
    Name  string `db:"name" type:"text"`
}

// Schema validated once at startup
users, _ := cereal.New[User](db, "users", postgres.New())

// Type-safe queries — "emial" would fail at initialization, not runtime
user, _ := users.Select().
    Where("email", "=", "user_email").
    Exec(ctx, map[string]any{"user_email": "alice@example.com"})
```

You get:

- **Schema validation** — field names checked against struct tags at initialization
- **Type-safe results** — queries return `*User` or `[]*User`, not `interface{}`
- **Zero reflection on hot path** — all introspection happens once at startup
- **Multi-database support** — same API for PostgreSQL, MySQL, SQLite, SQL Server

## Features

- **Type-safe queries** — full compile-time type checking with Go generics
- **Schema validation** — queries validated against your schema using [ASTQL](https://github.com/zoobzio/astql)
- **Schema from structs** — generate DBML schema from struct tags using [Sentinel](https://github.com/zoobzio/sentinel)
- **Fluent API** — chainable query builders for SELECT, INSERT, UPDATE, DELETE
- **Aggregate functions** — COUNT, SUM, AVG, MIN, MAX with FILTER clauses
- **Window functions** — ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, and more
- **CASE expressions** — fluent API for conditional SQL expressions
- **Compound queries** — UNION, INTERSECT, EXCEPT for combining result sets
- **Multi-database** — PostgreSQL, MySQL, SQLite, SQL Server via ASTQL providers

## Use Cases

- [Implement pagination](docs/4.cookbook/1.pagination.md) — LIMIT/OFFSET and cursor patterns
- [Add vector search](docs/4.cookbook/2.pgvector.md) — pgvector similarity queries

## Install

```bash
go get github.com/zoobzio/cereal
```

Requires Go 1.24+.

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/jmoiron/sqlx"
    _ "github.com/lib/pq"
    "github.com/zoobzio/astql/pkg/postgres"
    "github.com/zoobzio/cereal"
)

type User struct {
    ID    int64  `db:"id" type:"bigserial primary key"`
    Email string `db:"email" type:"text unique not null"`
    Name  string `db:"name" type:"text"`
    Age   int    `db:"age" type:"int"`
}

func main() {
    db, _ := sqlx.Connect("postgres", "postgres://localhost/mydb?sslmode=disable")
    defer db.Close()

    // Create instance — schema validated here
    users, _ := cereal.New[User](db, "users", postgres.New())
    ctx := context.Background()

    // Insert
    created, _ := users.Insert().Exec(ctx, &User{
        Email: "alice@example.com",
        Name:  "Alice",
        Age:   30,
    })
    fmt.Printf("Created: %d\n", created.ID)

    // Select one
    user, _ := users.Select().
        Where("email", "=", "email_param").
        Exec(ctx, map[string]any{"email_param": "alice@example.com"})
    fmt.Printf("Found: %s\n", user.Name)

    // Query many
    all, _ := users.Query().
        Where("age", ">=", "min_age").
        OrderBy("name", "asc").
        Limit(10).
        Exec(ctx, map[string]any{"min_age": 18})
    fmt.Printf("Users: %d\n", len(all))

    // Update
    updated, _ := users.Modify().
        Set("age", "new_age").
        Where("id", "=", "user_id").
        Exec(ctx, map[string]any{"new_age": 31, "user_id": created.ID})
    fmt.Printf("Updated age: %d\n", updated.Age)

    // Aggregate
    count, _ := users.Count().
        Where("age", ">=", "min_age").
        Exec(ctx, map[string]any{"min_age": 18})
    fmt.Printf("Count: %.0f\n", count)
}
```

## API Reference

| Function | Purpose |
|----------|---------|
| `New[T](db, table, renderer)` | Create instance for type T |
| `Select()` | Single-record SELECT (returns `*T`) |
| `Query()` | Multi-record SELECT (returns `[]*T`) |
| `Insert()` | INSERT with RETURNING |
| `Modify()` | UPDATE (requires WHERE) |
| `Remove()` | DELETE (requires WHERE) |
| `Count()`, `Sum(field)`, `Avg(field)`, `Min(field)`, `Max(field)` | Aggregates |
| `C(field, op, param)` | Create condition for WhereAnd/WhereOr |
| `Null(field)`, `NotNull(field)` | NULL conditions |
| `Between(field, low, high)` | BETWEEN condition |

See [API Reference](docs/5.reference/1.api.md) for complete documentation.

## Providers

Use the appropriate provider for your database:

```go
import (
    "github.com/zoobzio/astql/pkg/postgres"
    "github.com/zoobzio/astql/pkg/mysql"
    "github.com/zoobzio/astql/pkg/sqlite"
    "github.com/zoobzio/astql/pkg/mssql"
)

users, _ := cereal.New[User](db, "users", postgres.New())  // PostgreSQL
users, _ := cereal.New[User](db, "users", mysql.New())     // MySQL
users, _ := cereal.New[User](db, "users", sqlite.New())    // SQLite
users, _ := cereal.New[User](db, "users", mssql.New())     // SQL Server
```

Each provider handles dialect differences automatically.

## Documentation

- [Overview](docs/1.overview.md) — what cereal does and why
- **Learn**
  - [Quickstart](docs/2.learn/1.quickstart.md) — get started in minutes
  - [Concepts](docs/2.learn/2.concepts.md) — queries, conditions, builders
- **Guides**
  - [Queries](docs/3.guides/1.queries.md) — SELECT with filtering and ordering
  - [Mutations](docs/3.guides/2.mutations.md) — INSERT, UPDATE, DELETE
  - [Aggregates](docs/3.guides/3.aggregates.md) — COUNT, SUM, AVG, MIN, MAX
  - [Specs](docs/3.guides/4.specs.md) — JSON-serializable query definitions
  - [Compound Queries](docs/3.guides/5.compound.md) — UNION, INTERSECT, EXCEPT
- **Cookbook**
  - [Pagination](docs/4.cookbook/1.pagination.md) — LIMIT/OFFSET patterns
  - [Vector Search](docs/4.cookbook/2.pgvector.md) — pgvector similarity queries
- **Reference**
  - [API](docs/5.reference/1.api.md) — complete function documentation

## Contributing

Contributions welcome! Please ensure:

- Tests pass: `make test`
- Code is formatted: `go fmt ./...`
- No lint errors: `make lint`

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT License — see [LICENSE](LICENSE) for details.
