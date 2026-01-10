# soy

[![CI Status](https://github.com/zoobzio/soy/workflows/CI/badge.svg)](https://github.com/zoobzio/soy/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/zoobzio/soy/graph/badge.svg?branch=main)](https://codecov.io/gh/zoobzio/soy)
[![Go Report Card](https://goreportcard.com/badge/github.com/zoobzio/soy)](https://goreportcard.com/report/github.com/zoobzio/soy)
[![CodeQL](https://github.com/zoobzio/soy/workflows/CodeQL/badge.svg)](https://github.com/zoobzio/soy/security/code-scanning)
[![Go Reference](https://pkg.go.dev/badge/github.com/zoobzio/soy.svg)](https://pkg.go.dev/github.com/zoobzio/soy)
[![License](https://img.shields.io/github/license/zoobzio/soy)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/zoobzio/soy)](go.mod)
[![Release](https://img.shields.io/github/v/release/zoobzio/soy)](https://github.com/zoobzio/soy/releases)

Type-safe SQL query builder for Go with schema validation and multi-database support.

Extract schema from struct tags, validate queries at initialization, execute with zero reflection.

## Schema Once, Query Forever

```go
type User struct {
    ID    int64  `db:"id" type:"bigserial primary key"`
    Email string `db:"email" type:"text unique not null"`
    Name  string `db:"name" type:"text"`
}

// Schema extracted and validated here — once
users, _ := soy.New[User](db, "users", postgres.New())

// Every query after: type-safe, zero reflection, validated fields
user, _ := users.Select().
    Where("email", "=", "email_param").
    Exec(ctx, map[string]any{"email_param": "alice@example.com"})
// Returns *User, not interface{}
```

Field names validated against struct tags. Type-safe results. No reflection on the hot path.

## Install

```bash
go get github.com/zoobzio/soy
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
    "github.com/zoobzio/soy"
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
    users, _ := soy.New[User](db, "users", postgres.New())
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

## Capabilities

| Feature           | Description                                                                           | Docs                                        |
| ----------------- | ------------------------------------------------------------------------------------- | ------------------------------------------- |
| Type-Safe Queries | Generics return `*T` or `[]*T`, not `interface{}`                                     | [Queries](docs/3.guides/1.queries.md)       |
| Schema Validation | Field names checked against struct tags at init                                       | [Concepts](docs/2.learn/2.concepts.md)      |
| Multi-Database    | PostgreSQL, MariaDB, SQLite, SQL Server via [ASTQL](https://github.com/zoobzio/astql) | [Quickstart](docs/2.learn/1.quickstart.md)  |
| Fluent Builders   | Chainable API for SELECT, INSERT, UPDATE, DELETE                                      | [Mutations](docs/3.guides/2.mutations.md)   |
| Aggregates        | COUNT, SUM, AVG, MIN, MAX with FILTER clauses                                         | [Aggregates](docs/3.guides/3.aggregates.md) |
| Window Functions  | ROW_NUMBER, RANK, LAG, LEAD, and more                                                 | [API](docs/5.reference/1.api.md)            |
| Compound Queries  | UNION, INTERSECT, EXCEPT                                                              | [Compound](docs/3.guides/5.compound.md)     |
| Safety Guards     | DELETE/UPDATE require WHERE; prevents accidents                                       | [Concepts](docs/2.learn/2.concepts.md)      |

## Why soy?

- **Zero reflection on hot path** — all introspection happens once at `New()`
- **Type-safe results** — queries return `*T` or `[]*T`, never `interface{}`
- **Schema validation at init** — field name typos caught immediately, not at runtime
- **Multi-database parity** — same API across PostgreSQL, MariaDB, SQLite, SQL Server
- **Safety by default** — DELETE/UPDATE require WHERE; prevents accidental full-table operations
- **Minimal dependencies** — sqlx plus purpose-built libraries (astql, sentinel, atom)

## Type-Safe Database Layer

Soy enables a pattern: **define types once, query safely everywhere**.

Your struct definitions become the contract. [Sentinel](https://github.com/zoobzio/sentinel) extracts metadata from struct tags. [ASTQL](https://github.com/zoobzio/astql) validates queries against that schema. Soy wraps it all in a fluent API.

```go
// Your domain type — the single source of truth
type Order struct {
    ID        int64     `db:"id" type:"bigserial primary key"`
    UserID    int64     `db:"user_id" type:"bigint not null" references:"users(id)"`
    Total     float64   `db:"total" type:"numeric(10,2) not null"`
    Status    string    `db:"status" type:"text" check:"status IN ('pending','paid','shipped')"`
    CreatedAt time.Time `db:"created_at" type:"timestamptz default now()"`
}

// Soy validates against the schema
orders, _ := soy.New[Order](db, "orders", postgres.New())

// Invalid field? Caught at init, not runtime
orders.Select().Where("totla", "=", "x")  // Error: field "totla" not found
```

Three packages, one type definition, complete safety from struct tags to SQL execution.

## Documentation

- [Overview](docs/1.overview.md) — what soy does and why
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

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT License — see [LICENSE](LICENSE) for details.
