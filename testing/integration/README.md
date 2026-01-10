# Integration Tests

Integration tests for soy that run against real PostgreSQL databases using testcontainers-go.

## Prerequisites

- Docker (for testcontainers)
- Go 1.24+

## Running Tests

Run all integration tests:

```bash
make test-integration
```

Or directly:

```bash
go test -v -race ./testing/integration/...
```

## Test Structure

| File | Purpose |
|------|---------|
| `common_test.go` | Shared setup, test models, container management |
| `crud_test.go` | Basic Create, Read, Update, Delete operations |
| `aggregate_test.go` | COUNT, SUM, AVG, MIN, MAX with GROUP BY and HAVING |
| `atom_test.go` | Integration with atom package for schema generation |
| `compound_test.go` | UNION, INTERSECT, EXCEPT queries |
| `edge_cases_test.go` | NULL handling, empty results, boundary conditions |
| `transaction_test.go` | Transaction commit, rollback, isolation |
| `types_test.go` | PostgreSQL type mapping and conversion |
| `where_test.go` | Complex WHERE conditions, operators, patterns |
| `pgvector_test.go` | pgvector extension for similarity search |

## Test Models

Tests use these models defined in `common_test.go`:

```go
type User struct {
    ID        int64     `db:"id" type:"bigserial primary key"`
    Email     string    `db:"email" type:"text unique not null"`
    Name      string    `db:"name" type:"text"`
    Age       *int      `db:"age" type:"integer"`
    Status    string    `db:"status" type:"text default 'active'"`
    CreatedAt time.Time `db:"created_at" type:"timestamptz default now()"`
}
```

## Container Lifecycle

Tests share a single PostgreSQL container per test run:

1. Container starts on first test requiring database
2. Each test gets a fresh schema (tables truncated)
3. Container stops after all tests complete

This approach balances isolation with test speed.

## Writing New Integration Tests

```go
func TestFeature(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping integration test in short mode")
    }

    ctx := context.Background()
    db := setupTestDB(t) // Uses shared container

    users, err := soy.New[User](db, "users", postgres.New())
    require.NoError(t, err)

    // Test implementation...
}
```

## Skipping in CI

Integration tests are skipped when:

- Running with `-short` flag
- Docker is unavailable
- `SKIP_INTEGRATION_TESTS=true` environment variable is set

## Debugging

Enable verbose container logs:

```bash
TESTCONTAINERS_RYUK_DISABLED=true go test -v ./testing/integration/...
```

View PostgreSQL logs:

```bash
docker logs $(docker ps -q --filter ancestor=postgres:16)
```
