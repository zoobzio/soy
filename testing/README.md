# Testing

This directory contains testing utilities and test suites for the soy package.

## Structure

```
testing/
├── helpers.go          # Mock database and test utilities
├── helpers_test.go     # Tests for the helpers themselves
├── benchmarks/         # Performance benchmarks
│   └── builder_bench_test.go
└── integration/        # Integration tests with real databases
    ├── common_test.go  # Shared test setup
    ├── crud_test.go    # Basic CRUD operations
    ├── aggregate_test.go
    ├── compound_test.go
    ├── edge_cases_test.go
    ├── transaction_test.go
    ├── types_test.go
    ├── where_test.go
    └── pgvector_test.go
```

## Running Tests

### Unit Tests

```bash
make test
```

Or run with short mode (skips integration tests):

```bash
go test -short ./...
```

### Integration Tests

Integration tests require Docker and use testcontainers-go to spin up PostgreSQL:

```bash
make test-integration
```

### Benchmarks

```bash
make test-bench
```

Or run directly:

```bash
go test -bench=. -benchmem ./testing/benchmarks/
```

## Test Helpers

The `testing` package provides mock implementations for testing soy-based applications without a real database:

```go
import soytesting "github.com/zoobzio/soy/testing"

func TestMyQuery(t *testing.T) {
    mock := soytesting.NewMockDB(t)
    mock.ExpectQuery().
        WithRows([]MyModel{{ID: 1, Name: "Test"}}).
        Times(1)

    // Use mock in tests...
    mock.AssertExpectations()
}
```

### Available Mocks

- `MockDB` - Configurable mock for database operations
- `MockExtContext` - Implements `sqlx.ExtContext` for query execution
- `MockRows` - Mock implementation of database rows
- `MockTx` - Mock transaction support

## Writing Tests

### Unit Tests

Each `.go` file in the root package should have a corresponding `_test.go` file. Tests should:

1. Use table-driven tests where appropriate
2. Test both success and error cases
3. Verify error messages for user-facing errors
4. Use `t.Helper()` in helper functions

### Integration Tests

Integration tests verify behaviour against real databases. They:

1. Use testcontainers for database provisioning
2. Run in parallel where possible
3. Clean up test data after each test
4. Skip with `-short` flag

## Coverage

Generate coverage reports:

```bash
make coverage
```

This creates `coverage.html` with an interactive coverage report.

Target coverage: 70% overall, 80% for new code.
