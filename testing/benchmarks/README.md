# Benchmarks

Performance benchmarks for soy query builder operations.

## Running Benchmarks

```bash
make test-bench
```

Or directly:

```bash
go test -bench=. -benchmem -benchtime=100ms ./testing/benchmarks/
```

For more accurate results with longer runs:

```bash
go test -bench=. -benchmem -benchtime=1s -count=5 ./testing/benchmarks/
```

## Available Benchmarks

| Benchmark | Description |
|-----------|-------------|
| `BenchmarkSoyNew` | Instance creation overhead |
| `BenchmarkSelectBuilder` | Simple SELECT query building |
| `BenchmarkSelectBuilderComplex` | SELECT with multiple conditions |
| `BenchmarkQueryBuilder` | Multi-record SELECT with ordering |
| `BenchmarkQueryBuilderComplex` | Full query with GROUP BY, ORDER BY, LIMIT |
| `BenchmarkInsertBuilder` | Basic INSERT |
| `BenchmarkInsertBuilderOnConflict` | INSERT with ON CONFLICT DO UPDATE |
| `BenchmarkModifyBuilder` | UPDATE query building |
| `BenchmarkRemoveBuilder` | DELETE query building |
| `BenchmarkCountBuilder` | COUNT aggregate query |
| `BenchmarkConditionBuilding` | Condition helper functions |

## Interpreting Results

```
BenchmarkSelectBuilder-8    1000000    1050 ns/op    512 B/op    8 allocs/op
```

- `1000000` — iterations run
- `1050 ns/op` — nanoseconds per operation
- `512 B/op` — bytes allocated per operation
- `8 allocs/op` — allocations per operation

## Performance Goals

Query building (hot path):
- Simple queries: < 2000 ns/op
- Complex queries: < 5000 ns/op
- Minimal allocations per query

Instance creation (cold path):
- Acceptable to be slower as this happens once at startup
- Reflection cost amortised across all queries

## Comparing Versions

Save baseline results:

```bash
go test -bench=. -benchmem ./testing/benchmarks/ > old.txt
```

After changes:

```bash
go test -bench=. -benchmem ./testing/benchmarks/ > new.txt
benchstat old.txt new.txt
```

Install benchstat:

```bash
go install golang.org/x/perf/cmd/benchstat@latest
```
