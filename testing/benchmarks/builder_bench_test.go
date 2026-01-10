package benchmarks

import (
	"testing"

	"github.com/zoobzio/astql/postgres"
	"github.com/zoobzio/soy"
)

// User is a test model for benchmarking query building.
type User struct {
	ID        int64  `db:"id" type:"bigserial" constraints:"primary key"`
	Email     string `db:"email" type:"text" constraints:"not null unique"`
	Name      string `db:"name" type:"text"`
	Age       int    `db:"age" type:"integer"`
	Status    string `db:"status" type:"text"`
	CreatedAt string `db:"created_at" type:"timestamptz"`
}

// BenchmarkSelectBuilder measures the performance of building a SELECT query.
func BenchmarkSelectBuilder(b *testing.B) {
	s, err := soy.New[User](nil, "users", postgres.New())
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Select().
			Where("email", "=", "email_param")
	}
}

// BenchmarkSelectBuilderComplex measures building a SELECT with multiple conditions.
func BenchmarkSelectBuilderComplex(b *testing.B) {
	s, err := soy.New[User](nil, "users", postgres.New())
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Select().
			Fields("id", "email", "name").
			Where("status", "=", "status_param").
			WhereAnd(
				soy.C("age", ">=", "min_age"),
				soy.C("age", "<=", "max_age"),
			)
	}
}

// BenchmarkQueryBuilder measures the performance of building a Query (multi-record SELECT).
func BenchmarkQueryBuilder(b *testing.B) {
	s, err := soy.New[User](nil, "users", postgres.New())
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Query().
			Where("status", "=", "status_param").
			OrderBy("created_at", "desc").
			Limit(10)
	}
}

// BenchmarkQueryBuilderComplex measures building a Query with many clauses.
func BenchmarkQueryBuilderComplex(b *testing.B) {
	s, err := soy.New[User](nil, "users", postgres.New())
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Query().
			Fields("id", "email", "name", "age").
			Where("status", "=", "status_param").
			WhereAnd(
				soy.C("age", ">=", "min_age"),
				soy.NotNull("email"),
			).
			GroupBy("status").
			OrderBy("created_at", "desc").
			Limit(100).
			Offset(50)
	}
}

// BenchmarkInsertBuilder measures the performance of building an INSERT query.
func BenchmarkInsertBuilder(b *testing.B) {
	s, err := soy.New[User](nil, "users", postgres.New())
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Insert()
	}
}

// BenchmarkInsertBuilderOnConflict measures building an INSERT with ON CONFLICT.
func BenchmarkInsertBuilderOnConflict(b *testing.B) {
	s, err := soy.New[User](nil, "users", postgres.New())
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Insert().
			OnConflict("email").
			DoUpdate().
			Set("name", "name").
			Set("age", "age").
			Build()
	}
}

// BenchmarkModifyBuilder measures the performance of building an UPDATE query.
func BenchmarkModifyBuilder(b *testing.B) {
	s, err := soy.New[User](nil, "users", postgres.New())
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Modify().
			Set("name", "new_name").
			Where("id", "=", "user_id")
	}
}

// BenchmarkRemoveBuilder measures the performance of building a DELETE query.
func BenchmarkRemoveBuilder(b *testing.B) {
	s, err := soy.New[User](nil, "users", postgres.New())
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Remove().
			Where("id", "=", "user_id")
	}
}

// BenchmarkCountBuilder measures the performance of building a COUNT query.
func BenchmarkCountBuilder(b *testing.B) {
	s, err := soy.New[User](nil, "users", postgres.New())
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Count().
			Where("status", "=", "status_param")
	}
}

// BenchmarkSoyNew measures the overhead of creating a new Soy instance.
func BenchmarkSoyNew(b *testing.B) {
	provider := postgres.New()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = soy.New[User](nil, "users", provider)
	}
}

// BenchmarkConditionBuilding measures the overhead of building conditions.
func BenchmarkConditionBuilding(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = soy.C("email", "=", "email_param")
		_ = soy.C("age", ">=", "min_age")
		_ = soy.Null("deleted_at")
		_ = soy.NotNull("email")
		_ = soy.Between("age", "min_age", "max_age")
	}
}
