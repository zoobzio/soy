package cereal

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/zoobzio/sentinel"
)

type specTestUser struct {
	ID    int    `db:"id" type:"integer" constraints:"primarykey"`
	Email string `db:"email" type:"text" constraints:"notnull,unique"`
	Name  string `db:"name" type:"text"`
	Age   *int   `db:"age" type:"integer"`
}

func TestQueryFromSpec(t *testing.T) {
	// Register tags
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := sqlx.MustConnect("sqlite3", ":memory:")
	defer db.Close()

	db.MustExec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			email TEXT NOT NULL,
			name TEXT,
			age INTEGER
		)
	`)

	cereal, err := New[specTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("basic query with fields", func(t *testing.T) {
		spec := QuerySpec{
			Fields: []string{"id", "email"},
		}

		q := cereal.QueryFromSpec(spec)
		result := q.MustRender()

		expectedSQL := `SELECT "id", "email" FROM "users"`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("query with where conditions", func(t *testing.T) {
		spec := QuerySpec{
			Where: []ConditionSpec{
				{Field: "age", Operator: ">=", Param: "min_age"},
			},
		}

		q := cereal.QueryFromSpec(spec)
		result := q.MustRender()

		expectedSQL := `SELECT * FROM "users" WHERE "age" >= :min_age`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("query with order by", func(t *testing.T) {
		spec := QuerySpec{
			OrderBy: []OrderBySpec{
				{Field: "name", Direction: "asc"},
			},
		}

		q := cereal.QueryFromSpec(spec)
		result := q.MustRender()

		expectedSQL := `SELECT * FROM "users" ORDER BY "name" ASC`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("query with limit and offset", func(t *testing.T) {
		limit := 10
		offset := 20
		spec := QuerySpec{
			Limit:  &limit,
			Offset: &offset,
		}

		q := cereal.QueryFromSpec(spec)
		result := q.MustRender()

		expectedSQL := `SELECT * FROM "users" LIMIT 10 OFFSET 20`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("query with null conditions", func(t *testing.T) {
		spec := QuerySpec{
			Where: []ConditionSpec{
				{Field: "name", IsNull: true, Operator: "IS NULL"},
			},
		}

		q := cereal.QueryFromSpec(spec)
		result := q.MustRender()

		expectedSQL := `SELECT * FROM "users" WHERE "name" IS NULL`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("complete query from JSON", func(t *testing.T) {
		jsonSpec := `{
			"fields": ["id", "email", "name"],
			"where": [
				{"field": "age", "operator": ">=", "param": "min_age"}
			],
			"order_by": [{"field": "name", "direction": "asc"}],
			"limit": 10,
			"offset": 20
		}`

		var spec QuerySpec
		if err := json.Unmarshal([]byte(jsonSpec), &spec); err != nil {
			t.Fatalf("Failed to unmarshal JSON: %v", err)
		}

		q := cereal.QueryFromSpec(spec)
		result := q.MustRender()

		expectedSQL := `SELECT "id", "email", "name" FROM "users" WHERE "age" >= :min_age ORDER BY "name" ASC LIMIT 10 OFFSET 20`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})
}

func TestSelectFromSpec(t *testing.T) {
	// Register tags
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := sqlx.MustConnect("sqlite3", ":memory:")
	defer db.Close()

	db.MustExec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			email TEXT NOT NULL,
			name TEXT
		)
	`)

	cereal, err := New[specTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("select with where condition", func(t *testing.T) {
		spec := SelectSpec{
			Fields: []string{"id", "email"},
			Where: []ConditionSpec{
				{Field: "email", Operator: "=", Param: "user_email"},
			},
		}

		s := cereal.SelectFromSpec(spec)
		result := s.MustRender()

		expectedSQL := `SELECT "id", "email" FROM "users" WHERE "email" = :user_email`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("select from JSON", func(t *testing.T) {
		jsonSpec := `{
			"fields": ["id", "email"],
			"where": [
				{"field": "email", "operator": "=", "param": "user_email"}
			],
			"order_by": [{"field": "name", "direction": "desc"}],
			"limit": 1
		}`

		var spec SelectSpec
		if err := json.Unmarshal([]byte(jsonSpec), &spec); err != nil {
			t.Fatalf("Failed to unmarshal JSON: %v", err)
		}

		s := cereal.SelectFromSpec(spec)
		result := s.MustRender()

		expectedSQL := `SELECT "id", "email" FROM "users" WHERE "email" = :user_email ORDER BY "name" DESC LIMIT 1`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})
}

func TestModifyFromSpec(t *testing.T) {
	// Register tags
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := sqlx.MustConnect("sqlite3", ":memory:")
	defer db.Close()

	db.MustExec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT,
			email TEXT,
			age INTEGER
		)
	`)

	cereal, err := New[specTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("update with set and where", func(t *testing.T) {
		spec := UpdateSpec{
			Set: map[string]string{
				"name": "new_name",
			},
			Where: []ConditionSpec{
				{Field: "id", Operator: "=", Param: "user_id"},
			},
		}

		u := cereal.ModifyFromSpec(spec)
		result := u.MustRender()

		expectedSQL := `UPDATE "users" SET "name" = :new_name WHERE "id" = :user_id RETURNING "id", "email", "name", "age"`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("update from JSON", func(t *testing.T) {
		jsonSpec := `{
			"set": {
				"name": "new_name"
			},
			"where": [
				{"field": "id", "operator": "=", "param": "user_id"}
			]
		}`

		var spec UpdateSpec
		if err := json.Unmarshal([]byte(jsonSpec), &spec); err != nil {
			t.Fatalf("Failed to unmarshal JSON: %v", err)
		}

		u := cereal.ModifyFromSpec(spec)
		result := u.MustRender()

		expectedSQL := `UPDATE "users" SET "name" = :new_name WHERE "id" = :user_id RETURNING "id", "email", "name", "age"`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})
}

func TestRemoveFromSpec(t *testing.T) {
	// Register tags
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := sqlx.MustConnect("sqlite3", ":memory:")
	defer db.Close()

	db.MustExec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			email TEXT,
			name TEXT
		)
	`)

	cereal, err := New[specTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("delete with where condition", func(t *testing.T) {
		spec := DeleteSpec{
			Where: []ConditionSpec{
				{Field: "id", Operator: "=", Param: "user_id"},
			},
		}

		d := cereal.RemoveFromSpec(spec)
		result := d.MustRender()

		expectedSQL := `DELETE FROM "users" WHERE "id" = :user_id`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("delete from JSON", func(t *testing.T) {
		jsonSpec := `{
			"where": [
				{"field": "id", "operator": "=", "param": "user_id"}
			]
		}`

		var spec DeleteSpec
		if err := json.Unmarshal([]byte(jsonSpec), &spec); err != nil {
			t.Fatalf("Failed to unmarshal JSON: %v", err)
		}

		d := cereal.RemoveFromSpec(spec)
		result := d.MustRender()

		expectedSQL := `DELETE FROM "users" WHERE "id" = :user_id`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})
}

func TestAggregateFromSpec(t *testing.T) {
	// Register tags
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := sqlx.MustConnect("sqlite3", ":memory:")
	defer db.Close()

	db.MustExec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			email TEXT,
			name TEXT,
			age INTEGER
		)
	`)

	cereal, err := New[specTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("count from spec", func(t *testing.T) {
		spec := AggregateSpec{}

		c := cereal.CountFromSpec(spec)
		result := c.MustRender()

		expectedSQL := `SELECT COUNT(*) FROM "users"`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("sum from spec", func(t *testing.T) {
		spec := AggregateSpec{
			Field: "age",
		}

		s := cereal.SumFromSpec(spec)
		result := s.MustRender()

		expectedSQL := `SELECT SUM("age") FROM "users"`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("avg from spec", func(t *testing.T) {
		spec := AggregateSpec{
			Field: "age",
		}

		a := cereal.AvgFromSpec(spec)
		result := a.MustRender()

		expectedSQL := `SELECT AVG("age") FROM "users"`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("min from spec", func(t *testing.T) {
		spec := AggregateSpec{
			Field: "age",
		}

		m := cereal.MinFromSpec(spec)
		result := m.MustRender()

		expectedSQL := `SELECT MIN("age") FROM "users"`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("max from spec", func(t *testing.T) {
		spec := AggregateSpec{
			Field: "age",
		}

		m := cereal.MaxFromSpec(spec)
		result := m.MustRender()

		expectedSQL := `SELECT MAX("age") FROM "users"`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("aggregate from JSON", func(t *testing.T) {
		jsonSpec := `{
			"field": "age"
		}`

		var spec AggregateSpec
		if err := json.Unmarshal([]byte(jsonSpec), &spec); err != nil {
			t.Fatalf("Failed to unmarshal JSON: %v", err)
		}

		s := cereal.SumFromSpec(spec)
		result := s.MustRender()

		expectedSQL := `SELECT SUM("age") FROM "users"`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})
}

func TestEndToEndSpecExecution(t *testing.T) {
	// Register tags
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := sqlx.MustConnect("sqlite3", ":memory:")
	defer db.Close()

	db.MustExec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			email TEXT NOT NULL,
			name TEXT,
			age INTEGER
		)
	`)

	db.MustExec(`
		INSERT INTO users (email, name, age) VALUES
		('alice@example.com', 'Alice', 30),
		('bob@example.com', 'Bob', 25),
		('charlie@example.com', 'Charlie', 35)
	`)

	cereal, err := New[specTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	ctx := context.Background()

	t.Run("query execution from spec", func(t *testing.T) {
		jsonSpec := `{
			"fields": ["email", "name"],
			"order_by": [{"field": "name", "direction": "asc"}]
		}`

		var spec QuerySpec
		if err := json.Unmarshal([]byte(jsonSpec), &spec); err != nil {
			t.Fatalf("Failed to unmarshal JSON: %v", err)
		}

		q := cereal.QueryFromSpec(spec)
		users, err := q.Exec(ctx, map[string]any{})
		if err != nil {
			t.Fatalf("Query execution failed: %v", err)
		}

		if len(users) != 3 {
			t.Errorf("Expected 3 users, got %d", len(users))
		}
		if users[0].Name != "Alice" {
			t.Errorf("Expected first user to be Alice, got %s", users[0].Name)
		}
	})

	t.Run("count execution from spec", func(t *testing.T) {
		spec := AggregateSpec{}

		c := cereal.CountFromSpec(spec)
		count, err := c.Exec(ctx, map[string]any{})
		if err != nil {
			t.Fatalf("Count execution failed: %v", err)
		}

		if count != 3 {
			t.Errorf("Expected count 3, got %v", count)
		}
	})

	t.Run("avg execution from spec", func(t *testing.T) {
		spec := AggregateSpec{
			Field: "age",
		}

		a := cereal.AvgFromSpec(spec)
		avg, err := a.Exec(ctx, map[string]any{})
		if err != nil {
			t.Fatalf("Avg execution failed: %v", err)
		}

		expectedAvg := 30.0
		if avg != expectedAvg {
			t.Errorf("Expected average %.1f, got %.1f", expectedAvg, avg)
		}
	})
}
