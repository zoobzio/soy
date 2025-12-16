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

	t.Run("ForLocking modes", func(t *testing.T) {
		testCases := []struct {
			forLocking  string
			expectedSQL string
		}{
			{"update", `SELECT * FROM "users" FOR UPDATE`},
			{"no_key_update", `SELECT * FROM "users" FOR NO KEY UPDATE`},
			{"share", `SELECT * FROM "users" FOR SHARE`},
			{"key_share", `SELECT * FROM "users" FOR KEY SHARE`},
		}

		for _, tc := range testCases {
			t.Run(tc.forLocking, func(t *testing.T) {
				spec := QuerySpec{
					ForLocking: tc.forLocking,
				}

				q := cereal.QueryFromSpec(spec)
				result := q.MustRender()

				if result.SQL != tc.expectedSQL {
					t.Errorf("Expected SQL %q, got %q", tc.expectedSQL, result.SQL)
				}
			})
		}
	})

	t.Run("ForLocking empty string ignored", func(t *testing.T) {
		spec := QuerySpec{
			ForLocking: "",
		}

		q := cereal.QueryFromSpec(spec)
		result := q.MustRender()

		expectedSQL := `SELECT * FROM "users"`
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

	t.Run("ForLocking modes", func(t *testing.T) {
		testCases := []struct {
			forLocking  string
			expectedSQL string
		}{
			{"update", `SELECT * FROM "users" FOR UPDATE`},
			{"no_key_update", `SELECT * FROM "users" FOR NO KEY UPDATE`},
			{"share", `SELECT * FROM "users" FOR SHARE`},
			{"key_share", `SELECT * FROM "users" FOR KEY SHARE`},
		}

		for _, tc := range testCases {
			t.Run(tc.forLocking, func(t *testing.T) {
				spec := SelectSpec{
					ForLocking: tc.forLocking,
				}

				s := cereal.SelectFromSpec(spec)
				result := s.MustRender()

				if result.SQL != tc.expectedSQL {
					t.Errorf("Expected SQL %q, got %q", tc.expectedSQL, result.SQL)
				}
			})
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

func TestConditionGroups(t *testing.T) {
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

	t.Run("query with OR group", func(t *testing.T) {
		spec := QuerySpec{
			Where: []ConditionSpec{
				{
					Logic: "OR",
					Group: []ConditionSpec{
						{Field: "name", Operator: "=", Param: "name1"},
						{Field: "name", Operator: "=", Param: "name2"},
					},
				},
			},
		}

		q := cereal.QueryFromSpec(spec)
		result := q.MustRender()

		expectedSQL := `SELECT * FROM "users" WHERE ("name" = :name1 OR "name" = :name2)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("query with AND group", func(t *testing.T) {
		spec := QuerySpec{
			Where: []ConditionSpec{
				{
					Logic: "AND",
					Group: []ConditionSpec{
						{Field: "age", Operator: ">=", Param: "min_age"},
						{Field: "age", Operator: "<=", Param: "max_age"},
					},
				},
			},
		}

		q := cereal.QueryFromSpec(spec)
		result := q.MustRender()

		expectedSQL := `SELECT * FROM "users" WHERE ("age" >= :min_age AND "age" <= :max_age)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("query with mixed conditions and OR group", func(t *testing.T) {
		spec := QuerySpec{
			Where: []ConditionSpec{
				{Field: "age", Operator: ">=", Param: "min_age"},
				{
					Logic: "OR",
					Group: []ConditionSpec{
						{Field: "name", Operator: "=", Param: "name1"},
						{Field: "name", Operator: "=", Param: "name2"},
					},
				},
			},
		}

		q := cereal.QueryFromSpec(spec)
		result := q.MustRender()

		// Note: astql wraps combined conditions in a group
		expectedSQL := `SELECT * FROM "users" WHERE ("age" >= :min_age AND ("name" = :name1 OR "name" = :name2))`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("OR group from JSON", func(t *testing.T) {
		jsonSpec := `{
			"where": [
				{
					"logic": "OR",
					"group": [
						{"field": "name", "operator": "=", "param": "name1"},
						{"field": "name", "operator": "=", "param": "name2"}
					]
				}
			]
		}`

		var spec QuerySpec
		if err := json.Unmarshal([]byte(jsonSpec), &spec); err != nil {
			t.Fatalf("Failed to unmarshal JSON: %v", err)
		}

		q := cereal.QueryFromSpec(spec)
		result := q.MustRender()

		expectedSQL := `SELECT * FROM "users" WHERE ("name" = :name1 OR "name" = :name2)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("select with OR group", func(t *testing.T) {
		spec := SelectSpec{
			Where: []ConditionSpec{
				{
					Logic: "OR",
					Group: []ConditionSpec{
						{Field: "email", Operator: "=", Param: "email1"},
						{Field: "email", Operator: "=", Param: "email2"},
					},
				},
			},
		}

		s := cereal.SelectFromSpec(spec)
		result := s.MustRender()

		expectedSQL := `SELECT * FROM "users" WHERE ("email" = :email1 OR "email" = :email2)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("update with OR group", func(t *testing.T) {
		spec := UpdateSpec{
			Set: map[string]string{"name": "new_name"},
			Where: []ConditionSpec{
				{
					Logic: "OR",
					Group: []ConditionSpec{
						{Field: "id", Operator: "=", Param: "id1"},
						{Field: "id", Operator: "=", Param: "id2"},
					},
				},
			},
		}

		u := cereal.ModifyFromSpec(spec)
		result := u.MustRender()

		expectedSQL := `UPDATE "users" SET "name" = :new_name WHERE ("id" = :id1 OR "id" = :id2) RETURNING "id", "email", "name", "age"`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("delete with OR group", func(t *testing.T) {
		spec := DeleteSpec{
			Where: []ConditionSpec{
				{
					Logic: "OR",
					Group: []ConditionSpec{
						{Field: "id", Operator: "=", Param: "id1"},
						{Field: "id", Operator: "=", Param: "id2"},
					},
				},
			},
		}

		d := cereal.RemoveFromSpec(spec)
		result := d.MustRender()

		expectedSQL := `DELETE FROM "users" WHERE ("id" = :id1 OR "id" = :id2)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("count with OR group", func(t *testing.T) {
		spec := AggregateSpec{
			Where: []ConditionSpec{
				{
					Logic: "OR",
					Group: []ConditionSpec{
						{Field: "name", Operator: "=", Param: "name1"},
						{Field: "name", Operator: "=", Param: "name2"},
					},
				},
			},
		}

		c := cereal.CountFromSpec(spec)
		result := c.MustRender()

		expectedSQL := `SELECT COUNT(*) FROM "users" WHERE ("name" = :name1 OR "name" = :name2)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})
}

func TestConditionSpecHelpers(t *testing.T) {
	t.Run("IsGroup returns true for groups", func(t *testing.T) {
		spec := ConditionSpec{
			Logic: "OR",
			Group: []ConditionSpec{
				{Field: "a", Operator: "=", Param: "b"},
			},
		}
		if !spec.IsGroup() {
			t.Error("Expected IsGroup() to return true")
		}
	})

	t.Run("IsGroup returns false for simple conditions", func(t *testing.T) {
		spec := ConditionSpec{
			Field:    "a",
			Operator: "=",
			Param:    "b",
		}
		if spec.IsGroup() {
			t.Error("Expected IsGroup() to return false")
		}
	})

	t.Run("ToCondition creates simple condition", func(t *testing.T) {
		spec := ConditionSpec{
			Field:    "age",
			Operator: ">=",
			Param:    "min_age",
		}
		cond := spec.ToCondition()
		if cond.field != "age" || cond.operator != ">=" || cond.param != "min_age" {
			t.Error("ToCondition did not create expected condition")
		}
	})

	t.Run("ToCondition creates null condition", func(t *testing.T) {
		spec := ConditionSpec{
			Field:  "name",
			IsNull: true,
		}
		cond := spec.ToCondition()
		if cond.field != "name" || !cond.isNull {
			t.Error("ToCondition did not create expected null condition")
		}
	})

	t.Run("ToConditions flattens simple conditions", func(t *testing.T) {
		specs := []ConditionSpec{
			{Field: "a", Operator: "=", Param: "b"},
			{Field: "c", Operator: ">", Param: "d"},
		}
		conditions := ToConditions(specs)
		if len(conditions) != 2 {
			t.Errorf("Expected 2 conditions, got %d", len(conditions))
		}
	})

	t.Run("ToConditions skips groups", func(t *testing.T) {
		specs := []ConditionSpec{
			{Field: "a", Operator: "=", Param: "b"},
			{Logic: "OR", Group: []ConditionSpec{{Field: "x", Operator: "=", Param: "y"}}},
		}
		conditions := ToConditions(specs)
		if len(conditions) != 1 {
			t.Errorf("Expected 1 condition (groups skipped), got %d", len(conditions))
		}
	})
}

func TestOrderBySpecHelpers(t *testing.T) {
	t.Run("IsExpression returns true for expression ordering", func(t *testing.T) {
		spec := OrderBySpec{
			Field:     "embedding",
			Operator:  "<->",
			Param:     "query_vec",
			Direction: "asc",
		}
		if !spec.IsExpression() {
			t.Error("Expected IsExpression() to return true")
		}
	})

	t.Run("IsExpression returns false for simple ordering", func(t *testing.T) {
		spec := OrderBySpec{
			Field:     "name",
			Direction: "asc",
		}
		if spec.IsExpression() {
			t.Error("Expected IsExpression() to return false")
		}
	})

	t.Run("IsExpression returns false when only operator set", func(t *testing.T) {
		spec := OrderBySpec{
			Field:     "name",
			Operator:  "<->",
			Direction: "asc",
		}
		if spec.IsExpression() {
			t.Error("Expected IsExpression() to return false when param missing")
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

func TestGroupByHaving(t *testing.T) {
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

	t.Run("query with GROUP BY fluent API", func(t *testing.T) {
		q := cereal.Query().
			Fields("name").
			GroupBy("name")

		result := q.MustRender()

		expectedSQL := `SELECT "name" FROM "users" GROUP BY "name"`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("query with GROUP BY and HAVING fluent API", func(t *testing.T) {
		q := cereal.Query().
			Fields("name").
			GroupBy("name").
			Having("age", ">", "min_age")

		result := q.MustRender()

		expectedSQL := `SELECT "name" FROM "users" GROUP BY "name" HAVING "age" > :min_age`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("select with GROUP BY and HAVING fluent API", func(t *testing.T) {
		s := cereal.Select().
			Fields("name").
			GroupBy("name").
			Having("age", ">=", "min_age")

		result := s.MustRender()

		expectedSQL := `SELECT "name" FROM "users" GROUP BY "name" HAVING "age" >= :min_age`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("query from spec with GROUP BY", func(t *testing.T) {
		spec := QuerySpec{
			Fields:  []string{"name"},
			GroupBy: []string{"name"},
		}

		q := cereal.QueryFromSpec(spec)
		result := q.MustRender()

		expectedSQL := `SELECT "name" FROM "users" GROUP BY "name"`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("query from spec with GROUP BY and HAVING", func(t *testing.T) {
		spec := QuerySpec{
			Fields:  []string{"name"},
			GroupBy: []string{"name"},
			Having: []ConditionSpec{
				{Field: "age", Operator: ">", Param: "min_age"},
			},
		}

		q := cereal.QueryFromSpec(spec)
		result := q.MustRender()

		expectedSQL := `SELECT "name" FROM "users" GROUP BY "name" HAVING "age" > :min_age`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("select from spec with GROUP BY and HAVING", func(t *testing.T) {
		spec := SelectSpec{
			Fields:  []string{"name"},
			GroupBy: []string{"name"},
			Having: []ConditionSpec{
				{Field: "age", Operator: ">=", Param: "min_age"},
			},
		}

		s := cereal.SelectFromSpec(spec)
		result := s.MustRender()

		expectedSQL := `SELECT "name" FROM "users" GROUP BY "name" HAVING "age" >= :min_age`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("GROUP BY and HAVING from JSON", func(t *testing.T) {
		jsonSpec := `{
			"fields": ["name"],
			"group_by": ["name"],
			"having": [
				{"field": "age", "operator": ">", "param": "min_age"}
			]
		}`

		var spec QuerySpec
		if err := json.Unmarshal([]byte(jsonSpec), &spec); err != nil {
			t.Fatalf("Failed to unmarshal JSON: %v", err)
		}

		q := cereal.QueryFromSpec(spec)
		result := q.MustRender()

		expectedSQL := `SELECT "name" FROM "users" GROUP BY "name" HAVING "age" > :min_age`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("multiple GROUP BY fields", func(t *testing.T) {
		spec := QuerySpec{
			Fields:  []string{"name", "age"},
			GroupBy: []string{"name", "age"},
		}

		q := cereal.QueryFromSpec(spec)
		result := q.MustRender()

		expectedSQL := `SELECT "name", "age" FROM "users" GROUP BY "name", "age"`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("multiple HAVING conditions", func(t *testing.T) {
		q := cereal.Query().
			Fields("name").
			GroupBy("name").
			Having("age", ">", "min_age").
			Having("age", "<", "max_age")

		result := q.MustRender()

		expectedSQL := `SELECT "name" FROM "users" GROUP BY "name" HAVING "age" > :min_age AND "age" < :max_age`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})
}

func TestHavingAgg(t *testing.T) {
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

	t.Run("HavingAgg COUNT(*) fluent API", func(t *testing.T) {
		q := cereal.Query().
			Fields("name").
			GroupBy("name").
			HavingAgg("count", "", ">", "min_count")

		result := q.MustRender()

		expectedSQL := `SELECT "name" FROM "users" GROUP BY "name" HAVING COUNT(*) > :min_count`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("HavingAgg SUM fluent API", func(t *testing.T) {
		q := cereal.Query().
			Fields("name").
			GroupBy("name").
			HavingAgg("sum", "age", ">=", "min_total")

		result := q.MustRender()

		expectedSQL := `SELECT "name" FROM "users" GROUP BY "name" HAVING SUM("age") >= :min_total`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("HavingAgg AVG fluent API", func(t *testing.T) {
		q := cereal.Query().
			Fields("name").
			GroupBy("name").
			HavingAgg("avg", "age", "<", "max_avg")

		result := q.MustRender()

		expectedSQL := `SELECT "name" FROM "users" GROUP BY "name" HAVING AVG("age") < :max_avg`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("HavingAgg from spec", func(t *testing.T) {
		spec := QuerySpec{
			Fields:  []string{"name"},
			GroupBy: []string{"name"},
			HavingAgg: []HavingAggSpec{
				{Func: "count", Operator: ">", Param: "min_count"},
			},
		}

		q := cereal.QueryFromSpec(spec)
		result := q.MustRender()

		expectedSQL := `SELECT "name" FROM "users" GROUP BY "name" HAVING COUNT(*) > :min_count`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("HavingAgg SUM from spec", func(t *testing.T) {
		spec := QuerySpec{
			Fields:  []string{"name"},
			GroupBy: []string{"name"},
			HavingAgg: []HavingAggSpec{
				{Func: "sum", Field: "age", Operator: ">=", Param: "min_total"},
			},
		}

		q := cereal.QueryFromSpec(spec)
		result := q.MustRender()

		expectedSQL := `SELECT "name" FROM "users" GROUP BY "name" HAVING SUM("age") >= :min_total`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("HavingAgg from JSON", func(t *testing.T) {
		jsonSpec := `{
			"fields": ["name"],
			"group_by": ["name"],
			"having_agg": [
				{"func": "count", "operator": ">", "param": "min_count"}
			]
		}`

		var spec QuerySpec
		if err := json.Unmarshal([]byte(jsonSpec), &spec); err != nil {
			t.Fatalf("Failed to unmarshal JSON: %v", err)
		}

		q := cereal.QueryFromSpec(spec)
		result := q.MustRender()

		expectedSQL := `SELECT "name" FROM "users" GROUP BY "name" HAVING COUNT(*) > :min_count`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("mixed Having and HavingAgg", func(t *testing.T) {
		spec := QuerySpec{
			Fields:  []string{"name"},
			GroupBy: []string{"name"},
			Having: []ConditionSpec{
				{Field: "age", Operator: ">", Param: "min_age"},
			},
			HavingAgg: []HavingAggSpec{
				{Func: "count", Operator: ">", Param: "min_count"},
			},
		}

		q := cereal.QueryFromSpec(spec)
		result := q.MustRender()

		expectedSQL := `SELECT "name" FROM "users" GROUP BY "name" HAVING "age" > :min_age AND COUNT(*) > :min_count`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("Select HavingAgg from spec", func(t *testing.T) {
		spec := SelectSpec{
			Fields:  []string{"name"},
			GroupBy: []string{"name"},
			HavingAgg: []HavingAggSpec{
				{Func: "sum", Field: "age", Operator: ">=", Param: "min_total"},
			},
		}

		s := cereal.SelectFromSpec(spec)
		result := s.MustRender()

		expectedSQL := `SELECT "name" FROM "users" GROUP BY "name" HAVING SUM("age") >= :min_total`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})
}

func TestCompoundQueries(t *testing.T) {
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

	t.Run("UNION fluent API", func(t *testing.T) {
		q1 := cereal.Query().Where("age", ">=", "min_age")
		q2 := cereal.Query().Where("name", "=", "target_name")

		compound := q1.Union(q2)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_target_name)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("UNION ALL fluent API", func(t *testing.T) {
		q1 := cereal.Query().Where("age", ">=", "min_age")
		q2 := cereal.Query().Where("name", "=", "target_name")

		compound := q1.UnionAll(q2)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION ALL (SELECT * FROM "users" WHERE "name" = :q1_target_name)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("INTERSECT fluent API", func(t *testing.T) {
		q1 := cereal.Query().Where("age", ">=", "min_age")
		q2 := cereal.Query().Where("name", "LIKE", "pattern")

		compound := q1.Intersect(q2)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) INTERSECT (SELECT * FROM "users" WHERE "name" LIKE :q1_pattern)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("EXCEPT fluent API", func(t *testing.T) {
		q1 := cereal.Query().Where("age", ">=", "min_age")
		q2 := cereal.Query().Where("name", "=", "excluded_name")

		compound := q1.Except(q2)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) EXCEPT (SELECT * FROM "users" WHERE "name" = :q1_excluded_name)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("compound with ORDER BY", func(t *testing.T) {
		q1 := cereal.Query().Where("age", ">=", "min_age")
		q2 := cereal.Query().Where("name", "=", "target_name")

		compound := q1.Union(q2).OrderBy("name", "asc")
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_target_name) ORDER BY "name" ASC`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("compound with LIMIT and OFFSET", func(t *testing.T) {
		q1 := cereal.Query().Where("age", ">=", "min_age")
		q2 := cereal.Query().Where("name", "=", "target_name")

		compound := q1.Union(q2).Limit(10).Offset(5)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_target_name) LIMIT 10 OFFSET 5`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("chained set operations", func(t *testing.T) {
		q1 := cereal.Query().Where("age", ">=", "min_age")
		q2 := cereal.Query().Where("name", "=", "name1")
		q3 := cereal.Query().Where("name", "=", "name2")

		compound := q1.Union(q2).Union(q3)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_name1) UNION (SELECT * FROM "users" WHERE "name" = :q2_name2)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("mixed set operations", func(t *testing.T) {
		q1 := cereal.Query().Where("age", ">=", "min_age")
		q2 := cereal.Query().Where("name", "=", "name1")
		q3 := cereal.Query().Where("email", "LIKE", "pattern")

		compound := q1.Union(q2).Except(q3)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_name1) EXCEPT (SELECT * FROM "users" WHERE "email" LIKE :q2_pattern)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})
}

func TestCompoundFromSpec(t *testing.T) {
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

	t.Run("simple UNION from spec", func(t *testing.T) {
		spec := CompoundQuerySpec{
			Base: QuerySpec{
				Where: []ConditionSpec{{Field: "age", Operator: ">=", Param: "min_age"}},
			},
			Operands: []SetOperandSpec{
				{
					Operation: "union",
					Query:     QuerySpec{Where: []ConditionSpec{{Field: "name", Operator: "=", Param: "target_name"}}},
				},
			},
		}

		compound := cereal.CompoundFromSpec(spec)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_target_name)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("UNION ALL from spec", func(t *testing.T) {
		spec := CompoundQuerySpec{
			Base: QuerySpec{
				Where: []ConditionSpec{{Field: "age", Operator: ">=", Param: "min_age"}},
			},
			Operands: []SetOperandSpec{
				{
					Operation: "union_all",
					Query:     QuerySpec{Where: []ConditionSpec{{Field: "name", Operator: "=", Param: "target_name"}}},
				},
			},
		}

		compound := cereal.CompoundFromSpec(spec)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION ALL (SELECT * FROM "users" WHERE "name" = :q1_target_name)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("compound from spec with ORDER BY and LIMIT", func(t *testing.T) {
		limit := 10
		spec := CompoundQuerySpec{
			Base: QuerySpec{
				Where: []ConditionSpec{{Field: "age", Operator: ">=", Param: "min_age"}},
			},
			Operands: []SetOperandSpec{
				{
					Operation: "union",
					Query:     QuerySpec{Where: []ConditionSpec{{Field: "name", Operator: "=", Param: "target_name"}}},
				},
			},
			OrderBy: []OrderBySpec{{Field: "name", Direction: "asc"}},
			Limit:   &limit,
		}

		compound := cereal.CompoundFromSpec(spec)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_target_name) ORDER BY "name" ASC LIMIT 10`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("multiple operands from spec", func(t *testing.T) {
		spec := CompoundQuerySpec{
			Base: QuerySpec{
				Where: []ConditionSpec{{Field: "age", Operator: ">=", Param: "min_age"}},
			},
			Operands: []SetOperandSpec{
				{
					Operation: "union",
					Query:     QuerySpec{Where: []ConditionSpec{{Field: "name", Operator: "=", Param: "name1"}}},
				},
				{
					Operation: "except",
					Query:     QuerySpec{Where: []ConditionSpec{{Field: "email", Operator: "LIKE", Param: "pattern"}}},
				},
			},
		}

		compound := cereal.CompoundFromSpec(spec)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_name1) EXCEPT (SELECT * FROM "users" WHERE "email" LIKE :q2_pattern)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("compound from JSON", func(t *testing.T) {
		jsonSpec := `{
			"base": {
				"fields": ["id", "name"],
				"where": [{"field": "age", "operator": ">=", "param": "min_age"}]
			},
			"operands": [
				{
					"operation": "union",
					"query": {
						"fields": ["id", "name"],
						"where": [{"field": "name", "operator": "=", "param": "target_name"}]
					}
				}
			],
			"order_by": [{"field": "name", "direction": "asc"}],
			"limit": 10
		}`

		var spec CompoundQuerySpec
		if err := json.Unmarshal([]byte(jsonSpec), &spec); err != nil {
			t.Fatalf("Failed to unmarshal JSON: %v", err)
		}

		compound := cereal.CompoundFromSpec(spec)
		result := compound.MustRender()

		expectedSQL := `(SELECT "id", "name" FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT "id", "name" FROM "users" WHERE "name" = :q1_target_name) ORDER BY "name" ASC LIMIT 10`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("error on empty operands", func(t *testing.T) {
		spec := CompoundQuerySpec{
			Base:     QuerySpec{Where: []ConditionSpec{{Field: "age", Operator: ">=", Param: "min_age"}}},
			Operands: []SetOperandSpec{},
		}

		compound := cereal.CompoundFromSpec(spec)
		_, err := compound.Render()

		if err == nil {
			t.Error("Expected error for empty operands")
		}
	})

	t.Run("error on invalid operation", func(t *testing.T) {
		spec := CompoundQuerySpec{
			Base: QuerySpec{Where: []ConditionSpec{{Field: "age", Operator: ">=", Param: "min_age"}}},
			Operands: []SetOperandSpec{
				{
					Operation: "invalid_op",
					Query:     QuerySpec{},
				},
			},
		}

		compound := cereal.CompoundFromSpec(spec)
		_, err := compound.Render()

		if err == nil {
			t.Error("Expected error for invalid operation")
		}
	})
}

func TestCompoundQueryExecution(t *testing.T) {
	// NOTE: Compound query execution tests are skipped because SQLite doesn't support
	// the parenthesized compound query syntax that PostgreSQL uses.
	// The rendered SQL is valid PostgreSQL syntax.
	// These tests would pass against a real PostgreSQL database.

	t.Run("render and params verification", func(t *testing.T) {
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

		cereal, err := New[specTestUser](db, "users")
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		q1 := cereal.Query().Where("age", ">=", "min_age")
		q2 := cereal.Query().Where("name", "=", "target_name")

		compound := q1.Union(q2).OrderBy("name", "asc")
		result := compound.MustRender()

		// Verify SQL is correct PostgreSQL syntax
		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_target_name) ORDER BY "name" ASC`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}

		// Verify required params are namespaced correctly
		expectedParams := []string{"q0_min_age", "q1_target_name"}
		if len(result.RequiredParams) != len(expectedParams) {
			t.Errorf("Expected %d params, got %d", len(expectedParams), len(result.RequiredParams))
		}
		for i, param := range expectedParams {
			if i < len(result.RequiredParams) && result.RequiredParams[i] != param {
				t.Errorf("Expected param %q at index %d, got %q", param, i, result.RequiredParams[i])
			}
		}
	})
}

func TestInsertFromSpec(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := sqlx.MustConnect("sqlite3", ":memory:")
	defer db.Close()

	db.MustExec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			email TEXT NOT NULL UNIQUE,
			name TEXT,
			age INTEGER
		)
	`)

	cereal, err := New[specTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("basic insert (empty spec)", func(t *testing.T) {
		spec := CreateSpec{}

		ins := cereal.InsertFromSpec(spec)
		result := ins.MustRender()

		if result.SQL == "" {
			t.Error("Expected non-empty SQL")
		}
		if !contains(result.SQL, "INSERT INTO") {
			t.Errorf("Expected INSERT INTO in SQL: %s", result.SQL)
		}
	})

	t.Run("insert with ON CONFLICT DO NOTHING", func(t *testing.T) {
		spec := CreateSpec{
			OnConflict:     []string{"email"},
			ConflictAction: "nothing",
		}

		ins := cereal.InsertFromSpec(spec)
		result := ins.MustRender()

		if !contains(result.SQL, "ON CONFLICT") {
			t.Errorf("Expected ON CONFLICT in SQL: %s", result.SQL)
		}
		if !contains(result.SQL, "DO NOTHING") {
			t.Errorf("Expected DO NOTHING in SQL: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("insert with ON CONFLICT DO UPDATE", func(t *testing.T) {
		spec := CreateSpec{
			OnConflict:     []string{"email"},
			ConflictAction: "update",
			ConflictSet: map[string]string{
				"name": "updated_name",
			},
		}

		ins := cereal.InsertFromSpec(spec)
		result := ins.MustRender()

		if !contains(result.SQL, "ON CONFLICT") {
			t.Errorf("Expected ON CONFLICT in SQL: %s", result.SQL)
		}
		if !contains(result.SQL, "DO UPDATE") {
			t.Errorf("Expected DO UPDATE in SQL: %s", result.SQL)
		}
		if !contains(result.SQL, "SET") {
			t.Errorf("Expected SET in SQL: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("insert from JSON spec", func(t *testing.T) {
		jsonSpec := `{
			"on_conflict": ["email"],
			"conflict_action": "nothing"
		}`

		var spec CreateSpec
		if err := json.Unmarshal([]byte(jsonSpec), &spec); err != nil {
			t.Fatalf("Failed to unmarshal JSON: %v", err)
		}

		ins := cereal.InsertFromSpec(spec)
		result := ins.MustRender()

		if !contains(result.SQL, "ON CONFLICT") {
			t.Errorf("Expected ON CONFLICT in SQL: %s", result.SQL)
		}
		if !contains(result.SQL, "DO NOTHING") {
			t.Errorf("Expected DO NOTHING in SQL: %s", result.SQL)
		}
	})

	t.Run("insert with conflict columns defaults to DO NOTHING", func(t *testing.T) {
		spec := CreateSpec{
			OnConflict: []string{"email"},
			// No ConflictAction specified
		}

		ins := cereal.InsertFromSpec(spec)
		result := ins.MustRender()

		if !contains(result.SQL, "DO NOTHING") {
			t.Errorf("Expected DO NOTHING as default: %s", result.SQL)
		}
	})

	t.Run("insert with case insensitive conflict action", func(t *testing.T) {
		specs := []CreateSpec{
			{OnConflict: []string{"email"}, ConflictAction: "NOTHING"},
			{OnConflict: []string{"email"}, ConflictAction: "Nothing"},
			{OnConflict: []string{"email"}, ConflictAction: "UPDATE"},
			{OnConflict: []string{"email"}, ConflictAction: "Update"},
		}

		for _, spec := range specs {
			ins := cereal.InsertFromSpec(spec)
			result := ins.MustRender()

			if !contains(result.SQL, "ON CONFLICT") {
				t.Errorf("Expected ON CONFLICT for action %q: %s", spec.ConflictAction, result.SQL)
			}
		}
	})
}
