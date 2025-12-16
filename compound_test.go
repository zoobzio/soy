package cereal

import (
	"encoding/json"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/zoobzio/sentinel"
)

type compoundTestUser struct {
	ID    int    `db:"id" type:"integer" constraints:"primarykey"`
	Email string `db:"email" type:"text" constraints:"notnull,unique"`
	Name  string `db:"name" type:"text"`
	Age   *int   `db:"age" type:"integer"`
}

func TestCompoundUnion(t *testing.T) {
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

	cereal, err := New[compoundTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("basic UNION", func(t *testing.T) {
		q1 := cereal.Query().Where("age", ">=", "min_age")
		q2 := cereal.Query().Where("name", "=", "target_name")

		compound := q1.Union(q2)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_target_name)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("UNION ALL", func(t *testing.T) {
		q1 := cereal.Query().Where("age", ">=", "min_age")
		q2 := cereal.Query().Where("name", "=", "target_name")

		compound := q1.UnionAll(q2)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION ALL (SELECT * FROM "users" WHERE "name" = :q1_target_name)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("UNION with fields", func(t *testing.T) {
		q1 := cereal.Query().Fields("id", "name").Where("age", ">=", "min_age")
		q2 := cereal.Query().Fields("id", "name").Where("name", "=", "target_name")

		compound := q1.Union(q2)
		result := compound.MustRender()

		expectedSQL := `(SELECT "id", "name" FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT "id", "name" FROM "users" WHERE "name" = :q1_target_name)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})
}

func TestCompoundIntersect(t *testing.T) {
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

	cereal, err := New[compoundTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("basic INTERSECT", func(t *testing.T) {
		q1 := cereal.Query().Where("age", ">=", "min_age")
		q2 := cereal.Query().Where("age", "<=", "max_age")

		compound := q1.Intersect(q2)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) INTERSECT (SELECT * FROM "users" WHERE "age" <= :q1_max_age)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("INTERSECT ALL", func(t *testing.T) {
		q1 := cereal.Query().Where("age", ">=", "min_age")
		q2 := cereal.Query().Where("age", "<=", "max_age")

		compound := q1.IntersectAll(q2)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) INTERSECT ALL (SELECT * FROM "users" WHERE "age" <= :q1_max_age)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})
}

func TestCompoundExcept(t *testing.T) {
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

	cereal, err := New[compoundTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("basic EXCEPT", func(t *testing.T) {
		q1 := cereal.Query().Where("age", ">=", "min_age")
		q2 := cereal.Query().Where("name", "=", "excluded_name")

		compound := q1.Except(q2)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) EXCEPT (SELECT * FROM "users" WHERE "name" = :q1_excluded_name)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("EXCEPT ALL", func(t *testing.T) {
		q1 := cereal.Query().Where("age", ">=", "min_age")
		q2 := cereal.Query().Where("name", "=", "excluded_name")

		compound := q1.ExceptAll(q2)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) EXCEPT ALL (SELECT * FROM "users" WHERE "name" = :q1_excluded_name)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})
}

func TestCompoundModifiers(t *testing.T) {
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

	cereal, err := New[compoundTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("ORDER BY", func(t *testing.T) {
		q1 := cereal.Query().Where("age", ">=", "min_age")
		q2 := cereal.Query().Where("name", "=", "target_name")

		compound := q1.Union(q2).OrderBy("name", "asc")
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_target_name) ORDER BY "name" ASC`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("ORDER BY DESC", func(t *testing.T) {
		q1 := cereal.Query().Where("age", ">=", "min_age")
		q2 := cereal.Query().Where("name", "=", "target_name")

		compound := q1.Union(q2).OrderBy("age", "desc")
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_target_name) ORDER BY "age" DESC`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("LIMIT", func(t *testing.T) {
		q1 := cereal.Query().Where("age", ">=", "min_age")
		q2 := cereal.Query().Where("name", "=", "target_name")

		compound := q1.Union(q2).Limit(10)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_target_name) LIMIT 10`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("OFFSET", func(t *testing.T) {
		q1 := cereal.Query().Where("age", ">=", "min_age")
		q2 := cereal.Query().Where("name", "=", "target_name")

		compound := q1.Union(q2).Offset(5)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_target_name) OFFSET 5`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("ORDER BY + LIMIT + OFFSET", func(t *testing.T) {
		q1 := cereal.Query().Where("age", ">=", "min_age")
		q2 := cereal.Query().Where("name", "=", "target_name")

		compound := q1.Union(q2).OrderBy("name", "asc").Limit(10).Offset(5)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_target_name) ORDER BY "name" ASC LIMIT 10 OFFSET 5`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})
}

func TestCompoundChaining(t *testing.T) {
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

	cereal, err := New[compoundTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("chained UNION", func(t *testing.T) {
		q1 := cereal.Query().Where("age", ">=", "age1")
		q2 := cereal.Query().Where("name", "=", "name1")
		q3 := cereal.Query().Where("email", "LIKE", "pattern")

		compound := q1.Union(q2).Union(q3)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_age1) UNION (SELECT * FROM "users" WHERE "name" = :q1_name1) UNION (SELECT * FROM "users" WHERE "email" LIKE :q2_pattern)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("mixed operations", func(t *testing.T) {
		q1 := cereal.Query().Where("age", ">=", "age1")
		q2 := cereal.Query().Where("name", "=", "name1")
		q3 := cereal.Query().Where("email", "LIKE", "pattern")

		compound := q1.Union(q2).Except(q3)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_age1) UNION (SELECT * FROM "users" WHERE "name" = :q1_name1) EXCEPT (SELECT * FROM "users" WHERE "email" LIKE :q2_pattern)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("four-way chain", func(t *testing.T) {
		q1 := cereal.Query().Where("id", "=", "id1")
		q2 := cereal.Query().Where("id", "=", "id2")
		q3 := cereal.Query().Where("id", "=", "id3")
		q4 := cereal.Query().Where("id", "=", "id4")

		compound := q1.Union(q2).Union(q3).Union(q4)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "id" = :q0_id1) UNION (SELECT * FROM "users" WHERE "id" = :q1_id2) UNION (SELECT * FROM "users" WHERE "id" = :q2_id3) UNION (SELECT * FROM "users" WHERE "id" = :q3_id4)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})
}

func TestCompoundFromSpecUnit(t *testing.T) {
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

	cereal, err := New[compoundTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("basic spec", func(t *testing.T) {
		spec := CompoundQuerySpec{
			Base: QuerySpec{
				Where: []ConditionSpec{{Field: "age", Operator: ">=", Param: "min_age"}},
			},
			Operands: []SetOperandSpec{
				{
					Operation: "union",
					Query:     QuerySpec{Where: []ConditionSpec{{Field: "name", Operator: "=", Param: "target"}}},
				},
			},
		}

		compound := cereal.CompoundFromSpec(spec)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_target)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("spec with all operations", func(t *testing.T) {
		tests := []struct {
			operation string
			expected  string
		}{
			{"union", "UNION"},
			{"union_all", "UNION ALL"},
			{"intersect", "INTERSECT"},
			{"intersect_all", "INTERSECT ALL"},
			{"except", "EXCEPT"},
			{"except_all", "EXCEPT ALL"},
		}

		for _, tc := range tests {
			t.Run(tc.operation, func(t *testing.T) {
				spec := CompoundQuerySpec{
					Base: QuerySpec{
						Where: []ConditionSpec{{Field: "id", Operator: "=", Param: "id1"}},
					},
					Operands: []SetOperandSpec{
						{
							Operation: tc.operation,
							Query:     QuerySpec{Where: []ConditionSpec{{Field: "id", Operator: "=", Param: "id2"}}},
						},
					},
				}

				compound := cereal.CompoundFromSpec(spec)
				result := compound.MustRender()

				expectedSQL := `(SELECT * FROM "users" WHERE "id" = :q0_id1) ` + tc.expected + ` (SELECT * FROM "users" WHERE "id" = :q1_id2)`
				if result.SQL != expectedSQL {
					t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
				}
			})
		}
	})

	t.Run("spec with ORDER BY and LIMIT", func(t *testing.T) {
		limit := 10
		offset := 5
		spec := CompoundQuerySpec{
			Base: QuerySpec{
				Where: []ConditionSpec{{Field: "age", Operator: ">=", Param: "min_age"}},
			},
			Operands: []SetOperandSpec{
				{
					Operation: "union",
					Query:     QuerySpec{Where: []ConditionSpec{{Field: "name", Operator: "=", Param: "target"}}},
				},
			},
			OrderBy: []OrderBySpec{{Field: "name", Direction: "asc"}},
			Limit:   &limit,
			Offset:  &offset,
		}

		compound := cereal.CompoundFromSpec(spec)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_target) ORDER BY "name" ASC LIMIT 10 OFFSET 5`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("spec from JSON", func(t *testing.T) {
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
						"where": [{"field": "name", "operator": "=", "param": "target"}]
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

		expectedSQL := `(SELECT "id", "name" FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT "id", "name" FROM "users" WHERE "name" = :q1_target) ORDER BY "name" ASC LIMIT 10`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("multiple operands", func(t *testing.T) {
		spec := CompoundQuerySpec{
			Base: QuerySpec{
				Where: []ConditionSpec{{Field: "id", Operator: "=", Param: "id1"}},
			},
			Operands: []SetOperandSpec{
				{Operation: "union", Query: QuerySpec{Where: []ConditionSpec{{Field: "id", Operator: "=", Param: "id2"}}}},
				{Operation: "union", Query: QuerySpec{Where: []ConditionSpec{{Field: "id", Operator: "=", Param: "id3"}}}},
				{Operation: "except", Query: QuerySpec{Where: []ConditionSpec{{Field: "id", Operator: "=", Param: "id4"}}}},
			},
		}

		compound := cereal.CompoundFromSpec(spec)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "id" = :q0_id1) UNION (SELECT * FROM "users" WHERE "id" = :q1_id2) UNION (SELECT * FROM "users" WHERE "id" = :q2_id3) EXCEPT (SELECT * FROM "users" WHERE "id" = :q3_id4)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})
}

func TestCompoundErrors(t *testing.T) {
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

	cereal, err := New[compoundTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("empty operands", func(t *testing.T) {
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

	t.Run("invalid operation", func(t *testing.T) {
		spec := CompoundQuerySpec{
			Base: QuerySpec{Where: []ConditionSpec{{Field: "age", Operator: ">=", Param: "min_age"}}},
			Operands: []SetOperandSpec{
				{Operation: "invalid_op", Query: QuerySpec{}},
			},
		}

		compound := cereal.CompoundFromSpec(spec)
		_, err := compound.Render()

		if err == nil {
			t.Error("Expected error for invalid operation")
		}
	})

	t.Run("invalid direction in OrderBy", func(t *testing.T) {
		q1 := cereal.Query().Where("age", ">=", "min_age")
		q2 := cereal.Query().Where("name", "=", "target")

		compound := q1.Union(q2).OrderBy("name", "invalid")
		_, err := compound.Render()

		if err == nil {
			t.Error("Expected error for invalid direction")
		}
	})
}

func TestCompoundParams(t *testing.T) {
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

	cereal, err := New[compoundTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("params are namespaced", func(t *testing.T) {
		q1 := cereal.Query().Where("age", ">=", "min_age")
		q2 := cereal.Query().Where("age", "<=", "max_age")

		compound := q1.Union(q2)
		result := compound.MustRender()

		// Params should be namespaced as q0_ and q1_
		expectedParams := []string{"q0_min_age", "q1_max_age"}
		if len(result.RequiredParams) != len(expectedParams) {
			t.Errorf("Expected %d params, got %d", len(expectedParams), len(result.RequiredParams))
		}
		for i, param := range expectedParams {
			if i < len(result.RequiredParams) && result.RequiredParams[i] != param {
				t.Errorf("Expected param %q at index %d, got %q", param, i, result.RequiredParams[i])
			}
		}
	})

	t.Run("chained params are namespaced correctly", func(t *testing.T) {
		q1 := cereal.Query().Where("id", "=", "id")
		q2 := cereal.Query().Where("id", "=", "id")
		q3 := cereal.Query().Where("id", "=", "id")

		compound := q1.Union(q2).Union(q3)
		result := compound.MustRender()

		// Each query's 'id' param should be namespaced differently
		expectedParams := []string{"q0_id", "q1_id", "q2_id"}
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
