package soy

import (
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql/postgres"
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

	db := &sqlx.DB{}

	soy, err := New[compoundTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("basic UNION", func(t *testing.T) {
		q1 := soy.Query().Where("age", ">=", "min_age")
		q2 := soy.Query().Where("name", "=", "target_name")

		compound := q1.Union(q2)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_target_name)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("UNION ALL", func(t *testing.T) {
		q1 := soy.Query().Where("age", ">=", "min_age")
		q2 := soy.Query().Where("name", "=", "target_name")

		compound := q1.UnionAll(q2)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION ALL (SELECT * FROM "users" WHERE "name" = :q1_target_name)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("UNION with fields", func(t *testing.T) {
		q1 := soy.Query().Fields("id", "name").Where("age", ">=", "min_age")
		q2 := soy.Query().Fields("id", "name").Where("name", "=", "target_name")

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

	db := &sqlx.DB{}

	soy, err := New[compoundTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("basic INTERSECT", func(t *testing.T) {
		q1 := soy.Query().Where("age", ">=", "min_age")
		q2 := soy.Query().Where("age", "<=", "max_age")

		compound := q1.Intersect(q2)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) INTERSECT (SELECT * FROM "users" WHERE "age" <= :q1_max_age)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("INTERSECT ALL", func(t *testing.T) {
		q1 := soy.Query().Where("age", ">=", "min_age")
		q2 := soy.Query().Where("age", "<=", "max_age")

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

	db := &sqlx.DB{}

	soy, err := New[compoundTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("basic EXCEPT", func(t *testing.T) {
		q1 := soy.Query().Where("age", ">=", "min_age")
		q2 := soy.Query().Where("name", "=", "excluded_name")

		compound := q1.Except(q2)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) EXCEPT (SELECT * FROM "users" WHERE "name" = :q1_excluded_name)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("EXCEPT ALL", func(t *testing.T) {
		q1 := soy.Query().Where("age", ">=", "min_age")
		q2 := soy.Query().Where("name", "=", "excluded_name")

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

	db := &sqlx.DB{}

	soy, err := New[compoundTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("ORDER BY", func(t *testing.T) {
		q1 := soy.Query().Where("age", ">=", "min_age")
		q2 := soy.Query().Where("name", "=", "target_name")

		compound := q1.Union(q2).OrderBy("name", "asc")
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_target_name) ORDER BY "name" ASC`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("ORDER BY DESC", func(t *testing.T) {
		q1 := soy.Query().Where("age", ">=", "min_age")
		q2 := soy.Query().Where("name", "=", "target_name")

		compound := q1.Union(q2).OrderBy("age", "desc")
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_target_name) ORDER BY "age" DESC`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("LIMIT", func(t *testing.T) {
		q1 := soy.Query().Where("age", ">=", "min_age")
		q2 := soy.Query().Where("name", "=", "target_name")

		compound := q1.Union(q2).Limit(10)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_target_name) LIMIT 10`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("OFFSET", func(t *testing.T) {
		q1 := soy.Query().Where("age", ">=", "min_age")
		q2 := soy.Query().Where("name", "=", "target_name")

		compound := q1.Union(q2).Offset(5)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_target_name) OFFSET 5`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("ORDER BY + LIMIT + OFFSET", func(t *testing.T) {
		q1 := soy.Query().Where("age", ">=", "min_age")
		q2 := soy.Query().Where("name", "=", "target_name")

		compound := q1.Union(q2).OrderBy("name", "asc").Limit(10).Offset(5)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_target_name) ORDER BY "name" ASC LIMIT 10 OFFSET 5`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})
}

func TestCompoundParamModifiers(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}

	soy, err := New[compoundTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("LimitParam", func(t *testing.T) {
		q1 := soy.Query().Where("age", ">=", "min_age")
		q2 := soy.Query().Where("name", "=", "target_name")

		compound := q1.Union(q2).LimitParam("page_size")
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_target_name) LIMIT :page_size`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}

		// Check that page_size is in required params
		found := false
		for _, p := range result.RequiredParams {
			if p == "page_size" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected page_size in required params: %v", result.RequiredParams)
		}
	})

	t.Run("OffsetParam", func(t *testing.T) {
		q1 := soy.Query().Where("age", ">=", "min_age")
		q2 := soy.Query().Where("name", "=", "target_name")

		compound := q1.Union(q2).OffsetParam("page_offset")
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_target_name) OFFSET :page_offset`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}

		// Check that page_offset is in required params
		found := false
		for _, p := range result.RequiredParams {
			if p == "page_offset" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected page_offset in required params: %v", result.RequiredParams)
		}
	})

	t.Run("LimitParam + OffsetParam", func(t *testing.T) {
		q1 := soy.Query().Where("age", ">=", "min_age")
		q2 := soy.Query().Where("name", "=", "target_name")

		compound := q1.Union(q2).LimitParam("limit").OffsetParam("offset")
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_min_age) UNION (SELECT * FROM "users" WHERE "name" = :q1_target_name) LIMIT :limit OFFSET :offset`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})
}

func TestCompoundChaining(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}

	soy, err := New[compoundTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("chained UNION", func(t *testing.T) {
		q1 := soy.Query().Where("age", ">=", "age1")
		q2 := soy.Query().Where("name", "=", "name1")
		q3 := soy.Query().Where("email", "LIKE", "pattern")

		compound := q1.Union(q2).Union(q3)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_age1) UNION (SELECT * FROM "users" WHERE "name" = :q1_name1) UNION (SELECT * FROM "users" WHERE "email" LIKE :q2_pattern)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("mixed operations", func(t *testing.T) {
		q1 := soy.Query().Where("age", ">=", "age1")
		q2 := soy.Query().Where("name", "=", "name1")
		q3 := soy.Query().Where("email", "LIKE", "pattern")

		compound := q1.Union(q2).Except(q3)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "age" >= :q0_age1) UNION (SELECT * FROM "users" WHERE "name" = :q1_name1) EXCEPT (SELECT * FROM "users" WHERE "email" LIKE :q2_pattern)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("four-way chain", func(t *testing.T) {
		q1 := soy.Query().Where("id", "=", "id1")
		q2 := soy.Query().Where("id", "=", "id2")
		q3 := soy.Query().Where("id", "=", "id3")
		q4 := soy.Query().Where("id", "=", "id4")

		compound := q1.Union(q2).Union(q3).Union(q4)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "id" = :q0_id1) UNION (SELECT * FROM "users" WHERE "id" = :q1_id2) UNION (SELECT * FROM "users" WHERE "id" = :q2_id3) UNION (SELECT * FROM "users" WHERE "id" = :q3_id4)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})
}

func TestCompoundErrors(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}

	soy, err := New[compoundTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("invalid direction in OrderBy", func(t *testing.T) {
		q1 := soy.Query().Where("age", ">=", "min_age")
		q2 := soy.Query().Where("name", "=", "target")

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

	db := &sqlx.DB{}

	soy, err := New[compoundTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("params are namespaced", func(t *testing.T) {
		q1 := soy.Query().Where("age", ">=", "min_age")
		q2 := soy.Query().Where("age", "<=", "max_age")

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
		q1 := soy.Query().Where("id", "=", "id")
		q2 := soy.Query().Where("id", "=", "id")
		q3 := soy.Query().Where("id", "=", "id")

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

func TestCompoundChainingAllOperations(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}

	soy, err := New[compoundTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("Compound.UnionAll chaining", func(t *testing.T) {
		q1 := soy.Query().Where("id", "=", "id1")
		q2 := soy.Query().Where("id", "=", "id2")
		q3 := soy.Query().Where("id", "=", "id3")

		// First Union returns Compound, then UnionAll on Compound
		compound := q1.Union(q2).UnionAll(q3)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "id" = :q0_id1) UNION (SELECT * FROM "users" WHERE "id" = :q1_id2) UNION ALL (SELECT * FROM "users" WHERE "id" = :q2_id3)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("Compound.Intersect chaining", func(t *testing.T) {
		q1 := soy.Query().Where("id", "=", "id1")
		q2 := soy.Query().Where("id", "=", "id2")
		q3 := soy.Query().Where("id", "=", "id3")

		compound := q1.Union(q2).Intersect(q3)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "id" = :q0_id1) UNION (SELECT * FROM "users" WHERE "id" = :q1_id2) INTERSECT (SELECT * FROM "users" WHERE "id" = :q2_id3)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("Compound.IntersectAll chaining", func(t *testing.T) {
		q1 := soy.Query().Where("id", "=", "id1")
		q2 := soy.Query().Where("id", "=", "id2")
		q3 := soy.Query().Where("id", "=", "id3")

		compound := q1.Union(q2).IntersectAll(q3)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "id" = :q0_id1) UNION (SELECT * FROM "users" WHERE "id" = :q1_id2) INTERSECT ALL (SELECT * FROM "users" WHERE "id" = :q2_id3)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("Compound.Except chaining", func(t *testing.T) {
		q1 := soy.Query().Where("id", "=", "id1")
		q2 := soy.Query().Where("id", "=", "id2")
		q3 := soy.Query().Where("id", "=", "id3")

		compound := q1.Union(q2).Except(q3)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "id" = :q0_id1) UNION (SELECT * FROM "users" WHERE "id" = :q1_id2) EXCEPT (SELECT * FROM "users" WHERE "id" = :q2_id3)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("Compound.ExceptAll chaining", func(t *testing.T) {
		q1 := soy.Query().Where("id", "=", "id1")
		q2 := soy.Query().Where("id", "=", "id2")
		q3 := soy.Query().Where("id", "=", "id3")

		compound := q1.Union(q2).ExceptAll(q3)
		result := compound.MustRender()

		expectedSQL := `(SELECT * FROM "users" WHERE "id" = :q0_id1) UNION (SELECT * FROM "users" WHERE "id" = :q1_id2) EXCEPT ALL (SELECT * FROM "users" WHERE "id" = :q2_id3)`
		if result.SQL != expectedSQL {
			t.Errorf("Expected SQL %q, got %q", expectedSQL, result.SQL)
		}
	})

	t.Run("Instance returns ASTQL", func(t *testing.T) {
		q1 := soy.Query().Where("id", "=", "id1")
		q2 := soy.Query().Where("id", "=", "id2")

		compound := q1.Union(q2)
		instance := compound.Instance()

		if instance == nil {
			t.Error("Instance() returned nil")
		}
	})
}

func TestCompoundErrorPropagation(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}

	soy, err := New[compoundTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("error propagates through Union", func(t *testing.T) {
		q1 := soy.Query().Where("invalid_field", "=", "id1")
		q2 := soy.Query().Where("id", "=", "id2")

		compound := q1.Union(q2)
		_, err := compound.Render()

		if err == nil {
			t.Error("Expected error for invalid field")
		}
	})

	t.Run("error propagates through UnionAll", func(t *testing.T) {
		q1 := soy.Query().Where("id", "=", "id1")
		q2 := soy.Query().Where("invalid_field", "=", "id2")

		compound := q1.Union(q2).UnionAll(soy.Query().Where("id", "=", "id3"))
		// First union should fail because q2 has invalid field
		// Actually q1.Union(q2) already has error from q2
		_, err := compound.Render()

		if err == nil {
			t.Error("Expected error for invalid field in chained query")
		}
	})

	t.Run("error in compound propagates to chained Intersect", func(t *testing.T) {
		q1 := soy.Query().Where("id", "=", "id1")
		q2 := soy.Query().Where("id", "=", "id2")

		compound := q1.Union(q2).OrderBy("invalid_field", "asc")
		compound2 := compound.Intersect(soy.Query().Where("id", "=", "id3"))
		_, err := compound2.Render()

		if err == nil {
			t.Error("Expected error to propagate")
		}
	})

	t.Run("error in compound propagates to chained IntersectAll", func(t *testing.T) {
		q1 := soy.Query().Where("id", "=", "id1")
		q2 := soy.Query().Where("id", "=", "id2")

		compound := q1.Union(q2).OrderBy("name", "invalid_direction")
		compound2 := compound.IntersectAll(soy.Query().Where("id", "=", "id3"))
		_, err := compound2.Render()

		if err == nil {
			t.Error("Expected error to propagate")
		}
	})

	t.Run("error in compound propagates to chained Except", func(t *testing.T) {
		q1 := soy.Query().Where("id", "=", "id1")
		q2 := soy.Query().Where("id", "=", "id2")

		compound := q1.Union(q2).OrderBy("name", "bad")
		compound2 := compound.Except(soy.Query().Where("id", "=", "id3"))
		_, err := compound2.Render()

		if err == nil {
			t.Error("Expected error to propagate")
		}
	})

	t.Run("error in compound propagates to chained ExceptAll", func(t *testing.T) {
		q1 := soy.Query().Where("id", "=", "id1")
		q2 := soy.Query().Where("id", "=", "id2")

		compound := q1.Union(q2).OrderBy("name", "wrong")
		compound2 := compound.ExceptAll(soy.Query().Where("id", "=", "id3"))
		_, err := compound2.Render()

		if err == nil {
			t.Error("Expected error to propagate")
		}
	})

	t.Run("other query error propagates through UnionAll", func(t *testing.T) {
		q1 := soy.Query().Where("id", "=", "id1")
		q2 := soy.Query().Where("id", "=", "id2")
		qBad := soy.Query().Where("bad_field", "=", "id3")

		compound := q1.Union(q2).UnionAll(qBad)
		_, err := compound.Render()

		if err == nil {
			t.Error("Expected error for invalid field in other query")
		}
	})

	t.Run("other query error propagates through Intersect", func(t *testing.T) {
		q1 := soy.Query().Where("id", "=", "id1")
		q2 := soy.Query().Where("id", "=", "id2")
		qBad := soy.Query().Where("bad_field", "=", "id3")

		compound := q1.Union(q2).Intersect(qBad)
		_, err := compound.Render()

		if err == nil {
			t.Error("Expected error for invalid field in other query")
		}
	})

	t.Run("other query error propagates through IntersectAll", func(t *testing.T) {
		q1 := soy.Query().Where("id", "=", "id1")
		q2 := soy.Query().Where("id", "=", "id2")
		qBad := soy.Query().Where("bad_field", "=", "id3")

		compound := q1.Union(q2).IntersectAll(qBad)
		_, err := compound.Render()

		if err == nil {
			t.Error("Expected error for invalid field in other query")
		}
	})

	t.Run("other query error propagates through Except", func(t *testing.T) {
		q1 := soy.Query().Where("id", "=", "id1")
		q2 := soy.Query().Where("id", "=", "id2")
		qBad := soy.Query().Where("bad_field", "=", "id3")

		compound := q1.Union(q2).Except(qBad)
		_, err := compound.Render()

		if err == nil {
			t.Error("Expected error for invalid field in other query")
		}
	})

	t.Run("other query error propagates through ExceptAll", func(t *testing.T) {
		q1 := soy.Query().Where("id", "=", "id1")
		q2 := soy.Query().Where("id", "=", "id2")
		qBad := soy.Query().Where("bad_field", "=", "id3")

		compound := q1.Union(q2).ExceptAll(qBad)
		_, err := compound.Render()

		if err == nil {
			t.Error("Expected error for invalid field in other query")
		}
	})
}
