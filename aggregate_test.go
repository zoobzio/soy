package cereal

import (
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/sentinel"
)

// Test model for aggregate tests.
type aggregateTestUser struct {
	ID    int    `db:"id" type:"integer" constraints:"primarykey"`
	Email string `db:"email" type:"text" constraints:"notnull,unique"`
	Name  string `db:"name" type:"text"`
	Age   *int   `db:"age" type:"integer"`
}

func TestMin_Basic(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")

	db := &sqlx.DB{}
	cereal, err := New[aggregateTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("MIN with field", func(t *testing.T) {
		result, err := cereal.Min("age").Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "SELECT") {
			t.Errorf("SQL missing SELECT: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "MIN") {
			t.Errorf("SQL missing MIN: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"age"`) {
			t.Errorf("SQL missing age field: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"users"`) {
			t.Errorf("SQL missing table name: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("MIN with WHERE", func(t *testing.T) {
		result, err := cereal.Min("age").
			Where("age", ">=", "min_age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "MIN") {
			t.Errorf("SQL missing MIN: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "WHERE") {
			t.Errorf("SQL missing WHERE: %s", result.SQL)
		}

		if len(result.RequiredParams) != 1 {
			t.Errorf("Expected 1 required param, got %d", len(result.RequiredParams))
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("MIN with WhereAnd", func(t *testing.T) {
		result, err := cereal.Min("age").
			WhereAnd(
				C("age", ">=", "min_age"),
				C("age", "<=", "max_age"),
			).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "MIN") {
			t.Errorf("SQL missing MIN: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "AND") {
			t.Errorf("SQL missing AND: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("MIN with WhereOr", func(t *testing.T) {
		result, err := cereal.Min("age").
			WhereOr(
				C("age", "<", "young_age"),
				C("age", ">", "old_age"),
			).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "MIN") {
			t.Errorf("SQL missing MIN: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "OR") {
			t.Errorf("SQL missing OR: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("MIN with WhereNotNull", func(t *testing.T) {
		result, err := cereal.Min("age").
			WhereNotNull("age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "IS NOT NULL") {
			t.Errorf("SQL missing IS NOT NULL: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})
}

func TestMax_Basic(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")

	db := &sqlx.DB{}
	cereal, err := New[aggregateTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("MAX with field", func(t *testing.T) {
		result, err := cereal.Max("age").Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "SELECT") {
			t.Errorf("SQL missing SELECT: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "MAX") {
			t.Errorf("SQL missing MAX: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"age"`) {
			t.Errorf("SQL missing age field: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"users"`) {
			t.Errorf("SQL missing table name: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("MAX with WHERE", func(t *testing.T) {
		result, err := cereal.Max("age").
			Where("age", ">=", "min_age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "MAX") {
			t.Errorf("SQL missing MAX: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "WHERE") {
			t.Errorf("SQL missing WHERE: %s", result.SQL)
		}

		if len(result.RequiredParams) != 1 {
			t.Errorf("Expected 1 required param, got %d", len(result.RequiredParams))
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("MAX with WhereAnd", func(t *testing.T) {
		result, err := cereal.Max("age").
			WhereAnd(
				C("age", ">=", "min_age"),
				C("age", "<=", "max_age"),
			).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "MAX") {
			t.Errorf("SQL missing MAX: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "AND") {
			t.Errorf("SQL missing AND: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("MAX with WhereOr", func(t *testing.T) {
		result, err := cereal.Max("age").
			WhereOr(
				C("age", "<", "young_age"),
				C("age", ">", "old_age"),
			).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "MAX") {
			t.Errorf("SQL missing MAX: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "OR") {
			t.Errorf("SQL missing OR: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("MAX with WhereNotNull", func(t *testing.T) {
		result, err := cereal.Max("age").
			WhereNotNull("age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "IS NOT NULL") {
			t.Errorf("SQL missing IS NOT NULL: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})
}

func TestSum_Basic(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")

	db := &sqlx.DB{}
	cereal, err := New[aggregateTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("SUM with field", func(t *testing.T) {
		result, err := cereal.Sum("age").Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "SELECT") {
			t.Errorf("SQL missing SELECT: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "SUM") {
			t.Errorf("SQL missing SUM: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"age"`) {
			t.Errorf("SQL missing age field: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"users"`) {
			t.Errorf("SQL missing table name: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("SUM with WHERE", func(t *testing.T) {
		result, err := cereal.Sum("age").
			Where("age", ">=", "min_age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "SUM") {
			t.Errorf("SQL missing SUM: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "WHERE") {
			t.Errorf("SQL missing WHERE: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"age"`) {
			t.Errorf("SQL missing age field: %s", result.SQL)
		}

		if len(result.RequiredParams) != 1 {
			t.Errorf("Expected 1 required param, got %d", len(result.RequiredParams))
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("SUM with WhereAnd", func(t *testing.T) {
		result, err := cereal.Sum("age").
			WhereAnd(
				C("age", ">=", "min_age"),
				C("age", "<=", "max_age"),
			).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "SUM") {
			t.Errorf("SQL missing SUM: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "AND") {
			t.Errorf("SQL missing AND: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("SUM with WhereOr", func(t *testing.T) {
		result, err := cereal.Sum("age").
			WhereOr(
				C("age", "<", "young_age"),
				C("age", ">", "old_age"),
			).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "SUM") {
			t.Errorf("SQL missing SUM: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "OR") {
			t.Errorf("SQL missing OR: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("SUM with WhereNotNull", func(t *testing.T) {
		result, err := cereal.Sum("age").
			WhereNotNull("age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "IS NOT NULL") {
			t.Errorf("SQL missing IS NOT NULL: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})
}

func TestAvg_Basic(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")

	db := &sqlx.DB{}
	cereal, err := New[aggregateTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("AVG with field", func(t *testing.T) {
		result, err := cereal.Avg("age").Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "SELECT") {
			t.Errorf("SQL missing SELECT: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "AVG") {
			t.Errorf("SQL missing AVG: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"age"`) {
			t.Errorf("SQL missing age field: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"users"`) {
			t.Errorf("SQL missing table name: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("AVG with WHERE", func(t *testing.T) {
		result, err := cereal.Avg("age").
			Where("age", ">=", "min_age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "AVG") {
			t.Errorf("SQL missing AVG: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "WHERE") {
			t.Errorf("SQL missing WHERE: %s", result.SQL)
		}

		if len(result.RequiredParams) != 1 {
			t.Errorf("Expected 1 required param, got %d", len(result.RequiredParams))
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("AVG with WhereAnd", func(t *testing.T) {
		result, err := cereal.Avg("age").
			WhereAnd(
				C("age", ">=", "min_age"),
				C("age", "<=", "max_age"),
			).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "AVG") {
			t.Errorf("SQL missing AVG: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "AND") {
			t.Errorf("SQL missing AND: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("AVG with WhereOr", func(t *testing.T) {
		result, err := cereal.Avg("age").
			WhereOr(
				C("age", "<", "young_age"),
				C("age", ">", "old_age"),
			).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "AVG") {
			t.Errorf("SQL missing AVG: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "OR") {
			t.Errorf("SQL missing OR: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("AVG with WhereNotNull", func(t *testing.T) {
		result, err := cereal.Avg("age").
			WhereNotNull("age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "IS NOT NULL") {
			t.Errorf("SQL missing IS NOT NULL: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})
}

func TestCount_Basic(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")

	db := &sqlx.DB{}
	cereal, err := New[aggregateTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("COUNT all records", func(t *testing.T) {
		result, err := cereal.Count().Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "SELECT") {
			t.Errorf("SQL missing SELECT: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "COUNT(*)") {
			t.Errorf("SQL missing COUNT(*): %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"users"`) {
			t.Errorf("SQL missing table name: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("COUNT with WHERE", func(t *testing.T) {
		result, err := cereal.Count().
			Where("age", ">=", "min_age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "COUNT(*)") {
			t.Errorf("SQL missing COUNT(*): %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "WHERE") {
			t.Errorf("SQL missing WHERE: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"age"`) {
			t.Errorf("SQL missing age field: %s", result.SQL)
		}

		if len(result.RequiredParams) != 1 {
			t.Errorf("Expected 1 required param, got %d", len(result.RequiredParams))
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("COUNT with multiple WHERE (AND)", func(t *testing.T) {
		result, err := cereal.Count().
			Where("age", ">=", "min_age").
			Where("age", "<=", "max_age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "COUNT(*)") {
			t.Errorf("SQL missing COUNT(*): %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "AND") {
			t.Errorf("SQL missing AND: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("COUNT with WhereAnd", func(t *testing.T) {
		result, err := cereal.Count().
			WhereAnd(
				C("age", ">=", "min_age"),
				C("age", "<=", "max_age"),
			).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "COUNT(*)") {
			t.Errorf("SQL missing COUNT(*): %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "AND") {
			t.Errorf("SQL missing AND: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("COUNT with WhereOr", func(t *testing.T) {
		result, err := cereal.Count().
			WhereOr(
				C("age", "<", "young_age"),
				C("age", ">", "old_age"),
			).
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "COUNT(*)") {
			t.Errorf("SQL missing COUNT(*): %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "OR") {
			t.Errorf("SQL missing OR: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("COUNT with WhereNull", func(t *testing.T) {
		result, err := cereal.Count().
			WhereNull("age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "COUNT(*)") {
			t.Errorf("SQL missing COUNT(*): %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "IS NULL") {
			t.Errorf("SQL missing IS NULL: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("COUNT with WhereNotNull", func(t *testing.T) {
		result, err := cereal.Count().
			WhereNotNull("age").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "COUNT(*)") {
			t.Errorf("SQL missing COUNT(*): %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "IS NOT NULL") {
			t.Errorf("SQL missing IS NOT NULL: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})
}

func TestCount_ComplexConditions(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")

	db := &sqlx.DB{}
	cereal, err := New[aggregateTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("COUNT with complex conditions", func(t *testing.T) {
		result, err := cereal.Count().
			WhereAnd(
				C("age", ">=", "min_age"),
				C("age", "<=", "max_age"),
			).
			WhereNotNull("email").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "COUNT(*)") {
			t.Error("Missing COUNT(*)")
		}
		if !strings.Contains(result.SQL, "WHERE") {
			t.Error("Missing WHERE")
		}
		if !strings.Contains(result.SQL, "AND") {
			t.Error("Missing AND")
		}
		if !strings.Contains(result.SQL, "IS NOT NULL") {
			t.Error("Missing IS NOT NULL")
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})
}

func TestAggregate_InstanceAccess(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[aggregateTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tests := []struct {
		name    string
		builder *Aggregate[aggregateTestUser]
	}{
		{"Min", cereal.Min("age")},
		{"Max", cereal.Max("age")},
		{"Sum", cereal.Sum("age")},
		{"Avg", cereal.Avg("age")},
		{"Count", cereal.Count()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instance := tt.builder.Instance()

			if instance == nil {
				t.Fatal("Instance() returned nil")
			}

			field := instance.F("email")
			if field.GetName() != "email" {
				t.Errorf("Field name = %s, want email", field.GetName())
			}
		})
	}
}

func TestAggregate_MustRender(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[aggregateTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("successful MustRender", func(t *testing.T) {
		builders := map[string]*Aggregate[aggregateTestUser]{
			"Min":   cereal.Min("age"),
			"Max":   cereal.Max("age"),
			"Sum":   cereal.Sum("age"),
			"Avg":   cereal.Avg("age"),
			"Count": cereal.Count(),
		}

		for name, builder := range builders {
			t.Run(name, func(t *testing.T) {
				result := builder.MustRender()
				if result == nil {
					t.Fatal("MustRender() returned nil")
				}
				if result.SQL == "" {
					t.Error("MustRender() returned empty SQL")
				}
			})
		}
	})

	t.Run("MustRender panics on invalid field", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("MustRender() did not panic with invalid field")
			}
		}()
		cereal.Min("nonexistent_field").MustRender()
	})

	t.Run("MustRender panics on invalid operator", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("MustRender() did not panic with invalid operator")
			}
		}()
		cereal.Min("age").
			Where("age", "INVALID", "value").
			MustRender()
	})
}

func TestAggregate_Validation(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	cereal, err := New[aggregateTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("all supported operators in WHERE", func(t *testing.T) {
		operators := []string{"=", "!=", ">", ">=", "<", "<="}
		for _, op := range operators {
			result, err := cereal.Min("age").
				Where("age", op, "value").
				Render()
			if err != nil {
				t.Errorf("Operator %s failed: %v", op, err)
			}
			if !strings.Contains(result.SQL, "WHERE") {
				t.Errorf("Operator %s produced invalid SQL: %s", op, result.SQL)
			}
		}
	})
}
