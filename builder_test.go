package cereal

import (
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql/pkg/postgres"
	"github.com/zoobzio/sentinel"
)

// builderTestUser is a test struct for builder tests.
type builderTestUser struct {
	ID        int    `db:"id" type:"integer" constraints:"primarykey"`
	Email     string `db:"email" type:"text" constraints:"notnull,unique"`
	Name      string `db:"name" type:"text"`
	Age       *int   `db:"age" type:"integer"`
	Status    string `db:"status" type:"text"`
	Amount    *int   `db:"amount" type:"numeric"`
	CreatedAt string `db:"created_at" type:"timestamp"`
	UpdatedAt string `db:"updated_at" type:"timestamp"`
}

func setupBuilderTest(t *testing.T) *Cereal[builderTestUser] {
	t.Helper()

	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	c, err := New[builderTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	return c
}

// --- Validator Tests ---

func TestValidateOperator(t *testing.T) {
	tests := []struct {
		name    string
		op      string
		wantErr bool
	}{
		{"equals", "=", false},
		{"not equals", "!=", false},
		{"greater than", ">", false},
		{"greater or equal", ">=", false},
		{"less than", "<", false},
		{"less or equal", "<=", false},
		{"LIKE", "LIKE", false},
		{"NOT LIKE", "NOT LIKE", false},
		{"ILIKE", "ILIKE", false},
		{"IN", "IN", false},
		{"NOT IN", "NOT IN", false},
		{"regex match", "~", false},
		{"array contains", "@>", false},
		{"vector distance", "<->", false},
		{"invalid operator", "INVALID", true},
		{"empty operator", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := validateOperator(tt.op)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateOperator(%q) error = %v, wantErr %v", tt.op, err, tt.wantErr)
			}
		})
	}
}

func TestValidateDirection(t *testing.T) {
	tests := []struct {
		name    string
		dir     string
		wantErr bool
	}{
		{"asc lowercase", "asc", false},
		{"desc lowercase", "desc", false},
		{"ASC uppercase", "ASC", false},
		{"DESC uppercase", "DESC", false},
		{"Asc mixed", "Asc", false},
		{"invalid", "INVALID", true},
		{"empty", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := validateDirection(tt.dir)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateDirection(%q) error = %v, wantErr %v", tt.dir, err, tt.wantErr)
			}
		})
	}
}

func TestValidateNullsOrdering(t *testing.T) {
	tests := []struct {
		name    string
		nulls   string
		wantErr bool
	}{
		{"first lowercase", "first", false},
		{"last lowercase", "last", false},
		{"FIRST uppercase", "FIRST", false},
		{"LAST uppercase", "LAST", false},
		{"invalid", "INVALID", true},
		{"empty", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := validateNulls(tt.nulls)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateNulls(%q) error = %v, wantErr %v", tt.nulls, err, tt.wantErr)
			}
		})
	}
}

// --- Fields Tests via Select/Query ---

func TestFieldsImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	t.Run("valid fields", func(t *testing.T) {
		result, err := c.Select().
			Fields("id", "email", "name").
			Render()
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}

		if !strings.Contains(result.SQL, `"id"`) {
			t.Errorf("SQL missing id: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"email"`) {
			t.Errorf("SQL missing email: %s", result.SQL)
		}
	})

	t.Run("invalid field returns error", func(t *testing.T) {
		_, err := c.Select().
			Fields("nonexistent").
			Render()
		if err == nil {
			t.Error("Expected error for invalid field")
		}
	})
}

// --- WHERE Tests via Select/Query ---

func TestWhereImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	t.Run("valid WHERE", func(t *testing.T) {
		result, err := c.Select().
			Where("age", ">=", "min_age").
			Render()
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}

		if !strings.Contains(result.SQL, "WHERE") {
			t.Errorf("SQL missing WHERE: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"age"`) {
			t.Errorf("SQL missing age: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, ":min_age") {
			t.Errorf("SQL missing param: %s", result.SQL)
		}
	})

	t.Run("invalid operator returns error", func(t *testing.T) {
		_, err := c.Select().
			Where("age", "INVALID", "param").
			Render()
		if err == nil {
			t.Error("Expected error for invalid operator")
		}
	})

	t.Run("invalid field returns error", func(t *testing.T) {
		_, err := c.Select().
			Where("nonexistent", "=", "param").
			Render()
		if err == nil {
			t.Error("Expected error for invalid field")
		}
	})
}

func TestWhereAndImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	t.Run("multiple AND conditions", func(t *testing.T) {
		result, err := c.Select().
			WhereAnd(
				C("age", ">=", "min_age"),
				C("age", "<=", "max_age"),
			).
			Render()
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}

		if !strings.Contains(result.SQL, "AND") {
			t.Errorf("SQL missing AND: %s", result.SQL)
		}
	})
}

func TestWhereOrImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	t.Run("multiple OR conditions", func(t *testing.T) {
		result, err := c.Select().
			WhereOr(
				C("status", "=", "active"),
				C("status", "=", "pending"),
			).
			Render()
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}

		if !strings.Contains(result.SQL, "OR") {
			t.Errorf("SQL missing OR: %s", result.SQL)
		}
	})
}

func TestWhereNullImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	t.Run("IS NULL", func(t *testing.T) {
		result, err := c.Select().
			WhereNull("age").
			Render()
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}

		if !strings.Contains(result.SQL, "IS NULL") {
			t.Errorf("SQL missing IS NULL: %s", result.SQL)
		}
	})

	t.Run("invalid field returns error", func(t *testing.T) {
		_, err := c.Select().
			WhereNull("nonexistent").
			Render()
		if err == nil {
			t.Error("Expected error for invalid field")
		}
	})
}

func TestWhereNotNullImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	t.Run("IS NOT NULL", func(t *testing.T) {
		result, err := c.Select().
			WhereNotNull("age").
			Render()
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}

		if !strings.Contains(result.SQL, "IS NOT NULL") {
			t.Errorf("SQL missing IS NOT NULL: %s", result.SQL)
		}
	})
}

func TestWhereBetweenImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	t.Run("BETWEEN", func(t *testing.T) {
		result, err := c.Select().
			WhereBetween("age", "min_age", "max_age").
			Render()
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}

		if !strings.Contains(result.SQL, "BETWEEN") {
			t.Errorf("SQL missing BETWEEN: %s", result.SQL)
		}
	})
}

func TestWhereNotBetweenImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	t.Run("NOT BETWEEN", func(t *testing.T) {
		result, err := c.Select().
			WhereNotBetween("age", "min_age", "max_age").
			Render()
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}

		if !strings.Contains(result.SQL, "NOT BETWEEN") {
			t.Errorf("SQL missing NOT BETWEEN: %s", result.SQL)
		}
	})
}

func TestWhereFieldsImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	t.Run("field comparison", func(t *testing.T) {
		result, err := c.Select().
			WhereFields("created_at", "<", "updated_at").
			Render()
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}

		if !strings.Contains(result.SQL, `"created_at"`) {
			t.Errorf("SQL missing created_at: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"updated_at"`) {
			t.Errorf("SQL missing updated_at: %s", result.SQL)
		}
	})
}

// --- ORDER BY Tests ---

func TestOrderByImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	t.Run("ORDER BY ASC", func(t *testing.T) {
		result, err := c.Select().
			OrderBy("name", "asc").
			Render()
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}

		if !strings.Contains(result.SQL, "ORDER BY") {
			t.Errorf("SQL missing ORDER BY: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "ASC") {
			t.Errorf("SQL missing ASC: %s", result.SQL)
		}
	})

	t.Run("ORDER BY DESC", func(t *testing.T) {
		result, err := c.Select().
			OrderBy("age", "desc").
			Render()
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}

		if !strings.Contains(result.SQL, "DESC") {
			t.Errorf("SQL missing DESC: %s", result.SQL)
		}
	})

	t.Run("invalid direction returns error", func(t *testing.T) {
		_, err := c.Select().
			OrderBy("name", "invalid").
			Render()
		if err == nil {
			t.Error("Expected error for invalid direction")
		}
	})
}

func TestOrderByNullsImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	t.Run("NULLS LAST", func(t *testing.T) {
		result, err := c.Select().
			OrderByNulls("age", "desc", "last").
			Render()
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}

		if !strings.Contains(result.SQL, "NULLS LAST") {
			t.Errorf("SQL missing NULLS LAST: %s", result.SQL)
		}
	})

	t.Run("NULLS FIRST", func(t *testing.T) {
		result, err := c.Select().
			OrderByNulls("age", "asc", "first").
			Render()
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}

		if !strings.Contains(result.SQL, "NULLS FIRST") {
			t.Errorf("SQL missing NULLS FIRST: %s", result.SQL)
		}
	})
}

// --- GROUP BY / HAVING Tests ---

func TestGroupByImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	t.Run("GROUP BY single field", func(t *testing.T) {
		result, err := c.Select().
			GroupBy("status").
			Render()
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}

		if !strings.Contains(result.SQL, "GROUP BY") {
			t.Errorf("SQL missing GROUP BY: %s", result.SQL)
		}
	})

	t.Run("GROUP BY multiple fields", func(t *testing.T) {
		result, err := c.Select().
			GroupBy("status", "age").
			Render()
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}

		if !strings.Contains(result.SQL, `"status"`) {
			t.Errorf("SQL missing status: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"age"`) {
			t.Errorf("SQL missing age: %s", result.SQL)
		}
	})
}

func TestHavingImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	t.Run("HAVING condition", func(t *testing.T) {
		result, err := c.Select().
			GroupBy("status").
			Having("age", ">", "min_age").
			Render()
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}

		if !strings.Contains(result.SQL, "HAVING") {
			t.Errorf("SQL missing HAVING: %s", result.SQL)
		}
	})
}

// --- SELECT Expression Tests ---

func TestSelectUpperImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	result, err := c.Select().
		SelectUpper("name", "upper_name").
		Render()
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "UPPER") {
		t.Errorf("SQL missing UPPER: %s", result.SQL)
	}
	if !strings.Contains(result.SQL, `"upper_name"`) {
		t.Errorf("SQL missing alias: %s", result.SQL)
	}
}

func TestSelectLowerImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	result, err := c.Select().
		SelectLower("email", "lower_email").
		Render()
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "LOWER") {
		t.Errorf("SQL missing LOWER: %s", result.SQL)
	}
}

func TestSelectCountImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	result, err := c.Select().
		SelectCount("id", "total").
		Render()
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "COUNT") {
		t.Errorf("SQL missing COUNT: %s", result.SQL)
	}
}

func TestSelectSumImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	result, err := c.Select().
		SelectSum("amount", "total_amount").
		Render()
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "SUM") {
		t.Errorf("SQL missing SUM: %s", result.SQL)
	}
}

func TestSelectAvgImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	result, err := c.Select().
		SelectAvg("age", "avg_age").
		Render()
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "AVG") {
		t.Errorf("SQL missing AVG: %s", result.SQL)
	}
}

func TestSelectMinImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	result, err := c.Select().
		SelectMin("age", "min_age").
		Render()
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "MIN") {
		t.Errorf("SQL missing MIN: %s", result.SQL)
	}
}

func TestSelectMaxImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	result, err := c.Select().
		SelectMax("age", "max_age").
		Render()
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "MAX") {
		t.Errorf("SQL missing MAX: %s", result.SQL)
	}
}

func TestSelectConcatImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	t.Run("valid CONCAT", func(t *testing.T) {
		result, err := c.Select().
			SelectConcat("full_name", "name", "email").
			Render()
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}

		if !strings.Contains(result.SQL, "CONCAT") {
			t.Errorf("SQL missing CONCAT: %s", result.SQL)
		}
	})

	t.Run("CONCAT requires at least 2 fields", func(t *testing.T) {
		_, err := c.Select().
			SelectConcat("alias", "single_field").
			Render()
		if err == nil {
			t.Error("Expected error for single field CONCAT")
		}
	})
}

func TestSelectCoalesceImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	t.Run("valid COALESCE", func(t *testing.T) {
		result, err := c.Select().
			SelectCoalesce("result", "val1", "val2", "val3").
			Render()
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}

		if !strings.Contains(result.SQL, "COALESCE") {
			t.Errorf("SQL missing COALESCE: %s", result.SQL)
		}
	})

	t.Run("COALESCE requires at least 2 params", func(t *testing.T) {
		_, err := c.Select().
			SelectCoalesce("alias", "single_param").
			Render()
		if err == nil {
			t.Error("Expected error for single param COALESCE")
		}
	})
}

// --- Aggregate FILTER Tests ---

func TestSelectSumFilterImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	result, err := c.Select().
		SelectSumFilter("amount", "status", "=", "active", "active_total").
		Render()
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "SUM") {
		t.Errorf("SQL missing SUM: %s", result.SQL)
	}
	if !strings.Contains(result.SQL, "FILTER") {
		t.Errorf("SQL missing FILTER: %s", result.SQL)
	}
}

// --- LIMIT/OFFSET Tests ---

func TestLimitParamImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	result, err := c.Select().
		LimitParam("page_size").
		Render()
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "LIMIT") {
		t.Errorf("SQL missing LIMIT: %s", result.SQL)
	}
	if !strings.Contains(result.SQL, ":page_size") {
		t.Errorf("SQL missing param: %s", result.SQL)
	}
}

func TestOffsetParamImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	result, err := c.Select().
		OffsetParam("page_offset").
		Render()
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "OFFSET") {
		t.Errorf("SQL missing OFFSET: %s", result.SQL)
	}
}

// --- DISTINCT Tests ---

func TestDistinctOnImpl_ViaSelect(t *testing.T) {
	c := setupBuilderTest(t)

	t.Run("DISTINCT ON single field", func(t *testing.T) {
		result, err := c.Select().
			DistinctOn("email").
			Render()
		if err != nil {
			t.Fatalf("Render() error = %v", err)
		}

		if !strings.Contains(result.SQL, "DISTINCT ON") {
			t.Errorf("SQL missing DISTINCT ON: %s", result.SQL)
		}
	})
}

// --- Query Builder Tests (ensure *Impl functions work for both Select and Query) ---

func TestFieldsImpl_ViaQuery(t *testing.T) {
	c := setupBuilderTest(t)

	result, err := c.Query().
		Fields("id", "email", "name").
		Render()
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, `"id"`) {
		t.Errorf("SQL missing id: %s", result.SQL)
	}
}

func TestWhereImpl_ViaQuery(t *testing.T) {
	c := setupBuilderTest(t)

	result, err := c.Query().
		Where("age", ">=", "min_age").
		Render()
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "WHERE") {
		t.Errorf("SQL missing WHERE: %s", result.SQL)
	}
}

func TestOrderByImpl_ViaQuery(t *testing.T) {
	c := setupBuilderTest(t)

	result, err := c.Query().
		OrderBy("name", "asc").
		Render()
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}

	if !strings.Contains(result.SQL, "ORDER BY") {
		t.Errorf("SQL missing ORDER BY: %s", result.SQL)
	}
}

func TestValidateNulls(t *testing.T) {
	tests := []struct {
		input       string
		expectError bool
	}{
		{"FIRST", false},
		{"first", false},
		{"First", false},
		{"LAST", false},
		{"last", false},
		{"Last", false},
		{"INVALID", true},
		{"", true},
		{"NULL", true},
	}

	for _, tc := range tests {
		_, err := validateNulls(tc.input)
		gotError := err != nil
		if gotError != tc.expectError {
			t.Errorf("validateNulls(%q) error = %v, want error = %v", tc.input, gotError, tc.expectError)
		}
	}
}
