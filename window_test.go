package soy

import (
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql/pkg/postgres"
	"github.com/zoobzio/sentinel"
)

// windowTestUser is the test struct for window function tests.
type windowTestUser struct {
	ID     int    `db:"id" type:"integer" constraints:"primarykey"`
	Name   string `db:"name" type:"text"`
	Age    *int   `db:"age" type:"integer"`
	Status string `db:"status" type:"text"`
}

func setupWindowTest(t *testing.T) *Soy[windowTestUser] {
	t.Helper()

	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	c, err := New[windowTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	return c
}

// TestValidateFrameBound tests the validateFrameBound function.
func TestValidateFrameBound(t *testing.T) {
	tests := []struct {
		name    string
		bound   string
		wantErr bool
	}{
		{
			name:    "UNBOUNDED PRECEDING",
			bound:   "UNBOUNDED PRECEDING",
			wantErr: false,
		},
		{
			name:    "CURRENT ROW",
			bound:   "CURRENT ROW",
			wantErr: false,
		},
		{
			name:    "UNBOUNDED FOLLOWING",
			bound:   "UNBOUNDED FOLLOWING",
			wantErr: false,
		},
		{
			name:    "lowercase unbounded preceding",
			bound:   "unbounded preceding",
			wantErr: false,
		},
		{
			name:    "lowercase current row",
			bound:   "current row",
			wantErr: false,
		},
		{
			name:    "lowercase unbounded following",
			bound:   "unbounded following",
			wantErr: false,
		},
		{
			name:    "mixed case",
			bound:   "Unbounded Preceding",
			wantErr: false,
		},
		{
			name:    "invalid bound",
			bound:   "INVALID",
			wantErr: true,
		},
		{
			name:    "empty string",
			bound:   "",
			wantErr: true,
		},
		{
			name:    "partial match",
			bound:   "UNBOUNDED",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := validateFrameBound(tt.bound)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateFrameBound(%q) error = %v, wantErr %v", tt.bound, err, tt.wantErr)
			}
		})
	}
}

// TestWindowState_PartitionByImpl tests the partitionByImpl method.
func TestWindowState_PartitionByImpl(t *testing.T) {
	users := setupWindowTest(t)

	t.Run("valid partition fields", func(t *testing.T) {
		result, err := users.Query().
			SelectRowNumber().
			PartitionBy("name", "status").
			OrderBy("id", "asc").
			As("row_num").
			End().
			Render()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		sql := result.SQL
		if sql == "" {
			t.Fatal("expected SQL to be generated")
		}
		t.Logf("SQL: %s", sql)
	})

	t.Run("invalid partition field returns error", func(t *testing.T) {
		_, err := users.Query().
			SelectRowNumber().
			PartitionBy("nonexistent").
			As("row_num").
			End().
			Render()

		if err == nil {
			t.Fatal("expected error for invalid partition field")
		}
	})
}

// TestWindowState_OrderByImpl tests the orderByImpl method.
func TestWindowState_OrderByImpl(t *testing.T) {
	users := setupWindowTest(t)

	t.Run("valid order by ASC", func(t *testing.T) {
		result, err := users.Query().
			SelectRowNumber().
			OrderBy("age", "asc").
			As("row_num").
			End().
			Render()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		sql := result.SQL
		if sql == "" {
			t.Fatal("expected SQL to be generated")
		}
		t.Logf("SQL: %s", sql)
	})

	t.Run("valid order by DESC", func(t *testing.T) {
		result, err := users.Query().
			SelectRowNumber().
			OrderBy("age", "desc").
			As("row_num").
			End().
			Render()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		sql := result.SQL
		if sql == "" {
			t.Fatal("expected SQL to be generated")
		}
		t.Logf("SQL: %s", sql)
	})

	t.Run("invalid direction returns error", func(t *testing.T) {
		_, err := users.Query().
			SelectRowNumber().
			OrderBy("age", "invalid").
			As("row_num").
			End().
			Render()

		if err == nil {
			t.Fatal("expected error for invalid direction")
		}
	})

	t.Run("invalid field returns error", func(t *testing.T) {
		_, err := users.Query().
			SelectRowNumber().
			OrderBy("nonexistent", "asc").
			As("row_num").
			End().
			Render()

		if err == nil {
			t.Fatal("expected error for invalid field")
		}
	})
}

// TestWindowState_FrameImpl tests the frameImpl method.
func TestWindowState_FrameImpl(t *testing.T) {
	users := setupWindowTest(t)

	t.Run("valid frame", func(t *testing.T) {
		result, err := users.Query().
			SelectSumOver("age").
			OrderBy("id", "asc").
			Frame("UNBOUNDED PRECEDING", "CURRENT ROW").
			As("running_sum").
			End().
			Render()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		sql := result.SQL
		if sql == "" {
			t.Fatal("expected SQL to be generated")
		}
		t.Logf("SQL: %s", sql)
	})

	t.Run("invalid frame start", func(t *testing.T) {
		_, err := users.Query().
			SelectSumOver("age").
			OrderBy("id", "asc").
			Frame("INVALID", "CURRENT ROW").
			As("running_sum").
			End().
			Render()

		if err == nil {
			t.Fatal("expected error for invalid frame start")
		}
	})

	t.Run("invalid frame end", func(t *testing.T) {
		_, err := users.Query().
			SelectSumOver("age").
			OrderBy("id", "asc").
			Frame("UNBOUNDED PRECEDING", "INVALID").
			As("running_sum").
			End().
			Render()

		if err == nil {
			t.Fatal("expected error for invalid frame end")
		}
	})
}

// TestWindowCreationFunctions tests the window creation helper functions.
func TestWindowCreationFunctions(t *testing.T) {
	users := setupWindowTest(t)

	t.Run("createRowNumberWindow", func(t *testing.T) {
		result, err := users.Query().
			SelectRowNumber().
			OrderBy("id", "asc").
			As("rn").
			End().
			Render()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("createRankWindow", func(t *testing.T) {
		result, err := users.Query().
			SelectRank().
			OrderBy("age", "desc").
			As("rank").
			End().
			Render()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("createDenseRankWindow", func(t *testing.T) {
		result, err := users.Query().
			SelectDenseRank().
			OrderBy("age", "desc").
			As("dense_rank").
			End().
			Render()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("createNtileWindow", func(t *testing.T) {
		result, err := users.Query().
			SelectNtile("num_buckets").
			OrderBy("age", "asc").
			As("quartile").
			End().
			Render()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("createNtileWindow empty param", func(t *testing.T) {
		_, err := users.Query().
			SelectNtile("").
			OrderBy("age", "asc").
			As("quartile").
			End().
			Render()

		if err == nil {
			t.Fatal("expected error for empty ntile param")
		}
	})

	t.Run("createLagWindow", func(t *testing.T) {
		result, err := users.Query().
			SelectLag("age", "offset").
			OrderBy("id", "asc").
			As("prev_age").
			End().
			Render()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("createLagWindow invalid field", func(t *testing.T) {
		_, err := users.Query().
			SelectLag("nonexistent", "offset").
			OrderBy("id", "asc").
			As("prev").
			End().
			Render()

		if err == nil {
			t.Fatal("expected error for invalid lag field")
		}
	})

	t.Run("createLeadWindow", func(t *testing.T) {
		result, err := users.Query().
			SelectLead("age", "offset").
			OrderBy("id", "asc").
			As("next_age").
			End().
			Render()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("createFirstValueWindow", func(t *testing.T) {
		result, err := users.Query().
			SelectFirstValue("age").
			OrderBy("id", "asc").
			As("first_age").
			End().
			Render()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("createLastValueWindow", func(t *testing.T) {
		result, err := users.Query().
			SelectLastValue("age").
			OrderBy("id", "asc").
			As("last_age").
			End().
			Render()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("createSumOverWindow", func(t *testing.T) {
		result, err := users.Query().
			SelectSumOver("age").
			PartitionBy("status").
			As("sum_age").
			End().
			Render()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("createAvgOverWindow", func(t *testing.T) {
		result, err := users.Query().
			SelectAvgOver("age").
			PartitionBy("status").
			As("avg_age").
			End().
			Render()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("createCountOverWindow", func(t *testing.T) {
		result, err := users.Query().
			SelectCountOver().
			PartitionBy("status").
			As("count").
			End().
			Render()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("createMinOverWindow", func(t *testing.T) {
		result, err := users.Query().
			SelectMinOver("age").
			PartitionBy("status").
			As("min_age").
			End().
			Render()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("createMaxOverWindow", func(t *testing.T) {
		result, err := users.Query().
			SelectMaxOver("age").
			PartitionBy("status").
			As("max_age").
			End().
			Render()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		t.Logf("SQL: %s", result.SQL)
	})
}

// TestWindowBuilder_ErrorPropagation tests that errors propagate correctly through the window builder chain.
func TestWindowBuilder_ErrorPropagation(t *testing.T) {
	users := setupWindowTest(t)

	t.Run("error in partition propagates to end", func(t *testing.T) {
		_, err := users.Query().
			SelectRowNumber().
			PartitionBy("invalid_field").
			OrderBy("id", "asc"). // This should be skipped due to previous error
			As("rn").
			End().
			Render()

		if err == nil {
			t.Fatal("expected error to propagate")
		}
	})

	t.Run("error in order by propagates to end", func(t *testing.T) {
		_, err := users.Query().
			SelectRowNumber().
			PartitionBy("status").
			OrderBy("invalid_field", "asc").
			As("rn").
			End().
			Render()

		if err == nil {
			t.Fatal("expected error to propagate")
		}
	})

	t.Run("error in frame propagates to end", func(t *testing.T) {
		_, err := users.Query().
			SelectSumOver("age").
			OrderBy("id", "asc").
			Frame("INVALID", "CURRENT ROW").
			As("sum").
			End().
			Render()

		if err == nil {
			t.Fatal("expected error to propagate")
		}
	})
}

// TestSelectWindowBuilder tests window functions on Select builder.
func TestSelectWindowBuilder(t *testing.T) {
	users := setupWindowTest(t)

	t.Run("SelectWindowBuilder full chain", func(t *testing.T) {
		result, err := users.Select().
			Fields("id", "name").
			SelectRowNumber().
			PartitionBy("status").
			OrderBy("age", "desc").
			Frame("UNBOUNDED PRECEDING", "CURRENT ROW").
			As("row_num").
			End().
			Where("status", "=", "status").
			Render()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		sql := result.SQL
		if sql == "" {
			t.Fatal("expected SQL to be generated")
		}
		t.Logf("SQL: %s", sql)
	})
}

// TestQueryWindowBuilder tests window functions on Query builder.
func TestQueryWindowBuilder(t *testing.T) {
	users := setupWindowTest(t)

	t.Run("QueryWindowBuilder full chain", func(t *testing.T) {
		result, err := users.Query().
			Fields("id", "name").
			SelectRowNumber().
			PartitionBy("status").
			OrderBy("age", "desc").
			Frame("UNBOUNDED PRECEDING", "CURRENT ROW").
			As("row_num").
			End().
			Where("status", "=", "status").
			OrderBy("id", "asc").
			Render()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		sql := result.SQL
		if sql == "" {
			t.Fatal("expected SQL to be generated")
		}
		t.Logf("SQL: %s", sql)
	})
}
