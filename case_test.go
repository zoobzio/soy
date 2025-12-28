package cereal

import (
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql/pkg/postgres"
	"github.com/zoobzio/sentinel"
)

// caseTestUser is the test struct for CASE expression tests.
type caseTestUser struct {
	ID     int    `db:"id" type:"integer" constraints:"primarykey"`
	Name   string `db:"name" type:"text"`
	Age    *int   `db:"age" type:"integer"`
	Status string `db:"status" type:"text"`
}

func setupCaseTest(t *testing.T) *Cereal[caseTestUser] {
	t.Helper()

	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	c, err := New[caseTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	return c
}

// TestCaseState_WhenImpl tests the whenImpl method.
func TestCaseState_WhenImpl(t *testing.T) {
	users := setupCaseTest(t)

	t.Run("valid WHEN condition", func(t *testing.T) {
		result, err := users.Query().
			SelectCase().
			When("status", "=", "status_val", "result_val").
			As("case_result").
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

	t.Run("invalid field returns error", func(t *testing.T) {
		_, err := users.Query().
			SelectCase().
			When("nonexistent", "=", "param", "result").
			As("case_result").
			End().
			Render()

		if err == nil {
			t.Fatal("expected error for invalid field")
		}
	})

	t.Run("invalid operator returns error", func(t *testing.T) {
		_, err := users.Query().
			SelectCase().
			When("status", "INVALID", "param", "result").
			As("case_result").
			End().
			Render()

		if err == nil {
			t.Fatal("expected error for invalid operator")
		}
	})

	t.Run("empty param returns error", func(t *testing.T) {
		_, err := users.Query().
			SelectCase().
			When("status", "=", "", "result").
			As("case_result").
			End().
			Render()

		if err == nil {
			t.Fatal("expected error for empty param")
		}
	})

	t.Run("empty result param returns error", func(t *testing.T) {
		_, err := users.Query().
			SelectCase().
			When("status", "=", "param", "").
			As("case_result").
			End().
			Render()

		if err == nil {
			t.Fatal("expected error for empty result param")
		}
	})
}

// TestCaseState_WhenNullImpl tests the whenNullImpl method.
func TestCaseState_WhenNullImpl(t *testing.T) {
	users := setupCaseTest(t)

	t.Run("valid WHEN NULL condition", func(t *testing.T) {
		result, err := users.Query().
			SelectCase().
			WhenNull("age", "result_null").
			As("null_check").
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

	t.Run("invalid field returns error", func(t *testing.T) {
		_, err := users.Query().
			SelectCase().
			WhenNull("nonexistent", "result").
			As("null_check").
			End().
			Render()

		if err == nil {
			t.Fatal("expected error for invalid field")
		}
	})

	t.Run("empty result param returns error", func(t *testing.T) {
		_, err := users.Query().
			SelectCase().
			WhenNull("age", "").
			As("null_check").
			End().
			Render()

		if err == nil {
			t.Fatal("expected error for empty result param")
		}
	})
}

// TestCaseState_WhenNotNullImpl tests the whenNotNullImpl method.
func TestCaseState_WhenNotNullImpl(t *testing.T) {
	users := setupCaseTest(t)

	t.Run("valid WHEN NOT NULL condition", func(t *testing.T) {
		result, err := users.Query().
			SelectCase().
			WhenNotNull("age", "result_not_null").
			As("not_null_check").
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

	t.Run("invalid field returns error", func(t *testing.T) {
		_, err := users.Query().
			SelectCase().
			WhenNotNull("nonexistent", "result").
			As("not_null_check").
			End().
			Render()

		if err == nil {
			t.Fatal("expected error for invalid field")
		}
	})
}

// TestCaseState_ElseImpl tests the elseImpl method.
func TestCaseState_ElseImpl(t *testing.T) {
	users := setupCaseTest(t)

	t.Run("valid ELSE clause", func(t *testing.T) {
		result, err := users.Query().
			SelectCase().
			When("status", "=", "active", "result_active").
			Else("result_default").
			As("status_result").
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

	t.Run("empty result param returns error", func(t *testing.T) {
		_, err := users.Query().
			SelectCase().
			When("status", "=", "active", "result_active").
			Else("").
			As("status_result").
			End().
			Render()

		if err == nil {
			t.Fatal("expected error for empty result param")
		}
	})
}

// TestCaseBuilder_ErrorPropagation tests that errors propagate correctly through the case builder chain.
func TestCaseBuilder_ErrorPropagation(t *testing.T) {
	users := setupCaseTest(t)

	t.Run("error in When propagates to End", func(t *testing.T) {
		_, err := users.Query().
			SelectCase().
			When("nonexistent", "=", "param", "result").
			WhenNull("age", "result2"). // This should be skipped due to previous error
			As("case_result").
			End().
			Render()

		if err == nil {
			t.Fatal("expected error to propagate")
		}
	})

	t.Run("error in WhenNull propagates to End", func(t *testing.T) {
		_, err := users.Query().
			SelectCase().
			WhenNull("nonexistent", "result").
			Else("default"). // This should be skipped due to previous error
			As("case_result").
			End().
			Render()

		if err == nil {
			t.Fatal("expected error to propagate")
		}
	})

	t.Run("error in Else propagates to End", func(t *testing.T) {
		_, err := users.Query().
			SelectCase().
			When("status", "=", "active", "result_active").
			Else("").
			As("case_result").
			End().
			Render()

		if err == nil {
			t.Fatal("expected error to propagate")
		}
	})
}

// TestSelectCaseBuilder tests CASE expressions on Select builder.
func TestSelectCaseBuilder(t *testing.T) {
	users := setupCaseTest(t)

	t.Run("SelectCaseBuilder full chain", func(t *testing.T) {
		result, err := users.Select().
			Fields("id", "name").
			SelectCase().
			When("status", "=", "active", "result_active").
			When("status", "=", "pending", "result_pending").
			WhenNull("status", "result_unknown").
			Else("result_default").
			As("status_label").
			End().
			Where("id", "=", "user_id").
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

// TestQueryCaseBuilder tests CASE expressions on Query builder.
func TestQueryCaseBuilder(t *testing.T) {
	users := setupCaseTest(t)

	t.Run("QueryCaseBuilder full chain", func(t *testing.T) {
		result, err := users.Query().
			Fields("id", "name").
			SelectCase().
			When("status", "=", "active", "result_active").
			When("status", "=", "pending", "result_pending").
			WhenNotNull("age", "result_has_age").
			Else("result_default").
			As("computed_field").
			End().
			Where("status", "IN", "statuses").
			OrderBy("name", "asc").
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

// TestMultipleCaseExpressions tests multiple CASE expressions in one query.
func TestMultipleCaseExpressions(t *testing.T) {
	users := setupCaseTest(t)

	t.Run("multiple CASE expressions", func(t *testing.T) {
		result, err := users.Query().
			Fields("id").
			SelectCase().
			When("status", "=", "active", "label_active").
			Else("label_other").
			As("status_label").
			End().
			SelectCase().
			When("age", ">=", "adult_age", "is_adult").
			Else("is_minor").
			As("age_group").
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
}
