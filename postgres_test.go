package cereal

import (
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/sentinel"
)

// Tests for PostgreSQL-specific features: OrderByNulls, Distinct, DistinctOn, row locking

type pgTestUser struct {
	ID    int    `db:"id" type:"integer" constraints:"primarykey"`
	Email string `db:"email" type:"text" constraints:"notnull,unique"`
	Name  string `db:"name" type:"text"`
	Age   *int   `db:"age" type:"integer"`
}

func TestQuery_OrderByNulls(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	c, err := New[pgTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("NULLS FIRST", func(t *testing.T) {
		result, err := c.Query().
			OrderByNulls("age", "ASC", "FIRST").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "NULLS FIRST") {
			t.Errorf("SQL missing NULLS FIRST: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "ORDER BY") {
			t.Errorf("SQL missing ORDER BY: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("NULLS LAST", func(t *testing.T) {
		result, err := c.Query().
			OrderByNulls("age", "DESC", "LAST").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "NULLS LAST") {
			t.Errorf("SQL missing NULLS LAST: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("case insensitive nulls", func(t *testing.T) {
		nullsOptions := []string{"first", "FIRST", "First", "last", "LAST", "Last"}
		for _, nulls := range nullsOptions {
			result, err := c.Query().
				OrderByNulls("age", "ASC", nulls).
				Render()
			if err != nil {
				t.Errorf("OrderByNulls with %s failed: %v", nulls, err)
				continue
			}
			if !strings.Contains(result.SQL, "NULLS") {
				t.Errorf("SQL missing NULLS for %s: %s", nulls, result.SQL)
			}
		}
	})

	t.Run("invalid nulls returns error", func(t *testing.T) {
		_, err := c.Query().
			OrderByNulls("age", "ASC", "INVALID").
			Render()
		if err == nil {
			t.Error("expected error for invalid nulls option")
		}
	})

	t.Run("invalid field returns error", func(t *testing.T) {
		_, err := c.Query().
			OrderByNulls("nonexistent", "ASC", "FIRST").
			Render()
		if err == nil {
			t.Error("expected error for invalid field")
		}
	})

	t.Run("invalid direction returns error", func(t *testing.T) {
		_, err := c.Query().
			OrderByNulls("age", "INVALID", "FIRST").
			Render()
		if err == nil {
			t.Error("expected error for invalid direction")
		}
	})
}

func TestSelect_OrderByNulls(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	c, err := New[pgTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("NULLS FIRST", func(t *testing.T) {
		result, err := c.Select().
			OrderByNulls("age", "ASC", "FIRST").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "NULLS FIRST") {
			t.Errorf("SQL missing NULLS FIRST: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("NULLS LAST", func(t *testing.T) {
		result, err := c.Select().
			OrderByNulls("age", "DESC", "LAST").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "NULLS LAST") {
			t.Errorf("SQL missing NULLS LAST: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("invalid nulls returns error", func(t *testing.T) {
		_, err := c.Select().
			OrderByNulls("age", "ASC", "INVALID").
			Render()
		if err == nil {
			t.Error("expected error for invalid nulls option")
		}
	})
}

func TestQuery_Distinct(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	c, err := New[pgTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("DISTINCT", func(t *testing.T) {
		result, err := c.Query().
			Fields("name").
			Distinct().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "SELECT DISTINCT") {
			t.Errorf("SQL missing SELECT DISTINCT: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})
}

func TestQuery_DistinctOn(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	c, err := New[pgTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("DISTINCT ON single field", func(t *testing.T) {
		result, err := c.Query().
			DistinctOn("name").
			OrderBy("name", "ASC").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "DISTINCT ON") {
			t.Errorf("SQL missing DISTINCT ON: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"name"`) {
			t.Errorf("SQL missing name field: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("DISTINCT ON multiple fields", func(t *testing.T) {
		result, err := c.Query().
			DistinctOn("name", "email").
			OrderBy("name", "ASC").
			OrderBy("email", "ASC").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "DISTINCT ON") {
			t.Errorf("SQL missing DISTINCT ON: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("invalid field returns error", func(t *testing.T) {
		_, err := c.Query().
			DistinctOn("nonexistent").
			Render()
		if err == nil {
			t.Error("expected error for invalid field")
		}
	})

	t.Run("empty DistinctOn is ignored", func(t *testing.T) {
		result, err := c.Query().
			DistinctOn().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		// Should not contain DISTINCT ON when no fields provided
		if strings.Contains(result.SQL, "DISTINCT ON") {
			t.Errorf("SQL should not contain DISTINCT ON when no fields: %s", result.SQL)
		}
	})
}

func TestSelect_DistinctOn(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	c, err := New[pgTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("DISTINCT ON", func(t *testing.T) {
		result, err := c.Select().
			DistinctOn("name").
			OrderBy("name", "ASC").
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "DISTINCT ON") {
			t.Errorf("SQL missing DISTINCT ON: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("invalid field returns error", func(t *testing.T) {
		_, err := c.Select().
			DistinctOn("nonexistent").
			Render()
		if err == nil {
			t.Error("expected error for invalid field")
		}
	})
}

func TestQuery_RowLocking(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	c, err := New[pgTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("FOR UPDATE", func(t *testing.T) {
		result, err := c.Query().
			Where("id", "=", "user_id").
			ForUpdate().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "FOR UPDATE") {
			t.Errorf("SQL missing FOR UPDATE: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("FOR NO KEY UPDATE", func(t *testing.T) {
		result, err := c.Query().
			Where("id", "=", "user_id").
			ForNoKeyUpdate().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "FOR NO KEY UPDATE") {
			t.Errorf("SQL missing FOR NO KEY UPDATE: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("FOR SHARE", func(t *testing.T) {
		result, err := c.Query().
			Where("id", "=", "user_id").
			ForShare().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "FOR SHARE") {
			t.Errorf("SQL missing FOR SHARE: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("FOR KEY SHARE", func(t *testing.T) {
		result, err := c.Query().
			Where("id", "=", "user_id").
			ForKeyShare().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "FOR KEY SHARE") {
			t.Errorf("SQL missing FOR KEY SHARE: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})
}

func TestSelect_RowLocking(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	c, err := New[pgTestUser](db, "users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("FOR UPDATE", func(t *testing.T) {
		result, err := c.Select().
			Where("id", "=", "user_id").
			ForUpdate().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "FOR UPDATE") {
			t.Errorf("SQL missing FOR UPDATE: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("FOR NO KEY UPDATE", func(t *testing.T) {
		result, err := c.Select().
			Where("id", "=", "user_id").
			ForNoKeyUpdate().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "FOR NO KEY UPDATE") {
			t.Errorf("SQL missing FOR NO KEY UPDATE: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("FOR SHARE", func(t *testing.T) {
		result, err := c.Select().
			Where("id", "=", "user_id").
			ForShare().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "FOR SHARE") {
			t.Errorf("SQL missing FOR SHARE: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("FOR KEY SHARE", func(t *testing.T) {
		result, err := c.Select().
			Where("id", "=", "user_id").
			ForKeyShare().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "FOR KEY SHARE") {
			t.Errorf("SQL missing FOR KEY SHARE: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})
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
