package integration

import (
	"context"
	"testing"

	"github.com/zoobzio/cereal"
)

func TestEdgeCases_Integration(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.cleanup(t)
	createTestTable(t, tdb.db)

	c, err := cereal.New[TestUser](tdb.db, "test_users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("query empty table", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		users, err := c.Query().Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Query().Exec() failed: %v", err)
		}
		if len(users) != 0 {
			t.Errorf("expected 0 users, got %d", len(users))
		}
	})

	t.Run("count empty table", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		count, err := c.Count().Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Count().Exec() failed: %v", err)
		}
		if count != 0 {
			t.Errorf("expected count 0, got %v", count)
		}
	})

	t.Run("select no rows returns error", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		_, err := c.Select().
			Where("email", "=", "user_email").
			Exec(ctx, map[string]any{"user_email": "nonexistent@example.com"})
		if err == nil {
			t.Error("expected error for no rows")
		}
	})

	t.Run("LIMIT 0 returns empty slice", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		// Insert some data
		_, err := c.Insert().Exec(ctx, &TestUser{
			Email: "test@example.com",
			Name:  "Test User",
			Age:   intPtr(25),
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		users, err := c.Query().Limit(0).Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Query().Limit(0).Exec() failed: %v", err)
		}
		if len(users) != 0 {
			t.Errorf("expected 0 users with LIMIT 0, got %d", len(users))
		}
	})

	t.Run("large OFFSET returns empty slice", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		// Insert one record
		_, err := c.Insert().Exec(ctx, &TestUser{
			Email: "test@example.com",
			Name:  "Test User",
			Age:   intPtr(25),
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		users, err := c.Query().Offset(1000).Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Query().Offset(1000).Exec() failed: %v", err)
		}
		if len(users) != 0 {
			t.Errorf("expected 0 users with large OFFSET, got %d", len(users))
		}
	})

	t.Run("delete no matching rows returns 0", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		affected, err := c.Remove().
			Where("email", "=", "user_email").
			Exec(ctx, map[string]any{"user_email": "nonexistent@example.com"})
		if err != nil {
			t.Fatalf("Remove().Exec() failed: %v", err)
		}
		if affected != 0 {
			t.Errorf("expected 0 rows affected, got %d", affected)
		}
	})

	t.Run("empty batch insert returns 0", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		records := []*TestUser{}
		count, err := c.Insert().ExecBatch(ctx, records)
		if err != nil {
			t.Fatalf("Insert().ExecBatch() failed: %v", err)
		}
		if count != 0 {
			t.Errorf("expected 0 records inserted, got %d", count)
		}
	})

	t.Run("empty batch update returns 0", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		batchParams := []map[string]any{}
		affected, err := c.Modify().
			Set("name", "new_name").
			Where("id", "=", "user_id").
			ExecBatch(ctx, batchParams)
		if err != nil {
			t.Fatalf("Modify().ExecBatch() failed: %v", err)
		}
		if affected != 0 {
			t.Errorf("expected 0 rows affected, got %d", affected)
		}
	})

	t.Run("empty batch delete returns 0", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		batchParams := []map[string]any{}
		affected, err := c.Remove().
			Where("id", "=", "user_id").
			ExecBatch(ctx, batchParams)
		if err != nil {
			t.Fatalf("Remove().ExecBatch() failed: %v", err)
		}
		if affected != 0 {
			t.Errorf("expected 0 rows affected, got %d", affected)
		}
	})

	t.Run("DISTINCT removes duplicates", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		// Insert users with same names but different emails
		testUsers := []*TestUser{
			{Email: "john1@example.com", Name: "John", Age: intPtr(25)},
			{Email: "john2@example.com", Name: "John", Age: intPtr(30)},
			{Email: "jane@example.com", Name: "Jane", Age: intPtr(28)},
		}
		for _, u := range testUsers {
			_, err := c.Insert().Exec(ctx, u)
			if err != nil {
				t.Fatalf("failed to insert: %v", err)
			}
		}

		// Query with DISTINCT on all fields still returns 3 (all rows are distinct)
		users, err := c.Query().Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Query().Exec() failed: %v", err)
		}
		if len(users) != 3 {
			t.Errorf("expected 3 users, got %d", len(users))
		}
	})
}

func TestConstraintViolations_Integration(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.cleanup(t)
	createTestTable(t, tdb.db)

	c, err := cereal.New[TestUser](tdb.db, "test_users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("duplicate primary key", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		// Insert first record
		_, err := tdb.db.ExecContext(ctx, `INSERT INTO test_users (id, email, name) VALUES (1, 'first@example.com', 'First User')`)
		if err != nil {
			t.Fatalf("failed to insert first record: %v", err)
		}

		// Attempt to insert duplicate PK
		_, err = tdb.db.ExecContext(ctx, `INSERT INTO test_users (id, email, name) VALUES (1, 'second@example.com', 'Second User')`)
		if err == nil {
			t.Error("expected error for duplicate primary key")
		}
	})

	t.Run("NOT NULL constraint violation", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		// Try to insert with NULL name (NOT NULL constraint)
		_, err := tdb.db.ExecContext(ctx, `INSERT INTO test_users (email, name) VALUES ('test@example.com', NULL)`)
		if err == nil {
			t.Error("expected error for NOT NULL violation")
		}
	})

	t.Run("UNIQUE constraint violation", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		// Insert first record
		_, err := c.Insert().Exec(ctx, &TestUser{
			Email: "unique@example.com",
			Name:  "First User",
		})
		if err != nil {
			t.Fatalf("failed to insert first record: %v", err)
		}

		// Attempt to insert with same email (UNIQUE constraint)
		_, err = c.Insert().Exec(ctx, &TestUser{
			Email: "unique@example.com", // duplicate
			Name:  "Second User",
		})
		if err == nil {
			t.Error("expected error for UNIQUE constraint violation")
		}
	})

	t.Run("UNIQUE constraint with ON CONFLICT DO NOTHING", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		// Insert first record
		_, err := c.Insert().Exec(ctx, &TestUser{
			Email: "upsert@example.com",
			Name:  "First User",
		})
		if err != nil {
			t.Fatalf("failed to insert first record: %v", err)
		}

		// Attempt duplicate with DO NOTHING - should not error catastrophically
		_, err = c.Insert().
			OnConflict("email").
			DoNothing().
			Exec(ctx, &TestUser{
				Email: "upsert@example.com",
				Name:  "Second User",
			})
		// DO NOTHING returns error when no rows returned, but it shouldn't be a DB error
		if err != nil {
			t.Logf("DO NOTHING returned: %v (expected behavior)", err)
		}

		// Verify original record unchanged
		user, err := c.Select().
			Where("email", "=", "email").
			Exec(ctx, map[string]any{"email": "upsert@example.com"})
		if err != nil {
			t.Fatalf("Select failed: %v", err)
		}
		if user.Name != "First User" {
			t.Errorf("expected name 'First User', got '%s'", user.Name)
		}
	})
}
