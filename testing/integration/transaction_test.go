package integration

import (
	"context"
	"testing"

	"github.com/zoobzio/cereal"
)

func TestTransactionEdgeCases_Integration(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.cleanup(t)
	createTestTable(t, tdb.db)

	c, err := cereal.New[TestUser](tdb.db, "test_users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("rollback after partial success", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		tx, err := tdb.db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}

		// First insert succeeds
		_, err = c.Insert().ExecTx(ctx, tx, &TestUser{
			Email: "first@example.com",
			Name:  "First User",
			Age:   intPtr(25),
		})
		if err != nil {
			tx.Rollback()
			t.Fatalf("first insert failed: %v", err)
		}

		// Second insert succeeds
		_, err = c.Insert().ExecTx(ctx, tx, &TestUser{
			Email: "second@example.com",
			Name:  "Second User",
			Age:   intPtr(30),
		})
		if err != nil {
			tx.Rollback()
			t.Fatalf("second insert failed: %v", err)
		}

		// Now rollback
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback() failed: %v", err)
		}

		// Verify nothing was committed
		users, err := c.Query().Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Query().Exec() failed: %v", err)
		}
		if len(users) != 0 {
			t.Errorf("expected 0 users after rollback, got %d", len(users))
		}
	})

	t.Run("multiple operations in single transaction", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		tx, err := tdb.db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}

		// Insert
		user, err := c.Insert().ExecTx(ctx, tx, &TestUser{
			Email: "tx_user@example.com",
			Name:  "TX User",
			Age:   intPtr(25),
		})
		if err != nil {
			tx.Rollback()
			t.Fatalf("Insert failed: %v", err)
		}

		// Update
		_, err = c.Modify().
			Set("name", "new_name").
			Set("age", "new_age").
			Where("email", "=", "user_email").
			ExecTx(ctx, tx, map[string]any{
				"user_email": "tx_user@example.com",
				"new_name":   "Updated TX User",
				"new_age":    30,
			})
		if err != nil {
			tx.Rollback()
			t.Fatalf("Update failed: %v", err)
		}

		// Query within transaction sees uncommitted changes
		users, err := c.Query().ExecTx(ctx, tx, nil)
		if err != nil {
			tx.Rollback()
			t.Fatalf("Query failed: %v", err)
		}
		if len(users) != 1 {
			tx.Rollback()
			t.Fatalf("expected 1 user, got %d", len(users))
		}
		if users[0].Name != "Updated TX User" {
			t.Errorf("expected Updated TX User, got %s", users[0].Name)
		}

		// Count within transaction
		count, err := c.Count().ExecTx(ctx, tx, nil)
		if err != nil {
			tx.Rollback()
			t.Fatalf("Count failed: %v", err)
		}
		if count != 1 {
			t.Errorf("expected count 1, got %v", count)
		}

		// Commit
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit() failed: %v", err)
		}

		// Verify changes persisted
		final, err := c.Select().
			Where("email", "=", "user_email").
			Exec(ctx, map[string]any{"user_email": "tx_user@example.com"})
		if err != nil {
			t.Fatalf("Select failed: %v", err)
		}
		if final.Name != "Updated TX User" || *final.Age != 30 {
			t.Errorf("expected Updated TX User, 30, got %s, %d", final.Name, *final.Age)
		}

		t.Logf("Transaction user ID: %d", user.ID)
	})

	t.Run("delete within transaction", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		// Insert data outside transaction
		_, err := c.Insert().Exec(ctx, &TestUser{
			Email: "delete_me@example.com",
			Name:  "Delete Me",
			Age:   intPtr(25),
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		tx, err := tdb.db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}

		// Delete within transaction
		affected, err := c.Remove().
			Where("email", "=", "user_email").
			ExecTx(ctx, tx, map[string]any{"user_email": "delete_me@example.com"})
		if err != nil {
			tx.Rollback()
			t.Fatalf("Delete failed: %v", err)
		}
		if affected != 1 {
			t.Errorf("expected 1 row deleted, got %d", affected)
		}

		// Rollback
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback() failed: %v", err)
		}

		// Verify record still exists
		user, err := c.Select().
			Where("email", "=", "user_email").
			Exec(ctx, map[string]any{"user_email": "delete_me@example.com"})
		if err != nil {
			t.Fatalf("Select failed after rollback: %v", err)
		}
		if user.Name != "Delete Me" {
			t.Errorf("expected Delete Me, got %s", user.Name)
		}
	})
}

func TestTransaction_Integration(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.cleanup(t)
	createTestTable(t, tdb.db)

	c, err := cereal.New[TestUser](tdb.db, "test_users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("successful transaction", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		tx, err := tdb.db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}

		// Insert within transaction
		user, err := c.Insert().ExecTx(ctx, tx, &TestUser{
			Email: "tx_user@example.com",
			Name:  "Transaction User",
			Age:   intPtr(25),
		})
		if err != nil {
			tx.Rollback()
			t.Fatalf("Insert().ExecTx() failed: %v", err)
		}

		// Update within transaction
		_, err = c.Modify().
			Set("name", "new_name").
			Where("email", "=", "user_email").
			ExecTx(ctx, tx, map[string]any{
				"user_email": "tx_user@example.com",
				"new_name":   "Updated TX User",
			})
		if err != nil {
			tx.Rollback()
			t.Fatalf("Modify().ExecTx() failed: %v", err)
		}

		// Commit
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit() failed: %v", err)
		}

		// Verify the changes persisted
		updated, err := c.Select().
			Where("email", "=", "user_email").
			Exec(ctx, map[string]any{"user_email": "tx_user@example.com"})
		if err != nil {
			t.Fatalf("Select().Exec() failed: %v", err)
		}
		if updated.Name != "Updated TX User" {
			t.Errorf("expected name Updated TX User, got %s", updated.Name)
		}

		t.Logf("Transaction user ID: %d", user.ID)
	})

	t.Run("rolled back transaction", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		tx, err := tdb.db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}

		// Insert within transaction
		_, err = c.Insert().ExecTx(ctx, tx, &TestUser{
			Email: "rollback_user@example.com",
			Name:  "Rollback User",
			Age:   intPtr(30),
		})
		if err != nil {
			tx.Rollback()
			t.Fatalf("Insert().ExecTx() failed: %v", err)
		}

		// Rollback instead of commit
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback() failed: %v", err)
		}

		// Verify the user does not exist
		_, err = c.Select().
			Where("email", "=", "user_email").
			Exec(ctx, map[string]any{"user_email": "rollback_user@example.com"})
		if err == nil {
			t.Error("expected error for non-existent user after rollback")
		}
	})

	t.Run("transaction with query", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		// Insert some data outside transaction
		_, err := c.Insert().Exec(ctx, &TestUser{
			Email: "existing@example.com",
			Name:  "Existing User",
			Age:   intPtr(40),
		})
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}

		tx, err := tdb.db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}

		// Query within transaction
		users, err := c.Query().ExecTx(ctx, tx, nil)
		if err != nil {
			tx.Rollback()
			t.Fatalf("Query().ExecTx() failed: %v", err)
		}
		if len(users) != 1 {
			t.Errorf("expected 1 user, got %d", len(users))
		}

		// Count within transaction
		count, err := c.Count().ExecTx(ctx, tx, nil)
		if err != nil {
			tx.Rollback()
			t.Fatalf("Count().ExecTx() failed: %v", err)
		}
		if count != 1 {
			t.Errorf("expected count 1, got %v", count)
		}

		tx.Commit()
	})
}

func TestExecBatchTx_Integration(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.cleanup(t)
	createTestTable(t, tdb.db)

	c, err := cereal.New[TestUser](tdb.db, "test_users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("Update ExecBatchTx success", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		// Insert test data
		_, err := c.Insert().Exec(ctx, &TestUser{Email: "user1@example.com", Name: "User 1", Age: intPtr(25)})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
		_, err = c.Insert().Exec(ctx, &TestUser{Email: "user2@example.com", Name: "User 2", Age: intPtr(30)})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		tx, err := tdb.db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}

		// Batch update within transaction
		paramsList := []map[string]any{
			{"user_email": "user1@example.com", "new_name": "Updated User 1"},
			{"user_email": "user2@example.com", "new_name": "Updated User 2"},
		}

		affected, err := c.Modify().
			Set("name", "new_name").
			Where("email", "=", "user_email").
			ExecBatchTx(ctx, tx, paramsList)
		if err != nil {
			tx.Rollback()
			t.Fatalf("ExecBatchTx failed: %v", err)
		}
		if affected != 2 {
			t.Errorf("expected 2 rows affected, got %d", affected)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// Verify updates
		users, err := c.Query().Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		for _, u := range users {
			if !containsString(u.Name, "Updated") {
				t.Errorf("expected name to contain 'Updated', got %s", u.Name)
			}
		}
	})

	t.Run("Update ExecBatchTx rollback", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		// Insert test data
		_, err := c.Insert().Exec(ctx, &TestUser{Email: "user1@example.com", Name: "User 1", Age: intPtr(25)})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		tx, err := tdb.db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}

		paramsList := []map[string]any{
			{"user_email": "user1@example.com", "new_name": "Should Not Persist"},
		}

		_, err = c.Modify().
			Set("name", "new_name").
			Where("email", "=", "user_email").
			ExecBatchTx(ctx, tx, paramsList)
		if err != nil {
			tx.Rollback()
			t.Fatalf("ExecBatchTx failed: %v", err)
		}

		// Rollback
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback failed: %v", err)
		}

		// Verify no changes
		user, err := c.Select().Where("email", "=", "email").Exec(ctx, map[string]any{"email": "user1@example.com"})
		if err != nil {
			t.Fatalf("Select failed: %v", err)
		}
		if user.Name != "User 1" {
			t.Errorf("expected name 'User 1', got %s", user.Name)
		}
	})

	t.Run("Delete ExecBatchTx success", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		// Insert test data
		_, err := c.Insert().Exec(ctx, &TestUser{Email: "delete1@example.com", Name: "Delete 1", Age: intPtr(25)})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
		_, err = c.Insert().Exec(ctx, &TestUser{Email: "delete2@example.com", Name: "Delete 2", Age: intPtr(30)})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
		_, err = c.Insert().Exec(ctx, &TestUser{Email: "keep@example.com", Name: "Keep", Age: intPtr(35)})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		tx, err := tdb.db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}

		// Batch delete within transaction
		paramsList := []map[string]any{
			{"user_email": "delete1@example.com"},
			{"user_email": "delete2@example.com"},
		}

		affected, err := c.Remove().
			Where("email", "=", "user_email").
			ExecBatchTx(ctx, tx, paramsList)
		if err != nil {
			tx.Rollback()
			t.Fatalf("ExecBatchTx failed: %v", err)
		}
		if affected != 2 {
			t.Errorf("expected 2 rows affected, got %d", affected)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// Verify deletes
		count, err := c.Count().Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Count failed: %v", err)
		}
		if count != 1 {
			t.Errorf("expected 1 user remaining, got %v", count)
		}
	})

	t.Run("Delete ExecBatchTx rollback", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		// Insert test data
		_, err := c.Insert().Exec(ctx, &TestUser{Email: "user1@example.com", Name: "User 1", Age: intPtr(25)})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		tx, err := tdb.db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}

		paramsList := []map[string]any{
			{"user_email": "user1@example.com"},
		}

		_, err = c.Remove().
			Where("email", "=", "user_email").
			ExecBatchTx(ctx, tx, paramsList)
		if err != nil {
			tx.Rollback()
			t.Fatalf("ExecBatchTx failed: %v", err)
		}

		// Rollback
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback failed: %v", err)
		}

		// Verify user still exists
		count, err := c.Count().Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Count failed: %v", err)
		}
		if count != 1 {
			t.Errorf("expected 1 user after rollback, got %v", count)
		}
	})
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || s != "" && containsStringHelper(s, substr))
}

func containsStringHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
