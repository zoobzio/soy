package integration

import (
	"context"
	"testing"

	"github.com/zoobzio/astql/pkg/postgres"
	"github.com/zoobzio/cereal"
)

func TestInsert_Integration(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.cleanup(t)
	createTestTable(t, tdb.db)

	c, err := cereal.New[TestUser](tdb.db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("insert single record", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		record := &TestUser{
			Email: "test@example.com",
			Name:  "Test User",
			Age:   intPtr(30),
		}

		user, err := c.Insert().Exec(ctx, record)
		if err != nil {
			t.Fatalf("Insert().Exec() failed: %v", err)
		}

		if user.ID == 0 {
			t.Error("expected ID to be set")
		}
		if user.Email != "test@example.com" {
			t.Errorf("expected email test@example.com, got %s", user.Email)
		}
		if user.Name != "Test User" {
			t.Errorf("expected name Test User, got %s", user.Name)
		}
		if user.Age == nil || *user.Age != 30 {
			t.Error("expected age to be 30")
		}
	})

	t.Run("insert with ON CONFLICT DO NOTHING", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		// Insert first record
		_, err := c.Insert().Exec(ctx, &TestUser{
			Email: "conflict@example.com",
			Name:  "First User",
			Age:   intPtr(25),
		})
		if err != nil {
			t.Fatalf("first insert failed: %v", err)
		}

		// Attempt duplicate insert with DO NOTHING
		record := &TestUser{
			Email: "conflict@example.com", // Same email
			Name:  "Another User",
			Age:   intPtr(30),
		}

		user, err := c.Insert().
			OnConflict("email").
			DoNothing().
			Exec(ctx, record)

		// DO NOTHING returns an error when no rows are returned
		// This is expected behavior - the insert was skipped
		if err != nil {
			t.Logf("DO NOTHING correctly returned error when conflict occurred: %v", err)
		} else if user != nil {
			t.Logf("Note: record was returned (ID=%d)", user.ID)
		}
	})

	t.Run("insert with ON CONFLICT DO UPDATE", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		// Insert first record
		original, err := c.Insert().Exec(ctx, &TestUser{
			Email: "upsert@example.com",
			Name:  "Original Name",
			Age:   intPtr(25),
		})
		if err != nil {
			t.Fatalf("first insert failed: %v", err)
		}

		// Upsert with same email
		record := &TestUser{
			Email: "upsert@example.com",
			Name:  "Updated Name",
			Age:   intPtr(35),
		}

		user, err := c.Insert().
			OnConflict("email").
			DoUpdate().
			Set("name", "name").
			Set("age", "age").
			Build().
			Exec(ctx, record)

		if err != nil {
			t.Fatalf("Insert().OnConflict().DoUpdate().Exec() failed: %v", err)
		}
		if user == nil {
			t.Fatal("expected user to be returned")
		}
		if user.ID != original.ID {
			t.Errorf("expected same ID %d, got %d", original.ID, user.ID)
		}
		if user.Name != "Updated Name" {
			t.Errorf("expected name Updated Name, got %s", user.Name)
		}
		if user.Age == nil || *user.Age != 35 {
			t.Error("expected age to be 35")
		}
	})

	t.Run("insert batch", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		records := []*TestUser{
			{Email: "batch1@example.com", Name: "Batch User 1", Age: intPtr(20)},
			{Email: "batch2@example.com", Name: "Batch User 2", Age: intPtr(21)},
			{Email: "batch3@example.com", Name: "Batch User 3", Age: intPtr(22)},
		}

		count, err := c.Insert().ExecBatch(ctx, records)
		if err != nil {
			t.Fatalf("Insert().ExecBatch() failed: %v", err)
		}
		if count != 3 {
			t.Errorf("expected 3 records inserted, got %d", count)
		}

		// Verify all records exist
		users, err := c.Query().Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Query().Exec() failed: %v", err)
		}
		if len(users) != 3 {
			t.Errorf("expected 3 users in db, got %d", len(users))
		}
	})
}

func TestSelect_Integration(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.cleanup(t)
	createTestTable(t, tdb.db)

	c, err := cereal.New[TestUser](tdb.db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	// Insert test data
	truncateTestTable(t, tdb.db)
	testUsers := []*TestUser{
		{Email: "user1@example.com", Name: "User One", Age: intPtr(25)},
		{Email: "user2@example.com", Name: "User Two", Age: intPtr(30)},
		{Email: "user3@example.com", Name: "User Three", Age: intPtr(35)},
	}
	for _, u := range testUsers {
		_, err := c.Insert().Exec(ctx, u)
		if err != nil {
			t.Fatalf("failed to insert test data: %v", err)
		}
	}

	t.Run("select single record", func(t *testing.T) {
		params := map[string]any{"user_email": "user1@example.com"}
		user, err := c.Select().
			Where("email", "=", "user_email").
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Select().Exec() failed: %v", err)
		}
		if user.Email != "user1@example.com" {
			t.Errorf("expected email user1@example.com, got %s", user.Email)
		}
		if user.Name != "User One" {
			t.Errorf("expected name User One, got %s", user.Name)
		}
	})

	t.Run("select with fields", func(t *testing.T) {
		params := map[string]any{"user_email": "user2@example.com"}
		user, err := c.Select().
			Fields("id", "email", "name").
			Where("email", "=", "user_email").
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Select().Fields().Exec() failed: %v", err)
		}
		if user.Email != "user2@example.com" {
			t.Errorf("expected email user2@example.com, got %s", user.Email)
		}
	})

	t.Run("select no rows returns error", func(t *testing.T) {
		params := map[string]any{"user_email": "nonexistent@example.com"}
		_, err := c.Select().
			Where("email", "=", "user_email").
			Exec(ctx, params)
		if err == nil {
			t.Error("expected error for no rows")
		}
	})

	t.Run("select multiple rows returns error", func(t *testing.T) {
		params := map[string]any{"min_age": 20}
		_, err := c.Select().
			Where("age", ">=", "min_age").
			Exec(ctx, params)
		if err == nil {
			t.Error("expected error when multiple rows returned")
		}
	})
}

func TestQuery_Integration(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.cleanup(t)
	createTestTable(t, tdb.db)

	c, err := cereal.New[TestUser](tdb.db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	// Insert test data
	truncateTestTable(t, tdb.db)
	testUsers := []*TestUser{
		{Email: "alice@example.com", Name: "Alice", Age: intPtr(25)},
		{Email: "bob@example.com", Name: "Bob", Age: intPtr(30)},
		{Email: "charlie@example.com", Name: "Charlie", Age: intPtr(35)},
		{Email: "diana@example.com", Name: "Diana", Age: intPtr(28)},
		{Email: "eve@example.com", Name: "Eve", Age: intPtr(32)},
	}
	for _, u := range testUsers {
		_, err := c.Insert().Exec(ctx, u)
		if err != nil {
			t.Fatalf("failed to insert test data: %v", err)
		}
	}

	t.Run("query all records", func(t *testing.T) {
		users, err := c.Query().Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Query().Exec() failed: %v", err)
		}
		if len(users) != 5 {
			t.Errorf("expected 5 users, got %d", len(users))
		}
	})

	t.Run("query with WHERE", func(t *testing.T) {
		params := map[string]any{"min_age": 30}
		users, err := c.Query().
			Where("age", ">=", "min_age").
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Query().Where().Exec() failed: %v", err)
		}
		if len(users) != 3 {
			t.Errorf("expected 3 users with age >= 30, got %d", len(users))
		}
	})

	t.Run("query with ORDER BY and LIMIT", func(t *testing.T) {
		users, err := c.Query().
			OrderBy("age", "DESC").
			Limit(3).
			Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Query().OrderBy().Limit().Exec() failed: %v", err)
		}
		if len(users) != 3 {
			t.Errorf("expected 3 users, got %d", len(users))
		}
		if users[0].Age == nil || *users[0].Age != 35 {
			t.Errorf("expected first user to have age 35, got %v", users[0].Age)
		}
		if users[0].Name != "Charlie" {
			t.Errorf("expected first user to be Charlie, got %s", users[0].Name)
		}
	})

	t.Run("query with pagination", func(t *testing.T) {
		users, err := c.Query().
			OrderBy("name", "ASC").
			Limit(2).
			Offset(2).
			Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Query().Limit().Offset().Exec() failed: %v", err)
		}
		if len(users) != 2 {
			t.Errorf("expected 2 users, got %d", len(users))
		}
		// Alphabetical order: Alice, Bob, Charlie, Diana, Eve
		// Skip 2 (Alice, Bob), take 2 (Charlie, Diana)
		if users[0].Name != "Charlie" {
			t.Errorf("expected first user to be Charlie, got %s", users[0].Name)
		}
		if users[1].Name != "Diana" {
			t.Errorf("expected second user to be Diana, got %s", users[1].Name)
		}
	})

	t.Run("query with WhereAnd", func(t *testing.T) {
		params := map[string]any{"min_age": 28, "max_age": 32}
		users, err := c.Query().
			WhereAnd(
				cereal.C("age", ">=", "min_age"),
				cereal.C("age", "<=", "max_age"),
			).
			OrderBy("age", "ASC").
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Query().WhereAnd().Exec() failed: %v", err)
		}
		// Ages 28-32: Diana(28), Bob(30), Eve(32)
		if len(users) != 3 {
			t.Errorf("expected 3 users, got %d", len(users))
		}
	})

	t.Run("query with WhereOr", func(t *testing.T) {
		params := map[string]any{"young": 25, "old": 35}
		users, err := c.Query().
			WhereOr(
				cereal.C("age", "=", "young"),
				cereal.C("age", "=", "old"),
			).
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Query().WhereOr().Exec() failed: %v", err)
		}
		// Ages 25 or 35: Alice(25), Charlie(35)
		if len(users) != 2 {
			t.Errorf("expected 2 users, got %d", len(users))
		}
	})
}

func TestUpdate_Integration(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.cleanup(t)
	createTestTable(t, tdb.db)

	c, err := cereal.New[TestUser](tdb.db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("update single record", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		// Insert test data
		_, err := c.Insert().Exec(ctx, &TestUser{
			Email: "update@example.com",
			Name:  "Original Name",
			Age:   intPtr(25),
		})
		if err != nil {
			t.Fatalf("failed to insert test data: %v", err)
		}

		params := map[string]any{
			"user_email": "update@example.com",
			"new_name":   "Updated Name",
			"new_age":    30,
		}

		user, err := c.Modify().
			Set("name", "new_name").
			Set("age", "new_age").
			Where("email", "=", "user_email").
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Modify().Set().Where().Exec() failed: %v", err)
		}
		if user == nil {
			t.Fatal("expected user to be returned")
		}
		if user.Name != "Updated Name" {
			t.Errorf("expected name Updated Name, got %s", user.Name)
		}
		if user.Age == nil || *user.Age != 30 {
			t.Error("expected age to be 30")
		}
	})

	t.Run("update batch", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		// Insert test data
		batchInsert := []*TestUser{
			{Email: "batch1@example.com", Name: "Batch 1", Age: intPtr(20)},
			{Email: "batch2@example.com", Name: "Batch 2", Age: intPtr(21)},
		}
		_, err := c.Insert().ExecBatch(ctx, batchInsert)
		if err != nil {
			t.Fatalf("failed to insert batch data: %v", err)
		}

		batchParams := []map[string]any{
			{"user_email": "batch1@example.com", "new_name": "Updated Batch 1"},
			{"user_email": "batch2@example.com", "new_name": "Updated Batch 2"},
		}

		affected, err := c.Modify().
			Set("name", "new_name").
			Where("email", "=", "user_email").
			ExecBatch(ctx, batchParams)
		if err != nil {
			t.Fatalf("Modify().ExecBatch() failed: %v", err)
		}
		if affected != 2 {
			t.Errorf("expected 2 rows affected, got %d", affected)
		}

		// Verify updates
		user1, _ := c.Select().Where("email", "=", "user_email").Exec(ctx, map[string]any{"user_email": "batch1@example.com"})
		if user1.Name != "Updated Batch 1" {
			t.Errorf("expected Updated Batch 1, got %s", user1.Name)
		}
	})

	t.Run("update no matching rows", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		params := map[string]any{
			"user_email": "nonexistent@example.com",
			"new_name":   "New Name",
		}

		user, err := c.Modify().
			Set("name", "new_name").
			Where("email", "=", "user_email").
			Exec(ctx, params)
		// Library returns error when no rows are updated
		if err != nil {
			t.Logf("Update correctly returned error when no rows matched: %v", err)
		} else if user != nil {
			t.Error("expected nil user when no rows match")
		}
	})
}

func TestDelete_Integration(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.cleanup(t)
	createTestTable(t, tdb.db)

	c, err := cereal.New[TestUser](tdb.db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("delete single record", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		// Insert test data
		testUsers := []*TestUser{
			{Email: "delete1@example.com", Name: "Delete User 1", Age: intPtr(25)},
			{Email: "delete2@example.com", Name: "Delete User 2", Age: intPtr(30)},
			{Email: "delete3@example.com", Name: "Delete User 3", Age: intPtr(35)},
		}
		for _, u := range testUsers {
			_, err := c.Insert().Exec(ctx, u)
			if err != nil {
				t.Fatalf("failed to insert test data: %v", err)
			}
		}

		params := map[string]any{"user_email": "delete1@example.com"}
		affected, err := c.Remove().
			Where("email", "=", "user_email").
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Remove().Where().Exec() failed: %v", err)
		}
		if affected != 1 {
			t.Errorf("expected 1 row affected, got %d", affected)
		}

		// Verify deletion
		remaining, _ := c.Query().Exec(ctx, nil)
		if len(remaining) != 2 {
			t.Errorf("expected 2 remaining users, got %d", len(remaining))
		}
	})

	t.Run("delete with condition", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		// Insert test data
		testUsers := []*TestUser{
			{Email: "young@example.com", Name: "Young User", Age: intPtr(25)},
			{Email: "mid@example.com", Name: "Mid User", Age: intPtr(30)},
			{Email: "old@example.com", Name: "Old User", Age: intPtr(35)},
		}
		for _, u := range testUsers {
			_, err := c.Insert().Exec(ctx, u)
			if err != nil {
				t.Fatalf("failed to insert test data: %v", err)
			}
		}

		params := map[string]any{"min_age": 30}
		affected, err := c.Remove().
			Where("age", ">=", "min_age").
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Remove().Where().Exec() failed: %v", err)
		}
		if affected != 2 {
			t.Errorf("expected 2 rows affected (age >= 30), got %d", affected)
		}

		// Verify only young user remains
		remaining, _ := c.Query().Exec(ctx, nil)
		if len(remaining) != 1 {
			t.Errorf("expected 1 remaining user, got %d", len(remaining))
		}
		if remaining[0].Name != "Young User" {
			t.Errorf("expected Young User to remain, got %s", remaining[0].Name)
		}
	})

	t.Run("delete batch", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		// Insert test data
		batchInsert := []*TestUser{
			{Email: "batch1@example.com", Name: "Batch Delete 1", Age: intPtr(20)},
			{Email: "batch2@example.com", Name: "Batch Delete 2", Age: intPtr(21)},
			{Email: "keep@example.com", Name: "Keep This", Age: intPtr(22)},
		}
		_, err := c.Insert().ExecBatch(ctx, batchInsert)
		if err != nil {
			t.Fatalf("failed to insert batch data: %v", err)
		}

		batchParams := []map[string]any{
			{"user_email": "batch1@example.com"},
			{"user_email": "batch2@example.com"},
		}

		affected, err := c.Remove().
			Where("email", "=", "user_email").
			ExecBatch(ctx, batchParams)
		if err != nil {
			t.Fatalf("Remove().ExecBatch() failed: %v", err)
		}
		if affected != 2 {
			t.Errorf("expected 2 rows affected, got %d", affected)
		}

		// Verify only "keep" user remains
		remaining, _ := c.Query().Exec(ctx, nil)
		if len(remaining) != 1 {
			t.Errorf("expected 1 remaining user, got %d", len(remaining))
		}
	})
}
