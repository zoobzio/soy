package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/zoobzio/astql/pkg/postgres"
	"github.com/zoobzio/cereal"
)

func TestTypeCoverage_Integration(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.cleanup(t)
	createExtendedTestTable(t, tdb.db)

	c, err := cereal.New[TestUserExtended](tdb.db, "test_users_extended", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("boolean type - true", func(t *testing.T) {
		truncateExtendedTestTable(t, tdb.db)

		record := &TestUserExtended{
			Email:    "active@example.com",
			Name:     "Active User",
			IsActive: boolPtr(true),
		}

		user, err := c.Insert().Exec(ctx, record)
		if err != nil {
			t.Fatalf("Insert().Exec() failed: %v", err)
		}
		if user.IsActive == nil || !*user.IsActive {
			t.Errorf("expected IsActive to be true")
		}

		// Query back
		fetched, err := c.Select().
			Where("email", "=", "email").
			Exec(ctx, map[string]any{"email": "active@example.com"})
		if err != nil {
			t.Fatalf("Select().Exec() failed: %v", err)
		}
		if fetched.IsActive == nil || !*fetched.IsActive {
			t.Errorf("expected fetched IsActive to be true")
		}
	})

	t.Run("boolean type - false", func(t *testing.T) {
		truncateExtendedTestTable(t, tdb.db)

		record := &TestUserExtended{
			Email:    "inactive@example.com",
			Name:     "Inactive User",
			IsActive: boolPtr(false),
		}

		user, err := c.Insert().Exec(ctx, record)
		if err != nil {
			t.Fatalf("Insert().Exec() failed: %v", err)
		}
		if user.IsActive == nil || *user.IsActive {
			t.Errorf("expected IsActive to be false")
		}
	})

	t.Run("boolean type - NULL", func(t *testing.T) {
		truncateExtendedTestTable(t, tdb.db)

		record := &TestUserExtended{
			Email:    "unknown@example.com",
			Name:     "Unknown Status User",
			IsActive: nil,
		}

		user, err := c.Insert().Exec(ctx, record)
		if err != nil {
			t.Fatalf("Insert().Exec() failed: %v", err)
		}
		if user.IsActive != nil {
			t.Errorf("expected IsActive to be nil")
		}
	})

	t.Run("timestamp type - with value", func(t *testing.T) {
		truncateExtendedTestTable(t, tdb.db)

		now := time.Now().UTC().Truncate(time.Microsecond) // PostgreSQL has microsecond precision
		record := &TestUserExtended{
			Email:     "timestamped@example.com",
			Name:      "Timestamped User",
			UpdatedAt: &now,
		}

		user, err := c.Insert().Exec(ctx, record)
		if err != nil {
			t.Fatalf("Insert().Exec() failed: %v", err)
		}
		if user.UpdatedAt == nil {
			t.Error("expected UpdatedAt to be set")
		} else {
			// Allow some tolerance for timezone conversion
			diff := user.UpdatedAt.Sub(now)
			if diff < -time.Second || diff > time.Second {
				t.Errorf("UpdatedAt differs too much: got %v, want ~%v", user.UpdatedAt, now)
			}
		}
	})

	t.Run("timestamp type - NULL", func(t *testing.T) {
		truncateExtendedTestTable(t, tdb.db)

		record := &TestUserExtended{
			Email:     "notimestamp@example.com",
			Name:      "No Timestamp User",
			UpdatedAt: nil,
		}

		user, err := c.Insert().Exec(ctx, record)
		if err != nil {
			t.Fatalf("Insert().Exec() failed: %v", err)
		}
		if user.UpdatedAt != nil {
			t.Errorf("expected UpdatedAt to be nil, got %v", user.UpdatedAt)
		}
	})

	t.Run("timestamp default value (created_at)", func(t *testing.T) {
		truncateExtendedTestTable(t, tdb.db)

		beforeInsert := time.Now().UTC()

		// Insert omitting created_at to let DB default apply
		_, err := tdb.db.Exec(`INSERT INTO test_users_extended (email, name) VALUES ($1, $2)`,
			"defaulttime@example.com", "Default Time User")
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}

		afterInsert := time.Now().UTC()

		// Verify the DB default was applied
		var createdAt *time.Time
		err = tdb.db.Get(&createdAt, `SELECT created_at FROM test_users_extended WHERE email = $1`, "defaulttime@example.com")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		if createdAt == nil {
			t.Error("expected DB to set created_at default")
		} else if createdAt.Before(beforeInsert.Add(-time.Second)) || createdAt.After(afterInsert.Add(time.Second)) {
			t.Errorf("CreatedAt %v not within expected range [%v, %v]", *createdAt, beforeInsert, afterInsert)
		}
	})

	t.Run("JSONB type - simple object", func(t *testing.T) {
		truncateExtendedTestTable(t, tdb.db)

		jsonData := `{"role": "admin", "permissions": ["read", "write"]}`
		record := &TestUserExtended{
			Email:    "jsonuser@example.com",
			Name:     "JSON User",
			Metadata: &jsonData,
		}

		user, err := c.Insert().Exec(ctx, record)
		if err != nil {
			t.Fatalf("Insert().Exec() failed: %v", err)
		}
		if user.Metadata == nil {
			t.Error("expected Metadata to be set")
		}

		// Query back
		fetched, err := c.Select().
			Where("email", "=", "email").
			Exec(ctx, map[string]any{"email": "jsonuser@example.com"})
		if err != nil {
			t.Fatalf("Select().Exec() failed: %v", err)
		}
		if fetched.Metadata == nil {
			t.Error("expected fetched Metadata to be set")
		}
		// JSONB may reorder keys, so just check it contains expected content
		if fetched.Metadata != nil && !strings.Contains(*fetched.Metadata, "admin") {
			t.Errorf("Metadata doesn't contain expected content: %s", *fetched.Metadata)
		}
	})

	t.Run("JSONB type - NULL", func(t *testing.T) {
		truncateExtendedTestTable(t, tdb.db)

		record := &TestUserExtended{
			Email:    "nojson@example.com",
			Name:     "No JSON User",
			Metadata: nil,
		}

		user, err := c.Insert().Exec(ctx, record)
		if err != nil {
			t.Fatalf("Insert().Exec() failed: %v", err)
		}
		if user.Metadata != nil {
			t.Errorf("expected Metadata to be nil, got %s", *user.Metadata)
		}
	})

	t.Run("update timestamp", func(t *testing.T) {
		truncateExtendedTestTable(t, tdb.db)

		// Insert without updated_at
		_, err := c.Insert().Exec(ctx, &TestUserExtended{
			Email: "updatetime@example.com",
			Name:  "Update Time User",
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		// Update with timestamp
		now := time.Now().UTC().Truncate(time.Microsecond)
		params := map[string]any{
			"email":      "updatetime@example.com",
			"updated_at": now,
		}
		updated, err := c.Modify().
			Set("updated_at", "updated_at").
			Where("email", "=", "email").
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Modify().Exec() failed: %v", err)
		}
		if updated.UpdatedAt == nil {
			t.Error("expected UpdatedAt to be set after update")
		}
	})

	t.Run("query with boolean WHERE", func(t *testing.T) {
		truncateExtendedTestTable(t, tdb.db)

		// Insert mix of active/inactive users
		users := []*TestUserExtended{
			{Email: "active1@example.com", Name: "Active 1", IsActive: boolPtr(true)},
			{Email: "active2@example.com", Name: "Active 2", IsActive: boolPtr(true)},
			{Email: "inactive1@example.com", Name: "Inactive 1", IsActive: boolPtr(false)},
			{Email: "unknown1@example.com", Name: "Unknown 1", IsActive: nil},
		}
		for _, u := range users {
			_, err := c.Insert().Exec(ctx, u)
			if err != nil {
				t.Fatalf("failed to insert: %v", err)
			}
		}

		// Query active users
		params := map[string]any{"active": true}
		activeUsers, err := c.Query().
			Where("is_active", "=", "active").
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Query().Where(bool).Exec() failed: %v", err)
		}
		if len(activeUsers) != 2 {
			t.Errorf("expected 2 active users, got %d", len(activeUsers))
		}

		// Query inactive users
		params = map[string]any{"active": false}
		inactiveUsers, err := c.Query().
			Where("is_active", "=", "active").
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Query().Where(bool).Exec() failed: %v", err)
		}
		if len(inactiveUsers) != 1 {
			t.Errorf("expected 1 inactive user, got %d", len(inactiveUsers))
		}

		// Query users with NULL is_active
		nullActiveUsers, err := c.Query().
			WhereNull("is_active").
			Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Query().WhereNull().Exec() failed: %v", err)
		}
		if len(nullActiveUsers) != 1 {
			t.Errorf("expected 1 user with NULL is_active, got %d", len(nullActiveUsers))
		}
	})
}
