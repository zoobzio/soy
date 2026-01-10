package integration

import (
	"context"
	"testing"

	"github.com/lib/pq"
	"github.com/zoobzio/astql/postgres"
	"github.com/zoobzio/soy"
)

func TestNullHandling_Integration(t *testing.T) {
	db := getTestDB(t)

	c, err := soy.New[TestUser](db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("insert with explicit NULL", func(t *testing.T) {
		truncateTestTable(t, db)

		// Insert record with NULL age
		record := &TestUser{
			Email: "nullage@example.com",
			Name:  "Null Age User",
			Age:   nil, // explicit NULL
		}

		user, err := c.Insert().Exec(ctx, record)
		if err != nil {
			t.Fatalf("Insert().Exec() failed: %v", err)
		}
		if user.Age != nil {
			t.Errorf("expected age to be nil, got %d", *user.Age)
		}

		// Verify via query
		fetched, err := c.Select().
			Where("email", "=", "user_email").
			Exec(ctx, map[string]any{"user_email": "nullage@example.com"})
		if err != nil {
			t.Fatalf("Select().Exec() failed: %v", err)
		}
		if fetched.Age != nil {
			t.Errorf("expected fetched age to be nil, got %d", *fetched.Age)
		}
	})

	t.Run("update field to NULL", func(t *testing.T) {
		truncateTestTable(t, db)

		// Insert record with non-NULL age
		_, err := c.Insert().Exec(ctx, &TestUser{
			Email: "updatetonull@example.com",
			Name:  "Update To Null",
			Age:   intPtr(30),
		})
		if err != nil {
			t.Fatalf("Insert().Exec() failed: %v", err)
		}

		// Update age to NULL (pass nil via params)
		params := map[string]any{
			"user_email": "updatetonull@example.com",
			"new_age":    nil,
		}
		updated, err := c.Modify().
			Set("age", "new_age").
			Where("email", "=", "user_email").
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Modify().Exec() failed: %v", err)
		}
		if updated.Age != nil {
			t.Errorf("expected updated age to be nil, got %d", *updated.Age)
		}
	})

	t.Run("WHERE IS NULL", func(t *testing.T) {
		truncateTestTable(t, db)

		// Insert users with and without age
		testUsers := []*TestUser{
			{Email: "hasage1@example.com", Name: "Has Age 1", Age: intPtr(25)},
			{Email: "hasage2@example.com", Name: "Has Age 2", Age: intPtr(30)},
			{Email: "noage1@example.com", Name: "No Age 1", Age: nil},
			{Email: "noage2@example.com", Name: "No Age 2", Age: nil},
		}
		for _, u := range testUsers {
			_, err := c.Insert().Exec(ctx, u)
			if err != nil {
				t.Fatalf("failed to insert: %v", err)
			}
		}

		// Query users with NULL age
		users, err := c.Query().
			WhereNull("age").
			Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Query().WhereNull().Exec() failed: %v", err)
		}
		if len(users) != 2 {
			t.Errorf("expected 2 users with NULL age, got %d", len(users))
		}
	})

	t.Run("WHERE IS NOT NULL", func(t *testing.T) {
		truncateTestTable(t, db)

		// Insert users with and without age
		testUsers := []*TestUser{
			{Email: "hasage@example.com", Name: "Has Age", Age: intPtr(25)},
			{Email: "noage@example.com", Name: "No Age", Age: nil},
		}
		for _, u := range testUsers {
			_, err := c.Insert().Exec(ctx, u)
			if err != nil {
				t.Fatalf("failed to insert: %v", err)
			}
		}

		// Query users with non-NULL age
		users, err := c.Query().
			WhereNotNull("age").
			Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Query().WhereNotNull().Exec() failed: %v", err)
		}
		if len(users) != 1 {
			t.Errorf("expected 1 user with non-NULL age, got %d", len(users))
		}
		if users[0].Email != "hasage@example.com" {
			t.Errorf("expected hasage@example.com, got %s", users[0].Email)
		}
	})

	t.Run("delete WHERE IS NULL", func(t *testing.T) {
		truncateTestTable(t, db)

		// Insert users with and without age
		testUsers := []*TestUser{
			{Email: "keep@example.com", Name: "Keep Me", Age: intPtr(25)},
			{Email: "delete@example.com", Name: "Delete Me", Age: nil},
		}
		for _, u := range testUsers {
			_, err := c.Insert().Exec(ctx, u)
			if err != nil {
				t.Fatalf("failed to insert: %v", err)
			}
		}

		// Delete users with NULL age
		affected, err := c.Remove().
			WhereNull("age").
			Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Remove().WhereNull().Exec() failed: %v", err)
		}
		if affected != 1 {
			t.Errorf("expected 1 row deleted, got %d", affected)
		}

		// Verify only one user remains
		remaining, err := c.Query().Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Query().Exec() failed: %v", err)
		}
		if len(remaining) != 1 {
			t.Errorf("expected 1 remaining user, got %d", len(remaining))
		}
	})
}

func TestComplexWhere_Integration(t *testing.T) {
	db := getTestDB(t)

	c, err := soy.New[TestUser](db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	// Insert test data
	truncateTestTable(t, db)
	testUsers := []*TestUser{
		{Email: "alice@example.com", Name: "Alice Smith", Age: intPtr(25)},
		{Email: "bob@example.com", Name: "Bob Jones", Age: intPtr(30)},
		{Email: "charlie@example.com", Name: "Charlie Brown", Age: intPtr(35)},
		{Email: "diana@test.org", Name: "Diana Prince", Age: intPtr(28)},
		{Email: "eve@test.org", Name: "Eve Wilson", Age: intPtr(32)},
	}
	for _, u := range testUsers {
		_, err := c.Insert().Exec(ctx, u)
		if err != nil {
			t.Fatalf("failed to insert test data: %v", err)
		}
	}

	t.Run("LIKE pattern matching", func(t *testing.T) {
		params := map[string]any{"pattern": "%Smith%"}
		users, err := c.Query().
			Where("name", "LIKE", "pattern").
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Query().Where(LIKE).Exec() failed: %v", err)
		}
		if len(users) != 1 {
			t.Errorf("expected 1 user matching 'Smith', got %d", len(users))
		}
		if len(users) > 0 && users[0].Name != "Alice Smith" {
			t.Errorf("expected Alice Smith, got %s", users[0].Name)
		}
	})

	t.Run("LIKE prefix matching", func(t *testing.T) {
		params := map[string]any{"pattern": "%@test.org"}
		users, err := c.Query().
			Where("email", "LIKE", "pattern").
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Query().Where(LIKE).Exec() failed: %v", err)
		}
		if len(users) != 2 {
			t.Errorf("expected 2 users with @test.org email, got %d", len(users))
		}
	})

	t.Run("NOT LIKE pattern", func(t *testing.T) {
		params := map[string]any{"pattern": "%@test.org"}
		users, err := c.Query().
			Where("email", "NOT LIKE", "pattern").
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Query().Where(NOT LIKE).Exec() failed: %v", err)
		}
		if len(users) != 3 {
			t.Errorf("expected 3 users without @test.org email, got %d", len(users))
		}
	})

	t.Run("IN operator", func(t *testing.T) {
		params := map[string]any{"ages": pq.Array([]int{25, 30, 35})}
		users, err := c.Query().
			Where("age", "IN", "ages").
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Query().Where(IN).Exec() failed: %v", err)
		}
		if len(users) != 3 {
			t.Errorf("expected 3 users with ages in (25, 30, 35), got %d", len(users))
		}
	})

	t.Run("NOT IN operator", func(t *testing.T) {
		params := map[string]any{"ages": pq.Array([]int{25, 30, 35})}
		users, err := c.Query().
			Where("age", "NOT IN", "ages").
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Query().Where(NOT IN).Exec() failed: %v", err)
		}
		if len(users) != 2 {
			t.Errorf("expected 2 users with ages not in (25, 30, 35), got %d", len(users))
		}
	})

	t.Run("combined AND and OR conditions", func(t *testing.T) {
		// (age >= 30 AND age <= 35) - should get Bob, Charlie, Eve
		params := map[string]any{"min_age": 30, "max_age": 35}
		users, err := c.Query().
			WhereAnd(
				soy.C("age", ">=", "min_age"),
				soy.C("age", "<=", "max_age"),
			).
			OrderBy("age", "ASC").
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Query().WhereAnd().Exec() failed: %v", err)
		}
		if len(users) != 3 {
			t.Errorf("expected 3 users with age 30-35, got %d", len(users))
		}
	})

	t.Run("OR with multiple conditions", func(t *testing.T) {
		// age = 25 OR age = 35 - should get Alice, Charlie
		params := map[string]any{"age1": 25, "age2": 35}
		users, err := c.Query().
			WhereOr(
				soy.C("age", "=", "age1"),
				soy.C("age", "=", "age2"),
			).
			OrderBy("age", "ASC").
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Query().WhereOr().Exec() failed: %v", err)
		}
		if len(users) != 2 {
			t.Errorf("expected 2 users with age 25 or 35, got %d", len(users))
		}
	})

	t.Run("combined WhereAnd with Null condition", func(t *testing.T) {
		// Add a user with NULL age
		_, err := c.Insert().Exec(ctx, &TestUser{
			Email: "noage@example.com",
			Name:  "No Age User",
			Age:   nil,
		})
		if err != nil {
			t.Fatalf("failed to insert: %v", err)
		}

		// Query: age IS NOT NULL AND age >= 30
		params := map[string]any{"min_age": 30}
		users, err := c.Query().
			WhereAnd(
				soy.NotNull("age"),
				soy.C("age", ">=", "min_age"),
			).
			OrderBy("age", "ASC").
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Query().WhereAnd(NotNull).Exec() failed: %v", err)
		}
		if len(users) != 3 {
			t.Errorf("expected 3 users with non-null age >= 30, got %d", len(users))
		}
	})

	t.Run("WhereOr with Null condition", func(t *testing.T) {
		// age IS NULL OR age = 25
		params := map[string]any{"target_age": 25}
		users, err := c.Query().
			WhereOr(
				soy.Null("age"),
				soy.C("age", "=", "target_age"),
			).
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Query().WhereOr(Null).Exec() failed: %v", err)
		}
		if len(users) != 2 {
			t.Errorf("expected 2 users (null age or age=25), got %d", len(users))
		}
	})
}
