package integration

import (
	"context"
	"testing"

	"github.com/zoobzio/astql/pkg/postgres"
	"github.com/zoobzio/soy"
)

func TestCompound_Integration(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.cleanup(t)
	createTestTable(t, tdb.db)

	c, err := soy.New[TestUser](tdb.db, "test_users", postgres.New())
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

	t.Run("UNION combines results", func(t *testing.T) {
		// Get users with age >= 30 OR name = 'Alice' (who is 25)
		q1 := c.Query().Where("age", ">=", "min_age")
		q2 := c.Query().Where("name", "=", "target_name")

		compound := q1.Union(q2).OrderBy("name", "ASC")
		users, err := compound.Exec(ctx, map[string]any{
			"q0_min_age":     30,
			"q1_target_name": "Alice",
		})
		if err != nil {
			t.Fatalf("Union().Exec() failed: %v", err)
		}

		// Should get Alice (25, by name), Bob (30), Charlie (35), Eve (32)
		if len(users) != 4 {
			t.Errorf("expected 4 users, got %d", len(users))
		}

		// Verify alphabetical order
		expectedNames := []string{"Alice", "Bob", "Charlie", "Eve"}
		for i, name := range expectedNames {
			if i < len(users) && users[i].Name != name {
				t.Errorf("position %d: expected %s, got %s", i, name, users[i].Name)
			}
		}
	})

	t.Run("UNION ALL keeps duplicates", func(t *testing.T) {
		// Get users with age >= 30 (Bob, Charlie, Eve) AND age <= 35 (all)
		// The intersection (Bob, Charlie, Eve) should appear twice with UNION ALL
		q1 := c.Query().Where("age", ">=", "min_age")
		q2 := c.Query().Where("age", ">=", "threshold")

		compound := q1.UnionAll(q2).OrderBy("name", "ASC")
		users, err := compound.Exec(ctx, map[string]any{
			"q0_min_age":   30,
			"q1_threshold": 32, // Eve and Charlie
		})
		if err != nil {
			t.Fatalf("UnionAll().Exec() failed: %v", err)
		}

		// q1: Bob(30), Charlie(35), Eve(32) = 3 users
		// q2: Charlie(35), Eve(32) = 2 users
		// Total with duplicates: 5 users
		if len(users) != 5 {
			t.Errorf("expected 5 users (with duplicates), got %d", len(users))
		}
	})

	t.Run("INTERSECT returns common rows", func(t *testing.T) {
		// Get users with age >= 30 AND age <= 32
		q1 := c.Query().Where("age", ">=", "min_age")
		q2 := c.Query().Where("age", "<=", "max_age")

		compound := q1.Intersect(q2).OrderBy("age", "ASC")
		users, err := compound.Exec(ctx, map[string]any{
			"q0_min_age": 30,
			"q1_max_age": 32,
		})
		if err != nil {
			t.Fatalf("Intersect().Exec() failed: %v", err)
		}

		// Should get Bob (30) and Eve (32)
		if len(users) != 2 {
			t.Errorf("expected 2 users in intersection, got %d", len(users))
		}
		if len(users) >= 2 {
			if users[0].Name != "Bob" {
				t.Errorf("expected first user to be Bob, got %s", users[0].Name)
			}
			if users[1].Name != "Eve" {
				t.Errorf("expected second user to be Eve, got %s", users[1].Name)
			}
		}
	})

	t.Run("EXCEPT removes matching rows", func(t *testing.T) {
		// Get users with age >= 28 EXCEPT those with age >= 32
		q1 := c.Query().Where("age", ">=", "min_age")
		q2 := c.Query().Where("age", ">=", "exclude_age")

		compound := q1.Except(q2).OrderBy("age", "ASC")
		users, err := compound.Exec(ctx, map[string]any{
			"q0_min_age":     28,
			"q1_exclude_age": 32,
		})
		if err != nil {
			t.Fatalf("Except().Exec() failed: %v", err)
		}

		// q1: Diana(28), Bob(30), Eve(32), Charlie(35) = 4 users
		// q2: Eve(32), Charlie(35) = 2 users
		// Except: Diana(28), Bob(30) = 2 users
		if len(users) != 2 {
			t.Errorf("expected 2 users after except, got %d", len(users))
		}
		if len(users) >= 2 {
			if users[0].Name != "Diana" {
				t.Errorf("expected first user to be Diana, got %s", users[0].Name)
			}
			if users[1].Name != "Bob" {
				t.Errorf("expected second user to be Bob, got %s", users[1].Name)
			}
		}
	})

	t.Run("compound with LIMIT", func(t *testing.T) {
		q1 := c.Query().Where("age", ">=", "min_age")
		q2 := c.Query().Where("name", "=", "target_name")

		compound := q1.Union(q2).OrderBy("name", "ASC").Limit(2)
		users, err := compound.Exec(ctx, map[string]any{
			"q0_min_age":     30,
			"q1_target_name": "Alice",
		})
		if err != nil {
			t.Fatalf("Union().Limit().Exec() failed: %v", err)
		}

		if len(users) != 2 {
			t.Errorf("expected 2 users with LIMIT 2, got %d", len(users))
		}
		// Should be Alice and Bob (first 2 alphabetically)
		if len(users) >= 2 {
			if users[0].Name != "Alice" {
				t.Errorf("expected first user to be Alice, got %s", users[0].Name)
			}
			if users[1].Name != "Bob" {
				t.Errorf("expected second user to be Bob, got %s", users[1].Name)
			}
		}
	})

	t.Run("compound with OFFSET", func(t *testing.T) {
		q1 := c.Query().Where("age", ">=", "min_age")
		q2 := c.Query().Where("name", "=", "target_name")

		compound := q1.Union(q2).OrderBy("name", "ASC").Offset(2)
		users, err := compound.Exec(ctx, map[string]any{
			"q0_min_age":     30,
			"q1_target_name": "Alice",
		})
		if err != nil {
			t.Fatalf("Union().Offset().Exec() failed: %v", err)
		}

		// Total 4 users (Alice, Bob, Charlie, Eve), skip 2
		if len(users) != 2 {
			t.Errorf("expected 2 users with OFFSET 2, got %d", len(users))
		}
		// Should be Charlie and Eve (last 2 alphabetically)
		if len(users) >= 2 {
			if users[0].Name != "Charlie" {
				t.Errorf("expected first user to be Charlie, got %s", users[0].Name)
			}
			if users[1].Name != "Eve" {
				t.Errorf("expected second user to be Eve, got %s", users[1].Name)
			}
		}
	})

	t.Run("chained set operations", func(t *testing.T) {
		// (age >= 30) UNION (name = 'Alice') UNION (name = 'Diana')
		q1 := c.Query().Where("age", ">=", "min_age")
		q2 := c.Query().Where("name", "=", "name1")
		q3 := c.Query().Where("name", "=", "name2")

		compound := q1.Union(q2).Union(q3).OrderBy("name", "ASC")
		users, err := compound.Exec(ctx, map[string]any{
			"q0_min_age": 30,
			"q1_name1":   "Alice",
			"q2_name2":   "Diana",
		})
		if err != nil {
			t.Fatalf("Union().Union().Exec() failed: %v", err)
		}

		// Should get Alice, Bob, Charlie, Diana, Eve (all 5)
		if len(users) != 5 {
			t.Errorf("expected 5 users, got %d", len(users))
		}
	})
}
