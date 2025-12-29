package integration

import (
	"context"
	"testing"

	"github.com/zoobzio/astql/pkg/postgres"
	"github.com/zoobzio/soy"
)

func TestAggregates_Integration(t *testing.T) {
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
		{Email: "agg1@example.com", Name: "User 1", Age: intPtr(20)},
		{Email: "agg2@example.com", Name: "User 2", Age: intPtr(30)},
		{Email: "agg3@example.com", Name: "User 3", Age: intPtr(40)},
		{Email: "agg4@example.com", Name: "User 4", Age: intPtr(50)},
		{Email: "agg5@example.com", Name: "User 5", Age: nil}, // NULL age
	}
	for _, u := range testUsers {
		_, err := c.Insert().Exec(ctx, u)
		if err != nil {
			t.Fatalf("failed to insert test data: %v", err)
		}
	}

	t.Run("count all", func(t *testing.T) {
		count, err := c.Count().Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Count().Exec() failed: %v", err)
		}
		if count != 5 {
			t.Errorf("expected count 5, got %v", count)
		}
	})

	t.Run("count with WHERE", func(t *testing.T) {
		params := map[string]any{"min_age": 30}
		count, err := c.Count().
			Where("age", ">=", "min_age").
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Count().Where().Exec() failed: %v", err)
		}
		if count != 3 {
			t.Errorf("expected count 3, got %v", count)
		}
	})

	t.Run("count with WhereNotNull", func(t *testing.T) {
		count, err := c.Count().
			WhereNotNull("age").
			Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Count().WhereNotNull().Exec() failed: %v", err)
		}
		if count != 4 {
			t.Errorf("expected count 4 (excluding NULL age), got %v", count)
		}
	})

	t.Run("sum", func(t *testing.T) {
		sum, err := c.Sum("age").Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Sum().Exec() failed: %v", err)
		}
		// 20 + 30 + 40 + 50 = 140 (NULL is ignored)
		if sum != 140 {
			t.Errorf("expected sum 140, got %v", sum)
		}
	})

	t.Run("avg", func(t *testing.T) {
		avg, err := c.Avg("age").Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Avg().Exec() failed: %v", err)
		}
		// (20 + 30 + 40 + 50) / 4 = 35 (NULL is ignored)
		if avg != 35 {
			t.Errorf("expected avg 35, got %v", avg)
		}
	})

	t.Run("min", func(t *testing.T) {
		minVal, err := c.Min("age").Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Min().Exec() failed: %v", err)
		}
		if minVal != 20 {
			t.Errorf("expected min 20, got %v", minVal)
		}
	})

	t.Run("max", func(t *testing.T) {
		maxVal, err := c.Max("age").Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Max().Exec() failed: %v", err)
		}
		if maxVal != 50 {
			t.Errorf("expected max 50, got %v", maxVal)
		}
	})
}

func TestAggregateEdgeCases_Integration(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.cleanup(t)
	createTestTable(t, tdb.db)

	c, err := soy.New[TestUser](tdb.db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("count on empty table", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		count, err := c.Count().Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Count().Exec() failed: %v", err)
		}
		if count != 0 {
			t.Errorf("expected count 0, got %v", count)
		}
	})

	t.Run("sum on empty table", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		sum, err := c.Sum("age").Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Sum().Exec() failed: %v", err)
		}
		// SUM of empty set returns NULL, which maps to 0
		if sum != 0 {
			t.Errorf("expected sum 0, got %v", sum)
		}
	})

	t.Run("avg on empty table", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		avg, err := c.Avg("age").Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Avg().Exec() failed: %v", err)
		}
		// AVG of empty set returns NULL, which maps to 0
		if avg != 0 {
			t.Errorf("expected avg 0, got %v", avg)
		}
	})

	t.Run("min/max on empty table", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		minVal, err := c.Min("age").Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Min().Exec() failed: %v", err)
		}
		if minVal != 0 {
			t.Errorf("expected min 0, got %v", minVal)
		}

		maxVal, err := c.Max("age").Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Max().Exec() failed: %v", err)
		}
		if maxVal != 0 {
			t.Errorf("expected max 0, got %v", maxVal)
		}
	})

	t.Run("aggregates with all NULL values", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		// Insert records with all NULL ages
		testUsers := []*TestUser{
			{Email: "null1@example.com", Name: "Null 1", Age: nil},
			{Email: "null2@example.com", Name: "Null 2", Age: nil},
			{Email: "null3@example.com", Name: "Null 3", Age: nil},
		}
		for _, u := range testUsers {
			_, err := c.Insert().Exec(ctx, u)
			if err != nil {
				t.Fatalf("failed to insert: %v", err)
			}
		}

		// COUNT should return 3 (counts rows, not values)
		count, err := c.Count().Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Count().Exec() failed: %v", err)
		}
		if count != 3 {
			t.Errorf("expected count 3, got %v", count)
		}

		// SUM of all NULLs returns NULL (0)
		sum, err := c.Sum("age").Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Sum().Exec() failed: %v", err)
		}
		if sum != 0 {
			t.Errorf("expected sum 0 (all nulls), got %v", sum)
		}

		// AVG of all NULLs returns NULL (0)
		avg, err := c.Avg("age").Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Avg().Exec() failed: %v", err)
		}
		if avg != 0 {
			t.Errorf("expected avg 0 (all nulls), got %v", avg)
		}
	})

	t.Run("aggregates with mixed NULL and values", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

		// Insert records with mix of NULL and non-NULL ages
		testUsers := []*TestUser{
			{Email: "val1@example.com", Name: "Val 1", Age: intPtr(10)},
			{Email: "null1@example.com", Name: "Null 1", Age: nil},
			{Email: "val2@example.com", Name: "Val 2", Age: intPtr(20)},
			{Email: "null2@example.com", Name: "Null 2", Age: nil},
			{Email: "val3@example.com", Name: "Val 3", Age: intPtr(30)},
		}
		for _, u := range testUsers {
			_, err := c.Insert().Exec(ctx, u)
			if err != nil {
				t.Fatalf("failed to insert: %v", err)
			}
		}

		// COUNT should return 5 (all rows)
		count, err := c.Count().Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Count().Exec() failed: %v", err)
		}
		if count != 5 {
			t.Errorf("expected count 5, got %v", count)
		}

		// SUM ignores NULLs: 10 + 20 + 30 = 60
		sum, err := c.Sum("age").Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Sum().Exec() failed: %v", err)
		}
		if sum != 60 {
			t.Errorf("expected sum 60, got %v", sum)
		}

		// AVG ignores NULLs: (10 + 20 + 30) / 3 = 20
		avg, err := c.Avg("age").Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Avg().Exec() failed: %v", err)
		}
		if avg != 20 {
			t.Errorf("expected avg 20, got %v", avg)
		}

		// MIN ignores NULLs: 10
		minVal, err := c.Min("age").Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Min().Exec() failed: %v", err)
		}
		if minVal != 10 {
			t.Errorf("expected min 10, got %v", minVal)
		}

		// MAX ignores NULLs: 30
		maxVal, err := c.Max("age").Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Max().Exec() failed: %v", err)
		}
		if maxVal != 30 {
			t.Errorf("expected max 30, got %v", maxVal)
		}
	})
}
