package integration

import (
	"context"
	"testing"

	astqlpg "github.com/zoobzio/astql/postgres"
	"github.com/zoobzio/soy"
)

func TestPgvector_Integration(t *testing.T) {
	db := getTestDB(t)

	c, err := soy.New[TestVectorWithPgvector](db, "test_vectors", astqlpg.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("insert vectors", func(t *testing.T) {
		truncateVectorTestTable(t, db)

		// Insert vectors directly using raw SQL since soy may not handle vector syntax
		_, err := db.Exec(`INSERT INTO test_vectors (name, embedding) VALUES
			('origin', '[0,0,0]'),
			('unit_x', '[1,0,0]'),
			('unit_y', '[0,1,0]'),
			('unit_z', '[0,0,1]'),
			('diagonal', '[1,1,1]')
		`)
		if err != nil {
			t.Fatalf("failed to insert vectors: %v", err)
		}

		// Verify records exist
		count, err := c.Count().Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Count().Exec() failed: %v", err)
		}
		if count != 5 {
			t.Errorf("expected 5 vectors, got %v", count)
		}
	})

	t.Run("query vectors", func(t *testing.T) {
		truncateVectorTestTable(t, db)

		// Insert test vectors
		_, err := db.Exec(`INSERT INTO test_vectors (name, embedding) VALUES
			('origin', '[0,0,0]'),
			('unit_x', '[1,0,0]'),
			('unit_y', '[0,1,0]'),
			('unit_z', '[0,0,1]'),
			('diagonal', '[1,1,1]')
		`)
		if err != nil {
			t.Fatalf("failed to insert vectors: %v", err)
		}

		// Query all vectors
		vectors, err := c.Query().Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Query().Exec() failed: %v", err)
		}
		if len(vectors) != 5 {
			t.Errorf("expected 5 vectors, got %d", len(vectors))
		}
	})

	t.Run("order by L2 distance", func(t *testing.T) {
		truncateVectorTestTable(t, db)

		// Insert test vectors
		_, err := db.Exec(`INSERT INTO test_vectors (name, embedding) VALUES
			('origin', '[0,0,0]'),
			('unit_x', '[1,0,0]'),
			('far', '[10,10,10]')
		`)
		if err != nil {
			t.Fatalf("failed to insert vectors: %v", err)
		}

		// Query ordered by L2 distance from origin
		// Using OrderByExpr with the <-> operator
		params := map[string]any{"query_vec": "[0,0,0]"}
		vectors, err := c.Query().
			OrderByExpr("embedding", "<->", "query_vec", "ASC").
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Query().OrderByExpr(<->).Exec() failed: %v", err)
		}
		if len(vectors) != 3 {
			t.Errorf("expected 3 vectors, got %d", len(vectors))
		}

		// Verify order: origin (distance 0), unit_x (distance 1), far (distance ~17.3)
		if vectors[0].Name != "origin" {
			t.Errorf("expected first vector to be 'origin', got '%s'", vectors[0].Name)
		}
		if vectors[1].Name != "unit_x" {
			t.Errorf("expected second vector to be 'unit_x', got '%s'", vectors[1].Name)
		}
		if vectors[2].Name != "far" {
			t.Errorf("expected third vector to be 'far', got '%s'", vectors[2].Name)
		}
	})

	t.Run("order by cosine distance", func(t *testing.T) {
		truncateVectorTestTable(t, db)

		// Insert test vectors
		_, err := db.Exec(`INSERT INTO test_vectors (name, embedding) VALUES
			('positive_x', '[1,0,0]'),
			('positive_y', '[0,1,0]'),
			('negative_x', '[-1,0,0]'),
			('diagonal', '[1,1,0]')
		`)
		if err != nil {
			t.Fatalf("failed to insert vectors: %v", err)
		}

		// Query ordered by cosine distance from [1,0,0]
		// Using OrderByExpr with the <=> operator
		params := map[string]any{"query_vec": "[1,0,0]"}
		vectors, err := c.Query().
			OrderByExpr("embedding", "<=>", "query_vec", "ASC").
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Query().OrderByExpr(<=>).Exec() failed: %v", err)
		}
		if len(vectors) != 4 {
			t.Errorf("expected 4 vectors, got %d", len(vectors))
		}

		// Cosine distance from [1,0,0]:
		// positive_x [1,0,0]: 0 (identical)
		// diagonal [1,1,0]: ~0.29 (45 degrees)
		// positive_y [0,1,0]: 1 (90 degrees)
		// negative_x [-1,0,0]: 2 (180 degrees)
		if vectors[0].Name != "positive_x" {
			t.Errorf("expected first vector to be 'positive_x', got '%s'", vectors[0].Name)
		}
		if vectors[3].Name != "negative_x" {
			t.Errorf("expected last vector to be 'negative_x', got '%s'", vectors[3].Name)
		}
	})

	t.Run("order by L2 distance with LIMIT", func(t *testing.T) {
		truncateVectorTestTable(t, db)

		// Insert test vectors
		_, err := db.Exec(`INSERT INTO test_vectors (name, embedding) VALUES
			('v1', '[0,0,0]'),
			('v2', '[1,0,0]'),
			('v3', '[2,0,0]'),
			('v4', '[3,0,0]'),
			('v5', '[4,0,0]')
		`)
		if err != nil {
			t.Fatalf("failed to insert vectors: %v", err)
		}

		// Query top 3 nearest to [0,0,0]
		params := map[string]any{"query_vec": "[0,0,0]"}
		vectors, err := c.Query().
			OrderByExpr("embedding", "<->", "query_vec", "ASC").
			Limit(3).
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Query().OrderByExpr().Limit().Exec() failed: %v", err)
		}
		if len(vectors) != 3 {
			t.Errorf("expected 3 vectors, got %d", len(vectors))
		}
		// Should be v1, v2, v3 (nearest neighbors)
		expectedNames := []string{"v1", "v2", "v3"}
		for i, v := range vectors {
			if v.Name != expectedNames[i] {
				t.Errorf("position %d: expected '%s', got '%s'", i, expectedNames[i], v.Name)
			}
		}
	})

	t.Run("order by inner product distance", func(t *testing.T) {
		truncateVectorTestTable(t, db)

		// Insert test vectors
		_, err := db.Exec(`INSERT INTO test_vectors (name, embedding) VALUES
			('small', '[1,1,1]'),
			('medium', '[2,2,2]'),
			('large', '[3,3,3]')
		`)
		if err != nil {
			t.Fatalf("failed to insert vectors: %v", err)
		}

		// Query ordered by inner product distance from [1,1,1]
		// Note: <#> returns negative inner product for use with ORDER BY ASC
		params := map[string]any{"query_vec": "[1,1,1]"}
		vectors, err := c.Query().
			OrderByExpr("embedding", "<#>", "query_vec", "ASC").
			Exec(ctx, params)
		if err != nil {
			t.Fatalf("Query().OrderByExpr(<#>).Exec() failed: %v", err)
		}
		if len(vectors) != 3 {
			t.Errorf("expected 3 vectors, got %d", len(vectors))
		}
		// Inner product with [1,1,1]:
		// large [3,3,3]: 3+3+3 = 9 (highest, so -9 is lowest with ASC)
		// medium [2,2,2]: 2+2+2 = 6
		// small [1,1,1]: 1+1+1 = 3 (lowest, so -3 is highest with ASC)
		if vectors[0].Name != "large" {
			t.Errorf("expected first (highest inner product) to be 'large', got '%s'", vectors[0].Name)
		}
	})
}
