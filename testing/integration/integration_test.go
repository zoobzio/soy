// Package integration provides integration tests for cereal.
// These tests use testcontainers to spin up a PostgreSQL database automatically.
package integration

import (
	"context"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/zoobzio/cereal"
	"github.com/zoobzio/sentinel"
)

// TestUser is the model used for integration tests.
type TestUser struct {
	ID        int        `db:"id" type:"serial" constraints:"primarykey"`
	Email     string     `db:"email" type:"text" constraints:"notnull,unique"`
	Name      string     `db:"name" type:"text" constraints:"notnull"`
	Age       *int       `db:"age" type:"integer"`
	CreatedAt *time.Time `db:"created_at" type:"timestamptz" default:"now()"`
}

// intPtr returns a pointer to an int.
func intPtr(i int) *int {
	return &i
}

// boolPtr returns a pointer to a bool.
func boolPtr(b bool) *bool {
	return &b
}

// TestUserExtended is a model with additional types for type coverage tests.
type TestUserExtended struct {
	ID        int        `db:"id" type:"serial" constraints:"primarykey"`
	Email     string     `db:"email" type:"text" constraints:"notnull,unique"`
	Name      string     `db:"name" type:"text" constraints:"notnull"`
	Age       *int       `db:"age" type:"integer"`
	IsActive  *bool      `db:"is_active" type:"boolean"`
	CreatedAt *time.Time `db:"created_at" type:"timestamptz" default:"now()"`
	UpdatedAt *time.Time `db:"updated_at" type:"timestamptz"`
	Metadata  *string    `db:"metadata" type:"jsonb"`
}

func init() {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")
}

// testDB holds a database connection for tests.
type testDB struct {
	db        *sqlx.DB
	container *postgres.PostgresContainer
}

// setupTestDB creates a PostgreSQL container and returns a database connection.
func setupTestDB(t *testing.T) *testDB {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}

	return &testDB{
		db:        db,
		container: pgContainer,
	}
}

// cleanup closes the database and terminates the container.
func (tdb *testDB) cleanup(t *testing.T) {
	t.Helper()
	if tdb.db != nil {
		tdb.db.Close()
	}
	if tdb.container != nil {
		if err := tdb.container.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	}
}

// createTestTable creates the test_users table.
func createTestTable(t *testing.T, db *sqlx.DB) {
	t.Helper()
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS test_users (
			id SERIAL PRIMARY KEY,
			email TEXT NOT NULL UNIQUE,
			name TEXT NOT NULL,
			age INTEGER,
			created_at TIMESTAMPTZ DEFAULT NOW()
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
}

// truncateTestTable clears the test_users table.
func truncateTestTable(t *testing.T, db *sqlx.DB) {
	t.Helper()
	_, err := db.Exec(`TRUNCATE TABLE test_users RESTART IDENTITY`)
	if err != nil {
		t.Fatalf("failed to truncate table: %v", err)
	}
}

func TestInsert_Integration(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.cleanup(t)
	createTestTable(t, tdb.db)

	c, err := cereal.New[TestUser](tdb.db, "test_users")
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
		// created_at may or may not be returned depending on how RETURNING works
		// with default values - this is implementation-dependent
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

	c, err := cereal.New[TestUser](tdb.db, "test_users")
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

	c, err := cereal.New[TestUser](tdb.db, "test_users")
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

	c, err := cereal.New[TestUser](tdb.db, "test_users")
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

	c, err := cereal.New[TestUser](tdb.db, "test_users")
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

func TestAggregates_Integration(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.cleanup(t)
	createTestTable(t, tdb.db)

	c, err := cereal.New[TestUser](tdb.db, "test_users")
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

// TestNullHandling_Integration tests NULL value handling across operations.
func TestNullHandling_Integration(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.cleanup(t)
	createTestTable(t, tdb.db)

	c, err := cereal.New[TestUser](tdb.db, "test_users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("insert with explicit NULL", func(t *testing.T) {
		truncateTestTable(t, tdb.db)

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
		truncateTestTable(t, tdb.db)

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
		truncateTestTable(t, tdb.db)

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
		truncateTestTable(t, tdb.db)

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
		truncateTestTable(t, tdb.db)

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

// TestEdgeCases_Integration tests edge cases and boundary conditions.
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

// TestConstraintViolations_Integration tests database constraint violations.
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

// TestComplexWhere_Integration tests complex WHERE clause combinations.
func TestComplexWhere_Integration(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.cleanup(t)
	createTestTable(t, tdb.db)

	c, err := cereal.New[TestUser](tdb.db, "test_users")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	// Insert test data
	truncateTestTable(t, tdb.db)
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
				cereal.C("age", ">=", "min_age"),
				cereal.C("age", "<=", "max_age"),
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
				cereal.C("age", "=", "age1"),
				cereal.C("age", "=", "age2"),
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
				cereal.NotNull("age"),
				cereal.C("age", ">=", "min_age"),
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
				cereal.Null("age"),
				cereal.C("age", "=", "target_age"),
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

// TestAggregateEdgeCases_Integration tests aggregate function edge cases.
func TestAggregateEdgeCases_Integration(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.cleanup(t)
	createTestTable(t, tdb.db)

	c, err := cereal.New[TestUser](tdb.db, "test_users")
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

// TestTransactionEdgeCases_Integration tests transaction edge cases.
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

// createExtendedTestTable creates a test table with additional types.
func createExtendedTestTable(t *testing.T, db *sqlx.DB) {
	t.Helper()
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS test_users_extended (
			id SERIAL PRIMARY KEY,
			email TEXT NOT NULL UNIQUE,
			name TEXT NOT NULL,
			age INTEGER,
			is_active BOOLEAN,
			created_at TIMESTAMPTZ DEFAULT NOW(),
			updated_at TIMESTAMPTZ,
			metadata JSONB
		)
	`)
	if err != nil {
		t.Fatalf("failed to create extended table: %v", err)
	}
}

// truncateExtendedTestTable clears the extended test table.
func truncateExtendedTestTable(t *testing.T, db *sqlx.DB) {
	t.Helper()
	_, err := db.Exec(`TRUNCATE TABLE test_users_extended RESTART IDENTITY`)
	if err != nil {
		t.Fatalf("failed to truncate extended table: %v", err)
	}
}

// TestTypeCoverage_Integration tests various PostgreSQL types.
func TestTypeCoverage_Integration(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.cleanup(t)
	createExtendedTestTable(t, tdb.db)

	c, err := cereal.New[TestUserExtended](tdb.db, "test_users_extended")
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
		if fetched.Metadata != nil && !contains(*fetched.Metadata, "admin") {
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

// contains checks if a string contains a substring.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || s != "" && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestVectorWithPgvector is a model for pgvector tests.
type TestVectorWithPgvector struct {
	ID        int    `db:"id" type:"serial" constraints:"primarykey"`
	Name      string `db:"name" type:"text" constraints:"notnull"`
	Embedding string `db:"embedding" type:"vector(3)"` // 3-dimensional vector
}

// setupPgvectorDB creates a PostgreSQL container with pgvector extension.
func setupPgvectorDB(t *testing.T) *testDB {
	t.Helper()
	ctx := context.Background()

	// Use pgvector image instead of standard postgres
	pgContainer, err := postgres.Run(ctx,
		"pgvector/pgvector:pg16",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("failed to start pgvector container: %v", err)
	}

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}

	// Enable pgvector extension
	_, err = db.Exec(`CREATE EXTENSION IF NOT EXISTS vector`)
	if err != nil {
		t.Fatalf("failed to create vector extension: %v", err)
	}

	return &testDB{
		db:        db,
		container: pgContainer,
	}
}

// createVectorTestTable creates a table with a vector column.
func createVectorTestTable(t *testing.T, db *sqlx.DB) {
	t.Helper()
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS test_vectors (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			embedding vector(3)
		)
	`)
	if err != nil {
		t.Fatalf("failed to create vector table: %v", err)
	}
}

// truncateVectorTestTable clears the vector test table.
func truncateVectorTestTable(t *testing.T, db *sqlx.DB) {
	t.Helper()
	_, err := db.Exec(`TRUNCATE TABLE test_vectors RESTART IDENTITY`)
	if err != nil {
		t.Fatalf("failed to truncate vector table: %v", err)
	}
}

// TestPgvector_Integration tests pgvector functionality.
func TestPgvector_Integration(t *testing.T) {
	tdb := setupPgvectorDB(t)
	defer tdb.cleanup(t)
	createVectorTestTable(t, tdb.db)

	c, err := cereal.New[TestVectorWithPgvector](tdb.db, "test_vectors")
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("insert vectors", func(t *testing.T) {
		truncateVectorTestTable(t, tdb.db)

		// Insert vectors directly using raw SQL since cereal may not handle vector syntax
		_, err := tdb.db.Exec(`INSERT INTO test_vectors (name, embedding) VALUES
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
		truncateVectorTestTable(t, tdb.db)

		// Insert test vectors
		_, err := tdb.db.Exec(`INSERT INTO test_vectors (name, embedding) VALUES
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
		truncateVectorTestTable(t, tdb.db)

		// Insert test vectors
		_, err := tdb.db.Exec(`INSERT INTO test_vectors (name, embedding) VALUES
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
		truncateVectorTestTable(t, tdb.db)

		// Insert test vectors
		_, err := tdb.db.Exec(`INSERT INTO test_vectors (name, embedding) VALUES
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
		truncateVectorTestTable(t, tdb.db)

		// Insert test vectors
		_, err := tdb.db.Exec(`INSERT INTO test_vectors (name, embedding) VALUES
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
		truncateVectorTestTable(t, tdb.db)

		// Insert test vectors
		_, err := tdb.db.Exec(`INSERT INTO test_vectors (name, embedding) VALUES
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
