// Package integration provides integration tests for cereal.
// These tests use testcontainers to spin up a PostgreSQL database automatically.
package integration

import (
	"context"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
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

// intPtr returns a pointer to an int.
func intPtr(i int) *int {
	return &i
}

// boolPtr returns a pointer to a bool.
func boolPtr(b bool) *bool {
	return &b
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

// createExtendedTestTable creates the test_users_extended table.
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

// truncateExtendedTestTable clears the test_users_extended table.
func truncateExtendedTestTable(t *testing.T, db *sqlx.DB) {
	t.Helper()
	_, err := db.Exec(`TRUNCATE TABLE test_users_extended RESTART IDENTITY`)
	if err != nil {
		t.Fatalf("failed to truncate extended table: %v", err)
	}
}
