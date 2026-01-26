// Package integration provides integration tests for soy.
// These tests use testcontainers to spin up a PostgreSQL database automatically.
package integration

import (
	"context"
	"fmt"
	"log"
	"os"
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
	Internal  string     `db:"-"` // Skipped field for coverage of db:"-" handling
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

// TestVectorWithPgvector is a model for pgvector tests.
type TestVectorWithPgvector struct {
	ID        int    `db:"id" type:"serial" constraints:"primarykey"`
	Name      string `db:"name" type:"text" constraints:"notnull"`
	Embedding string `db:"embedding" type:"vector(3)"` // 3-dimensional vector
}

// TestVectorWithDistance is a model for pgvector tests that includes a distance score.
// Used with SelectExpr to retrieve computed distance values.
type TestVectorWithDistance struct {
	ID        int     `db:"id" type:"serial" constraints:"primarykey"`
	Name      string  `db:"name" type:"text" constraints:"notnull"`
	Embedding string  `db:"embedding" type:"vector(3)"` // 3-dimensional vector
	Distance  float64 `db:"distance"`                   // computed distance from SelectExpr
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

// Shared test database - initialized once in TestMain.
var sharedDB *sqlx.DB
var sharedContainer *postgres.PostgresContainer

// TestMain sets up a shared PostgreSQL container for all integration tests.
func TestMain(m *testing.M) {
	ctx := context.Background()

	// Start pgvector container (superset of standard postgres)
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
		log.Fatalf("failed to start postgres container: %v", err)
	}
	sharedContainer = pgContainer

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Fatalf("failed to get connection string: %v", err)
	}

	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	sharedDB = db

	// Enable pgvector extension
	if _, err := db.Exec(`CREATE EXTENSION IF NOT EXISTS vector`); err != nil {
		log.Fatalf("failed to create vector extension: %v", err)
	}

	// Create all tables upfront
	if err := createAllTables(db); err != nil {
		log.Fatalf("failed to create tables: %v", err)
	}

	// Run tests
	code := m.Run()

	// Cleanup
	if sharedDB != nil {
		sharedDB.Close()
	}
	if sharedContainer != nil {
		if err := sharedContainer.Terminate(context.Background()); err != nil {
			log.Printf("failed to terminate container: %v", err)
		}
	}

	os.Exit(code)
}

// createAllTables creates all tables needed for integration tests.
func createAllTables(db *sqlx.DB) error {
	tables := []string{
		`CREATE TABLE IF NOT EXISTS test_users (
			id SERIAL PRIMARY KEY,
			email TEXT NOT NULL UNIQUE,
			name TEXT NOT NULL,
			age INTEGER,
			created_at TIMESTAMPTZ DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS test_users_extended (
			id SERIAL PRIMARY KEY,
			email TEXT NOT NULL UNIQUE,
			name TEXT NOT NULL,
			age INTEGER,
			is_active BOOLEAN,
			created_at TIMESTAMPTZ DEFAULT NOW(),
			updated_at TIMESTAMPTZ,
			metadata JSONB
		)`,
		`CREATE TABLE IF NOT EXISTS test_vectors (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			embedding vector(3)
		)`,
	}

	for _, ddl := range tables {
		if _, err := db.Exec(ddl); err != nil {
			return fmt.Errorf("failed to execute DDL: %w", err)
		}
	}
	return nil
}

// getTestDB returns the shared database connection.
func getTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	if sharedDB == nil {
		t.Fatal("shared database not initialized - TestMain may not have run")
	}
	return sharedDB
}

// truncateTestTable clears the test_users table.
func truncateTestTable(t *testing.T, db *sqlx.DB) {
	t.Helper()
	_, err := db.Exec(`TRUNCATE TABLE test_users RESTART IDENTITY`)
	if err != nil {
		t.Fatalf("failed to truncate table: %v", err)
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

// truncateVectorTestTable clears the vector test table.
func truncateVectorTestTable(t *testing.T, db *sqlx.DB) {
	t.Helper()
	_, err := db.Exec(`TRUNCATE TABLE test_vectors RESTART IDENTITY`)
	if err != nil {
		t.Fatalf("failed to truncate vector table: %v", err)
	}
}
