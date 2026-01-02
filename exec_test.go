package soy

import (
	"context"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	astqlpg "github.com/zoobzio/astql/pkg/postgres"
	"github.com/zoobzio/sentinel"
)

// execTestUser is the model used for execution tests.
type execTestUser struct {
	ID    int    `db:"id" type:"serial" constraints:"primarykey"`
	Email string `db:"email" type:"text" constraints:"notnull,unique"`
	Name  string `db:"name" type:"text" constraints:"notnull"`
	Age   *int   `db:"age" type:"integer"`
}

// execTestDB holds a database connection for execution tests.
type execTestDB struct {
	db        *sqlx.DB
	container *postgres.PostgresContainer
}

// setupExecTestDB creates a PostgreSQL container and returns a database connection.
func setupExecTestDB(t *testing.T) *execTestDB {
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

	return &execTestDB{
		db:        db,
		container: pgContainer,
	}
}

// cleanup closes the database and terminates the container.
func (tdb *execTestDB) cleanup(t *testing.T) {
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

// createExecTestTable creates the test table.
func createExecTestTable(t *testing.T, db *sqlx.DB) {
	t.Helper()
	ctx := context.Background()
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS exec_test_users (
			id SERIAL PRIMARY KEY,
			email TEXT NOT NULL UNIQUE,
			name TEXT NOT NULL,
			age INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
}

// truncateExecTestTable clears the test table.
func truncateExecTestTable(t *testing.T, db *sqlx.DB) {
	t.Helper()
	ctx := context.Background()
	_, err := db.ExecContext(ctx, `TRUNCATE TABLE exec_test_users RESTART IDENTITY`)
	if err != nil {
		t.Fatalf("failed to truncate table: %v", err)
	}
}

// intPtrExec returns a pointer to an int.
func intPtrExec(i int) *int {
	return &i
}

func init() {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")
}

func TestExec_Insert(t *testing.T) {
	tdb := setupExecTestDB(t)
	defer tdb.cleanup(t)
	createExecTestTable(t, tdb.db)

	c, err := New[execTestUser](tdb.db, "exec_test_users", astqlpg.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("ConflictUpdate.Exec convenience method", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		// First insert
		_, err := c.Insert().Exec(ctx, &execTestUser{
			Email: "conflict@example.com",
			Name:  "Original",
			Age:   intPtrExec(25),
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		// Use the convenience Exec method directly on ConflictUpdate (not Build().Exec())
		user, err := c.Insert().
			OnConflict("email").
			DoUpdate().
			Set("name", "name").
			Exec(ctx, &execTestUser{
				Email: "conflict@example.com",
				Name:  "Updated",
				Age:   intPtrExec(30),
			})
		if err != nil {
			t.Fatalf("ConflictUpdate.Exec() failed: %v", err)
		}

		if user.Name != "Updated" {
			t.Errorf("expected name Updated, got %s", user.Name)
		}
	})

	t.Run("Insert.Exec", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		user, err := c.Insert().Exec(ctx, &execTestUser{
			Email: "test@example.com",
			Name:  "Test User",
			Age:   intPtrExec(25),
		})
		if err != nil {
			t.Fatalf("Insert().Exec() failed: %v", err)
		}

		if user.ID == 0 {
			t.Error("expected ID to be set")
		}
		if user.Email != "test@example.com" {
			t.Errorf("expected email test@example.com, got %s", user.Email)
		}
	})

	t.Run("Insert.ExecTx", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		tx, err := tdb.db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}

		user, err := c.Insert().ExecTx(ctx, tx, &execTestUser{
			Email: "tx@example.com",
			Name:  "TX User",
			Age:   intPtrExec(30),
		})
		if err != nil {
			tx.Rollback()
			t.Fatalf("Insert().ExecTx() failed: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit() failed: %v", err)
		}

		if user.ID == 0 {
			t.Error("expected ID to be set")
		}
	})

	t.Run("Insert.ExecAtom", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		atom, err := c.Insert().ExecAtom(ctx, map[string]any{
			"email": "insertatom@example.com",
			"name":  "Insert Atom User",
			"age":   40,
		})
		if err != nil {
			t.Fatalf("Insert().ExecAtom() failed: %v", err)
		}

		if atom.Ints["ID"] == 0 {
			t.Error("expected ID to be set")
		}
		if atom.Strings["Email"] != "insertatom@example.com" {
			t.Errorf("expected email insertatom@example.com, got %s", atom.Strings["Email"])
		}
		if atom.IntPtrs["Age"] == nil || *atom.IntPtrs["Age"] != 40 {
			t.Errorf("expected age 40, got %v", atom.IntPtrs["Age"])
		}
	})

	t.Run("Insert.ExecTxAtom", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		tx, err := tdb.db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}

		atom, err := c.Insert().ExecTxAtom(ctx, tx, map[string]any{
			"email": "txatom@example.com",
			"name":  "TX Atom User",
			"age":   45,
		})
		if err != nil {
			tx.Rollback()
			t.Fatalf("Insert().ExecTxAtom() failed: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit() failed: %v", err)
		}

		if atom.Ints["ID"] == 0 {
			t.Error("expected ID to be set")
		}
	})
}

func TestExec_Select(t *testing.T) {
	tdb := setupExecTestDB(t)
	defer tdb.cleanup(t)
	createExecTestTable(t, tdb.db)

	c, err := New[execTestUser](tdb.db, "exec_test_users", astqlpg.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("Select.Exec", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		// Insert test data
		_, err := c.Insert().Exec(ctx, &execTestUser{
			Email: "select@example.com",
			Name:  "Select User",
			Age:   intPtrExec(25),
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		user, err := c.Select().
			Where("email", "=", "email").
			Exec(ctx, map[string]any{"email": "select@example.com"})
		if err != nil {
			t.Fatalf("Select().Exec() failed: %v", err)
		}

		if user.Name != "Select User" {
			t.Errorf("expected name Select User, got %s", user.Name)
		}
	})

	t.Run("Select.ExecTx", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		// Insert test data
		_, err := c.Insert().Exec(ctx, &execTestUser{
			Email: "selecttx@example.com",
			Name:  "Select TX User",
			Age:   intPtrExec(30),
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		tx, err := tdb.db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}
		defer tx.Rollback()

		user, err := c.Select().
			Where("email", "=", "email").
			ExecTx(ctx, tx, map[string]any{"email": "selecttx@example.com"})
		if err != nil {
			t.Fatalf("Select().ExecTx() failed: %v", err)
		}

		if user.Name != "Select TX User" {
			t.Errorf("expected name Select TX User, got %s", user.Name)
		}

		tx.Commit()
	})

	t.Run("Select.ExecAtom", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		// Insert test data
		_, err := c.Insert().Exec(ctx, &execTestUser{
			Email: "selectatom@example.com",
			Name:  "Select Atom User",
			Age:   intPtrExec(35),
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		atom, err := c.Select().
			Where("email", "=", "email").
			ExecAtom(ctx, map[string]any{"email": "selectatom@example.com"})
		if err != nil {
			t.Fatalf("Select().ExecAtom() failed: %v", err)
		}

		if atom.Strings["Name"] != "Select Atom User" {
			t.Errorf("expected name Select Atom User, got %s", atom.Strings["Name"])
		}
		if atom.IntPtrs["Age"] == nil || *atom.IntPtrs["Age"] != 35 {
			t.Errorf("expected age 35, got %v", atom.IntPtrs["Age"])
		}
	})

	t.Run("Select.ExecTxAtom", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		// Insert test data
		_, err := c.Insert().Exec(ctx, &execTestUser{
			Email: "selecttxatom@example.com",
			Name:  "Select TX Atom User",
			Age:   intPtrExec(36),
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		tx, err := tdb.db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}
		defer tx.Rollback()

		atom, err := c.Select().
			Where("email", "=", "email").
			ExecTxAtom(ctx, tx, map[string]any{"email": "selecttxatom@example.com"})
		if err != nil {
			t.Fatalf("Select().ExecTxAtom() failed: %v", err)
		}

		if atom.Strings["Name"] != "Select TX Atom User" {
			t.Errorf("expected name Select TX Atom User, got %s", atom.Strings["Name"])
		}

		tx.Commit()
	})
}

func TestExec_Query(t *testing.T) {
	tdb := setupExecTestDB(t)
	defer tdb.cleanup(t)
	createExecTestTable(t, tdb.db)

	c, err := New[execTestUser](tdb.db, "exec_test_users", astqlpg.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("Query.Exec", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		// Insert test data
		for _, email := range []string{"a@example.com", "b@example.com", "c@example.com"} {
			_, err := c.Insert().Exec(ctx, &execTestUser{
				Email: email,
				Name:  "User",
				Age:   intPtrExec(25),
			})
			if err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
		}

		users, err := c.Query().Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Query().Exec() failed: %v", err)
		}

		if len(users) != 3 {
			t.Errorf("expected 3 users, got %d", len(users))
		}
	})

	t.Run("Query.ExecTx", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		// Insert test data
		_, err := c.Insert().Exec(ctx, &execTestUser{
			Email: "querytx@example.com",
			Name:  "Query TX User",
			Age:   intPtrExec(30),
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		tx, err := tdb.db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}
		defer tx.Rollback()

		users, err := c.Query().ExecTx(ctx, tx, nil)
		if err != nil {
			t.Fatalf("Query().ExecTx() failed: %v", err)
		}

		if len(users) != 1 {
			t.Errorf("expected 1 user, got %d", len(users))
		}

		tx.Commit()
	})

	t.Run("Query.ExecAtom", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		// Insert test data
		for i, email := range []string{"atom1@example.com", "atom2@example.com"} {
			_, err := c.Insert().Exec(ctx, &execTestUser{
				Email: email,
				Name:  "Atom User",
				Age:   intPtrExec(20 + i*5),
			})
			if err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
		}

		atoms, err := c.Query().OrderBy("email", "asc").ExecAtom(ctx, nil)
		if err != nil {
			t.Fatalf("Query().ExecAtom() failed: %v", err)
		}

		if len(atoms) != 2 {
			t.Fatalf("expected 2 atoms, got %d", len(atoms))
		}

		if atoms[0].Strings["Email"] != "atom1@example.com" {
			t.Errorf("expected first email atom1@example.com, got %s", atoms[0].Strings["Email"])
		}
		if atoms[1].IntPtrs["Age"] == nil || *atoms[1].IntPtrs["Age"] != 25 {
			t.Errorf("expected second age 25, got %v", atoms[1].IntPtrs["Age"])
		}
	})

	t.Run("Query.ExecTxAtom", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		// Insert test data
		_, err := c.Insert().Exec(ctx, &execTestUser{
			Email: "querytxatom@example.com",
			Name:  "Query TX Atom User",
			Age:   intPtrExec(50),
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		tx, err := tdb.db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}
		defer tx.Rollback()

		atoms, err := c.Query().ExecTxAtom(ctx, tx, nil)
		if err != nil {
			t.Fatalf("Query().ExecTxAtom() failed: %v", err)
		}

		if len(atoms) != 1 {
			t.Fatalf("expected 1 atom, got %d", len(atoms))
		}

		if atoms[0].Strings["Name"] != "Query TX Atom User" {
			t.Errorf("expected name Query TX Atom User, got %s", atoms[0].Strings["Name"])
		}

		tx.Commit()
	})
}

func TestExec_Update(t *testing.T) {
	tdb := setupExecTestDB(t)
	defer tdb.cleanup(t)
	createExecTestTable(t, tdb.db)

	c, err := New[execTestUser](tdb.db, "exec_test_users", astqlpg.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("Modify.Exec", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		// Insert test data
		_, err := c.Insert().Exec(ctx, &execTestUser{
			Email: "update@example.com",
			Name:  "Update User",
			Age:   intPtrExec(25),
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		updated, err := c.Modify().
			Set("name", "new_name").
			Where("email", "=", "email").
			Exec(ctx, map[string]any{
				"email":    "update@example.com",
				"new_name": "Updated User",
			})
		if err != nil {
			t.Fatalf("Modify().Exec() failed: %v", err)
		}

		if updated.Name != "Updated User" {
			t.Errorf("expected name Updated User, got %s", updated.Name)
		}
	})

	t.Run("Modify.ExecTx", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		// Insert test data
		_, err := c.Insert().Exec(ctx, &execTestUser{
			Email: "updatetx@example.com",
			Name:  "Update TX User",
			Age:   intPtrExec(30),
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		tx, err := tdb.db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}

		updated, err := c.Modify().
			Set("name", "new_name").
			Where("email", "=", "email").
			ExecTx(ctx, tx, map[string]any{
				"email":    "updatetx@example.com",
				"new_name": "Updated TX User",
			})
		if err != nil {
			tx.Rollback()
			t.Fatalf("Modify().ExecTx() failed: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit() failed: %v", err)
		}

		if updated.Name != "Updated TX User" {
			t.Errorf("expected name Updated TX User, got %s", updated.Name)
		}
	})
}

func TestExec_Delete(t *testing.T) {
	tdb := setupExecTestDB(t)
	defer tdb.cleanup(t)
	createExecTestTable(t, tdb.db)

	c, err := New[execTestUser](tdb.db, "exec_test_users", astqlpg.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("Remove.ExecTx", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		// Insert test data
		_, err := c.Insert().Exec(ctx, &execTestUser{
			Email: "deletetx@example.com",
			Name:  "Delete TX User",
			Age:   intPtrExec(30),
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		tx, err := tdb.db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}

		affected, err := c.Remove().
			Where("email", "=", "email").
			ExecTx(ctx, tx, map[string]any{"email": "deletetx@example.com"})
		if err != nil {
			tx.Rollback()
			t.Fatalf("Remove().ExecTx() failed: %v", err)
		}

		if affected != 1 {
			t.Errorf("expected 1 row affected, got %d", affected)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit() failed: %v", err)
		}

		// Verify deletion
		users, err := c.Query().Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(users) != 0 {
			t.Errorf("expected 0 users, got %d", len(users))
		}
	})
}

func TestExec_Aggregate(t *testing.T) {
	tdb := setupExecTestDB(t)
	defer tdb.cleanup(t)
	createExecTestTable(t, tdb.db)

	c, err := New[execTestUser](tdb.db, "exec_test_users", astqlpg.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("Count.ExecTx", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		// Insert test data
		for i := 0; i < 3; i++ {
			_, err := c.Insert().Exec(ctx, &execTestUser{
				Email: "count" + string(rune('a'+i)) + "@example.com",
				Name:  "Count User",
				Age:   intPtrExec(25 + i),
			})
			if err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
		}

		tx, err := tdb.db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}
		defer tx.Rollback()

		count, err := c.Count().ExecTx(ctx, tx, nil)
		if err != nil {
			t.Fatalf("Count().ExecTx() failed: %v", err)
		}

		if count != 3 {
			t.Errorf("expected count 3, got %v", count)
		}

		tx.Commit()
	})
}

func TestExec_Compound(t *testing.T) {
	tdb := setupExecTestDB(t)
	defer tdb.cleanup(t)
	createExecTestTable(t, tdb.db)

	c, err := New[execTestUser](tdb.db, "exec_test_users", astqlpg.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("Compound.Exec", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		// Insert test data
		users := []*execTestUser{
			{Email: "alice@example.com", Name: "Alice", Age: intPtrExec(25)},
			{Email: "bob@example.com", Name: "Bob", Age: intPtrExec(30)},
			{Email: "charlie@example.com", Name: "Charlie", Age: intPtrExec(35)},
		}
		for _, u := range users {
			_, err := c.Insert().Exec(ctx, u)
			if err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
		}

		// UNION: age >= 30 OR name = 'Alice'
		q1 := c.Query().Where("age", ">=", "min_age")
		q2 := c.Query().Where("name", "=", "target_name")

		compound := q1.Union(q2).OrderBy("name", "ASC")
		results, err := compound.Exec(ctx, map[string]any{
			"q0_min_age":     30,
			"q1_target_name": "Alice",
		})
		if err != nil {
			t.Fatalf("Compound.Exec() failed: %v", err)
		}

		// Should get Alice, Bob, Charlie
		if len(results) != 3 {
			t.Errorf("expected 3 users, got %d", len(results))
		}
	})

	t.Run("Compound.ExecTx", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		// Insert test data
		users := []*execTestUser{
			{Email: "alice@example.com", Name: "Alice", Age: intPtrExec(25)},
			{Email: "bob@example.com", Name: "Bob", Age: intPtrExec(30)},
		}
		for _, u := range users {
			_, err := c.Insert().Exec(ctx, u)
			if err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
		}

		tx, err := tdb.db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}
		defer tx.Rollback()

		q1 := c.Query().Where("age", ">=", "min_age")
		q2 := c.Query().Where("name", "=", "target_name")

		compound := q1.Union(q2).OrderBy("name", "ASC")
		results, err := compound.ExecTx(ctx, tx, map[string]any{
			"q0_min_age":     30,
			"q1_target_name": "Alice",
		})
		if err != nil {
			t.Fatalf("Compound.ExecTx() failed: %v", err)
		}

		// Should get Alice and Bob
		if len(results) != 2 {
			t.Errorf("expected 2 users, got %d", len(results))
		}

		tx.Commit()
	})
}

func TestExec_Batch(t *testing.T) {
	tdb := setupExecTestDB(t)
	defer tdb.cleanup(t)
	createExecTestTable(t, tdb.db)

	c, err := New[execTestUser](tdb.db, "exec_test_users", astqlpg.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("Insert.ExecBatch", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		users := []*execTestUser{
			{Email: "batch1@example.com", Name: "Batch 1", Age: intPtrExec(25)},
			{Email: "batch2@example.com", Name: "Batch 2", Age: intPtrExec(30)},
			{Email: "batch3@example.com", Name: "Batch 3", Age: intPtrExec(35)},
		}

		affected, err := c.Insert().ExecBatch(ctx, users)
		if err != nil {
			t.Fatalf("Insert().ExecBatch() failed: %v", err)
		}

		if affected != 3 {
			t.Errorf("expected 3 rows affected, got %d", affected)
		}

		// Verify all users inserted
		allUsers, err := c.Query().Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(allUsers) != 3 {
			t.Errorf("expected 3 users, got %d", len(allUsers))
		}
	})

	t.Run("Insert.ExecBatchTx", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		tx, err := tdb.db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}

		users := []*execTestUser{
			{Email: "batchtx1@example.com", Name: "Batch TX 1", Age: intPtrExec(25)},
			{Email: "batchtx2@example.com", Name: "Batch TX 2", Age: intPtrExec(30)},
		}

		affected, err := c.Insert().ExecBatchTx(ctx, tx, users)
		if err != nil {
			tx.Rollback()
			t.Fatalf("Insert().ExecBatchTx() failed: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit() failed: %v", err)
		}

		if affected != 2 {
			t.Errorf("expected 2 rows affected, got %d", affected)
		}
	})

	t.Run("Modify.ExecBatch", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		// Insert test data
		for i := 0; i < 3; i++ {
			_, err := c.Insert().Exec(ctx, &execTestUser{
				Email: "modifybatch" + string(rune('a'+i)) + "@example.com",
				Name:  "User " + string(rune('A'+i)),
				Age:   intPtrExec(25),
			})
			if err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
		}

		params := []map[string]any{
			{"email": "modifybatcha@example.com", "new_name": "Updated A"},
			{"email": "modifybatchb@example.com", "new_name": "Updated B"},
		}

		affected, err := c.Modify().
			Set("name", "new_name").
			Where("email", "=", "email").
			ExecBatch(ctx, params)
		if err != nil {
			t.Fatalf("Modify().ExecBatch() failed: %v", err)
		}

		if affected != 2 {
			t.Errorf("expected 2 rows affected, got %d", affected)
		}
	})

	t.Run("Remove.ExecBatch", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		// Insert test data
		for i := 0; i < 3; i++ {
			_, err := c.Insert().Exec(ctx, &execTestUser{
				Email: "removebatch" + string(rune('a'+i)) + "@example.com",
				Name:  "User " + string(rune('A'+i)),
				Age:   intPtrExec(25),
			})
			if err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
		}

		params := []map[string]any{
			{"email": "removebatcha@example.com"},
			{"email": "removebatchb@example.com"},
		}

		affected, err := c.Remove().
			Where("email", "=", "email").
			ExecBatch(ctx, params)
		if err != nil {
			t.Fatalf("Remove().ExecBatch() failed: %v", err)
		}

		if affected != 2 {
			t.Errorf("expected 2 rows affected, got %d", affected)
		}

		// Verify only one user remains
		users, err := c.Query().Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(users) != 1 {
			t.Errorf("expected 1 user remaining, got %d", len(users))
		}
	})

	t.Run("Modify.ExecBatchTx", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		// Insert test data
		for i := 0; i < 3; i++ {
			_, err := c.Insert().Exec(ctx, &execTestUser{
				Email: "modifybatchtx" + string(rune('a'+i)) + "@example.com",
				Name:  "User " + string(rune('A'+i)),
				Age:   intPtrExec(25),
			})
			if err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
		}

		tx, err := tdb.db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}

		params := []map[string]any{
			{"email": "modifybatchtxa@example.com", "new_name": "Updated A"},
			{"email": "modifybatchtxb@example.com", "new_name": "Updated B"},
		}

		affected, err := c.Modify().
			Set("name", "new_name").
			Where("email", "=", "email").
			ExecBatchTx(ctx, tx, params)
		if err != nil {
			tx.Rollback()
			t.Fatalf("Modify().ExecBatchTx() failed: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit() failed: %v", err)
		}

		if affected != 2 {
			t.Errorf("expected 2 rows affected, got %d", affected)
		}

		// Verify updates
		user, err := c.Select().
			Where("email", "=", "email").
			Exec(ctx, map[string]any{"email": "modifybatchtxa@example.com"})
		if err != nil {
			t.Fatalf("Select failed: %v", err)
		}
		if user.Name != "Updated A" {
			t.Errorf("expected name 'Updated A', got %s", user.Name)
		}
	})

	t.Run("Remove.ExecBatchTx", func(t *testing.T) {
		truncateExecTestTable(t, tdb.db)

		// Insert test data
		for i := 0; i < 3; i++ {
			_, err := c.Insert().Exec(ctx, &execTestUser{
				Email: "removebatchtx" + string(rune('a'+i)) + "@example.com",
				Name:  "User " + string(rune('A'+i)),
				Age:   intPtrExec(25),
			})
			if err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
		}

		tx, err := tdb.db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}

		params := []map[string]any{
			{"email": "removebatchtxa@example.com"},
			{"email": "removebatchtxb@example.com"},
		}

		affected, err := c.Remove().
			Where("email", "=", "email").
			ExecBatchTx(ctx, tx, params)
		if err != nil {
			tx.Rollback()
			t.Fatalf("Remove().ExecBatchTx() failed: %v", err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit() failed: %v", err)
		}

		if affected != 2 {
			t.Errorf("expected 2 rows affected, got %d", affected)
		}

		// Verify only one user remains
		users, err := c.Query().Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if len(users) != 1 {
			t.Errorf("expected 1 user remaining, got %d", len(users))
		}
	})
}
