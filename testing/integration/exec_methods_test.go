package integration

import (
	"context"
	"testing"
	"time"

	"github.com/zoobzio/astql/postgres"
	"github.com/zoobzio/soy"
)

// TestExec_Insert tests all Insert execution method variants.
func TestExec_Insert(t *testing.T) {
	db := getTestDB(t)

	c, err := soy.New[TestUser](db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("ConflictUpdate.Exec convenience method", func(t *testing.T) {
		truncateTestTable(t, db)

		// First insert
		_, err := c.Insert().Exec(ctx, &TestUser{
			Email: "conflict@example.com",
			Name:  "Original",
			Age:   intPtr(25),
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		// Use the convenience Exec method directly on ConflictUpdate (not Build().Exec())
		user, err := c.Insert().
			OnConflict("email").
			DoUpdate().
			Set("name", "name").
			Exec(ctx, &TestUser{
				Email: "conflict@example.com",
				Name:  "Updated",
				Age:   intPtr(30),
			})
		if err != nil {
			t.Fatalf("ConflictUpdate.Exec() failed: %v", err)
		}

		if user.Name != "Updated" {
			t.Errorf("expected name Updated, got %s", user.Name)
		}
	})

	t.Run("Insert.Exec", func(t *testing.T) {
		truncateTestTable(t, db)

		user, err := c.Insert().Exec(ctx, &TestUser{
			Email: "test@example.com",
			Name:  "Test User",
			Age:   intPtr(25),
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
		truncateTestTable(t, db)

		tx, err := db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}

		user, err := c.Insert().ExecTx(ctx, tx, &TestUser{
			Email: "tx@example.com",
			Name:  "TX User",
			Age:   intPtr(30),
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
		truncateTestTable(t, db)

		now := time.Now()
		atom, err := c.Insert().ExecAtom(ctx, map[string]any{
			"email":      "insertatom@example.com",
			"name":       "Insert Atom User",
			"age":        40,
			"created_at": now,
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
		truncateTestTable(t, db)

		tx, err := db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}

		now := time.Now()
		atom, err := c.Insert().ExecTxAtom(ctx, tx, map[string]any{
			"email":      "txatom@example.com",
			"name":       "TX Atom User",
			"age":        45,
			"created_at": now,
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

// TestExec_Select tests all Select execution method variants.
func TestExec_Select(t *testing.T) {
	db := getTestDB(t)

	c, err := soy.New[TestUser](db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("Select.Exec", func(t *testing.T) {
		truncateTestTable(t, db)

		// Insert test data
		_, err := c.Insert().Exec(ctx, &TestUser{
			Email: "select@example.com",
			Name:  "Select User",
			Age:   intPtr(25),
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
		truncateTestTable(t, db)

		// Insert test data
		_, err := c.Insert().Exec(ctx, &TestUser{
			Email: "selecttx@example.com",
			Name:  "Select TX User",
			Age:   intPtr(30),
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		tx, err := db.BeginTxx(ctx, nil)
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
		truncateTestTable(t, db)

		// Insert test data
		_, err := c.Insert().Exec(ctx, &TestUser{
			Email: "selectatom@example.com",
			Name:  "Select Atom User",
			Age:   intPtr(35),
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
		truncateTestTable(t, db)

		// Insert test data
		_, err := c.Insert().Exec(ctx, &TestUser{
			Email: "selecttxatom@example.com",
			Name:  "Select TX Atom User",
			Age:   intPtr(36),
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		tx, err := db.BeginTxx(ctx, nil)
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

// TestExec_Query tests all Query execution method variants.
func TestExec_Query(t *testing.T) {
	db := getTestDB(t)

	c, err := soy.New[TestUser](db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("Query.Exec", func(t *testing.T) {
		truncateTestTable(t, db)

		// Insert test data
		for _, email := range []string{"a@example.com", "b@example.com", "c@example.com"} {
			_, err := c.Insert().Exec(ctx, &TestUser{
				Email: email,
				Name:  "User",
				Age:   intPtr(25),
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
		truncateTestTable(t, db)

		// Insert test data
		_, err := c.Insert().Exec(ctx, &TestUser{
			Email: "querytx@example.com",
			Name:  "Query TX User",
			Age:   intPtr(30),
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		tx, err := db.BeginTxx(ctx, nil)
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
		truncateTestTable(t, db)

		// Insert test data
		for i, email := range []string{"atom1@example.com", "atom2@example.com"} {
			_, err := c.Insert().Exec(ctx, &TestUser{
				Email: email,
				Name:  "Atom User",
				Age:   intPtr(20 + i*5),
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
		truncateTestTable(t, db)

		// Insert test data
		_, err := c.Insert().Exec(ctx, &TestUser{
			Email: "querytxatom@example.com",
			Name:  "Query TX Atom User",
			Age:   intPtr(50),
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		tx, err := db.BeginTxx(ctx, nil)
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

// TestExec_Update tests all Update execution method variants.
func TestExec_Update(t *testing.T) {
	db := getTestDB(t)

	c, err := soy.New[TestUser](db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("Modify.Exec", func(t *testing.T) {
		truncateTestTable(t, db)

		// Insert test data
		_, err := c.Insert().Exec(ctx, &TestUser{
			Email: "update@example.com",
			Name:  "Update User",
			Age:   intPtr(25),
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
		truncateTestTable(t, db)

		// Insert test data
		_, err := c.Insert().Exec(ctx, &TestUser{
			Email: "updatetx@example.com",
			Name:  "Update TX User",
			Age:   intPtr(30),
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		tx, err := db.BeginTxx(ctx, nil)
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

// TestExec_Delete tests all Delete execution method variants.
func TestExec_Delete(t *testing.T) {
	db := getTestDB(t)

	c, err := soy.New[TestUser](db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("Remove.ExecTx", func(t *testing.T) {
		truncateTestTable(t, db)

		// Insert test data
		_, err := c.Insert().Exec(ctx, &TestUser{
			Email: "deletetx@example.com",
			Name:  "Delete TX User",
			Age:   intPtr(30),
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		tx, err := db.BeginTxx(ctx, nil)
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

// TestExec_Aggregate tests all Aggregate execution method variants.
func TestExec_Aggregate(t *testing.T) {
	db := getTestDB(t)

	c, err := soy.New[TestUser](db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("Count.ExecTx", func(t *testing.T) {
		truncateTestTable(t, db)

		// Insert test data
		for i := 0; i < 3; i++ {
			_, err := c.Insert().Exec(ctx, &TestUser{
				Email: "count" + string(rune('a'+i)) + "@example.com",
				Name:  "Count User",
				Age:   intPtr(25 + i),
			})
			if err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
		}

		tx, err := db.BeginTxx(ctx, nil)
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

// TestExec_Compound tests all Compound execution method variants.
func TestExec_Compound(t *testing.T) {
	db := getTestDB(t)

	c, err := soy.New[TestUser](db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("Compound.Exec", func(t *testing.T) {
		truncateTestTable(t, db)

		// Insert test data
		users := []*TestUser{
			{Email: "alice@example.com", Name: "Alice", Age: intPtr(25)},
			{Email: "bob@example.com", Name: "Bob", Age: intPtr(30)},
			{Email: "charlie@example.com", Name: "Charlie", Age: intPtr(35)},
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
		truncateTestTable(t, db)

		// Insert test data
		users := []*TestUser{
			{Email: "alice@example.com", Name: "Alice", Age: intPtr(25)},
			{Email: "bob@example.com", Name: "Bob", Age: intPtr(30)},
		}
		for _, u := range users {
			_, err := c.Insert().Exec(ctx, u)
			if err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
		}

		tx, err := db.BeginTxx(ctx, nil)
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

// TestExec_Batch tests all Batch execution method variants.
func TestExec_Batch(t *testing.T) {
	db := getTestDB(t)

	c, err := soy.New[TestUser](db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("Insert.ExecBatch", func(t *testing.T) {
		truncateTestTable(t, db)

		users := []*TestUser{
			{Email: "batch1@example.com", Name: "Batch 1", Age: intPtr(25)},
			{Email: "batch2@example.com", Name: "Batch 2", Age: intPtr(30)},
			{Email: "batch3@example.com", Name: "Batch 3", Age: intPtr(35)},
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
		truncateTestTable(t, db)

		tx, err := db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx() failed: %v", err)
		}

		users := []*TestUser{
			{Email: "batchtx1@example.com", Name: "Batch TX 1", Age: intPtr(25)},
			{Email: "batchtx2@example.com", Name: "Batch TX 2", Age: intPtr(30)},
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
		truncateTestTable(t, db)

		// Insert test data
		for i := 0; i < 3; i++ {
			_, err := c.Insert().Exec(ctx, &TestUser{
				Email: "modifybatch" + string(rune('a'+i)) + "@example.com",
				Name:  "User " + string(rune('A'+i)),
				Age:   intPtr(25),
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
		truncateTestTable(t, db)

		// Insert test data
		for i := 0; i < 3; i++ {
			_, err := c.Insert().Exec(ctx, &TestUser{
				Email: "removebatch" + string(rune('a'+i)) + "@example.com",
				Name:  "User " + string(rune('A'+i)),
				Age:   intPtr(25),
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
		truncateTestTable(t, db)

		// Insert test data
		for i := 0; i < 3; i++ {
			_, err := c.Insert().Exec(ctx, &TestUser{
				Email: "modifybatchtx" + string(rune('a'+i)) + "@example.com",
				Name:  "User " + string(rune('A'+i)),
				Age:   intPtr(25),
			})
			if err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
		}

		tx, err := db.BeginTxx(ctx, nil)
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
		truncateTestTable(t, db)

		// Insert test data
		for i := 0; i < 3; i++ {
			_, err := c.Insert().Exec(ctx, &TestUser{
				Email: "removebatchtx" + string(rune('a'+i)) + "@example.com",
				Name:  "User " + string(rune('A'+i)),
				Age:   intPtr(25),
			})
			if err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
		}

		tx, err := db.BeginTxx(ctx, nil)
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
