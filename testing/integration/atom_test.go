package integration

import (
	"context"
	"testing"

	"github.com/zoobzio/astql/pkg/postgres"
	"github.com/zoobzio/soy"
)

func TestAtomScanning_Integration(t *testing.T) {
	db := getTestDB(t)

	c, err := soy.New[TestUser](db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	// Insert test data
	truncateTestTable(t, db)
	_, err = c.Insert().Exec(ctx, &TestUser{
		Email: "atom1@example.com",
		Name:  "Atom User 1",
		Age:   intPtr(25),
	})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	_, err = c.Insert().Exec(ctx, &TestUser{
		Email: "atom2@example.com",
		Name:  "Atom User 2",
		Age:   intPtr(30),
	})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	t.Run("Select.ExecAtom returns single atom", func(t *testing.T) {
		atom, err := c.Select().
			Where("email", "=", "user_email").
			ExecAtom(ctx, map[string]any{"user_email": "atom1@example.com"})

		if err != nil {
			t.Fatalf("Select().ExecAtom() failed: %v", err)
		}

		if atom == nil {
			t.Fatal("expected atom to be returned")
		}

		// Atom uses struct field names as keys, not db column names
		if email, ok := atom.Strings["Email"]; !ok || email != "atom1@example.com" {
			t.Errorf("expected Email atom1@example.com, got %v (ok=%v)", atom.Strings["Email"], ok)
		}
		if name, ok := atom.Strings["Name"]; !ok || name != "Atom User 1" {
			t.Errorf("expected Name Atom User 1, got %v (ok=%v)", atom.Strings["Name"], ok)
		}
	})

	t.Run("Query.ExecAtom returns multiple atoms", func(t *testing.T) {
		atoms, err := c.Query().
			OrderBy("email", "asc").
			ExecAtom(ctx, nil)

		if err != nil {
			t.Fatalf("Query().ExecAtom() failed: %v", err)
		}

		if len(atoms) != 2 {
			t.Fatalf("expected 2 atoms, got %d", len(atoms))
		}

		// Atom uses struct field names as keys
		if email, ok := atoms[0].Strings["Email"]; !ok || email != "atom1@example.com" {
			t.Errorf("expected first Email atom1@example.com, got %v", atoms[0].Strings["Email"])
		}

		if email, ok := atoms[1].Strings["Email"]; !ok || email != "atom2@example.com" {
			t.Errorf("expected second Email atom2@example.com, got %v", atoms[1].Strings["Email"])
		}
	})

	t.Run("Insert.ExecAtom returns inserted atom", func(t *testing.T) {
		// Note: ExecAtom for Insert requires all non-PK columns including those with defaults
		// The created_at field has a DB default but the INSERT query still includes it
		atom, err := c.Insert().ExecAtom(ctx, map[string]any{
			"email":      "atom3@example.com",
			"name":       "Atom User 3",
			"age":        35,
			"created_at": nil, // Let DB use default
		})

		if err != nil {
			t.Fatalf("Insert().ExecAtom() failed: %v", err)
		}

		if atom == nil {
			t.Fatal("expected atom to be returned")
		}

		// Atom uses struct field names as keys
		if email, ok := atom.Strings["Email"]; !ok || email != "atom3@example.com" {
			t.Errorf("expected Email atom3@example.com, got %v", atom.Strings["Email"])
		}
		if name, ok := atom.Strings["Name"]; !ok || name != "Atom User 3" {
			t.Errorf("expected Name Atom User 3, got %v", atom.Strings["Name"])
		}
		// ID should be auto-generated
		if _, ok := atom.Ints["ID"]; !ok {
			t.Error("expected ID to be set in atom")
		}
	})

	t.Run("Select.ExecAtom returns error for no rows", func(t *testing.T) {
		_, err := c.Select().
			Where("email", "=", "user_email").
			ExecAtom(ctx, map[string]any{"user_email": "nonexistent@example.com"})

		if err == nil {
			t.Error("expected error for no rows found")
		}
	})

	t.Run("Query.ExecAtom with WHERE returns filtered atoms", func(t *testing.T) {
		atoms, err := c.Query().
			Where("age", ">=", "min_age").
			ExecAtom(ctx, map[string]any{"min_age": 30})

		if err != nil {
			t.Fatalf("Query().ExecAtom() with WHERE failed: %v", err)
		}

		t.Logf("Got %d atoms", len(atoms))

		// Should only include users with age >= 30
		// Atom uses struct field names as keys
		for i, atom := range atoms {
			t.Logf("Atom %d: IntPtrs=%+v", i, atom.IntPtrs)
			if age, ok := atom.IntPtrs["Age"]; ok && age != nil && *age < 30 {
				t.Errorf("expected age >= 30, got %d", *age)
			}
		}
	})

	t.Run("Query.ExecAtom with no results returns nil", func(t *testing.T) {
		atoms, err := c.Query().
			Where("email", "=", "email").
			ExecAtom(ctx, map[string]any{"email": "nonexistent@example.com"})

		if err != nil {
			t.Fatalf("Query().ExecAtom() with no results should not error: %v", err)
		}

		// Consistent with Query.Exec - returns nil for empty results
		if len(atoms) != 0 {
			t.Errorf("expected 0 atoms, got %d", len(atoms))
		}
	})
}

func TestAtomScanning_Transaction(t *testing.T) {
	db := getTestDB(t)

	c, err := soy.New[TestUser](db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()
	truncateTestTable(t, db)

	t.Run("ExecTxAtom works within transaction", func(t *testing.T) {
		tx, err := db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx failed: %v", err)
		}
		defer tx.Rollback()

		// Insert via transaction
		atom, err := c.Insert().ExecTxAtom(ctx, tx, map[string]any{
			"email":      "tx@example.com",
			"name":       "Transaction User",
			"age":        50,
			"created_at": nil,
		})
		if err != nil {
			t.Fatalf("Insert().ExecTxAtom() failed: %v", err)
		}
		if atom == nil {
			t.Fatal("expected atom")
		}

		// Query within same transaction
		atoms, err := c.Query().ExecTxAtom(ctx, tx, nil)
		if err != nil {
			t.Fatalf("Query().ExecTxAtom() failed: %v", err)
		}
		if len(atoms) != 1 {
			t.Fatalf("expected 1 atom in transaction, got %d", len(atoms))
		}

		// Rollback - record should not be visible outside
		tx.Rollback()

		// Verify no records outside transaction
		atoms, err = c.Query().ExecAtom(ctx, nil)
		if err != nil {
			t.Fatalf("Query().ExecAtom() after rollback failed: %v", err)
		}
		if len(atoms) != 0 {
			t.Errorf("expected 0 atoms after rollback, got %d", len(atoms))
		}
	})

	t.Run("Select.ExecTxAtom works within transaction", func(t *testing.T) {
		tx, err := db.BeginTxx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTxx failed: %v", err)
		}
		defer tx.Rollback()

		// Insert via transaction
		_, err = c.Insert().ExecTxAtom(ctx, tx, map[string]any{
			"email":      "selecttx@example.com",
			"name":       "Select Tx User",
			"age":        55,
			"created_at": nil,
		})
		if err != nil {
			t.Fatalf("Insert().ExecTxAtom() failed: %v", err)
		}

		// Select within same transaction
		atom, err := c.Select().
			Where("email", "=", "email").
			ExecTxAtom(ctx, tx, map[string]any{"email": "selecttx@example.com"})
		if err != nil {
			t.Fatalf("Select().ExecTxAtom() failed: %v", err)
		}
		if atom == nil {
			t.Fatal("expected atom")
		}
		if name, ok := atom.Strings["Name"]; !ok || name != "Select Tx User" {
			t.Errorf("expected Name 'Select Tx User', got %v", atom.Strings["Name"])
		}
	})
}

// TestAtomScanning_FieldTypes verifies all field types scan correctly to their atom tables.
func TestAtomScanning_FieldTypes(t *testing.T) {
	db := getTestDB(t)

	c, err := soy.New[TestUser](db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()
	truncateTestTable(t, db)

	// Insert with known values
	testAge := 42
	testUser := &TestUser{
		Email: "fieldtest@example.com",
		Name:  "Field Test User",
		Age:   &testAge,
	}

	inserted, err := c.Insert().Exec(ctx, testUser)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	t.Run("atom fields match struct fields", func(t *testing.T) {
		// Fetch as struct
		structResult, err := c.Select().
			Where("id", "=", "id").
			Exec(ctx, map[string]any{"id": inserted.ID})
		if err != nil {
			t.Fatalf("Select().Exec() failed: %v", err)
		}

		// Fetch as atom
		atomResult, err := c.Select().
			Where("id", "=", "id").
			ExecAtom(ctx, map[string]any{"id": inserted.ID})
		if err != nil {
			t.Fatalf("Select().ExecAtom() failed: %v", err)
		}

		// Verify int field (ID) - stored in Ints
		if atomID, ok := atomResult.Ints["ID"]; !ok {
			t.Error("ID not found in atom.Ints")
		} else if atomID != int64(structResult.ID) {
			t.Errorf("ID mismatch: struct=%d, atom=%d", structResult.ID, atomID)
		}

		// Verify string fields (Email, Name) - stored in Strings
		if atomEmail, ok := atomResult.Strings["Email"]; !ok {
			t.Error("Email not found in atom.Strings")
		} else if atomEmail != structResult.Email {
			t.Errorf("Email mismatch: struct=%s, atom=%s", structResult.Email, atomEmail)
		}

		if atomName, ok := atomResult.Strings["Name"]; !ok {
			t.Error("Name not found in atom.Strings")
		} else if atomName != structResult.Name {
			t.Errorf("Name mismatch: struct=%s, atom=%s", structResult.Name, atomName)
		}

		// Verify *int field (Age) - stored in IntPtrs
		atomAge, hasAge := atomResult.IntPtrs["Age"]
		if !hasAge {
			t.Error("Age not found in atom.IntPtrs")
		} else {
			switch {
			case structResult.Age == nil && atomAge != nil:
				t.Error("Age mismatch: struct=nil, atom=non-nil")
			case structResult.Age != nil && atomAge == nil:
				t.Error("Age mismatch: struct=non-nil, atom=nil")
			case structResult.Age != nil && *atomAge != int64(*structResult.Age):
				t.Errorf("Age mismatch: struct=%d, atom=%d", *structResult.Age, *atomAge)
			}
		}

		// Verify *time.Time field (CreatedAt) - stored in TimePtrs
		atomCreatedAt, hasCreatedAt := atomResult.TimePtrs["CreatedAt"]
		if !hasCreatedAt {
			t.Error("CreatedAt not found in atom.TimePtrs")
		} else {
			switch {
			case structResult.CreatedAt == nil && atomCreatedAt != nil:
				t.Error("CreatedAt mismatch: struct=nil, atom=non-nil")
			case structResult.CreatedAt != nil && atomCreatedAt == nil:
				t.Error("CreatedAt mismatch: struct=non-nil, atom=nil")
			case structResult.CreatedAt != nil && !atomCreatedAt.Equal(*structResult.CreatedAt):
				t.Errorf("CreatedAt mismatch: struct=%v, atom=%v", *structResult.CreatedAt, *atomCreatedAt)
			}
		}
	})

	t.Run("null pointer fields handled correctly", func(t *testing.T) {
		// Insert user with nil Age
		_, err := c.Insert().Exec(ctx, &TestUser{
			Email: "nulltest@example.com",
			Name:  "Null Test User",
			Age:   nil,
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		atom, err := c.Select().
			Where("email", "=", "email").
			ExecAtom(ctx, map[string]any{"email": "nulltest@example.com"})
		if err != nil {
			t.Fatalf("Select().ExecAtom() failed: %v", err)
		}

		// Age should be present in IntPtrs but nil
		atomAge, hasAge := atom.IntPtrs["Age"]
		if !hasAge {
			t.Error("Age key not found in atom.IntPtrs for null value")
		} else if atomAge != nil {
			t.Errorf("expected nil Age, got %d", *atomAge)
		}
	})
}

// TestAtomScanning_QueryParity verifies Query.ExecAtom returns equivalent data to Query.Exec.
func TestAtomScanning_QueryParity(t *testing.T) {
	db := getTestDB(t)

	c, err := soy.New[TestUser](db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()
	truncateTestTable(t, db)

	// Insert multiple users
	ages := []int{25, 30, 35}
	for i, age := range ages {
		a := age
		_, err := c.Insert().Exec(ctx, &TestUser{
			Email: "parity" + string(rune('0'+i)) + "@example.com",
			Name:  "Parity User " + string(rune('0'+i)),
			Age:   &a,
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Fetch all as structs
	structs, err := c.Query().OrderBy("email", "asc").Exec(ctx, nil)
	if err != nil {
		t.Fatalf("Query().Exec() failed: %v", err)
	}

	// Fetch all as atoms
	atoms, err := c.Query().OrderBy("email", "asc").ExecAtom(ctx, nil)
	if err != nil {
		t.Fatalf("Query().ExecAtom() failed: %v", err)
	}

	if len(structs) != len(atoms) {
		t.Fatalf("count mismatch: structs=%d, atoms=%d", len(structs), len(atoms))
	}

	for i := range structs {
		s := structs[i]
		a := atoms[i]

		if int64(s.ID) != a.Ints["ID"] {
			t.Errorf("row %d: ID mismatch: struct=%d, atom=%d", i, s.ID, a.Ints["ID"])
		}
		if s.Email != a.Strings["Email"] {
			t.Errorf("row %d: Email mismatch: struct=%s, atom=%s", i, s.Email, a.Strings["Email"])
		}
		if s.Name != a.Strings["Name"] {
			t.Errorf("row %d: Name mismatch: struct=%s, atom=%s", i, s.Name, a.Strings["Name"])
		}
		if s.Age != nil && a.IntPtrs["Age"] != nil && int64(*s.Age) != *a.IntPtrs["Age"] {
			t.Errorf("row %d: Age mismatch: struct=%d, atom=%d", i, *s.Age, *a.IntPtrs["Age"])
		}
	}
}
