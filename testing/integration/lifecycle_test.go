package integration

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/zoobzio/astql/postgres"
	"github.com/zoobzio/soy"
)

func TestLifecycle_OnScan_Select(t *testing.T) {
	db := getTestDB(t)
	truncateTestTable(t, db)

	c, err := soy.New[TestUser](db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	// Insert test data
	_, err = c.Insert().Exec(ctx, &TestUser{
		Email: "scan-select@example.com",
		Name:  "Scan Select",
		Age:   intPtr(25),
	})
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	t.Run("fires on Select.Exec", func(t *testing.T) {
		var called bool
		c.OnScan(func(ctx context.Context, result *TestUser) error {
			called = true
			result.Name = strings.ToUpper(result.Name)
			return nil
		})
		defer c.OnScan(nil)

		user, err := c.Select().
			Where("email", "=", "user_email").
			Exec(ctx, map[string]any{"user_email": "scan-select@example.com"})
		if err != nil {
			t.Fatalf("Select().Exec() failed: %v", err)
		}
		if !called {
			t.Error("onScan was not called")
		}
		if user.Name != "SCAN SELECT" {
			t.Errorf("expected name SCAN SELECT, got %s", user.Name)
		}
	})

	t.Run("error aborts Select.Exec", func(t *testing.T) {
		c.OnScan(func(ctx context.Context, result *TestUser) error {
			return errors.New("scan rejected")
		})
		defer c.OnScan(nil)

		_, err := c.Select().
			Where("email", "=", "user_email").
			Exec(ctx, map[string]any{"user_email": "scan-select@example.com"})
		if err == nil {
			t.Error("expected error from onScan")
		}
		if !strings.Contains(err.Error(), "scan rejected") {
			t.Errorf("expected scan rejected error, got: %v", err)
		}
	})
}

func TestLifecycle_OnScan_Query(t *testing.T) {
	db := getTestDB(t)
	truncateTestTable(t, db)

	c, err := soy.New[TestUser](db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	// Insert test data
	for _, u := range []*TestUser{
		{Email: "q1@example.com", Name: "Alice", Age: intPtr(25)},
		{Email: "q2@example.com", Name: "Bob", Age: intPtr(30)},
	} {
		if _, err := c.Insert().Exec(ctx, u); err != nil {
			t.Fatalf("insert failed: %v", err)
		}
	}

	t.Run("fires per row on Query.Exec", func(t *testing.T) {
		var count atomic.Int32
		c.OnScan(func(ctx context.Context, result *TestUser) error {
			count.Add(1)
			result.Email = "redacted"
			return nil
		})
		defer c.OnScan(nil)

		users, err := c.Query().Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Query().Exec() failed: %v", err)
		}
		if count.Load() != 2 {
			t.Errorf("expected onScan called 2 times, got %d", count.Load())
		}
		for _, u := range users {
			if u.Email != "redacted" {
				t.Errorf("expected redacted email, got %s", u.Email)
			}
		}
	})

	t.Run("error aborts Query.Exec on first row", func(t *testing.T) {
		c.OnScan(func(ctx context.Context, result *TestUser) error {
			return errors.New("query scan rejected")
		})
		defer c.OnScan(nil)

		_, err := c.Query().Exec(ctx, nil)
		if err == nil {
			t.Error("expected error from onScan")
		}
	})
}

func TestLifecycle_OnRecord_Insert(t *testing.T) {
	db := getTestDB(t)
	truncateTestTable(t, db)

	c, err := soy.New[TestUser](db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("fires before Insert.Exec", func(t *testing.T) {
		var called bool
		c.OnRecord(func(ctx context.Context, record *TestUser) error {
			called = true
			// Normalize email before insert
			record.Email = strings.ToLower(record.Email)
			return nil
		})
		defer c.OnRecord(nil)

		user, err := c.Insert().Exec(ctx, &TestUser{
			Email: "UPPER@EXAMPLE.COM",
			Name:  "Record Hook",
			Age:   intPtr(28),
		})
		if err != nil {
			t.Fatalf("Insert().Exec() failed: %v", err)
		}
		if !called {
			t.Error("onRecord was not called")
		}
		if user.Email != "upper@example.com" {
			t.Errorf("expected lowercased email, got %s", user.Email)
		}
	})

	t.Run("error aborts Insert.Exec", func(t *testing.T) {
		c.OnRecord(func(ctx context.Context, record *TestUser) error {
			if record.Email == "" {
				return errors.New("email is required")
			}
			return nil
		})
		defer c.OnRecord(nil)

		_, err := c.Insert().Exec(ctx, &TestUser{
			Email: "",
			Name:  "No Email",
		})
		if err == nil {
			t.Error("expected error from onRecord")
		}
		if !strings.Contains(err.Error(), "email is required") {
			t.Errorf("expected email is required error, got: %v", err)
		}
	})
}

func TestLifecycle_OnScan_Insert(t *testing.T) {
	db := getTestDB(t)
	truncateTestTable(t, db)

	c, err := soy.New[TestUser](db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("fires after Insert.Exec scan", func(t *testing.T) {
		var called bool
		c.OnScan(func(ctx context.Context, result *TestUser) error {
			called = true
			result.Name = "post-scan"
			return nil
		})
		defer c.OnScan(nil)

		user, err := c.Insert().Exec(ctx, &TestUser{
			Email: "insert-scan@example.com",
			Name:  "Original",
			Age:   intPtr(20),
		})
		if err != nil {
			t.Fatalf("Insert().Exec() failed: %v", err)
		}
		if !called {
			t.Error("onScan was not called after insert")
		}
		if user.Name != "post-scan" {
			t.Errorf("expected post-scan name, got %s", user.Name)
		}
	})
}

func TestLifecycle_OnRecord_InsertBatch(t *testing.T) {
	db := getTestDB(t)
	truncateTestTable(t, db)

	c, err := soy.New[TestUser](db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("fires per record in ExecBatch", func(t *testing.T) {
		var count atomic.Int32
		c.OnRecord(func(ctx context.Context, record *TestUser) error {
			count.Add(1)
			record.Email = strings.ToLower(record.Email)
			return nil
		})
		defer c.OnRecord(nil)

		records := []*TestUser{
			{Email: "BATCH1@EXAMPLE.COM", Name: "Batch 1", Age: intPtr(20)},
			{Email: "BATCH2@EXAMPLE.COM", Name: "Batch 2", Age: intPtr(21)},
			{Email: "BATCH3@EXAMPLE.COM", Name: "Batch 3", Age: intPtr(22)},
		}

		n, err := c.Insert().ExecBatch(ctx, records)
		if err != nil {
			t.Fatalf("ExecBatch() failed: %v", err)
		}
		if n != 3 {
			t.Errorf("expected 3 rows inserted, got %d", n)
		}
		if count.Load() != 3 {
			t.Errorf("expected onRecord called 3 times, got %d", count.Load())
		}

		// Verify the lowercased emails were written
		users, err := c.Query().OrderBy("name", "ASC").Exec(ctx, nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		for _, u := range users {
			if u.Email != strings.ToLower(u.Email) {
				t.Errorf("expected lowercased email, got %s", u.Email)
			}
		}
	})

	t.Run("error aborts ExecBatch", func(t *testing.T) {
		truncateTestTable(t, db)

		c.OnRecord(func(ctx context.Context, record *TestUser) error {
			if record.Name == "Bad" {
				return errors.New("bad record")
			}
			return nil
		})
		defer c.OnRecord(nil)

		records := []*TestUser{
			{Email: "ok@example.com", Name: "OK", Age: intPtr(20)},
			{Email: "bad@example.com", Name: "Bad", Age: intPtr(21)},
		}

		_, err := c.Insert().ExecBatch(ctx, records)
		if err == nil {
			t.Error("expected error from onRecord")
		}
		if !strings.Contains(err.Error(), "bad record") {
			t.Errorf("expected bad record error, got: %v", err)
		}
	})
}

func TestLifecycle_OnScan_Update(t *testing.T) {
	db := getTestDB(t)
	truncateTestTable(t, db)

	c, err := soy.New[TestUser](db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	// Insert test data
	_, err = c.Insert().Exec(ctx, &TestUser{
		Email: "update-scan@example.com",
		Name:  "Before Update",
		Age:   intPtr(25),
	})
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	t.Run("fires after Modify.Exec scan", func(t *testing.T) {
		var called bool
		c.OnScan(func(ctx context.Context, result *TestUser) error {
			called = true
			result.Name = "post-update-scan"
			return nil
		})
		defer c.OnScan(nil)

		user, err := c.Modify().
			Set("name", "new_name").
			Where("email", "=", "user_email").
			Exec(ctx, map[string]any{
				"new_name":   "After Update",
				"user_email": "update-scan@example.com",
			})
		if err != nil {
			t.Fatalf("Modify().Exec() failed: %v", err)
		}
		if !called {
			t.Error("onScan was not called after update")
		}
		if user.Name != "post-update-scan" {
			t.Errorf("expected post-update-scan, got %s", user.Name)
		}
	})

	t.Run("error aborts Modify.Exec", func(t *testing.T) {
		c.OnScan(func(ctx context.Context, result *TestUser) error {
			return errors.New("update scan rejected")
		})
		defer c.OnScan(nil)

		_, err := c.Modify().
			Set("name", "new_name").
			Where("email", "=", "user_email").
			Exec(ctx, map[string]any{
				"new_name":   "Rejected",
				"user_email": "update-scan@example.com",
			})
		if err == nil {
			t.Error("expected error from onScan")
		}
	})
}

func TestLifecycle_OnScan_Compound(t *testing.T) {
	db := getTestDB(t)
	truncateTestTable(t, db)

	c, err := soy.New[TestUser](db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	// Insert test data
	for _, u := range []*TestUser{
		{Email: "c1@example.com", Name: "Alpha", Age: intPtr(25)},
		{Email: "c2@example.com", Name: "Beta", Age: intPtr(35)},
	} {
		if _, err := c.Insert().Exec(ctx, u); err != nil {
			t.Fatalf("insert failed: %v", err)
		}
	}

	t.Run("fires per row on Compound.Exec", func(t *testing.T) {
		var count atomic.Int32
		c.OnScan(func(ctx context.Context, result *TestUser) error {
			count.Add(1)
			result.Name = "compound-" + result.Name
			return nil
		})
		defer c.OnScan(nil)

		q1 := c.Query().Where("name", "=", "name1")
		q2 := c.Query().Where("name", "=", "name2")
		users, err := q1.Union(q2).Exec(ctx, map[string]any{
			"q0_name1": "Alpha",
			"q1_name2": "Beta",
		})
		if err != nil {
			t.Fatalf("Union().Exec() failed: %v", err)
		}
		if count.Load() != 2 {
			t.Errorf("expected onScan called 2 times, got %d", count.Load())
		}
		for _, u := range users {
			if !strings.HasPrefix(u.Name, "compound-") {
				t.Errorf("expected compound- prefix, got %s", u.Name)
			}
		}
	})
}

func TestLifecycle_OnRecord_Upsert(t *testing.T) {
	db := getTestDB(t)
	truncateTestTable(t, db)

	c, err := soy.New[TestUser](db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	t.Run("fires before upsert insert", func(t *testing.T) {
		var called bool
		c.OnRecord(func(ctx context.Context, record *TestUser) error {
			called = true
			record.Email = strings.ToLower(record.Email)
			return nil
		})
		defer c.OnRecord(nil)

		user, err := c.Insert().
			OnConflict("email").
			DoUpdate().
			Set("name", "name").
			Build().
			Exec(ctx, &TestUser{
				Email: "UPSERT@EXAMPLE.COM",
				Name:  "Upsert User",
				Age:   intPtr(30),
			})
		if err != nil {
			t.Fatalf("upsert failed: %v", err)
		}
		if !called {
			t.Error("onRecord was not called")
		}
		if user.Email != "upsert@example.com" {
			t.Errorf("expected lowercased email, got %s", user.Email)
		}
	})
}

func TestLifecycle_NilCallbacks_NoOverhead(t *testing.T) {
	db := getTestDB(t)
	truncateTestTable(t, db)

	c, err := soy.New[TestUser](db, "test_users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	// Insert and query with no callbacks registered — should work identically to before
	_, err = c.Insert().Exec(ctx, &TestUser{
		Email: "nil-cb@example.com",
		Name:  "Nil Callback",
		Age:   intPtr(40),
	})
	if err != nil {
		t.Fatalf("Insert().Exec() failed with nil callbacks: %v", err)
	}

	user, err := c.Select().
		Where("email", "=", "user_email").
		Exec(ctx, map[string]any{"user_email": "nil-cb@example.com"})
	if err != nil {
		t.Fatalf("Select().Exec() failed with nil callbacks: %v", err)
	}
	if user.Name != "Nil Callback" {
		t.Errorf("expected Nil Callback, got %s", user.Name)
	}

	users, err := c.Query().Exec(ctx, nil)
	if err != nil {
		t.Fatalf("Query().Exec() failed with nil callbacks: %v", err)
	}
	if len(users) != 1 {
		t.Errorf("expected 1 user, got %d", len(users))
	}
}
