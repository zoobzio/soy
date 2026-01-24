package soy

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/mariadb"
	"github.com/zoobzio/astql/mssql"
	"github.com/zoobzio/astql/postgres"
	"github.com/zoobzio/sentinel"
)

type createTestUser struct {
	ID    int    `db:"id" type:"integer" constraints:"primarykey"`
	Email string `db:"email" type:"text" constraints:"notnull,unique"`
	Name  string `db:"name" type:"text"`
	Age   *int   `db:"age" type:"integer"`
}

func TestCreate_Basic(t *testing.T) {
	// Register tags
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")

	db := &sqlx.DB{}
	soy, err := New[createTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("simple INSERT with RETURNING", func(t *testing.T) {
		result, err := soy.Insert().Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "INSERT INTO") {
			t.Errorf("SQL missing INSERT INTO: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"users"`) {
			t.Errorf("SQL missing table name: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "VALUES") {
			t.Errorf("SQL missing VALUES: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "RETURNING") {
			t.Errorf("SQL missing RETURNING: %s", result.SQL)
		}

		// Should have parameters for non-PK columns (email, name, age, created_at)
		if len(result.RequiredParams) < 2 {
			t.Errorf("Expected at least 2 params, got %d", len(result.RequiredParams))
		}

		t.Logf("SQL: %s", result.SQL)
		t.Logf("Params: %v", result.RequiredParams)
	})

	t.Run("INSERT with ON CONFLICT DO NOTHING", func(t *testing.T) {
		result, err := soy.Insert().
			OnConflict("email").
			DoNothing().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "ON CONFLICT") {
			t.Errorf("SQL missing ON CONFLICT: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "DO NOTHING") {
			t.Errorf("SQL missing DO NOTHING: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"email"`) {
			t.Errorf("SQL missing conflict column: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("INSERT with ON CONFLICT DO UPDATE", func(t *testing.T) {
		result, err := soy.Insert().
			OnConflict("email").
			DoUpdate().
			Set("name", "name").
			Set("age", "age").
			Build().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "ON CONFLICT") {
			t.Errorf("SQL missing ON CONFLICT: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "DO UPDATE") {
			t.Errorf("SQL missing DO UPDATE: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "SET") {
			t.Errorf("SQL missing SET: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"name"`) {
			t.Errorf("SQL missing updated field 'name': %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"age"`) {
			t.Errorf("SQL missing updated field 'age': %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})

	t.Run("INSERT with multi-column conflict", func(t *testing.T) {
		result, err := soy.Insert().
			OnConflict("email", "name").
			DoNothing().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, `"email"`) {
			t.Errorf("SQL missing first conflict column: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, `"name"`) {
			t.Errorf("SQL missing second conflict column: %s", result.SQL)
		}

		t.Logf("SQL: %s", result.SQL)
	})
}

func TestCreate_InstanceAccess(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[createTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	builder := soy.Insert()
	instance := builder.Instance()

	if instance == nil {
		t.Fatal("Instance() returned nil")
	}

	// Verify we can use instance methods for advanced queries
	field := instance.F("email")
	if field.GetName() != "email" {
		t.Errorf("Field name = %s, want email", field.GetName())
	}
}

func TestCreate_MustRender(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[createTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("successful MustRender", func(t *testing.T) {
		result := soy.Insert().MustRender()
		if result == nil {
			t.Fatal("MustRender() returned nil")
		}
		if result.SQL == "" {
			t.Error("MustRender() returned empty SQL")
		}
	})

	t.Run("MustRender panics on invalid conflict column", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("MustRender() did not panic with invalid column")
			}
		}()
		soy.Insert().
			OnConflict("nonexistent_field").
			DoNothing().
			MustRender()
	})
}

func TestCreate_ConflictChaining(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[createTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("DoUpdate allows chaining Set calls", func(t *testing.T) {
		result, err := soy.Insert().
			OnConflict("email").
			DoUpdate().
			Set("name", "name").
			Set("age", "age").
			Build().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		// Verify all SET clauses present
		setCount := strings.Count(result.SQL, "SET")
		if setCount != 1 {
			t.Errorf("Expected 1 SET keyword, got %d", setCount)
		}

		// Verify all fields in SET clause
		if !strings.Contains(result.SQL, `"name"`) {
			t.Error("Missing 'name' in SET clause")
		}
		if !strings.Contains(result.SQL, `"age"`) {
			t.Error("Missing 'age' in SET clause")
		}

		t.Logf("SQL: %s", result.SQL)
	})
}

func TestCreate_BatchOperations(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[createTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("ExecBatch renders query once for multiple records", func(t *testing.T) {
		// Just verify the query renders correctly - we can't execute without a real DB
		result, err := soy.Insert().Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		// Query should be same for batch as for single insert
		if !strings.Contains(result.SQL, "INSERT INTO") {
			t.Errorf("SQL missing INSERT INTO: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "VALUES") {
			t.Errorf("SQL missing VALUES: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "RETURNING") {
			t.Errorf("SQL missing RETURNING: %s", result.SQL)
		}

		t.Logf("Batch query SQL: %s", result.SQL)
	})

	t.Run("ExecBatch with ON CONFLICT", func(t *testing.T) {
		result, err := soy.Insert().
			OnConflict("email").
			DoNothing().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "ON CONFLICT") {
			t.Errorf("SQL missing ON CONFLICT: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "DO NOTHING") {
			t.Errorf("SQL missing DO NOTHING: %s", result.SQL)
		}

		t.Logf("Batch with conflict SQL: %s", result.SQL)
	})

	t.Run("ExecBatch with upsert", func(t *testing.T) {
		result, err := soy.Insert().
			OnConflict("email").
			DoUpdate().
			Set("name", "name").
			Build().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		if !strings.Contains(result.SQL, "ON CONFLICT") {
			t.Errorf("SQL missing ON CONFLICT: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "DO UPDATE") {
			t.Errorf("SQL missing DO UPDATE: %s", result.SQL)
		}

		t.Logf("Batch upsert SQL: %s", result.SQL)
	})
}

func TestCreate_DialectCapabilities(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}

	t.Run("MSSQL rejects ON CONFLICT at render time", func(t *testing.T) {
		// MSSQL doesn't support ON CONFLICT natively
		mssqlRenderer := mssql.New()
		soy, err := New[createTestUser](db, "users", mssqlRenderer)
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		// Attempting to render ON CONFLICT should fail for MSSQL
		_, err = soy.Insert().
			OnConflict("email").
			DoNothing().
			Render()
		if err == nil {
			t.Error("expected error for ON CONFLICT with MSSQL renderer")
		}
		if !strings.Contains(err.Error(), "ON CONFLICT") && !strings.Contains(err.Error(), "upsert") {
			t.Errorf("error should mention ON CONFLICT or upsert: %v", err)
		}
	})

	t.Run("MariaDB supports ON CONFLICT via ON DUPLICATE KEY", func(t *testing.T) {
		mariaRenderer := mariadb.New()
		soy, err := New[createTestUser](db, "users", mariaRenderer)
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		result, err := soy.Insert().
			OnConflict("email").
			DoUpdate().
			Set("name", "name").
			Build().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		// MariaDB uses ON DUPLICATE KEY UPDATE instead of ON CONFLICT
		if !strings.Contains(result.SQL, "ON DUPLICATE KEY UPDATE") {
			t.Errorf("SQL should use ON DUPLICATE KEY UPDATE: %s", result.SQL)
		}

		t.Logf("MariaDB SQL: %s", result.SQL)
	})

	t.Run("conflict tracking fields populated", func(t *testing.T) {
		soy, err := New[createTestUser](db, "users", postgres.New())
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}

		builder := soy.Insert().
			OnConflict("email", "name").
			DoUpdate().
			Set("age", "age").
			Build()

		// Verify internal tracking fields are populated
		if !builder.hasConflict {
			t.Error("hasConflict should be true after OnConflict()")
		}
		if len(builder.conflictColumns) != 2 {
			t.Errorf("conflictColumns should have 2 entries, got %d", len(builder.conflictColumns))
		}
		if len(builder.updateFields) != 1 {
			t.Errorf("updateFields should have 1 entry, got %d", len(builder.updateFields))
		}
		if builder.updateFields["age"] != "age" {
			t.Errorf("updateFields[age] should be 'age', got %q", builder.updateFields["age"])
		}
	})
}

func TestCreate_ErrorPaths(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	soy, err := New[createTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("invalid Set field propagates error", func(t *testing.T) {
		_, err := soy.Insert().
			OnConflict("email").
			DoUpdate().
			Set("nonexistent", "value").
			Build().
			Render()
		if err == nil {
			t.Error("expected error for invalid Set field")
		}
		if !strings.Contains(err.Error(), "nonexistent") {
			t.Errorf("error should mention invalid field: %v", err)
		}
	})

	t.Run("invalid Set param propagates error", func(t *testing.T) {
		_, err := soy.Insert().
			OnConflict("email").
			DoUpdate().
			Set("name", "").
			Build().
			Render()
		if err == nil {
			t.Error("expected error for empty param")
		}
	})

	t.Run("error propagates through DoUpdate Set chain", func(t *testing.T) {
		builder := soy.Insert().
			OnConflict("email").
			DoUpdate().
			Set("bad_field", "value").
			Set("name", "name"). // valid field shouldn't override error
			Build()
		_, err := builder.Render()
		if err == nil {
			t.Error("expected error to propagate through Set chain")
		}
	})

	t.Run("multiple valid sets work correctly", func(t *testing.T) {
		result, err := soy.Insert().
			OnConflict("email").
			DoUpdate().
			Set("name", "name").
			Set("age", "age").
			Build().
			Render()
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}
		if !strings.Contains(result.SQL, `"name"`) {
			t.Error("SQL missing name field")
		}
		if !strings.Contains(result.SQL, `"age"`) {
			t.Error("SQL missing age field")
		}
	})
}

func TestCreate_BatchMultiRowInsert(t *testing.T) {
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")

	db := &sqlx.DB{}
	s, err := New[createTestUser](db, "users", postgres.New())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("builds multi-row INSERT with indexed params", func(t *testing.T) {
		// Simulate what execBatch does internally: build multi-row INSERT
		instance := s.getInstance()
		metadata := s.getMetadata()

		tableName := s.getTableName()
		table, err := instance.TryT(tableName)
		if err != nil {
			t.Fatalf("TryT() failed: %v", err)
		}

		builder := astql.Insert(table)

		// Add 3 rows with indexed params (simulating 3 records)
		for i := 0; i < 3; i++ {
			values := instance.ValueMap()
			for _, field := range metadata.Fields {
				dbCol := field.Tags["db"]
				if dbCol == "" || dbCol == "-" {
					continue
				}
				constraints := field.Tags["constraints"]
				if contains(constraints, "primarykey") || contains(constraints, "primary_key") {
					continue
				}

				indexedParam := fmt.Sprintf("%s_%d", dbCol, i)
				f, _ := instance.TryF(dbCol)
				p, _ := instance.TryP(indexedParam)
				values[f] = p
			}
			builder = builder.Values(values)
		}

		result, err := builder.Render(s.renderer())
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		// Verify SQL has INSERT INTO
		if !strings.Contains(result.SQL, "INSERT INTO") {
			t.Errorf("SQL missing INSERT INTO: %s", result.SQL)
		}

		// Verify SQL has multiple VALUES tuples (3 sets of parentheses after VALUES)
		valuesIdx := strings.Index(result.SQL, "VALUES")
		if valuesIdx == -1 {
			t.Fatalf("SQL missing VALUES: %s", result.SQL)
		}
		valuesPart := result.SQL[valuesIdx:]
		tupleCount := strings.Count(valuesPart, "(")
		if tupleCount != 3 {
			t.Errorf("expected 3 value tuples, got %d in: %s", tupleCount, valuesPart)
		}

		// Verify indexed params are present
		if !strings.Contains(result.SQL, "email_0") {
			t.Errorf("SQL missing email_0 param: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "email_1") {
			t.Errorf("SQL missing email_1 param: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "email_2") {
			t.Errorf("SQL missing email_2 param: %s", result.SQL)
		}
		if !strings.Contains(result.SQL, "name_0") {
			t.Errorf("SQL missing name_0 param: %s", result.SQL)
		}

		t.Logf("Multi-row INSERT SQL: %s", result.SQL)
	})

	t.Run("single record produces single VALUES tuple", func(t *testing.T) {
		instance := s.getInstance()
		metadata := s.getMetadata()

		table, _ := instance.TryT(s.getTableName())
		builder := astql.Insert(table)

		values := instance.ValueMap()
		for _, field := range metadata.Fields {
			dbCol := field.Tags["db"]
			if dbCol == "" || dbCol == "-" {
				continue
			}
			constraints := field.Tags["constraints"]
			if contains(constraints, "primarykey") || contains(constraints, "primary_key") {
				continue
			}
			f, _ := instance.TryF(dbCol)
			p, _ := instance.TryP(dbCol + "_0")
			values[f] = p
		}
		builder = builder.Values(values)

		result, err := builder.Render(s.renderer())
		if err != nil {
			t.Fatalf("Render() failed: %v", err)
		}

		valuesIdx := strings.Index(result.SQL, "VALUES")
		if valuesIdx == -1 {
			t.Fatalf("SQL missing VALUES: %s", result.SQL)
		}
		valuesPart := result.SQL[valuesIdx:]
		tupleCount := strings.Count(valuesPart, "(")
		if tupleCount != 1 {
			t.Errorf("expected 1 value tuple, got %d in: %s", tupleCount, valuesPart)
		}
	})
}
