package testing

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type testModel struct {
	ID    int    `db:"id"`
	Name  string `db:"name"`
	Email string `db:"email"`
}

func TestMockResult(t *testing.T) {
	t.Run("LastInsertId", func(t *testing.T) {
		r := MockResult{lastInsertID: 42, rowsAffected: 1}
		id, err := r.LastInsertId()
		if err != nil {
			t.Fatalf("LastInsertId() error: %v", err)
		}
		if id != 42 {
			t.Errorf("LastInsertId() = %d, want 42", id)
		}
	})

	t.Run("RowsAffected", func(t *testing.T) {
		r := MockResult{lastInsertID: 1, rowsAffected: 5}
		affected, err := r.RowsAffected()
		if err != nil {
			t.Fatalf("RowsAffected() error: %v", err)
		}
		if affected != 5 {
			t.Errorf("RowsAffected() = %d, want 5", affected)
		}
	})

	t.Run("with error", func(t *testing.T) {
		expectedErr := errors.New("mock error")
		r := MockResult{err: expectedErr}
		_, err := r.LastInsertId()
		if !errors.Is(err, expectedErr) {
			t.Errorf("LastInsertId() error = %v, want %v", err, expectedErr)
		}
		_, err = r.RowsAffected()
		if !errors.Is(err, expectedErr) {
			t.Errorf("RowsAffected() error = %v, want %v", err, expectedErr)
		}
	})
}

func TestMockDB(t *testing.T) {
	t.Run("NewMockDB", func(t *testing.T) {
		mock := NewMockDB(t)
		if mock == nil {
			t.Fatal("NewMockDB() returned nil")
		}
		if mock.t != t {
			t.Error("mock.t not set correctly")
		}
	})

	t.Run("ExpectQuery", func(t *testing.T) {
		mock := NewMockDB(t)
		builder := mock.ExpectQuery()
		if builder == nil {
			t.Fatal("ExpectQuery() returned nil")
		}
		if len(mock.expectations) != 1 {
			t.Errorf("expected 1 expectation, got %d", len(mock.expectations))
		}
		if mock.expectations[0].queryType != "query" {
			t.Errorf("queryType = %s, want query", mock.expectations[0].queryType)
		}
	})

	t.Run("ExpectQuerySingle", func(t *testing.T) {
		mock := NewMockDB(t)
		builder := mock.ExpectQuerySingle()
		if builder == nil {
			t.Fatal("ExpectQuerySingle() returned nil")
		}
		if mock.expectations[0].queryType != "querySingle" {
			t.Errorf("queryType = %s, want querySingle", mock.expectations[0].queryType)
		}
	})

	t.Run("ExpectExec", func(t *testing.T) {
		mock := NewMockDB(t)
		builder := mock.ExpectExec()
		if builder == nil {
			t.Fatal("ExpectExec() returned nil")
		}
		if mock.expectations[0].queryType != "exec" {
			t.Errorf("queryType = %s, want exec", mock.expectations[0].queryType)
		}
	})

	t.Run("RecordCall", func(t *testing.T) {
		mock := NewMockDB(t)
		mock.RecordCall("SELECT * FROM users", map[string]any{"id": 1})

		calls := mock.Calls()
		if len(calls) != 1 {
			t.Fatalf("expected 1 call, got %d", len(calls))
		}
		if calls[0].Query != "SELECT * FROM users" {
			t.Errorf("Query = %s, want SELECT * FROM users", calls[0].Query)
		}
	})

	t.Run("NextExpectation", func(t *testing.T) {
		mock := NewMockDB(t)
		mock.ExpectQuery().Times(1)
		mock.ExpectExec().Times(1)

		exp1 := mock.NextExpectation()
		if exp1 == nil || exp1.queryType != "query" {
			t.Error("first NextExpectation should return query")
		}

		exp2 := mock.NextExpectation()
		if exp2 == nil || exp2.queryType != "exec" {
			t.Error("second NextExpectation should return exec")
		}

		exp3 := mock.NextExpectation()
		if exp3 != nil {
			t.Error("third NextExpectation should return nil")
		}
	})

	t.Run("CurrentExpectation", func(t *testing.T) {
		mock := NewMockDB(t)
		mock.ExpectQuery()

		exp := mock.CurrentExpectation()
		if exp == nil {
			t.Fatal("CurrentExpectation() returned nil")
		}
		if exp.queryType != "query" {
			t.Errorf("queryType = %s, want query", exp.queryType)
		}

		// Should still be the same
		exp2 := mock.CurrentExpectation()
		if exp2 != exp {
			t.Error("CurrentExpectation should return same expectation")
		}
	})

	t.Run("CallCount", func(t *testing.T) {
		mock := NewMockDB(t)
		if mock.CallCount() != 0 {
			t.Errorf("CallCount() = %d, want 0", mock.CallCount())
		}

		mock.RecordCall("query1", nil)
		mock.RecordCall("query2", nil)

		if mock.CallCount() != 2 {
			t.Errorf("CallCount() = %d, want 2", mock.CallCount())
		}
	})

	t.Run("Reset", func(t *testing.T) {
		mock := NewMockDB(t)
		mock.ExpectQuery()
		mock.RecordCall("query", nil)

		mock.Reset()

		if len(mock.expectations) != 0 {
			t.Error("Reset should clear expectations")
		}
		if mock.CallCount() != 0 {
			t.Error("Reset should clear calls")
		}
	})
}

func TestExpectationBuilder(t *testing.T) {
	t.Run("WithRows", func(t *testing.T) {
		mock := NewMockDB(t)
		rows := []testModel{{ID: 1, Name: "Test"}}
		builder := mock.ExpectQuery().WithRows(rows)

		if builder == nil {
			t.Fatal("WithRows() returned nil")
		}
		if mock.expectations[0].rows == nil {
			t.Error("rows not set")
		}
	})

	t.Run("WithResult", func(t *testing.T) {
		mock := NewMockDB(t)
		builder := mock.ExpectExec().WithResult(5, 10)

		if builder == nil {
			t.Fatal("WithResult() returned nil")
		}
		if mock.expectations[0].rowsAffected != 5 {
			t.Errorf("rowsAffected = %d, want 5", mock.expectations[0].rowsAffected)
		}
		if mock.expectations[0].lastInsertID != 10 {
			t.Errorf("lastInsertID = %d, want 10", mock.expectations[0].lastInsertID)
		}
	})

	t.Run("WithError", func(t *testing.T) {
		mock := NewMockDB(t)
		expectedErr := errors.New("test error")
		builder := mock.ExpectQuery().WithError(expectedErr)

		if builder == nil {
			t.Fatal("WithError() returned nil")
		}
		if !errors.Is(mock.expectations[0].err, expectedErr) {
			t.Error("error not set correctly")
		}
	})

	t.Run("Times", func(t *testing.T) {
		mock := NewMockDB(t)
		builder := mock.ExpectQuery().Times(3)

		if builder == nil {
			t.Fatal("Times() returned nil")
		}
		if mock.expectations[0].times != 3 {
			t.Errorf("times = %d, want 3", mock.expectations[0].times)
		}
	})

	t.Run("AnyTimes", func(t *testing.T) {
		mock := NewMockDB(t)
		builder := mock.ExpectQuery().AnyTimes()

		if builder == nil {
			t.Fatal("AnyTimes() returned nil")
		}
		if mock.expectations[0].times != -1 {
			t.Errorf("times = %d, want -1", mock.expectations[0].times)
		}
	})
}

func TestMockRows(t *testing.T) {
	t.Run("NewMockRows", func(t *testing.T) {
		data := []testModel{
			{ID: 1, Name: "Alice", Email: "alice@example.com"},
			{ID: 2, Name: "Bob", Email: "bob@example.com"},
		}
		rows := NewMockRows(data)

		if rows == nil {
			t.Fatal("NewMockRows() returned nil")
		}
		if rows.data.Len() != 2 {
			t.Errorf("data length = %d, want 2", rows.data.Len())
		}
	})

	t.Run("Next and Close", func(t *testing.T) {
		data := []testModel{
			{ID: 1, Name: "Alice"},
			{ID: 2, Name: "Bob"},
		}
		rows := NewMockRows(data)

		if !rows.Next() {
			t.Error("first Next() should return true")
		}
		if !rows.Next() {
			t.Error("second Next() should return true")
		}
		if rows.Next() {
			t.Error("third Next() should return false")
		}

		if err := rows.Close(); err != nil {
			t.Errorf("Close() error: %v", err)
		}
		if !rows.closed {
			t.Error("rows should be closed")
		}

		if rows.Next() {
			t.Error("Next() after Close() should return false")
		}
	})

	t.Run("Err", func(t *testing.T) {
		rows := NewMockRows([]testModel{})
		if rows.Err() != nil {
			t.Error("Err() should return nil initially")
		}

		rows.err = errors.New("test error")
		if rows.Err() == nil {
			t.Error("Err() should return set error")
		}
	})

	t.Run("Columns", func(t *testing.T) {
		data := []testModel{{ID: 1, Name: "Test", Email: "test@example.com"}}
		rows := NewMockRows(data)

		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("Columns() error: %v", err)
		}
		if len(cols) != 3 {
			t.Errorf("expected 3 columns, got %d", len(cols))
		}
	})

	t.Run("ColumnTypes", func(t *testing.T) {
		rows := NewMockRows([]testModel{})
		types, err := rows.ColumnTypes()
		if err != nil {
			t.Errorf("ColumnTypes() error: %v", err)
		}
		if types != nil {
			t.Error("ColumnTypes() should return nil")
		}
	})

	t.Run("Scan", func(t *testing.T) {
		data := []testModel{{ID: 42, Name: "Test", Email: "test@example.com"}}
		rows := NewMockRows(data)
		rows.Next()

		var id int
		var name string
		var email string
		err := rows.Scan(&id, &name, &email)
		if err != nil {
			t.Fatalf("Scan() error: %v", err)
		}
		if id != 42 {
			t.Errorf("id = %d, want 42", id)
		}
		if name != "Test" {
			t.Errorf("name = %s, want Test", name)
		}
	})

	t.Run("StructScan", func(t *testing.T) {
		data := []testModel{{ID: 42, Name: "Test", Email: "test@example.com"}}
		rows := NewMockRows(data)
		rows.Next()

		var dest testModel
		err := rows.StructScan(&dest)
		if err != nil {
			t.Fatalf("StructScan() error: %v", err)
		}
		if dest.ID != 42 {
			t.Errorf("ID = %d, want 42", dest.ID)
		}
		if dest.Name != "Test" {
			t.Errorf("Name = %s, want Test", dest.Name)
		}
	})

	t.Run("SliceScan", func(t *testing.T) {
		data := []testModel{{ID: 42, Name: "Test", Email: "test@example.com"}}
		rows := NewMockRows(data)
		rows.Next()

		slice, err := rows.SliceScan()
		if err != nil {
			t.Fatalf("SliceScan() error: %v", err)
		}
		if len(slice) != 3 {
			t.Errorf("expected 3 values, got %d", len(slice))
		}
		if slice[0].(int) != 42 {
			t.Errorf("first value = %v, want 42", slice[0])
		}
	})

	t.Run("MapScan", func(t *testing.T) {
		data := []testModel{{ID: 42, Name: "Test", Email: "test@example.com"}}
		rows := NewMockRows(data)
		rows.Next()

		dest := make(map[string]any)
		err := rows.MapScan(dest)
		if err != nil {
			t.Fatalf("MapScan() error: %v", err)
		}
		if dest["id"].(int) != 42 {
			t.Errorf("id = %v, want 42", dest["id"])
		}
		if dest["name"].(string) != "Test" {
			t.Errorf("name = %v, want Test", dest["name"])
		}
	})

	t.Run("Scan errors", func(t *testing.T) {
		rows := NewMockRows([]int{1, 2, 3}) // non-struct slice
		rows.Next()

		var id int
		err := rows.Scan(&id)
		if err == nil {
			t.Error("Scan on non-struct should error")
		}
	})

	t.Run("StructScan errors", func(t *testing.T) {
		data := []testModel{{ID: 42}}
		rows := NewMockRows(data)
		rows.Next()

		// Not a pointer
		var dest testModel
		err := rows.StructScan(dest)
		if err == nil {
			t.Error("StructScan with non-pointer should error")
		}

		// Pointer to non-struct
		var num int
		err = rows.StructScan(&num)
		if err == nil {
			t.Error("StructScan with pointer to non-struct should error")
		}
	})
}

func TestMockExtContext(t *testing.T) {
	t.Run("NewMockExtContext", func(t *testing.T) {
		mock := NewMockDB(t)
		ctx := NewMockExtContext(mock)

		if ctx == nil {
			t.Fatal("NewMockExtContext() returned nil")
		}
		if ctx.mock != mock {
			t.Error("mock not set correctly")
		}
	})

	t.Run("DriverName", func(t *testing.T) {
		mock := NewMockDB(t)
		ctx := NewMockExtContext(mock)

		if ctx.DriverName() != "postgres" {
			t.Errorf("DriverName() = %s, want postgres", ctx.DriverName())
		}
	})

	t.Run("Rebind", func(t *testing.T) {
		mock := NewMockDB(t)
		ctx := NewMockExtContext(mock)

		query := "SELECT * FROM users WHERE id = ?"
		result := ctx.Rebind(query)
		if result != query {
			t.Errorf("Rebind() = %s, want %s", result, query)
		}
	})

	t.Run("BindNamed", func(t *testing.T) {
		mock := NewMockDB(t)
		ctx := NewMockExtContext(mock)

		query := "SELECT * FROM users WHERE id = :id"
		_, _, err := ctx.BindNamed(query, map[string]any{"id": 1})
		if err != nil {
			t.Errorf("BindNamed() error: %v", err)
		}
	})

	t.Run("QueryContext", func(t *testing.T) {
		mock := NewMockDB(t)
		mock.ExpectQuery()
		ctx := NewMockExtContext(mock)

		rows, err := ctx.QueryContext(context.Background(), "SELECT * FROM users")
		if rows != nil {
			defer rows.Close()
		}
		if err != nil {
			t.Errorf("QueryContext() error: %v", err)
		}
		if mock.CallCount() != 1 {
			t.Errorf("expected 1 call, got %d", mock.CallCount())
		}
	})

	t.Run("QueryContext with error", func(t *testing.T) {
		mock := NewMockDB(t)
		expectedErr := errors.New("query error")
		mock.ExpectQuery().WithError(expectedErr)
		ctx := NewMockExtContext(mock)

		rows, err := ctx.QueryContext(context.Background(), "SELECT * FROM users")
		if rows != nil {
			defer rows.Close()
		}
		if !errors.Is(err, expectedErr) {
			t.Errorf("QueryContext() error = %v, want %v", err, expectedErr)
		}
	})

	t.Run("QueryContext unexpected call", func(t *testing.T) {
		mock := NewMockDB(t)
		ctx := NewMockExtContext(mock)

		rows, err := ctx.QueryContext(context.Background(), "SELECT * FROM users")
		if rows != nil {
			defer rows.Close()
		}
		if err == nil {
			t.Error("QueryContext() should error on unexpected call")
		}
	})

	t.Run("QueryxContext", func(t *testing.T) {
		mock := NewMockDB(t)
		mock.ExpectQuery()
		ctx := NewMockExtContext(mock)

		rows, err := ctx.QueryxContext(context.Background(), "SELECT * FROM users")
		if rows != nil {
			defer rows.Close()
		}
		if err != nil {
			t.Errorf("QueryxContext() error: %v", err)
		}
	})

	t.Run("ExecContext", func(t *testing.T) {
		mock := NewMockDB(t)
		mock.ExpectExec().WithResult(1, 42)
		ctx := NewMockExtContext(mock)

		result, err := ctx.ExecContext(context.Background(), "INSERT INTO users (name) VALUES ('test')")
		if err != nil {
			t.Fatalf("ExecContext() error: %v", err)
		}
		if result == nil {
			t.Fatal("ExecContext() returned nil result")
		}
	})
}

func TestMockQueryer(t *testing.T) {
	t.Run("NewMockQueryer", func(t *testing.T) {
		mock := NewMockDB(t)
		data := []testModel{{ID: 1}}
		queryer := NewMockQueryer(mock, data)

		if queryer == nil {
			t.Fatal("NewMockQueryer() returned nil")
		}
	})

	t.Run("GetRows", func(t *testing.T) {
		mock := NewMockDB(t)
		data := []testModel{{ID: 1}, {ID: 2}}
		queryer := NewMockQueryer(mock, data)

		rows := queryer.GetRows()
		if rows == nil {
			t.Fatal("GetRows() returned nil")
		}
		if rows.data.Len() != 2 {
			t.Errorf("rows length = %d, want 2", rows.data.Len())
		}
	})
}

func TestMockTx(t *testing.T) {
	t.Run("NewMockTx", func(t *testing.T) {
		mock := NewMockDB(t)
		tx := NewMockTx(mock)

		if tx == nil {
			t.Fatal("NewMockTx() returned nil")
		}
	})

	t.Run("Commit", func(t *testing.T) {
		mock := NewMockDB(t)
		tx := NewMockTx(mock)

		if err := tx.Commit(); err != nil {
			t.Errorf("Commit() error: %v", err)
		}
		if !tx.IsCommitted() {
			t.Error("IsCommitted() should return true")
		}
	})

	t.Run("Rollback", func(t *testing.T) {
		mock := NewMockDB(t)
		tx := NewMockTx(mock)

		if err := tx.Rollback(); err != nil {
			t.Errorf("Rollback() error: %v", err)
		}
		if !tx.IsRolledBack() {
			t.Error("IsRolledBack() should return true")
		}
	})
}

func TestMockDriver(t *testing.T) {
	t.Run("Open", func(t *testing.T) {
		mock := NewMockDB(t)
		driver := &MockDriver{mock: mock}

		conn, err := driver.Open("test")
		if err != nil {
			t.Fatalf("Open() error: %v", err)
		}
		if conn == nil {
			t.Fatal("Open() returned nil")
		}
	})
}

func TestMockConn(t *testing.T) {
	mock := NewMockDB(t)
	conn := &MockConn{mock: mock}

	t.Run("Prepare", func(t *testing.T) {
		stmt, err := conn.Prepare("SELECT * FROM users")
		if err != nil {
			t.Fatalf("Prepare() error: %v", err)
		}
		if stmt == nil {
			t.Fatal("Prepare() returned nil")
		}
	})

	t.Run("Close", func(t *testing.T) {
		if err := conn.Close(); err != nil {
			t.Errorf("Close() error: %v", err)
		}
		if !conn.closed {
			t.Error("conn should be closed")
		}
	})

	t.Run("Begin", func(t *testing.T) {
		tx, err := conn.Begin()
		if err != nil {
			t.Fatalf("Begin() error: %v", err)
		}
		if tx == nil {
			t.Fatal("Begin() returned nil")
		}
	})
}

func TestMockStmt(t *testing.T) {
	mock := NewMockDB(t)
	stmt := &MockStmt{mock: mock, query: "SELECT * FROM users"}

	t.Run("Close", func(t *testing.T) {
		if err := stmt.Close(); err != nil {
			t.Errorf("Close() error: %v", err)
		}
	})

	t.Run("NumInput", func(t *testing.T) {
		if stmt.NumInput() != -1 {
			t.Errorf("NumInput() = %d, want -1", stmt.NumInput())
		}
	})

	t.Run("Exec", func(t *testing.T) {
		result, err := stmt.Exec(nil)
		if err != nil {
			t.Fatalf("Exec() error: %v", err)
		}
		if result == nil {
			t.Fatal("Exec() returned nil")
		}
	})

	t.Run("Query", func(t *testing.T) {
		rows, err := stmt.Query(nil)
		if err != nil {
			t.Fatalf("Query() error: %v", err)
		}
		if rows == nil {
			t.Fatal("Query() returned nil")
		}
	})
}

func TestMockDriverTx(t *testing.T) {
	tx := &MockDriverTx{}

	t.Run("Commit", func(t *testing.T) {
		if err := tx.Commit(); err != nil {
			t.Errorf("Commit() error: %v", err)
		}
	})

	t.Run("Rollback", func(t *testing.T) {
		if err := tx.Rollback(); err != nil {
			t.Errorf("Rollback() error: %v", err)
		}
	})
}

func TestMockDriverResult(t *testing.T) {
	result := MockDriverResult{lastID: 42, affected: 5}

	t.Run("LastInsertId", func(t *testing.T) {
		id, err := result.LastInsertId()
		if err != nil {
			t.Fatalf("LastInsertId() error: %v", err)
		}
		if id != 42 {
			t.Errorf("LastInsertId() = %d, want 42", id)
		}
	})

	t.Run("RowsAffected", func(t *testing.T) {
		affected, err := result.RowsAffected()
		if err != nil {
			t.Fatalf("RowsAffected() error: %v", err)
		}
		if affected != 5 {
			t.Errorf("RowsAffected() = %d, want 5", affected)
		}
	})
}

func TestMockDriverRows(t *testing.T) {
	rows := &MockDriverRows{}

	t.Run("Columns", func(t *testing.T) {
		cols := rows.Columns()
		if len(cols) != 0 {
			t.Errorf("Columns() length = %d, want 0", len(cols))
		}
	})

	t.Run("Close", func(t *testing.T) {
		if err := rows.Close(); err != nil {
			t.Errorf("Close() error: %v", err)
		}
		if !rows.closed {
			t.Error("rows should be closed")
		}
	})

	t.Run("Next", func(t *testing.T) {
		if err := rows.Next(nil); err != nil {
			t.Errorf("Next() error: %v", err)
		}
	})
}

func TestAssertExpectations(t *testing.T) {
	// Use a mock testing.T to capture failures
	mockT := &testing.T{}

	t.Run("expectations met", func(_ *testing.T) {
		mock := NewMockDB(mockT)
		mock.ExpectQuery().Times(1)
		mock.NextExpectation() // Simulate call

		mock.AssertExpectations()
		// Should not fail
	})

	t.Run("AnyTimes expectations", func(_ *testing.T) {
		mock := NewMockDB(mockT)
		mock.ExpectQuery().AnyTimes()
		// No calls made, but AnyTimes should not fail

		mock.AssertExpectations()
	})
}

func TestAssertCalled(t *testing.T) {
	mockT := &testing.T{}

	t.Run("correct call count", func(_ *testing.T) {
		mock := NewMockDB(mockT)
		mock.RecordCall("query1", nil)
		mock.RecordCall("query2", nil)

		mock.AssertCalled(2)
	})
}

func TestMockRows_EdgeCases(t *testing.T) {
	t.Run("pointer slice elements", func(t *testing.T) {
		data := []*testModel{
			{ID: 1, Name: "Alice"},
			{ID: 2, Name: "Bob"},
		}
		rows := NewMockRows(data)

		if !rows.Next() {
			t.Error("Next() should return true")
		}
		if rows.currentRow.Kind() != reflect.Struct {
			t.Error("currentRow should be dereferenced struct")
		}
	})

	t.Run("empty slice", func(t *testing.T) {
		rows := NewMockRows([]testModel{})

		if rows.Next() {
			t.Error("Next() on empty slice should return false")
		}

		cols, _ := rows.Columns()
		if len(cols) != 0 {
			t.Error("Columns() on empty slice should return empty")
		}
	})

	t.Run("error prevents Next", func(t *testing.T) {
		data := []testModel{{ID: 1}}
		rows := NewMockRows(data)
		rows.err = errors.New("test error")

		if rows.Next() {
			t.Error("Next() with error should return false")
		}
	})
}
