// Package testing provides test utilities and mocks for cereal-based applications.
//
// This package includes mock database implementations, result builders, and
// assertion helpers to make testing cereal queries easier and more comprehensive.
//
// Example usage:
//
//	func TestMyQuery(t *testing.T) {
//		mock := testing.NewMockDB(t)
//		mock.ExpectQuery().
//			WithRows([]MyModel{{ID: 1, Name: "Test"}}).
//			Times(1)
//
//		cereal, _ := cereal.New[MyModel](mock.DB(), "my_table")
//		results, err := cereal.Query().Exec(ctx, nil)
//
//		require.NoError(t, err)
//		assert.Len(t, results, 1)
//		mock.AssertExpectations()
//	}
package testing

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/jmoiron/sqlx"
)

// MockDB provides a configurable mock implementation of sqlx database operations.
// It tracks calls, allows configuring return values, and provides assertion methods.
type MockDB struct {
	t            *testing.T
	expectations []*Expectation
	calls        []MockCall
	mu           sync.Mutex
	currentIdx   int
}

// MockCall represents a single call to the mock database.
type MockCall struct {
	Query  string
	Params any
}

// Expectation represents an expected database call.
type Expectation struct {
	queryType    string // "query", "exec", "querySingle"
	rows         any    // rows to return for query
	result       MockResult
	err          error
	times        int // expected call count, -1 for any
	actualCalls  int
	rowsAffected int64
	lastInsertID int64
}

// MockResult implements sql.Result for testing.
type MockResult struct {
	lastInsertID int64
	rowsAffected int64
	err          error
}

// LastInsertId returns the configured last insert ID.
func (r MockResult) LastInsertId() (int64, error) {
	return r.lastInsertID, r.err
}

// RowsAffected returns the configured rows affected count.
func (r MockResult) RowsAffected() (int64, error) {
	return r.rowsAffected, r.err
}

// NewMockDB creates a new mock database for testing.
func NewMockDB(t *testing.T) *MockDB {
	return &MockDB{
		t:            t,
		expectations: make([]*Expectation, 0),
		calls:        make([]MockCall, 0),
	}
}

// ExpectQuery configures the mock to expect a query that returns rows.
func (m *MockDB) ExpectQuery() *ExpectationBuilder {
	exp := &Expectation{
		queryType: "query",
		times:     1,
	}
	m.mu.Lock()
	m.expectations = append(m.expectations, exp)
	m.mu.Unlock()
	return &ExpectationBuilder{exp: exp}
}

// ExpectQuerySingle configures the mock to expect a query that returns a single row.
func (m *MockDB) ExpectQuerySingle() *ExpectationBuilder {
	exp := &Expectation{
		queryType: "querySingle",
		times:     1,
	}
	m.mu.Lock()
	m.expectations = append(m.expectations, exp)
	m.mu.Unlock()
	return &ExpectationBuilder{exp: exp}
}

// ExpectExec configures the mock to expect an exec call.
func (m *MockDB) ExpectExec() *ExpectationBuilder {
	exp := &Expectation{
		queryType: "exec",
		times:     1,
	}
	m.mu.Lock()
	m.expectations = append(m.expectations, exp)
	m.mu.Unlock()
	return &ExpectationBuilder{exp: exp}
}

// RecordCall records a call to the mock database.
func (m *MockDB) RecordCall(query string, params any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, MockCall{Query: query, Params: params})
}

// NextExpectation returns the next expectation and increments the index.
func (m *MockDB) NextExpectation() *Expectation {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.currentIdx >= len(m.expectations) {
		return nil
	}
	exp := m.expectations[m.currentIdx]
	exp.actualCalls++
	if exp.actualCalls >= exp.times || exp.times == -1 {
		m.currentIdx++
	}
	return exp
}

// CurrentExpectation returns the current expectation without incrementing.
func (m *MockDB) CurrentExpectation() *Expectation {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.currentIdx >= len(m.expectations) {
		return nil
	}
	return m.expectations[m.currentIdx]
}

// Calls returns all recorded calls.
func (m *MockDB) Calls() []MockCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]MockCall, len(m.calls))
	copy(result, m.calls)
	return result
}

// CallCount returns the number of calls made.
func (m *MockDB) CallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

// Reset clears all expectations and recorded calls.
func (m *MockDB) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expectations = make([]*Expectation, 0)
	m.calls = make([]MockCall, 0)
	m.currentIdx = 0
}

// AssertExpectations verifies all expectations were met.
func (m *MockDB) AssertExpectations() {
	m.t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, exp := range m.expectations {
		if exp.times != -1 && exp.actualCalls != exp.times {
			m.t.Errorf("expectation %d: expected %d calls, got %d", i, exp.times, exp.actualCalls)
		}
	}
}

// AssertCalled verifies a specific number of calls were made.
func (m *MockDB) AssertCalled(expectedCalls int) {
	m.t.Helper()
	actualCalls := m.CallCount()
	if actualCalls != expectedCalls {
		m.t.Errorf("expected %d calls, got %d", expectedCalls, actualCalls)
	}
}

// ExpectationBuilder provides a fluent API for configuring expectations.
type ExpectationBuilder struct {
	exp *Expectation
}

// WithRows configures the rows to return for a query expectation.
func (b *ExpectationBuilder) WithRows(rows any) *ExpectationBuilder {
	b.exp.rows = rows
	return b
}

// WithResult configures the result for an exec expectation.
func (b *ExpectationBuilder) WithResult(rowsAffected, lastInsertID int64) *ExpectationBuilder {
	b.exp.rowsAffected = rowsAffected
	b.exp.lastInsertID = lastInsertID
	b.exp.result = MockResult{
		lastInsertID: lastInsertID,
		rowsAffected: rowsAffected,
	}
	return b
}

// WithError configures an error to return.
func (b *ExpectationBuilder) WithError(err error) *ExpectationBuilder {
	b.exp.err = err
	return b
}

// Times configures how many times this expectation should match.
func (b *ExpectationBuilder) Times(n int) *ExpectationBuilder {
	b.exp.times = n
	return b
}

// AnyTimes configures the expectation to match any number of times.
func (b *ExpectationBuilder) AnyTimes() *ExpectationBuilder {
	b.exp.times = -1
	return b
}

// MockRows implements sqlx.Rows for testing.
type MockRows struct {
	data       reflect.Value // slice of structs
	index      int
	columns    []string
	closed     bool
	currentRow reflect.Value
	err        error
}

// NewMockRows creates mock rows from a slice of structs.
func NewMockRows(data any) *MockRows {
	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	var columns []string
	if v.Kind() == reflect.Slice && v.Len() > 0 {
		elem := v.Index(0)
		if elem.Kind() == reflect.Ptr {
			elem = elem.Elem()
		}
		if elem.Kind() == reflect.Struct {
			t := elem.Type()
			for i := 0; i < t.NumField(); i++ {
				field := t.Field(i)
				dbTag := field.Tag.Get("db")
				if dbTag != "" && dbTag != "-" {
					columns = append(columns, dbTag)
				} else {
					columns = append(columns, field.Name)
				}
			}
		}
	}

	return &MockRows{
		data:    v,
		index:   -1,
		columns: columns,
	}
}

// Next advances to the next row.
func (r *MockRows) Next() bool {
	if r.closed || r.err != nil {
		return false
	}
	r.index++
	if r.index >= r.data.Len() {
		return false
	}
	r.currentRow = r.data.Index(r.index)
	if r.currentRow.Kind() == reflect.Ptr {
		r.currentRow = r.currentRow.Elem()
	}
	return true
}

// Close closes the rows.
func (r *MockRows) Close() error {
	r.closed = true
	return nil
}

// Err returns any error that occurred during iteration.
func (r *MockRows) Err() error {
	return r.err
}

// Columns returns the column names.
func (r *MockRows) Columns() ([]string, error) {
	return r.columns, nil
}

// ColumnTypes returns column type info (minimal implementation).
func (*MockRows) ColumnTypes() ([]*sql.ColumnType, error) {
	return nil, nil
}

// Scan copies the current row values into dest.
func (r *MockRows) Scan(dest ...any) error {
	if r.currentRow.Kind() != reflect.Struct {
		return errors.New("current row is not a struct")
	}

	t := r.currentRow.Type()
	for i := 0; i < t.NumField() && i < len(dest); i++ {
		fieldVal := r.currentRow.Field(i)
		destVal := reflect.ValueOf(dest[i])
		if destVal.Kind() != reflect.Ptr {
			return errors.New("dest must be a pointer")
		}
		destVal.Elem().Set(fieldVal)
	}
	return nil
}

// StructScan scans the current row into a struct.
func (r *MockRows) StructScan(dest any) error {
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr {
		return errors.New("dest must be a pointer")
	}
	destVal = destVal.Elem()
	if destVal.Kind() != reflect.Struct {
		return errors.New("dest must be a pointer to struct")
	}

	if r.currentRow.Kind() != reflect.Struct {
		return errors.New("current row is not a struct")
	}

	// Copy field by field matching by db tag or name
	srcType := r.currentRow.Type()
	destType := destVal.Type()

	for i := 0; i < srcType.NumField(); i++ {
		srcField := srcType.Field(i)
		srcTag := srcField.Tag.Get("db")
		if srcTag == "" {
			srcTag = srcField.Name
		}

		for j := 0; j < destType.NumField(); j++ {
			destField := destType.Field(j)
			destTag := destField.Tag.Get("db")
			if destTag == "" {
				destTag = destField.Name
			}

			if srcTag == destTag && srcField.Type == destField.Type {
				destVal.Field(j).Set(r.currentRow.Field(i))
				break
			}
		}
	}

	return nil
}

// SliceScan scans the current row into a slice.
func (r *MockRows) SliceScan() ([]any, error) {
	if r.currentRow.Kind() != reflect.Struct {
		return nil, errors.New("current row is not a struct")
	}

	result := make([]any, r.currentRow.NumField())
	for i := 0; i < r.currentRow.NumField(); i++ {
		result[i] = r.currentRow.Field(i).Interface()
	}
	return result, nil
}

// MapScan scans the current row into a map.
func (r *MockRows) MapScan(dest map[string]any) error {
	if r.currentRow.Kind() != reflect.Struct {
		return errors.New("current row is not a struct")
	}

	t := r.currentRow.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		dbTag := field.Tag.Get("db")
		if dbTag == "" {
			dbTag = field.Name
		}
		dest[dbTag] = r.currentRow.Field(i).Interface()
	}
	return nil
}

// MockExtContext implements sqlx.ExtContext for testing query execution.
type MockExtContext struct {
	mock   *MockDB
	driver string
}

// NewMockExtContext creates a new MockExtContext.
func NewMockExtContext(mock *MockDB) *MockExtContext {
	return &MockExtContext{
		mock:   mock,
		driver: "postgres",
	}
}

// DriverName returns the driver name.
func (m *MockExtContext) DriverName() string {
	return m.driver
}

// Rebind transforms a query for the database driver.
func (*MockExtContext) Rebind(query string) string {
	return query
}

// BindNamed binds a named query.
func (*MockExtContext) BindNamed(query string, arg any) (bound string, args []any, err error) {
	return sqlx.Named(query, arg)
}

// QueryContext executes a query and returns rows.
func (m *MockExtContext) QueryContext(_ context.Context, query string, args ...any) (*sql.Rows, error) {
	m.mock.RecordCall(query, args)
	exp := m.mock.NextExpectation()
	if exp == nil {
		return nil, errors.New("unexpected query call")
	}
	if exp.err != nil {
		return nil, exp.err
	}
	// Return nil rows - the mock rows are handled separately
	return nil, nil
}

// QueryxContext executes a query and returns sqlx.Rows.
func (m *MockExtContext) QueryxContext(_ context.Context, query string, args ...any) (*sqlx.Rows, error) {
	m.mock.RecordCall(query, args)
	exp := m.mock.NextExpectation()
	if exp == nil {
		return nil, errors.New("unexpected query call")
	}
	if exp.err != nil {
		return nil, exp.err
	}
	return nil, nil
}

// ExecContext executes a query without returning rows.
func (m *MockExtContext) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	m.mock.RecordCall(query, args)
	exp := m.mock.NextExpectation()
	if exp == nil {
		return nil, errors.New("unexpected exec call")
	}
	if exp.err != nil {
		return nil, exp.err
	}
	return exp.result, nil
}

// MockNamedQueryResult wraps MockRows to be returned from named queries.
type MockNamedQueryResult struct {
	*MockRows
}

// MockQueryer provides a queryable interface for testing with sqlx.NamedQueryContext.
type MockQueryer struct {
	*MockExtContext
	rows *MockRows
}

// NewMockQueryer creates a MockQueryer with pre-configured rows.
func NewMockQueryer(mock *MockDB, rows any) *MockQueryer {
	return &MockQueryer{
		MockExtContext: NewMockExtContext(mock),
		rows:           NewMockRows(rows),
	}
}

// GetRows returns the mock rows for iteration.
func (m *MockQueryer) GetRows() *MockRows {
	return m.rows
}

// MockTx implements a mock transaction.
type MockTx struct {
	*MockExtContext
	committed  bool
	rolledBack bool
}

// NewMockTx creates a new mock transaction.
func NewMockTx(mock *MockDB) *MockTx {
	return &MockTx{
		MockExtContext: NewMockExtContext(mock),
	}
}

// Commit commits the transaction.
func (m *MockTx) Commit() error {
	m.committed = true
	return nil
}

// Rollback rolls back the transaction.
func (m *MockTx) Rollback() error {
	m.rolledBack = true
	return nil
}

// IsCommitted returns whether the transaction was committed.
func (m *MockTx) IsCommitted() bool {
	return m.committed
}

// IsRolledBack returns whether the transaction was rolled back.
func (m *MockTx) IsRolledBack() bool {
	return m.rolledBack
}

// MockDriver implements database/sql/driver interfaces for testing.
type MockDriver struct {
	mock *MockDB
}

// Open opens a new connection.
func (d *MockDriver) Open(_ string) (driver.Conn, error) {
	return &MockConn{mock: d.mock}, nil
}

// MockConn implements driver.Conn.
type MockConn struct {
	mock   *MockDB
	closed bool
}

// Prepare prepares a statement.
func (c *MockConn) Prepare(query string) (driver.Stmt, error) {
	return &MockStmt{mock: c.mock, query: query}, nil
}

// Close closes the connection.
func (c *MockConn) Close() error {
	c.closed = true
	return nil
}

// Begin starts a transaction.
func (*MockConn) Begin() (driver.Tx, error) {
	return &MockDriverTx{}, nil
}

// MockStmt implements driver.Stmt.
type MockStmt struct {
	mock  *MockDB
	query string
}

// Close closes the statement.
func (*MockStmt) Close() error {
	return nil
}

// NumInput returns the number of placeholders.
func (*MockStmt) NumInput() int {
	return -1
}

// Exec executes the statement.
func (s *MockStmt) Exec(args []driver.Value) (driver.Result, error) {
	s.mock.RecordCall(s.query, args)
	exp := s.mock.CurrentExpectation()
	if exp != nil && exp.err != nil {
		return nil, exp.err
	}
	return MockDriverResult{}, nil
}

// Query executes a query.
func (s *MockStmt) Query(args []driver.Value) (driver.Rows, error) {
	s.mock.RecordCall(s.query, args)
	exp := s.mock.CurrentExpectation()
	if exp != nil && exp.err != nil {
		return nil, exp.err
	}
	return &MockDriverRows{}, nil
}

// MockDriverTx implements driver.Tx.
type MockDriverTx struct{}

// Commit commits the transaction.
func (*MockDriverTx) Commit() error { return nil }

// Rollback rolls back the transaction.
func (*MockDriverTx) Rollback() error { return nil }

// MockDriverResult implements driver.Result.
type MockDriverResult struct {
	lastID   int64
	affected int64
}

// LastInsertId returns the last insert ID.
func (r MockDriverResult) LastInsertId() (int64, error) {
	return r.lastID, nil
}

// RowsAffected returns the rows affected.
func (r MockDriverResult) RowsAffected() (int64, error) {
	return r.affected, nil
}

// MockDriverRows implements driver.Rows.
type MockDriverRows struct {
	closed bool
}

// Columns returns column names.
func (*MockDriverRows) Columns() []string {
	return []string{}
}

// Close closes the rows.
func (r *MockDriverRows) Close() error {
	r.closed = true
	return nil
}

// Next advances to the next row.
func (*MockDriverRows) Next(_ []driver.Value) error {
	return nil
}
