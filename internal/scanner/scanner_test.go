package scanner

import (
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/zoobzio/atom"
	"github.com/zoobzio/sentinel"
)

// mockColScanner implements ColScanner for testing.
type mockColScanner struct {
	columns []string
	rows    [][]any
	rowIdx  int
	scanErr error
	colErr  error
	iterErr error
}

func (m *mockColScanner) Columns() ([]string, error) {
	if m.colErr != nil {
		return nil, m.colErr
	}
	return m.columns, nil
}

func (m *mockColScanner) Scan(dest ...any) error {
	if m.scanErr != nil {
		return m.scanErr
	}
	if m.rowIdx >= len(m.rows) {
		return sql.ErrNoRows
	}
	row := m.rows[m.rowIdx]
	for i, d := range dest {
		if i >= len(row) {
			break
		}
		assignDest(d, row[i])
	}
	m.rowIdx++
	return nil
}

func (m *mockColScanner) Err() error {
	return m.iterErr
}

// assignDest assigns a value to a scan destination.
func assignDest(dest, val any) {
	switch d := dest.(type) {
	case *string:
		if v, ok := val.(string); ok {
			*d = v
		}
	case *int64:
		if v, ok := val.(int64); ok {
			*d = v
		}
	case *uint64:
		if v, ok := val.(uint64); ok {
			*d = v
		}
	case *float64:
		if v, ok := val.(float64); ok {
			*d = v
		}
	case *bool:
		if v, ok := val.(bool); ok {
			*d = v
		}
	case *time.Time:
		if v, ok := val.(time.Time); ok {
			*d = v
		}
	case *[]byte:
		if v, ok := val.([]byte); ok {
			*d = v
		}
	case *sql.NullString:
		if v, ok := val.(*string); ok {
			if v != nil {
				d.String = *v
				d.Valid = true
			} else {
				d.Valid = false
			}
		} else if v, ok := val.(string); ok {
			d.String = v
			d.Valid = true
		}
	case *sql.NullInt64:
		if v, ok := val.(*int64); ok {
			if v != nil {
				d.Int64 = *v
				d.Valid = true
			} else {
				d.Valid = false
			}
		} else if v, ok := val.(int64); ok {
			d.Int64 = v
			d.Valid = true
		}
	case *sql.NullFloat64:
		if v, ok := val.(*float64); ok {
			if v != nil {
				d.Float64 = *v
				d.Valid = true
			} else {
				d.Valid = false
			}
		} else if v, ok := val.(float64); ok {
			d.Float64 = v
			d.Valid = true
		}
	case *sql.NullBool:
		if v, ok := val.(*bool); ok {
			if v != nil {
				d.Bool = *v
				d.Valid = true
			} else {
				d.Valid = false
			}
		} else if v, ok := val.(bool); ok {
			d.Bool = v
			d.Valid = true
		}
	case *sql.NullTime:
		if v, ok := val.(*time.Time); ok {
			if v != nil {
				d.Time = *v
				d.Valid = true
			} else {
				d.Valid = false
			}
		} else if v, ok := val.(time.Time); ok {
			d.Time = v
			d.Valid = true
		}
	case *any:
		*d = val
	}
}

// Test types for scanner tests.
type testUser struct {
	ID    int64  `db:"id"`
	Name  string `db:"name"`
	Email string `db:"email"`
	Age   int64  `db:"age"`
}

type testUserWithPointers struct {
	ID       int64   `db:"id"`
	Name     string  `db:"name"`
	Nickname *string `db:"nickname"`
	Age      *int64  `db:"age"`
}

type testUserWithTypes struct {
	ID        int64     `db:"id"`
	Name      string    `db:"name"`
	Score     float64   `db:"score"`
	Active    bool      `db:"active"`
	CreatedAt time.Time `db:"created_at"`
	Data      []byte    `db:"data"`
}

type testNoDBTags struct {
	ID   int64
	Name string
}

// buildMetadata creates sentinel.Metadata for a type.
func buildMetadata[T any]() sentinel.Metadata {
	var zero T
	t := reflect.TypeOf(zero)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	fields := make([]sentinel.FieldMetadata, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if !sf.IsExported() {
			continue
		}

		tags := make(map[string]string)
		if dbTag := sf.Tag.Get("db"); dbTag != "" {
			tags["db"] = dbTag
		}

		fields = append(fields, sentinel.FieldMetadata{
			Name:        sf.Name,
			Type:        sf.Type.String(),
			ReflectType: sf.Type,
			Tags:        tags,
			Index:       []int{i},
		})
	}

	return sentinel.Metadata{
		ReflectType: t,
		TypeName:    t.Name(),
		Fields:      fields,
	}
}

func TestNew(t *testing.T) {
	t.Run("creates scanner from metadata", func(t *testing.T) {
		metadata := buildMetadata[testUser]()
		s, err := New(metadata)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if s == nil {
			t.Fatal("New() returned nil scanner")
		}
		// Should have 4 columns mapped
		if len(s.byColumn) != 4 {
			t.Errorf("expected 4 columns, got %d", len(s.byColumn))
		}
	})

	t.Run("handles type with no db tags", func(t *testing.T) {
		metadata := buildMetadata[testNoDBTags]()
		s, err := New(metadata)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if len(s.byColumn) != 0 {
			t.Errorf("expected 0 columns for type without db tags, got %d", len(s.byColumn))
		}
	})

	t.Run("handles pointer fields", func(t *testing.T) {
		metadata := buildMetadata[testUserWithPointers]()
		s, err := New(metadata)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if len(s.byColumn) != 4 {
			t.Errorf("expected 4 columns, got %d", len(s.byColumn))
		}
		// Check nullable fields
		if plan := s.byColumn["nickname"]; plan != nil && !plan.nullable {
			t.Error("nickname should be nullable")
		}
		if plan := s.byColumn["age"]; plan != nil && !plan.nullable {
			t.Error("age pointer should be nullable")
		}
	})
}

func TestScan(t *testing.T) {
	t.Run("scans single row", func(t *testing.T) {
		metadata := buildMetadata[testUser]()
		s, err := New(metadata)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}

		mock := &mockColScanner{
			columns: []string{"id", "name", "email", "age"},
			rows: [][]any{
				{int64(1), "Alice", "alice@example.com", int64(30)},
			},
		}

		result, err := s.Scan(mock)
		if err != nil {
			t.Fatalf("Scan() error = %v", err)
		}

		if result.Ints["ID"] != 1 {
			t.Errorf("ID = %d, want 1", result.Ints["ID"])
		}
		if result.Strings["Name"] != "Alice" {
			t.Errorf("Name = %q, want %q", result.Strings["Name"], "Alice")
		}
		if result.Strings["Email"] != "alice@example.com" {
			t.Errorf("Email = %q, want %q", result.Strings["Email"], "alice@example.com")
		}
		if result.Ints["Age"] != 30 {
			t.Errorf("Age = %d, want 30", result.Ints["Age"])
		}
	})

	t.Run("handles columns error", func(t *testing.T) {
		metadata := buildMetadata[testUser]()
		s, err := New(metadata)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}

		mock := &mockColScanner{
			colErr: errors.New("columns error"),
		}

		_, err = s.Scan(mock)
		if err == nil {
			t.Fatal("expected error from Scan()")
		}
	})

	t.Run("handles scan error", func(t *testing.T) {
		metadata := buildMetadata[testUser]()
		s, err := New(metadata)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}

		mock := &mockColScanner{
			columns: []string{"id", "name"},
			scanErr: errors.New("scan error"),
		}

		_, err = s.Scan(mock)
		if err == nil {
			t.Fatal("expected error from Scan()")
		}
	})

	t.Run("ignores unknown columns", func(t *testing.T) {
		metadata := buildMetadata[testUser]()
		s, err := New(metadata)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}

		mock := &mockColScanner{
			columns: []string{"id", "unknown_column", "name"},
			rows: [][]any{
				{int64(1), "ignored", "Alice"},
			},
		}

		result, err := s.Scan(mock)
		if err != nil {
			t.Fatalf("Scan() error = %v", err)
		}

		if result.Ints["ID"] != 1 {
			t.Errorf("ID = %d, want 1", result.Ints["ID"])
		}
		if result.Strings["Name"] != "Alice" {
			t.Errorf("Name = %q, want %q", result.Strings["Name"], "Alice")
		}
	})

	t.Run("scans various types", func(t *testing.T) {
		metadata := buildMetadata[testUserWithTypes]()
		s, err := New(metadata)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}

		now := time.Now().Truncate(time.Second)
		mock := &mockColScanner{
			columns: []string{"id", "name", "score", "active", "created_at", "data"},
			rows: [][]any{
				{int64(1), "Alice", float64(95.5), true, now, []byte("hello")},
			},
		}

		result, err := s.Scan(mock)
		if err != nil {
			t.Fatalf("Scan() error = %v", err)
		}

		if result.Ints["ID"] != 1 {
			t.Errorf("ID = %d, want 1", result.Ints["ID"])
		}
		if result.Strings["Name"] != "Alice" {
			t.Errorf("Name = %q, want %q", result.Strings["Name"], "Alice")
		}
		if result.Floats["Score"] != 95.5 {
			t.Errorf("Score = %f, want 95.5", result.Floats["Score"])
		}
		if result.Bools["Active"] != true {
			t.Errorf("Active = %v, want true", result.Bools["Active"])
		}
		if !result.Times["CreatedAt"].Equal(now) {
			t.Errorf("CreatedAt = %v, want %v", result.Times["CreatedAt"], now)
		}
		if string(result.Bytes["Data"]) != "hello" {
			t.Errorf("Data = %q, want %q", result.Bytes["Data"], "hello")
		}
	})
}

func TestScanAll(t *testing.T) {
	t.Run("scans multiple rows", func(t *testing.T) {
		metadata := buildMetadata[testUser]()
		s, err := New(metadata)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}

		mock := &mockColScanner{
			columns: []string{"id", "name", "email", "age"},
			rows: [][]any{
				{int64(1), "Alice", "alice@example.com", int64(30)},
				{int64(2), "Bob", "bob@example.com", int64(25)},
				{int64(3), "Charlie", "charlie@example.com", int64(35)},
			},
		}

		rowIdx := 0
		next := func() bool {
			if rowIdx >= len(mock.rows) {
				return false
			}
			rowIdx++
			return true
		}

		// Reset for ScanAll which calls Scan internally
		mock.rowIdx = 0
		results, err := s.ScanAll(mock, next)
		if err != nil {
			t.Fatalf("ScanAll() error = %v", err)
		}

		if len(results) != 3 {
			t.Fatalf("expected 3 results, got %d", len(results))
		}

		// Check first row
		if results[0].Ints["ID"] != 1 {
			t.Errorf("results[0].ID = %d, want 1", results[0].Ints["ID"])
		}
		if results[0].Strings["Name"] != "Alice" {
			t.Errorf("results[0].Name = %q, want %q", results[0].Strings["Name"], "Alice")
		}

		// Check second row
		if results[1].Ints["ID"] != 2 {
			t.Errorf("results[1].ID = %d, want 2", results[1].Ints["ID"])
		}
		if results[1].Strings["Name"] != "Bob" {
			t.Errorf("results[1].Name = %q, want %q", results[1].Strings["Name"], "Bob")
		}

		// Check third row
		if results[2].Ints["ID"] != 3 {
			t.Errorf("results[2].ID = %d, want 3", results[2].Ints["ID"])
		}
		if results[2].Strings["Name"] != "Charlie" {
			t.Errorf("results[2].Name = %q, want %q", results[2].Strings["Name"], "Charlie")
		}
	})

	t.Run("returns empty slice for no rows", func(t *testing.T) {
		metadata := buildMetadata[testUser]()
		s, err := New(metadata)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}

		mock := &mockColScanner{
			columns: []string{"id", "name", "email", "age"},
			rows:    [][]any{},
		}

		next := func() bool { return false }

		results, err := s.ScanAll(mock, next)
		if err != nil {
			t.Fatalf("ScanAll() error = %v", err)
		}

		if len(results) != 0 {
			t.Errorf("expected 0 results, got %d", len(results))
		}
	})

	t.Run("handles iteration error", func(t *testing.T) {
		metadata := buildMetadata[testUser]()
		s, err := New(metadata)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}

		mock := &mockColScanner{
			columns: []string{"id", "name"},
			rows:    [][]any{},
			iterErr: errors.New("iteration error"),
		}

		next := func() bool { return false }

		_, err = s.ScanAll(mock, next)
		if err == nil {
			t.Fatal("expected error from ScanAll()")
		}
	})
}

func TestFieldToTable(t *testing.T) {
	tests := []struct {
		name     string
		typ      reflect.Type
		wantTbl  atom.Table
		wantNull bool
	}{
		{"string", reflect.TypeOf(""), atom.TableStrings, false},
		{"int64", reflect.TypeOf(int64(0)), atom.TableInts, false},
		{"int", reflect.TypeOf(0), atom.TableInts, false},
		{"uint64", reflect.TypeOf(uint64(0)), atom.TableUints, false},
		{"float64", reflect.TypeOf(float64(0)), atom.TableFloats, false},
		{"bool", reflect.TypeOf(false), atom.TableBools, false},
		{"time.Time", reflect.TypeOf(time.Time{}), atom.TableTimes, false},
		{"[]byte", reflect.TypeOf([]byte{}), atom.TableBytes, false},
		{"*string", reflect.TypeOf((*string)(nil)), atom.TableStringPtrs, true},
		{"*int64", reflect.TypeOf((*int64)(nil)), atom.TableIntPtrs, true},
		{"*float64", reflect.TypeOf((*float64)(nil)), atom.TableFloatPtrs, true},
		{"*bool", reflect.TypeOf((*bool)(nil)), atom.TableBoolPtrs, true},
		{"*time.Time", reflect.TypeOf((*time.Time)(nil)), atom.TableTimePtrs, true},
		{"*[]byte", reflect.TypeOf((*[]byte)(nil)), atom.TableBytePtrs, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tbl, nullable := fieldToTable(tt.typ)
			if tbl != tt.wantTbl {
				t.Errorf("fieldToTable(%s) table = %v, want %v", tt.name, tbl, tt.wantTbl)
			}
			if nullable != tt.wantNull {
				t.Errorf("fieldToTable(%s) nullable = %v, want %v", tt.name, nullable, tt.wantNull)
			}
		})
	}
}

func TestAllocateAtom(t *testing.T) {
	tableSet := map[atom.Table]int{
		atom.TableStrings: 2,
		atom.TableInts:    1,
		atom.TableBools:   1,
	}

	a := allocateAtom(tableSet)

	if a.Strings == nil {
		t.Error("Strings map not allocated")
	}
	if a.Ints == nil {
		t.Error("Ints map not allocated")
	}
	if a.Bools == nil {
		t.Error("Bools map not allocated")
	}
	// Should not allocate maps not in tableSet
	if a.Floats != nil {
		t.Error("Floats map should not be allocated")
	}
	if a.Times != nil {
		t.Error("Times map should not be allocated")
	}
}

func TestFieldPath(t *testing.T) {
	tests := []struct {
		name      string
		path      []string
		fieldName string
		want      string
	}{
		{"empty path", nil, "Field", "Field"},
		{"empty path explicit", []string{}, "Name", "Name"},
		{"single level path", []string{"Parent"}, "Child", "Parent.Child"},
		{"multi level path", []string{"A", "B", "C"}, "D", "A.B.C.D"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fieldPath(tt.path, tt.fieldName)
			if got != tt.want {
				t.Errorf("fieldPath(%v, %q) = %q, want %q", tt.path, tt.fieldName, got, tt.want)
			}
		})
	}
}

func TestScanAll_PointerAliasing(t *testing.T) {
	// Test that pointer fields don't alias across rows
	// This was a bug where all atoms would point to the same memory
	metadata := buildMetadata[testUserWithPointers]()
	s, err := New(metadata)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Create mock with nullable values
	age1, age2, age3 := int64(25), int64(30), int64(35)
	nick1, nick2 := "alice_nick", "bob_nick"

	mock := &mockColScanner{
		columns: []string{"id", "name", "nickname", "age"},
		rows: [][]any{
			{int64(1), "Alice", &nick1, &age1},
			{int64(2), "Bob", &nick2, &age2},
			{int64(3), "Charlie", (*string)(nil), &age3},
		},
	}

	rowIdx := 0
	next := func() bool {
		if rowIdx >= len(mock.rows) {
			return false
		}
		rowIdx++
		return true
	}

	results, err := s.ScanAll(mock, next)
	if err != nil {
		t.Fatalf("ScanAll() error = %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Verify each atom has distinct pointer values
	if results[0].IntPtrs["Age"] == nil {
		t.Error("results[0].Age should not be nil")
	} else if *results[0].IntPtrs["Age"] != 25 {
		t.Errorf("results[0].Age = %d, want 25", *results[0].IntPtrs["Age"])
	}

	if results[1].IntPtrs["Age"] == nil {
		t.Error("results[1].Age should not be nil")
	} else if *results[1].IntPtrs["Age"] != 30 {
		t.Errorf("results[1].Age = %d, want 30", *results[1].IntPtrs["Age"])
	}

	if results[2].IntPtrs["Age"] == nil {
		t.Error("results[2].Age should not be nil")
	} else if *results[2].IntPtrs["Age"] != 35 {
		t.Errorf("results[2].Age = %d, want 35", *results[2].IntPtrs["Age"])
	}

	// Verify pointer addresses are different (not aliased)
	if results[0].IntPtrs["Age"] == results[1].IntPtrs["Age"] {
		t.Error("results[0].Age and results[1].Age should not point to same memory")
	}
	if results[1].IntPtrs["Age"] == results[2].IntPtrs["Age"] {
		t.Error("results[1].Age and results[2].Age should not point to same memory")
	}

	// Verify string pointers
	if results[0].StringPtrs["Nickname"] == nil {
		t.Error("results[0].Nickname should not be nil")
	} else if *results[0].StringPtrs["Nickname"] != "alice_nick" {
		t.Errorf("results[0].Nickname = %q, want %q", *results[0].StringPtrs["Nickname"], "alice_nick")
	}

	if results[2].StringPtrs["Nickname"] != nil {
		t.Error("results[2].Nickname should be nil")
	}
}
