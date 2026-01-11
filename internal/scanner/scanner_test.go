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
		} else if v, ok := val.(*uint64); ok {
			// Handle uint64 pointers scanned into NullInt64
			if v != nil {
				d.Int64 = int64(*v)
				d.Valid = true
			} else {
				d.Valid = false
			}
		} else if v, ok := val.(int64); ok {
			d.Int64 = v
			d.Valid = true
		} else if v, ok := val.(uint64); ok {
			d.Int64 = int64(v)
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

// Additional test types for comprehensive coverage.
type testAllTypes struct {
	ID        int64     `db:"id"`
	Name      string    `db:"name"`
	Age       int64     `db:"age"`
	Count     uint64    `db:"count"`
	Score     float64   `db:"score"`
	Active    bool      `db:"active"`
	CreatedAt time.Time `db:"created_at"`
	Data      []byte    `db:"data"`
}

type testAllPointerTypes struct {
	ID        int64      `db:"id"`
	Name      *string    `db:"name"`
	Age       *int64     `db:"age"`
	Count     *uint64    `db:"count"`
	Score     *float64   `db:"score"`
	Active    *bool      `db:"active"`
	CreatedAt *time.Time `db:"created_at"`
	Data      *[]byte    `db:"data"`
}

func TestMakeScanDest(t *testing.T) {
	tests := []struct {
		name     string
		table    atom.Table
		nullable bool
		wantType string
	}{
		{"string", atom.TableStrings, false, "*string"},
		{"int", atom.TableInts, false, "*int64"},
		{"uint", atom.TableUints, false, "*uint64"},
		{"float", atom.TableFloats, false, "*float64"},
		{"bool", atom.TableBools, false, "*bool"},
		{"time", atom.TableTimes, false, "*time.Time"},
		{"bytes", atom.TableBytes, false, "*[]uint8"},
		{"nullable string", atom.TableStringPtrs, true, "*sql.NullString"},
		{"nullable int", atom.TableIntPtrs, true, "*sql.NullInt64"},
		{"nullable uint", atom.TableUintPtrs, true, "*sql.NullInt64"},
		{"nullable float", atom.TableFloatPtrs, true, "*sql.NullFloat64"},
		{"nullable bool", atom.TableBoolPtrs, true, "*sql.NullBool"},
		{"nullable time", atom.TableTimePtrs, true, "*sql.NullTime"},
		{"nullable bytes", atom.TableBytePtrs, true, "*[]uint8"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dest := makeScanDest(tt.table, tt.nullable)
			gotType := reflect.TypeOf(dest).String()
			if gotType != tt.wantType {
				t.Errorf("makeScanDest(%v, %v) type = %s, want %s", tt.table, tt.nullable, gotType, tt.wantType)
			}
		})
	}

	t.Run("unknown table returns *any", func(t *testing.T) {
		dest := makeScanDest(atom.Table("unknown"), false)
		gotType := reflect.TypeOf(dest).String()
		if gotType != "*interface {}" {
			t.Errorf("makeScanDest(unknown, false) type = %s, want *interface {}", gotType)
		}
	})
}

func TestResetDests(t *testing.T) {
	t.Run("resets all dest types", func(t *testing.T) {
		str := "hello"
		i64 := int64(42)
		u64 := uint64(100)
		f64 := float64(3.14)
		b := true
		tm := time.Now()
		by := []byte("data")

		dests := []any{
			&str,
			&i64,
			&u64,
			&f64,
			&b,
			&tm,
			&by,
			&sql.NullString{String: "test", Valid: true},
			&sql.NullInt64{Int64: 123, Valid: true},
			&sql.NullFloat64{Float64: 1.5, Valid: true},
			&sql.NullBool{Bool: true, Valid: true},
			&sql.NullTime{Time: time.Now(), Valid: true},
			new(any),
		}

		resetDests(dests)

		if str != "" {
			t.Errorf("string not reset, got %q", str)
		}
		if i64 != 0 {
			t.Errorf("int64 not reset, got %d", i64)
		}
		if u64 != 0 {
			t.Errorf("uint64 not reset, got %d", u64)
		}
		if f64 != 0 {
			t.Errorf("float64 not reset, got %f", f64)
		}
		if b != false {
			t.Errorf("bool not reset, got %v", b)
		}
		if !tm.IsZero() {
			t.Errorf("time not reset, got %v", tm)
		}
		if by != nil {
			t.Errorf("bytes not reset, got %v", by)
		}

		ns := dests[7].(*sql.NullString)
		if ns.Valid || ns.String != "" {
			t.Errorf("NullString not reset, got %+v", ns)
		}
		ni := dests[8].(*sql.NullInt64)
		if ni.Valid || ni.Int64 != 0 {
			t.Errorf("NullInt64 not reset, got %+v", ni)
		}
		nf := dests[9].(*sql.NullFloat64)
		if nf.Valid || nf.Float64 != 0 {
			t.Errorf("NullFloat64 not reset, got %+v", nf)
		}
		nb := dests[10].(*sql.NullBool)
		if nb.Valid || nb.Bool != false {
			t.Errorf("NullBool not reset, got %+v", nb)
		}
		nt := dests[11].(*sql.NullTime)
		if nt.Valid || !nt.Time.IsZero() {
			t.Errorf("NullTime not reset, got %+v", nt)
		}
	})
}

func TestAllocateAtom_AllTypes(t *testing.T) {
	tableSet := map[atom.Table]int{
		atom.TableStrings:    1,
		atom.TableInts:       1,
		atom.TableUints:      1,
		atom.TableFloats:     1,
		atom.TableBools:      1,
		atom.TableTimes:      1,
		atom.TableBytes:      1,
		atom.TableStringPtrs: 1,
		atom.TableIntPtrs:    1,
		atom.TableUintPtrs:   1,
		atom.TableFloatPtrs:  1,
		atom.TableBoolPtrs:   1,
		atom.TableTimePtrs:   1,
		atom.TableBytePtrs:   1,
	}

	a := allocateAtom(tableSet)

	if a.Strings == nil {
		t.Error("Strings map not allocated")
	}
	if a.Ints == nil {
		t.Error("Ints map not allocated")
	}
	if a.Uints == nil {
		t.Error("Uints map not allocated")
	}
	if a.Floats == nil {
		t.Error("Floats map not allocated")
	}
	if a.Bools == nil {
		t.Error("Bools map not allocated")
	}
	if a.Times == nil {
		t.Error("Times map not allocated")
	}
	if a.Bytes == nil {
		t.Error("Bytes map not allocated")
	}
	if a.StringPtrs == nil {
		t.Error("StringPtrs map not allocated")
	}
	if a.IntPtrs == nil {
		t.Error("IntPtrs map not allocated")
	}
	if a.UintPtrs == nil {
		t.Error("UintPtrs map not allocated")
	}
	if a.FloatPtrs == nil {
		t.Error("FloatPtrs map not allocated")
	}
	if a.BoolPtrs == nil {
		t.Error("BoolPtrs map not allocated")
	}
	if a.TimePtrs == nil {
		t.Error("TimePtrs map not allocated")
	}
	if a.BytePtrs == nil {
		t.Error("BytePtrs map not allocated")
	}
}

func TestPointerTable(t *testing.T) {
	tests := []struct {
		base atom.Table
		want atom.Table
	}{
		{atom.TableStrings, atom.TableStringPtrs},
		{atom.TableInts, atom.TableIntPtrs},
		{atom.TableUints, atom.TableUintPtrs},
		{atom.TableFloats, atom.TableFloatPtrs},
		{atom.TableBools, atom.TableBoolPtrs},
		{atom.TableTimes, atom.TableTimePtrs},
		{atom.TableBytes, atom.TableBytePtrs},
		{atom.Table("unknown"), atom.Table("")},
	}

	for _, tt := range tests {
		t.Run(string(tt.base), func(t *testing.T) {
			got := pointerTable(tt.base)
			if got != tt.want {
				t.Errorf("pointerTable(%s) = %s, want %s", tt.base, got, tt.want)
			}
		})
	}
}

func TestScan_AllTypes(t *testing.T) {
	metadata := buildMetadata[testAllTypes]()
	s, err := New(metadata)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	now := time.Now().Truncate(time.Second)
	mock := &mockColScanner{
		columns: []string{"id", "name", "age", "count", "score", "active", "created_at", "data"},
		rows: [][]any{
			{int64(1), "Alice", int64(30), uint64(100), float64(95.5), true, now, []byte("hello")},
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
	if result.Ints["Age"] != 30 {
		t.Errorf("Age = %d, want 30", result.Ints["Age"])
	}
	if result.Uints["Count"] != 100 {
		t.Errorf("Count = %d, want 100", result.Uints["Count"])
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
}

func TestScan_AllPointerTypes(t *testing.T) {
	metadata := buildMetadata[testAllPointerTypes]()
	s, err := New(metadata)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	now := time.Now().Truncate(time.Second)
	name := "Alice"
	age := int64(30)
	count := uint64(100)
	score := float64(95.5)
	active := true
	data := []byte("hello")

	mock := &mockColScanner{
		columns: []string{"id", "name", "age", "count", "score", "active", "created_at", "data"},
		rows: [][]any{
			{int64(1), &name, &age, &count, &score, &active, &now, data},
		},
	}

	result, err := s.Scan(mock)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	if result.Ints["ID"] != 1 {
		t.Errorf("ID = %d, want 1", result.Ints["ID"])
	}
	if result.StringPtrs["Name"] == nil || *result.StringPtrs["Name"] != "Alice" {
		t.Errorf("Name = %v, want Alice", result.StringPtrs["Name"])
	}
	if result.IntPtrs["Age"] == nil || *result.IntPtrs["Age"] != 30 {
		t.Errorf("Age = %v, want 30", result.IntPtrs["Age"])
	}
	if result.UintPtrs["Count"] == nil || *result.UintPtrs["Count"] != 100 {
		t.Errorf("Count = %v, want 100", result.UintPtrs["Count"])
	}
	if result.FloatPtrs["Score"] == nil || *result.FloatPtrs["Score"] != 95.5 {
		t.Errorf("Score = %v, want 95.5", result.FloatPtrs["Score"])
	}
	if result.BoolPtrs["Active"] == nil || *result.BoolPtrs["Active"] != true {
		t.Errorf("Active = %v, want true", result.BoolPtrs["Active"])
	}
	if result.TimePtrs["CreatedAt"] == nil || !result.TimePtrs["CreatedAt"].Equal(now) {
		t.Errorf("CreatedAt = %v, want %v", result.TimePtrs["CreatedAt"], now)
	}
	if result.BytePtrs["Data"] == nil || string(*result.BytePtrs["Data"]) != "hello" {
		t.Errorf("Data = %v, want hello", result.BytePtrs["Data"])
	}
}

func TestScan_NullPointerTypes(t *testing.T) {
	metadata := buildMetadata[testAllPointerTypes]()
	s, err := New(metadata)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	mock := &mockColScanner{
		columns: []string{"id", "name", "age", "count", "score", "active", "created_at", "data"},
		rows: [][]any{
			{int64(1), (*string)(nil), (*int64)(nil), (*uint64)(nil), (*float64)(nil), (*bool)(nil), (*time.Time)(nil), ([]byte)(nil)},
		},
	}

	result, err := s.Scan(mock)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	if result.Ints["ID"] != 1 {
		t.Errorf("ID = %d, want 1", result.Ints["ID"])
	}
	if result.StringPtrs["Name"] != nil {
		t.Errorf("Name should be nil, got %v", result.StringPtrs["Name"])
	}
	if result.IntPtrs["Age"] != nil {
		t.Errorf("Age should be nil, got %v", result.IntPtrs["Age"])
	}
	if result.UintPtrs["Count"] != nil {
		t.Errorf("Count should be nil, got %v", result.UintPtrs["Count"])
	}
	if result.FloatPtrs["Score"] != nil {
		t.Errorf("Score should be nil, got %v", result.FloatPtrs["Score"])
	}
	if result.BoolPtrs["Active"] != nil {
		t.Errorf("Active should be nil, got %v", result.BoolPtrs["Active"])
	}
	if result.TimePtrs["CreatedAt"] != nil {
		t.Errorf("CreatedAt should be nil, got %v", result.TimePtrs["CreatedAt"])
	}
	if result.BytePtrs["Data"] != nil {
		t.Errorf("Data should be nil, got %v", result.BytePtrs["Data"])
	}
}

func TestFieldToTable_EdgeCases(t *testing.T) {
	t.Run("byte array", func(t *testing.T) {
		typ := reflect.TypeOf([32]byte{})
		tbl, nullable := fieldToTable(typ)
		if tbl != atom.TableBytes {
			t.Errorf("fieldToTable([32]byte) = %v, want %v", tbl, atom.TableBytes)
		}
		if nullable {
			t.Error("byte array should not be nullable")
		}
	})

	t.Run("unsupported slice type", func(t *testing.T) {
		typ := reflect.TypeOf([]int{})
		tbl, _ := fieldToTable(typ)
		if tbl != "" {
			t.Errorf("fieldToTable([]int) = %v, want empty", tbl)
		}
	})

	t.Run("unsupported pointer type", func(t *testing.T) {
		type nested struct{}
		typ := reflect.TypeOf((*nested)(nil))
		tbl, _ := fieldToTable(typ)
		if tbl != "" {
			t.Errorf("fieldToTable(*nested) = %v, want empty", tbl)
		}
	})

	t.Run("pointer to byte slice", func(t *testing.T) {
		typ := reflect.TypeOf((*[]byte)(nil))
		tbl, nullable := fieldToTable(typ)
		if tbl != atom.TableBytePtrs {
			t.Errorf("fieldToTable(*[]byte) = %v, want %v", tbl, atom.TableBytePtrs)
		}
		if !nullable {
			t.Error("*[]byte should be nullable")
		}
	})

	t.Run("int variants", func(t *testing.T) {
		intTypes := []reflect.Type{
			reflect.TypeOf(int8(0)),
			reflect.TypeOf(int16(0)),
			reflect.TypeOf(int32(0)),
		}
		for _, typ := range intTypes {
			tbl, _ := fieldToTable(typ)
			if tbl != atom.TableInts {
				t.Errorf("fieldToTable(%s) = %v, want %v", typ, tbl, atom.TableInts)
			}
		}
	})

	t.Run("uint variants", func(t *testing.T) {
		uintTypes := []reflect.Type{
			reflect.TypeOf(uint(0)),
			reflect.TypeOf(uint8(0)),
			reflect.TypeOf(uint16(0)),
			reflect.TypeOf(uint32(0)),
		}
		for _, typ := range uintTypes {
			tbl, _ := fieldToTable(typ)
			if tbl != atom.TableUints {
				t.Errorf("fieldToTable(%s) = %v, want %v", typ, tbl, atom.TableUints)
			}
		}
	})

	t.Run("float32", func(t *testing.T) {
		typ := reflect.TypeOf(float32(0))
		tbl, _ := fieldToTable(typ)
		if tbl != atom.TableFloats {
			t.Errorf("fieldToTable(float32) = %v, want %v", tbl, atom.TableFloats)
		}
	})

	t.Run("pointer to uint", func(t *testing.T) {
		typ := reflect.TypeOf((*uint64)(nil))
		tbl, nullable := fieldToTable(typ)
		if tbl != atom.TableUintPtrs {
			t.Errorf("fieldToTable(*uint64) = %v, want %v", tbl, atom.TableUintPtrs)
		}
		if !nullable {
			t.Error("*uint64 should be nullable")
		}
	})
}

func TestNew_DuplicateColumn(t *testing.T) {
	type duplicateColumns struct {
		ID    int64  `db:"id"`
		Name  string `db:"name"`
		Alias string `db:"name"` // duplicate column
	}

	metadata := buildMetadata[duplicateColumns]()
	_, err := New(metadata)
	if err == nil {
		t.Error("expected error for duplicate column")
	}
}

func TestNew_SkipsEmptyDBTag(t *testing.T) {
	type emptyTag struct {
		ID     int64  `db:"id"`
		Unused string `db:""`
		Skip   string `db:"-"`
	}

	metadata := buildMetadata[emptyTag]()
	s, err := New(metadata)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Should only have plan for "id"
	if len(s.byColumn) != 1 {
		t.Errorf("expected 1 column plan, got %d", len(s.byColumn))
	}
	if _, ok := s.byColumn["id"]; !ok {
		t.Error("expected plan for 'id' column")
	}
}
