// Package scanner provides database row scanning into atom.Atom values.
// This is extracted from atom's pre-v1.0.0 Scanner implementation.
package scanner

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/zoobzio/atom"
	"github.com/zoobzio/sentinel"
)

// ColScanner is the interface for database row scanning.
// Satisfied by sqlx.Rows and other database libraries.
type ColScanner interface {
	Columns() ([]string, error)
	Scan(dest ...any) error
	Err() error
}

// Scanner efficiently scans database rows directly into Atoms.
type Scanner struct {
	byColumn map[string]*scanFieldPlan
	tableSet map[atom.Table]int
	spec     sentinel.Metadata
}

// scanFieldPlan describes how to scan a single column into an Atom.
type scanFieldPlan struct {
	fieldName string
	column    string
	table     atom.Table
	path      []string
	nullable  bool
}

var timeType = reflect.TypeFor[time.Time]()

// New creates a Scanner from sentinel metadata.
// The metadata provides struct field information including db tags for column mapping.
func New(metadata sentinel.Metadata) (*Scanner, error) {
	s := &Scanner{
		spec:     metadata,
		byColumn: make(map[string]*scanFieldPlan),
		tableSet: make(map[atom.Table]int),
	}

	if err := s.buildPlans(metadata.Fields, nil); err != nil {
		return nil, err
	}

	return s, nil
}

// buildPlans recursively builds scan plans from metadata fields.
func (s *Scanner) buildPlans(fields []sentinel.FieldMetadata, path []string) error {
	for _, field := range fields {
		dbTag, hasDB := field.Tags["db"]
		if !hasDB || dbTag == "" || dbTag == "-" {
			continue
		}

		table, nullable := fieldToTable(field.ReflectType)
		if table == "" {
			// Skip unsupported types (slices, maps, nested structs without db tags)
			continue
		}

		// Check for column name collision
		if existing, ok := s.byColumn[dbTag]; ok {
			return fmt.Errorf("column %q maps to multiple fields: %s and %s",
				dbTag, fieldPath(existing.path, existing.fieldName), fieldPath(path, field.Name))
		}

		plan := &scanFieldPlan{
			fieldName: field.Name,
			column:    dbTag,
			table:     table,
			nullable:  nullable,
			path:      path,
		}
		s.byColumn[dbTag] = plan
		s.tableSet[table]++
	}

	return nil
}

// fieldPath formats a field path for error messages.
func fieldPath(path []string, fieldName string) string {
	if len(path) == 0 {
		return fieldName
	}
	return strings.Join(path, ".") + "." + fieldName
}

// fieldToTable maps a reflect.Type to its atom.Table.
// Returns the table and whether the type is nullable (pointer).
func fieldToTable(t reflect.Type) (atom.Table, bool) {
	// Handle pointer types
	if t.Kind() == reflect.Ptr {
		elemType := t.Elem()

		// Pointer to []byte
		if elemType.Kind() == reflect.Slice && elemType.Elem().Kind() == reflect.Uint8 {
			return atom.TableBytePtrs, true
		}

		// Pointer to scalar
		if table, ok := scalarToTable(elemType); ok {
			return pointerTable(table), true
		}

		return "", false
	}

	// Handle []byte as scalar
	if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
		return atom.TableBytes, false
	}

	// Handle fixed-size byte arrays
	if t.Kind() == reflect.Array && t.Elem().Kind() == reflect.Uint8 {
		return atom.TableBytes, false
	}

	// Handle scalar types
	if table, ok := scalarToTable(t); ok {
		return table, false
	}

	return "", false
}

// scalarToTable maps a reflect.Type to its base atom.Table.
func scalarToTable(t reflect.Type) (atom.Table, bool) {
	switch t.Kind() {
	case reflect.String:
		return atom.TableStrings, true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return atom.TableInts, true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return atom.TableUints, true
	case reflect.Float32, reflect.Float64:
		return atom.TableFloats, true
	case reflect.Bool:
		return atom.TableBools, true
	}

	if t == timeType {
		return atom.TableTimes, true
	}

	return "", false
}

// pointerTable returns the pointer variant of a base table.
func pointerTable(base atom.Table) atom.Table {
	switch base {
	case atom.TableStrings:
		return atom.TableStringPtrs
	case atom.TableInts:
		return atom.TableIntPtrs
	case atom.TableUints:
		return atom.TableUintPtrs
	case atom.TableFloats:
		return atom.TableFloatPtrs
	case atom.TableBools:
		return atom.TableBoolPtrs
	case atom.TableTimes:
		return atom.TableTimePtrs
	case atom.TableBytes:
		return atom.TableBytePtrs
	default:
		return ""
	}
}

// Scan reads a single row into an Atom.
func (s *Scanner) Scan(cs ColScanner) (*atom.Atom, error) {
	cols, err := cs.Columns()
	if err != nil {
		return nil, fmt.Errorf("getting columns: %w", err)
	}

	plans, dests := s.prepareScan(cols)

	if err := cs.Scan(dests...); err != nil {
		return nil, fmt.Errorf("scanning row: %w", err)
	}

	return s.buildAtom(plans, dests), nil
}

// ScanAll reads all rows into Atoms.
// The next function should return true while there are more rows (typically rows.Next).
func (s *Scanner) ScanAll(cs ColScanner, next func() bool) ([]*atom.Atom, error) {
	cols, err := cs.Columns()
	if err != nil {
		return nil, fmt.Errorf("getting columns: %w", err)
	}

	plans, dests := s.prepareScan(cols)

	var atoms []*atom.Atom
	for next() {
		if err := cs.Scan(dests...); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		atoms = append(atoms, s.buildAtom(plans, dests))
		// Reset destinations for next row
		resetDests(dests)
	}

	if err := cs.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	return atoms, nil
}

// prepareScan creates the scan plan and destinations for a column set.
func (s *Scanner) prepareScan(cols []string) (plans []*scanFieldPlan, dests []any) {
	plans = make([]*scanFieldPlan, len(cols))
	dests = make([]any, len(cols))

	for i, col := range cols {
		plan := s.byColumn[col]
		plans[i] = plan

		if plan == nil {
			// Unknown column, use a discard destination
			dests[i] = new(any)
			continue
		}

		dests[i] = makeScanDest(plan.table, plan.nullable)
	}

	return plans, dests
}

// makeScanDest creates a typed scan destination for a table.
func makeScanDest(table atom.Table, nullable bool) any {
	if nullable {
		switch table {
		case atom.TableStringPtrs:
			return new(sql.NullString)
		case atom.TableIntPtrs:
			return new(sql.NullInt64)
		case atom.TableUintPtrs:
			// sql package lacks NullUint64. Values > MaxInt64 will overflow.
			return new(sql.NullInt64)
		case atom.TableFloatPtrs:
			return new(sql.NullFloat64)
		case atom.TableBoolPtrs:
			return new(sql.NullBool)
		case atom.TableTimePtrs:
			return new(sql.NullTime)
		case atom.TableBytePtrs:
			return new([]byte)
		}
	}

	switch table {
	case atom.TableStrings:
		return new(string)
	case atom.TableInts:
		return new(int64)
	case atom.TableUints:
		return new(uint64)
	case atom.TableFloats:
		return new(float64)
	case atom.TableBools:
		return new(bool)
	case atom.TableTimes:
		return new(time.Time)
	case atom.TableBytes:
		return new([]byte)
	}

	return new(any)
}

// resetDests resets scan destinations for reuse.
func resetDests(dests []any) {
	for i, d := range dests {
		switch v := d.(type) {
		case *string:
			*v = ""
		case *int64:
			*v = 0
		case *uint64:
			*v = 0
		case *float64:
			*v = 0
		case *bool:
			*v = false
		case *time.Time:
			*v = time.Time{}
		case *[]byte:
			*v = nil
		case *sql.NullString:
			*v = sql.NullString{}
		case *sql.NullInt64:
			*v = sql.NullInt64{}
		case *sql.NullFloat64:
			*v = sql.NullFloat64{}
		case *sql.NullBool:
			*v = sql.NullBool{}
		case *sql.NullTime:
			*v = sql.NullTime{}
		case *any:
			dests[i] = new(any)
		}
	}
}

// buildAtom constructs an Atom from scanned destinations.
func (s *Scanner) buildAtom(plans []*scanFieldPlan, dests []any) *atom.Atom {
	result := allocateAtom(s.tableSet)
	result.Spec = s.spec

	for i, plan := range plans {
		if plan == nil {
			continue
		}

		dest := dests[i]
		assignValue(result, plan, dest)
	}

	return result
}

// allocateAtom pre-allocates maps for an atom based on expected table usage.
func allocateAtom(tableSet map[atom.Table]int) *atom.Atom {
	a := &atom.Atom{}

	for table, count := range tableSet {
		switch table {
		case atom.TableStrings:
			a.Strings = make(map[string]string, count)
		case atom.TableInts:
			a.Ints = make(map[string]int64, count)
		case atom.TableUints:
			a.Uints = make(map[string]uint64, count)
		case atom.TableFloats:
			a.Floats = make(map[string]float64, count)
		case atom.TableBools:
			a.Bools = make(map[string]bool, count)
		case atom.TableTimes:
			a.Times = make(map[string]time.Time, count)
		case atom.TableBytes:
			a.Bytes = make(map[string][]byte, count)
		case atom.TableStringPtrs:
			a.StringPtrs = make(map[string]*string, count)
		case atom.TableIntPtrs:
			a.IntPtrs = make(map[string]*int64, count)
		case atom.TableUintPtrs:
			a.UintPtrs = make(map[string]*uint64, count)
		case atom.TableFloatPtrs:
			a.FloatPtrs = make(map[string]*float64, count)
		case atom.TableBoolPtrs:
			a.BoolPtrs = make(map[string]*bool, count)
		case atom.TableTimePtrs:
			a.TimePtrs = make(map[string]*time.Time, count)
		case atom.TableBytePtrs:
			a.BytePtrs = make(map[string]*[]byte, count)
		}
	}

	return a
}

// assignValue assigns a scanned value to the appropriate Atom table.
func assignValue(a *atom.Atom, plan *scanFieldPlan, dest any) {
	switch plan.table {
	case atom.TableStrings:
		if v, ok := dest.(*string); ok {
			if a.Strings == nil {
				a.Strings = make(map[string]string)
			}
			a.Strings[plan.fieldName] = *v
		}
	case atom.TableInts:
		if v, ok := dest.(*int64); ok {
			if a.Ints == nil {
				a.Ints = make(map[string]int64)
			}
			a.Ints[plan.fieldName] = *v
		}
	case atom.TableUints:
		if v, ok := dest.(*uint64); ok {
			if a.Uints == nil {
				a.Uints = make(map[string]uint64)
			}
			a.Uints[plan.fieldName] = *v
		}
	case atom.TableFloats:
		if v, ok := dest.(*float64); ok {
			if a.Floats == nil {
				a.Floats = make(map[string]float64)
			}
			a.Floats[plan.fieldName] = *v
		}
	case atom.TableBools:
		if v, ok := dest.(*bool); ok {
			if a.Bools == nil {
				a.Bools = make(map[string]bool)
			}
			a.Bools[plan.fieldName] = *v
		}
	case atom.TableTimes:
		if v, ok := dest.(*time.Time); ok {
			if a.Times == nil {
				a.Times = make(map[string]time.Time)
			}
			a.Times[plan.fieldName] = *v
		}
	case atom.TableBytes:
		if v, ok := dest.(*[]byte); ok {
			if a.Bytes == nil {
				a.Bytes = make(map[string][]byte)
			}
			a.Bytes[plan.fieldName] = *v
		}

	// Nullable types
	case atom.TableStringPtrs:
		if v, ok := dest.(*sql.NullString); ok {
			if a.StringPtrs == nil {
				a.StringPtrs = make(map[string]*string)
			}
			if v.Valid {
				a.StringPtrs[plan.fieldName] = &v.String
			} else {
				a.StringPtrs[plan.fieldName] = nil
			}
		}
	case atom.TableIntPtrs:
		if v, ok := dest.(*sql.NullInt64); ok {
			if a.IntPtrs == nil {
				a.IntPtrs = make(map[string]*int64)
			}
			if v.Valid {
				a.IntPtrs[plan.fieldName] = &v.Int64
			} else {
				a.IntPtrs[plan.fieldName] = nil
			}
		}
	case atom.TableUintPtrs:
		if v, ok := dest.(*sql.NullInt64); ok {
			if a.UintPtrs == nil {
				a.UintPtrs = make(map[string]*uint64)
			}
			if v.Valid {
				u := uint64(v.Int64) //nolint:gosec // database values assumed valid
				a.UintPtrs[plan.fieldName] = &u
			} else {
				a.UintPtrs[plan.fieldName] = nil
			}
		}
	case atom.TableFloatPtrs:
		if v, ok := dest.(*sql.NullFloat64); ok {
			if a.FloatPtrs == nil {
				a.FloatPtrs = make(map[string]*float64)
			}
			if v.Valid {
				a.FloatPtrs[plan.fieldName] = &v.Float64
			} else {
				a.FloatPtrs[plan.fieldName] = nil
			}
		}
	case atom.TableBoolPtrs:
		if v, ok := dest.(*sql.NullBool); ok {
			if a.BoolPtrs == nil {
				a.BoolPtrs = make(map[string]*bool)
			}
			if v.Valid {
				a.BoolPtrs[plan.fieldName] = &v.Bool
			} else {
				a.BoolPtrs[plan.fieldName] = nil
			}
		}
	case atom.TableTimePtrs:
		if v, ok := dest.(*sql.NullTime); ok {
			if a.TimePtrs == nil {
				a.TimePtrs = make(map[string]*time.Time)
			}
			if v.Valid {
				a.TimePtrs[plan.fieldName] = &v.Time
			} else {
				a.TimePtrs[plan.fieldName] = nil
			}
		}
	case atom.TableBytePtrs:
		if v, ok := dest.(*[]byte); ok {
			if a.BytePtrs == nil {
				a.BytePtrs = make(map[string]*[]byte)
			}
			if v != nil && *v != nil {
				a.BytePtrs[plan.fieldName] = v
			} else {
				a.BytePtrs[plan.fieldName] = nil
			}
		}
	}
}
