package cereal

import (
	"fmt"
	"strings"

	"github.com/zoobzio/dbml"
	"github.com/zoobzio/sentinel"
)

// buildDBMLFromStruct creates a DBML project from a struct's Sentinel metadata.
// This converts struct tags (db, type, constraints, etc.) into a complete DBML schema.
func buildDBMLFromStruct(metadata sentinel.Metadata, tableName string) (*dbml.Project, error) {
	project := dbml.NewProject(tableName).
		WithDatabaseType("PostgreSQL")

	table := dbml.NewTable(tableName).
		WithSchema("public")

	// Track which columns are part of indexes
	indexedColumns := make(map[string]string) // column name -> index name

	// First pass: collect index information
	for _, field := range metadata.Fields {
		if indexName, hasIndex := field.Tags["index"]; hasIndex {
			if dbTag, ok := field.Tags["db"]; ok && dbTag != "" {
				indexedColumns[dbTag] = indexName
			}
		}
	}

	// Second pass: build columns
	for _, field := range metadata.Fields {
		// Skip fields without db tag
		dbTag, ok := field.Tags["db"]
		if !ok || dbTag == "" {
			continue
		}

		// Get SQL type (explicit or inferred)
		sqlType := field.Tags["type"]
		if sqlType == "" {
			sqlType = inferPostgresType(field.Type)
		}

		// Create column
		col := dbml.NewColumn(dbTag, sqlType)

		// Parse constraints tag
		if constraints, ok := field.Tags["constraints"]; ok {
			notNull, unique, primaryKey := parseConstraintsTag(constraints)

			if primaryKey {
				col.WithPrimaryKey()
			}
			if unique {
				col.WithUnique()
			}
			if !notNull {
				// DBML defaults to NOT NULL, so only set if nullable
				col.WithNull()
			}
		} else {
			// No constraints tag means nullable by default
			col.WithNull()
		}

		// Default value
		if defaultVal, ok := field.Tags["default"]; ok {
			col.WithDefault(defaultVal)
		}

		// Check constraint
		if checkExpr, ok := field.Tags["check"]; ok {
			col.WithCheck(checkExpr)
		}

		// Foreign key reference
		if references, ok := field.Tags["references"]; ok {
			refTable, refColumn, err := parseReferenceTag(references)
			if err != nil {
				return nil, fmt.Errorf("field %q: %w", field.Name, err)
			}

			// Determine relationship type (default to many-to-one)
			relType := dbml.ManyToOne

			// Create inline reference
			col.WithRef(relType, "public", refTable, refColumn)

			// Note: ON DELETE/ON UPDATE actions would need to be standalone Ref objects
			// For now, inline refs don't support these actions in our DBML
			// We could add them as standalone Ref objects if needed
		}

		table.AddColumn(col)
	}

	// Third pass: build indexes
	for columnName, indexName := range indexedColumns {
		index := dbml.NewIndex(columnName).
			WithName(indexName)
		table.AddIndex(index)
	}

	project.AddTable(table)

	// Validate the generated DBML
	if err := project.Validate(); err != nil {
		return nil, fmt.Errorf("generated DBML is invalid: %w", err)
	}

	return project, nil
}

// parseReferenceTag parses a references tag value.
// Expected format: "table(column)"
// Returns table name, column name, and error.
func parseReferenceTag(ref string) (table, column string, err error) {
	// Find opening parenthesis
	idx := strings.Index(ref, "(")
	if idx == -1 {
		return "", "", fmt.Errorf("invalid references format %q, expected 'table(column)'", ref)
	}

	// Check closing parenthesis
	if !strings.HasSuffix(ref, ")") {
		return "", "", fmt.Errorf("invalid references format %q, expected 'table(column)'", ref)
	}

	table = ref[:idx]
	column = strings.TrimSuffix(ref[idx+1:], ")")

	if table == "" {
		return "", "", fmt.Errorf("empty table name in references %q", ref)
	}
	if column == "" {
		return "", "", fmt.Errorf("empty column name in references %q", ref)
	}

	return table, column, nil
}

// parseConstraintsTag parses the constraints tag into individual flags.
// Example: "unique,not_null,primary_key" → (notNull=true, unique=true, primaryKey=true).
func parseConstraintsTag(constraintsTag string) (notNull, unique, primaryKey bool) {
	if constraintsTag == "" {
		return false, false, false
	}

	constraints := strings.Split(constraintsTag, ",")
	for _, c := range constraints {
		c = strings.TrimSpace(c)
		switch c {
		case "unique":
			unique = true
		case "not_null":
			notNull = true
		case "primary_key":
			primaryKey = true
		}
	}

	return notNull, unique, primaryKey
}

const (
	pgTypeSmallInt = "SMALLINT"
)

// inferPostgresType maps Go types to default Postgres types.
// This is the same logic from the OLD cereal code.
func inferPostgresType(goType string) string {
	// Handle pointer types
	goType = strings.TrimPrefix(goType, "*")

	// Handle array/slice types
	if strings.HasPrefix(goType, "[]") {
		elementType := strings.TrimPrefix(goType, "[]")

		// Special case: []byte is BYTEA, not an array
		if elementType == "byte" || elementType == "uint8" {
			return "BYTEA"
		}

		// Map element type and add array suffix
		baseType := inferPostgresType(elementType)
		return baseType + "[]"
	}

	switch goType {
	case "string":
		return "TEXT"
	case "int", "int32":
		return "INTEGER"
	case "int64":
		return "BIGINT"
	case "int16":
		return pgTypeSmallInt
	case "int8":
		return pgTypeSmallInt
	case "uint", "uint32":
		return "INTEGER"
	case "uint64":
		return "BIGINT"
	case "uint16":
		return pgTypeSmallInt
	case "uint8":
		return pgTypeSmallInt
	case "float32":
		return "REAL"
	case "float64":
		return "DOUBLE PRECISION"
	case "bool":
		return "BOOLEAN"
	case "time.Time":
		return "TIMESTAMPTZ"
	default:
		// For map types, use JSONB
		if strings.HasPrefix(goType, "map[") {
			return "JSONB"
		}
		// For custom types (package-qualified or not), use JSONB
		return "JSONB"
	}
}
