package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/zoobzio/astql"
	"github.com/zoobzio/astql/providers/postgres"
	"github.com/zoobzio/cereal"
	"github.com/zoobzio/sentinel"
)

// Provider implements the cereal.Provider interface for PostgreSQL.
type Provider struct {
	db      *sqlx.DB
	queries map[string]*postgres.AST // Stores PostgreSQL AST structures
	mu      sync.RWMutex
}

// New creates a new PostgreSQL provider.
func New(dsn string) (*Provider, error) {
	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	return &Provider{
		db:      db,
		queries: make(map[string]*postgres.AST),
	}, nil
}

// Get retrieves a single record from the database.
func (p *Provider) Get(ctx context.Context, table, keyField, keyValue string) ([]byte, error) {
	// Build SELECT query using PostgreSQL builder
	builder := postgres.Select(astql.T(table)).
		Where(astql.C(astql.F(keyField), astql.EQ, astql.P("key"))).
		Limit(1)

	ast, err := builder.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	// Execute the query
	return p.executePostgresAST(ctx, ast, map[string]interface{}{"key": keyValue})
}

// Set inserts or updates a record in the database.
func (p *Provider) Set(ctx context.Context, table, keyField, keyValue string, data []byte) error {
	// Parse the data to extract fields
	var fields map[string]interface{}
	if err := json.Unmarshal(data, &fields); err != nil {
		return fmt.Errorf("failed to parse data: %w", err)
	}

	// Add the key to fields
	fields[keyField] = keyValue

	// Build INSERT ... ON CONFLICT UPDATE query
	values := make(map[astql.Field]astql.Param)
	updates := make(map[astql.Field]astql.Param)

	for name := range fields {
		field := astql.F(name)
		param := astql.P(name)
		values[field] = param
		if name != keyField { // Don't update the key field
			updates[field] = param
		}
	}

	// Use postgres builder with ON CONFLICT on the specified key field
	builder := postgres.Insert(astql.T(table)).
		Values(values).
		OnConflict(astql.F(keyField)).DoUpdate(updates)

	ast, err := builder.Build()
	if err != nil {
		return fmt.Errorf("failed to build query: %w", err)
	}

	// Execute the query
	_, err = p.executePostgresAST(ctx, ast, fields)
	return err
}

// Delete removes a record from the database.
func (p *Provider) Delete(ctx context.Context, table, keyField, keyValue string) error {
	// Build DELETE query using PostgreSQL builder
	builder := postgres.Delete(astql.T(table)).
		Where(astql.C(astql.F(keyField), astql.EQ, astql.P("key")))

	ast, err := builder.Build()
	if err != nil {
		return fmt.Errorf("failed to build query: %w", err)
	}

	// Execute the query
	result, err := p.executePostgresAST(ctx, ast, map[string]interface{}{"key": keyValue})
	if err != nil {
		return err
	}

	// Check if any rows were affected
	var affected int64
	if err := json.Unmarshal(result, &affected); err == nil && affected == 0 {
		return cereal.ErrNotFound
	}

	return nil
}

// Exists checks if a record exists in the database.
func (p *Provider) Exists(ctx context.Context, table, keyField, keyValue string) (bool, error) {
	// Build COUNT query using PostgreSQL builder
	builder := postgres.Count(astql.T(table)).
		Where(astql.C(astql.F(keyField), astql.EQ, astql.P("key")))

	ast, err := builder.Build()
	if err != nil {
		return false, fmt.Errorf("failed to build query: %w", err)
	}

	// Execute the query
	result, err := p.executePostgresAST(ctx, ast, map[string]interface{}{"key": keyValue})
	if err != nil {
		return false, err
	}

	// Parse count result
	var count int64
	if err := json.Unmarshal(result, &count); err != nil {
		return false, fmt.Errorf("failed to parse count result: %w", err)
	}

	return count > 0, nil
}

// BatchGet retrieves multiple records from the database.
func (p *Provider) BatchGet(ctx context.Context, table, keyField string, keyValues []string) ([][]byte, []error) {
	results := make([][]byte, len(keyValues))
	errors := make([]error, len(keyValues))

	// Build interface slice for IN query
	ids := make([]interface{}, len(keyValues))
	indexMap := make(map[string]int)
	for i, key := range keyValues {
		ids[i] = key
		indexMap[key] = i
	}

	// Build IN query
	builder := postgres.Select(astql.T(table)).
		Where(astql.C(astql.F(keyField), astql.IN, astql.P("keys")))

	ast, err := builder.Build()
	if err != nil {
		for i := range errors {
			errors[i] = fmt.Errorf("failed to build query: %w", err)
		}
		return results, errors
	}

	// Execute the query
	result, err := p.executePostgresAST(ctx, ast, map[string]interface{}{"keys": ids})
	if err != nil {
		for i := range errors {
			errors[i] = err
		}
		return results, errors
	}

	// Parse results
	var rows []map[string]interface{}
	if err := json.Unmarshal(result, &rows); err != nil {
		for i := range errors {
			errors[i] = fmt.Errorf("failed to parse results: %w", err)
		}
		return results, errors
	}

	// Map results back to indices
	for _, row := range rows {
		if keyVal, ok := row[keyField]; ok {
			keyStr := fmt.Sprintf("%v", keyVal) // Convert to string
			if idx, found := indexMap[keyStr]; found {
				rowData, _ := json.Marshal(row)
				results[idx] = rowData
				delete(indexMap, keyStr) // Mark as found
			}
		}
	}

	// Mark not found entries
	for _, idx := range indexMap {
		errors[idx] = cereal.ErrNotFound
	}

	return results, errors
}

// BatchSet inserts or updates multiple records in the database.
func (p *Provider) BatchSet(ctx context.Context, table, keyField string, keyValues []string, data [][]byte) []error {
	if len(keyValues) != len(data) {
		errors := make([]error, len(keyValues))
		for i := range errors {
			errors[i] = cereal.ErrBatchSizeMismatch
		}
		return errors
	}

	errors := make([]error, len(keyValues))

	// Process each item individually for now
	// TODO: Optimize with bulk operations
	for i := range keyValues {
		errors[i] = p.Set(ctx, table, keyField, keyValues[i], data[i])
	}

	return errors
}

// BatchDelete removes multiple records from the database.
func (p *Provider) BatchDelete(ctx context.Context, table, keyField string, keyValues []string) []error {
	errors := make([]error, len(keyValues))

	// Build interface slice for IN query
	ids := make([]interface{}, len(keyValues))
	for i, key := range keyValues {
		ids[i] = key
	}

	// Build DELETE IN query
	builder := postgres.Delete(astql.T(table)).
		Where(astql.C(astql.F(keyField), astql.IN, astql.P("keys")))

	ast, err := builder.Build()
	if err != nil {
		for i := range errors {
			errors[i] = fmt.Errorf("failed to build query: %w", err)
		}
		return errors
	}

	// Execute the query
	_, err = p.executePostgresAST(ctx, ast, map[string]interface{}{"keys": ids})
	if err != nil {
		for i := range errors {
			errors[i] = err
		}
	}

	return errors
}

// RegisterQuery stores a named query AST for later execution.
func (p *Provider) RegisterQuery(name string, ast interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.queries[name]; exists {
		return cereal.ErrQueryExists
	}

	// Ensure we're storing a postgres AST
	pgAst, ok := ast.(*postgres.AST)
	if !ok {
		return fmt.Errorf("expected *postgres.AST, got %T", ast)
	}

	p.queries[name] = pgAst
	return nil
}

// UnregisterQuery removes a named query.
func (p *Provider) UnregisterQuery(name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.queries[name]; !exists {
		return cereal.ErrQueryNotFound
	}

	delete(p.queries, name)
	return nil
}

// GetQuery retrieves a registered query AST by name.
func (p *Provider) GetQuery(name string) (interface{}, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	ast, exists := p.queries[name]
	if !exists {
		return nil, cereal.ErrQueryNotFound
	}

	return ast, nil
}

// ListQueries returns all registered query names.
func (p *Provider) ListQueries() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	names := make([]string, 0, len(p.queries))
	for name := range p.queries {
		names = append(names, name)
	}
	return names
}

// Execute runs a named query with parameters.
func (p *Provider) Execute(ctx context.Context, name string, parameters map[string]interface{}) ([]byte, error) {
	// Look up the named query
	storedAST, err := p.GetQuery(name)
	if err != nil {
		return nil, err
	}

	ast, ok := storedAST.(*postgres.AST)
	if !ok {
		return nil, fmt.Errorf("stored query is not a postgres AST")
	}

	// Execute the postgres AST
	return p.executePostgresAST(ctx, ast, parameters)
}

// CreateTable generates and optionally executes a CREATE TABLE statement.
func (p *Provider) CreateTable(ctx context.Context, metadata interface{}, execute bool) (string, error) {
	// Type assert to sentinel.ModelMetadata
	modelMeta, ok := metadata.(sentinel.ModelMetadata)
	if !ok {
		return "", fmt.Errorf("metadata must be sentinel.ModelMetadata")
	}

	// Generate CREATE TABLE SQL
	sql := p.generateCreateTable(modelMeta)

	if execute && p.db != nil {
		_, err := p.db.ExecContext(ctx, sql)
		if err != nil {
			return sql, fmt.Errorf("failed to create table: %w", err)
		}
	}

	return sql, nil
}

// generateCreateTable builds a CREATE TABLE statement from metadata.
func (p *Provider) generateCreateTable(meta sentinel.ModelMetadata) string {
	var b strings.Builder
	tableName := strings.ToLower(meta.TypeName) + "s"

	b.WriteString("CREATE TABLE IF NOT EXISTS ")
	b.WriteString(tableName)
	b.WriteString(" (\n")

	var columns []string
	var primaryKeys []string
	var foreignKeys []string

	for _, field := range meta.Fields {
		// Get column name from db tag or field name
		columnName := field.Tags["db"]
		if columnName == "" {
			columnName = strings.ToLower(field.Name)
		}

		// Get SQL type
		sqlType := p.mapGoTypeToSQL(field.Type, field.Tags)

		// Build column definition
		colDef := fmt.Sprintf("    %s %s", columnName, sqlType)

		// Parse constraints
		if constraints, ok := field.Tags["constraints"]; ok {
			constraintList := strings.Split(constraints, ",")
			for _, c := range constraintList {
				switch strings.TrimSpace(c) {
				case "pk":
					primaryKeys = append(primaryKeys, columnName)
				case "unique":
					colDef += " UNIQUE"
				case "not_null":
					colDef += " NOT NULL"
				case "fk":
					// Handle foreign key - extract table name from field type
					refTable := p.extractTableFromType(field.Type)
					foreignKeys = append(foreignKeys,
						fmt.Sprintf("    FOREIGN KEY (%s) REFERENCES %s(id)", columnName, refTable))
				}
			}
		}

		columns = append(columns, colDef)
	}

	// Add columns
	b.WriteString(strings.Join(columns, ",\n"))

	// Add primary key constraint
	if len(primaryKeys) > 0 {
		b.WriteString(",\n    PRIMARY KEY (")
		b.WriteString(strings.Join(primaryKeys, ", "))
		b.WriteString(")")
	}

	// Add foreign key constraints
	for _, fk := range foreignKeys {
		b.WriteString(",\n")
		b.WriteString(fk)
	}

	b.WriteString("\n);")
	return b.String()
}

// mapGoTypeToSQL maps Go types to PostgreSQL types.
func (p *Provider) mapGoTypeToSQL(goType string, tags map[string]string) string {
	// Check for explicit type tag
	if sqlType, ok := tags["type"]; ok {
		return strings.ToUpper(sqlType)
	}

	// Check validate tag for type hints
	if validate, ok := tags["validate"]; ok {
		// Check for specific validators that map to PostgreSQL types
		if strings.Contains(validate, "uuid") {
			return "UUID"
		}
		if strings.Contains(validate, "ip") || strings.Contains(validate, "ipv4") || strings.Contains(validate, "ipv6") {
			return "INET"
		}
		if strings.Contains(validate, "cidr") {
			return "CIDR"
		}
		if strings.Contains(validate, "mac") {
			return "MACADDR"
		}
		if strings.Contains(validate, "json") {
			return "JSONB"
		}
		if strings.Contains(validate, "latitude") || strings.Contains(validate, "longitude") {
			return "DECIMAL(11,8)" // Sufficient precision for coordinates
		}
		if strings.Contains(validate, "numeric") {
			return "NUMERIC"
		}
	}

	// Handle foreign key fields
	if constraints, ok := tags["constraints"]; ok && strings.Contains(constraints, "fk") {
		// Check if there's a validate tag that gives us a hint
		if validate, ok := tags["validate"]; ok && strings.Contains(validate, "uuid") {
			return "UUID"
		}
		// For struct types, assume UUID (common for modern schemas)
		// For basic types, use the type directly
		if strings.Contains(goType, ".") {
			return "UUID" // Struct foreign key, assume UUID
		}
		// Basic type foreign keys
		if strings.Contains(goType, "string") {
			return "TEXT"
		}
		return "BIGINT"
	}

	// Default type mappings
	switch goType {
	case "string":
		return "TEXT"
	case "int", "int64":
		return "BIGINT"
	case "int32":
		return "INTEGER"
	case "bool":
		return "BOOLEAN"
	case "float64":
		return "DOUBLE PRECISION"
	case "float32":
		return "REAL"
	case "time.Time":
		return "TIMESTAMPTZ"
	case "[]uint8", "[]byte":
		return "BYTEA"
	default:
		// Handle slices of basic types
		if strings.HasPrefix(goType, "[]") {
			elemType := strings.TrimPrefix(goType, "[]")
			switch elemType {
			case "string":
				return "TEXT[]"
			case "int", "int64":
				return "BIGINT[]"
			case "int32":
				return "INTEGER[]"
			case "float64":
				return "DOUBLE PRECISION[]"
			case "float32":
				return "REAL[]"
			case "bool":
				return "BOOLEAN[]"
			default:
				return "JSONB" // Complex array types
			}
		}

		// Complex types become JSONB
		if strings.Contains(goType, ".") {
			return "JSONB"
		}
		return "TEXT"
	}
}

// extractTableFromType extracts table name from a type string.
func (p *Provider) extractTableFromType(typeStr string) string {
	// Remove package prefix if present
	parts := strings.Split(typeStr, ".")
	typeName := parts[len(parts)-1]

	// Remove pointer prefix
	typeName = strings.TrimPrefix(typeName, "*")

	// Handle slice types
	typeName = strings.TrimPrefix(typeName, "[]")

	// Pluralize and lowercase
	return strings.ToLower(typeName) + "s"
}

// Close cleans up the database connection.
func (p *Provider) Close() error {
	if p.db == nil {
		return nil
	}
	return p.db.Close()
}

// executePostgresAST executes a PostgreSQL-specific AST and returns JSON results.
func (p *Provider) executePostgresAST(ctx context.Context, ast *postgres.AST, params map[string]interface{}) ([]byte, error) {
	// Render the AST to SQL
	provider := postgres.NewProvider()
	result, err := provider.Render(ast)
	if err != nil {
		return nil, fmt.Errorf("failed to render query: %w", err)
	}

	// Determine if we expect a single result
	expectSingle := false
	if ast.Operation == astql.OpSelect && ast.Limit != nil && *ast.Limit == 1 {
		expectSingle = true
	}

	// Execute based on operation type
	switch ast.Operation {
	case astql.OpSelect:
		return p.executeSelect(ctx, result.SQL, params, expectSingle)
	case astql.OpCount:
		// COUNT always returns a single value
		return p.executeSelect(ctx, result.SQL, params, true)
	case astql.OpInsert, astql.OpUpdate, astql.OpDelete:
		return p.executeModify(ctx, result.SQL, params)
	default:
		return nil, fmt.Errorf("unsupported operation: %s", ast.Operation)
	}
}

// executeSelect executes a SELECT query and returns JSON results.
func (p *Provider) executeSelect(ctx context.Context, query string, params map[string]interface{}, expectSingle bool) ([]byte, error) {
	rows, err := p.db.NamedQueryContext(ctx, query, params)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	// Collect all results
	var results []map[string]interface{}
	for rows.Next() {
		row := make(map[string]interface{})
		if err := rows.MapScan(row); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	// Handle single result vs array based on AST metadata
	if len(results) == 0 {
		return nil, cereal.ErrNotFound
	} else if len(results) == 1 && expectSingle {
		// Single result expected (e.g., LIMIT 1 query)
		return json.Marshal(results[0])
	}

	// Multiple results
	return json.Marshal(results)
}

// executeModify executes an INSERT/UPDATE/DELETE query and returns affected rows count.
func (p *Provider) executeModify(ctx context.Context, query string, params map[string]interface{}) ([]byte, error) {
	result, err := p.db.NamedExecContext(ctx, query, params)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("failed to get affected rows: %w", err)
	}

	return json.Marshal(affected)
}
