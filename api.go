// Package cereal provides a type-safe, schema-validated query builder for PostgreSQL.
//
// Cereal wraps ASTQL to offer a simplified API for building SQL queries with compile-time
// type safety and runtime schema validation. It uses reflection (via Sentinel) once at
// initialization, then provides a zero-allocation query building API.
//
// # Quick Start
//
// Define your model with struct tags:
//
//	type User struct {
//	    ID    int    `db:"id" type:"integer" constraints:"primarykey"`
//	    Email string `db:"email" type:"text" constraints:"notnull,unique"`
//	    Name  string `db:"name" type:"text"`
//	    Age   *int   `db:"age" type:"integer"`
//	}
//
// Create a Cereal instance:
//
//	cereal, err := cereal.New[User](db, "users")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// Build and execute queries:
//
//	// Select single record
//	user, err := cereal.Select().
//	    Where("email", "=", "user_email").
//	    Exec(ctx, map[string]any{"user_email": "test@example.com"})
//
//	// Query multiple records
//	users, err := cereal.Query().
//	    Where("age", ">=", "min_age").
//	    OrderBy("name", "ASC").
//	    Limit(10).
//	    Exec(ctx, map[string]any{"min_age": 18})
//
//	// Insert with upsert
//	user := &User{Email: "test@example.com", Name: "Test"}
//	inserted, err := cereal.Insert().
//	    OnConflict("email").
//	    DoUpdate().
//	    Set("name", "name").
//	    Build().
//	    Exec(ctx, user)
//
//	// Update
//	updated, err := cereal.Modify().
//	    Set("name", "new_name").
//	    Where("id", "=", "user_id").
//	    Exec(ctx, map[string]any{"new_name": "John", "user_id": 123})
//
//	// Delete
//	deleted, err := cereal.Remove().
//	    Where("id", "=", "user_id").
//	    Exec(ctx, map[string]any{"user_id": 123})
//
//	// Aggregates
//	count, err := cereal.Count().
//	    Where("status", "=", "active").
//	    Exec(ctx, map[string]any{"status": "active"})
//
// # Features
//
//   - Type-safe query building with compile-time guarantees
//   - Runtime schema validation against struct tags
//   - Zero reflection on the query hot path
//   - Named parameter placeholders for SQL injection safety
//   - Batch operations for inserts, updates, and deletes
//   - Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
//   - Complex WHERE conditions with AND/OR grouping
//   - DBML schema generation from struct tags
//   - Integration with capitan for structured logging
package cereal

import (
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql"
	"github.com/zoobzio/sentinel"
)

// CastType represents SQL cast types.
// Re-exported from astql for convenience.
type CastType = astql.CastType

// Cast type constants.
const (
	CastText            CastType = astql.CastText
	CastInteger         CastType = astql.CastInteger
	CastBigint          CastType = astql.CastBigint
	CastSmallint        CastType = astql.CastSmallint
	CastNumeric         CastType = astql.CastNumeric
	CastReal            CastType = astql.CastReal
	CastDoublePrecision CastType = astql.CastDoublePrecision
	CastBoolean         CastType = astql.CastBoolean
	CastDate            CastType = astql.CastDate
	CastTime            CastType = astql.CastTime
	CastTimestamp       CastType = astql.CastTimestamp
	CastTimestampTZ     CastType = astql.CastTimestampTZ
	CastInterval        CastType = astql.CastInterval
	CastUUID            CastType = astql.CastUUID
	CastJSON            CastType = astql.CastJSON
	CastJSONB           CastType = astql.CastJSONB
	CastBytea           CastType = astql.CastBytea
)

// Cereal provides a type-safe query API for a specific model type.
// Each instance holds the ASTQL schema and metadata for building validated queries.
type Cereal[T any] struct {
	db          *sqlx.DB
	tableName   string
	metadata    sentinel.Metadata
	instance    *astql.ASTQL
	sqlRenderer astql.Renderer
}

// New creates a new Cereal instance for type T with the given database connection, table name, and SQL renderer.
// This function performs type inspection via Sentinel and builds the ASTQL schema for validation.
// All reflection and schema building happens once at initialization, not on the hot path.
// If db is nil, the instance can still be used for query building but not execution.
//
// Available renderers from astql/pkg:
//   - postgres.New() for PostgreSQL
//   - mysql.New() for MySQL
//   - sqlite.New() for SQLite
//   - mssql.New() for Microsoft SQL Server
func New[T any](db *sqlx.DB, tableName string, renderer astql.Renderer) (*Cereal[T], error) {
	if tableName == "" {
		return nil, fmt.Errorf("cereal: table name cannot be empty")
	}

	if renderer == nil {
		return nil, fmt.Errorf("cereal: renderer cannot be nil")
	}

	// Register all tags we use
	sentinel.Tag("db")
	sentinel.Tag("type")
	sentinel.Tag("constraints")
	sentinel.Tag("default")
	sentinel.Tag("check")
	sentinel.Tag("index")
	sentinel.Tag("references")

	// Inspect type using Sentinel (cached after first call)
	metadata := sentinel.Inspect[T]()

	// Build DBML from struct metadata
	project, err := buildDBMLFromStruct(metadata, tableName)
	if err != nil {
		return nil, fmt.Errorf("cereal: failed to build DBML: %w", err)
	}

	// Create ASTQL instance for validation
	instance, err := astql.NewFromDBML(project)
	if err != nil {
		return nil, fmt.Errorf("cereal: failed to create ASTQL instance: %w", err)
	}

	c := &Cereal[T]{
		db:          db,
		tableName:   tableName,
		metadata:    metadata,
		instance:    instance,
		sqlRenderer: renderer,
	}

	return c, nil
}

// execer returns the database connection for query execution.
func (c *Cereal[T]) execer() sqlx.ExtContext {
	return c.db
}

// TableName returns the table name for this Cereal instance.
func (c *Cereal[T]) TableName() string {
	return c.tableName
}

// getTableName returns the table name (for interface implementation).
func (c *Cereal[T]) getTableName() string {
	return c.tableName
}

// Metadata returns the Sentinel metadata for type T.
func (c *Cereal[T]) Metadata() sentinel.Metadata {
	return c.metadata
}

// renderer returns the SQL renderer for query building.
func (c *Cereal[T]) renderer() astql.Renderer {
	return c.sqlRenderer
}

// Instance returns the underlying ASTQL instance for advanced query building.
// Use this escape hatch when you need ASTQL features not exposed by the s.
//
// Example:
//
//	instance := cereal.Instance()
//	query := astql.Select(instance.T("users")).
//	    Fields(instance.F("id"), instance.F("email")).
//	    Where(instance.C(instance.F("age"), ">=", instance.P("min_age")))
func (c *Cereal[T]) Instance() *astql.ASTQL {
	return c.instance
}

// Select returns a Select for building SELECT queries that return a single record.
// The  is pre-configured with the table for this Cereal instance
// and provides a simple string-based API that hides ASTQL complexity.
//
// Example with Render (for inspection):
//
//	result, err := cereal.Select().
//	    Fields("id", "email", "name").
//	    Where("id", "=", "user_id").
//	    Render()
//
// Example with Exec (execute and return single T):
//
//	user, err := cereal.Select().
//	    Where("email", "=", "user_email").
//	    Exec(ctx, map[string]any{"user_email": "test@example.com"})
//
// For complex queries with AND/OR logic:
//
//	user, err := cereal.Select().
//	    WhereAnd(
//	        cereal.C("age", ">=", "min_age"),
//	        cereal.C("status", "=", "active"),
//	    ).
//	    Exec(ctx, params)
//
// For advanced ASTQL features not exposed by Select, use Instance():
//
//	instance := .Instance()
//	// Use instance.F(), instance.C(), etc. for advanced queries
func (c *Cereal[T]) Select() *Select[T] {
	t, err := c.instance.TryT(c.tableName)
	if err != nil {
		// Table should always be valid since it was validated in New()
		// Return  with error stored
		return &Select[T]{
			instance: c.instance,
			cereal:   c,
			err:      fmt.Errorf("invalid table %q: %w", c.tableName, err),
		}
	}

	builder := astql.Select(t)

	return &Select[T]{
		instance: c.instance,
		builder:  builder,
		cereal:   c,
	}
}

// Query returns a Query for building SELECT queries that return multiple records.
// The  is pre-configured with the table for this Cereal instance
// and provides a simple string-based API that hides ASTQL complexity.
//
// Example (basic query):
//
//	users, err := cereal.Query().
//	    Where("age", ">=", "min_age").
//	    OrderBy("name", "ASC").
//	    Exec(ctx, map[string]any{"min_age": 18})
//
// Example (with pagination):
//
//	users, err := cereal.Query().
//	    Where("status", "=", "active").
//	    OrderBy("created_at", "DESC").
//	    Limit(10).
//	    Offset(20).
//	    Exec(ctx, params)
//
// Example (complex conditions):
//
//	users, err := cereal.Query().
//	    WhereAnd(
//	        cereal.C("age", ">=", "min_age"),
//	        cereal.C("status", "=", "active"),
//	    ).
//	    Exec(ctx, params)
func (c *Cereal[T]) Query() *Query[T] {
	t, err := c.instance.TryT(c.tableName)
	if err != nil {
		// Table should always be valid since it was validated in New()
		// Return  with error stored
		return &Query[T]{
			instance: c.instance,
			cereal:   c,
			err:      fmt.Errorf("invalid table %q: %w", c.tableName, err),
		}
	}

	builder := astql.Select(t)

	return &Query[T]{
		instance: c.instance,
		builder:  builder,
		cereal:   c,
	}
}

// Count returns an Aggregate for building COUNT queries.
// The builder is pre-configured with the table for this Cereal instance
// and provides a simple string-based API for counting records.
//
// Example (count all):
//
//	count, err := cereal.Count().Exec(ctx, nil)
//
// Example (count with conditions):
//
//	count, err := cereal.Count().
//	    Where("age", ">=", "min_age").
//	    Where("status", "=", "active").
//	    Exec(ctx, map[string]any{"min_age": 18, "status": "active"})
func (c *Cereal[T]) Count() *Aggregate[T] {
	t, err := c.instance.TryT(c.tableName)
	if err != nil {
		// Table should always be valid since it was validated in New()
		// Return builder with error stored
		return &Aggregate[T]{
			agg: &aggregateBuilder[T]{
				instance: c.instance,
				cereal:   c,
				funcName: "COUNT",
				err:      fmt.Errorf("invalid table %q: %w", c.tableName, err),
			},
		}
	}

	// Build COUNT query
	builder := astql.Count(t)

	return &Aggregate[T]{
		agg: newAggregateBuilder[T](c.instance, builder, c, "", "COUNT"),
	}
}

// Sum returns an Aggregate for building SUM aggregate queries.
// Returns the sum of the specified field for matching records.
//
// Example:
//
//	total, err := cereal.Sum("amount").
//	    Where("status", "=", "paid").
//	    Exec(ctx, map[string]any{"status": "paid"})
func (c *Cereal[T]) Sum(field string) *Aggregate[T] {
	return c.buildFieldAggregate(field, "SUM")
}

// Avg returns an Aggregate for building AVG aggregate queries.
// Returns the average of the specified field for matching records.
//
// Example:
//
//	average, err := cereal.Avg("age").
//	    Where("status", "=", "active").
//	    Exec(ctx, map[string]any{"status": "active"})
func (c *Cereal[T]) Avg(field string) *Aggregate[T] {
	return c.buildFieldAggregate(field, "AVG")
}

// Min returns an Aggregate for building MIN aggregate queries.
// Returns the minimum value of the specified field for matching records.
//
// Example:
//
//	minPrice, err := cereal.Min("price").
//	    Where("category", "=", "electronics").
//	    Exec(ctx, map[string]any{"category": "electronics"})
func (c *Cereal[T]) Min(field string) *Aggregate[T] {
	return c.buildFieldAggregate(field, "MIN")
}

// Max returns an Aggregate for building MAX aggregate queries.
// Returns the maximum value of the specified field for matching records.
//
// Example:
//
//	maxPrice, err := cereal.Max("price").
//	    Where("category", "=", "electronics").
//	    Exec(ctx, map[string]any{"category": "electronics"})
func (c *Cereal[T]) Max(field string) *Aggregate[T] {
	return c.buildFieldAggregate(field, "MAX")
}

// buildFieldAggregate is a helper to build field-based aggregate queries (SUM, AVG, MIN, MAX).
func (c *Cereal[T]) buildFieldAggregate(field, funcName string) *Aggregate[T] {
	t, err := c.instance.TryT(c.tableName)
	if err != nil {
		return &Aggregate[T]{
			agg: &aggregateBuilder[T]{
				instance: c.instance,
				cereal:   c,
				field:    field,
				funcName: funcName,
				err:      fmt.Errorf("invalid table %q: %w", c.tableName, err),
			},
		}
	}

	f, err := c.instance.TryF(field)
	if err != nil {
		return &Aggregate[T]{
			agg: &aggregateBuilder[T]{
				instance: c.instance,
				cereal:   c,
				field:    field,
				funcName: funcName,
				err:      fmt.Errorf("invalid field %q: %w", field, err),
			},
		}
	}

	// Build the appropriate aggregate expression based on funcName
	var builder *astql.Builder
	switch funcName {
	case "SUM":
		builder = astql.Select(t).SelectExpr(astql.Sum(f))
	case "AVG":
		builder = astql.Select(t).SelectExpr(astql.Avg(f))
	case "MIN":
		builder = astql.Select(t).SelectExpr(astql.Min(f))
	case "MAX":
		builder = astql.Select(t).SelectExpr(astql.Max(f))
	default:
		return &Aggregate[T]{
			agg: &aggregateBuilder[T]{
				instance: c.instance,
				cereal:   c,
				field:    field,
				funcName: funcName,
				err:      fmt.Errorf("unsupported aggregate function: %s", funcName),
			},
		}
	}

	return &Aggregate[T]{
		agg: newAggregateBuilder[T](c.instance, builder, c, field, funcName),
	}
}

// Insert returns a Create for building INSERT queries.
// The  is pre-configured to insert into the table for this Cereal instance
// and automatically sets up VALUES from the struct fields and RETURNING all columns.
//
// Example (simple insert):
//
//	user := &User{Email: "test@example.com", Name: "Test"}
//	inserted, err := cereal.Insert().Exec(ctx, user)
//
// Example (upsert with ON CONFLICT):
//
//	user := &User{Email: "test@example.com", Name: "Test"}
//	inserted, err := cereal.Insert().
//	    OnConflict("email").
//	    DoUpdate().
//	    Set("name", "name").
//	    Exec(ctx, user)
func (c *Cereal[T]) Insert() *Create[T] {
	t, err := c.instance.TryT(c.tableName)
	if err != nil {
		return &Create[T]{
			instance: c.instance,
			cereal:   c,
			err:      fmt.Errorf("invalid table %q: %w", c.tableName, err),
		}
	}

	builder := astql.Insert(t)

	// Build VALUES map using factory - include all non-PK columns
	values := c.instance.ValueMap()
	for _, field := range c.metadata.Fields {
		dbCol := field.Tags["db"]
		if dbCol == "" || dbCol == "-" {
			continue
		}
		// Skip primary key columns (they're usually auto-generated)
		constraints := field.Tags["constraints"]
		if contains(constraints, "primarykey") || contains(constraints, "primary_key") {
			continue
		}

		f, err := c.instance.TryF(dbCol)
		if err != nil {
			return &Create[T]{
				instance: c.instance,
				cereal:   c,
				err:      fmt.Errorf("invalid field %q: %w", dbCol, err),
			}
		}

		p, err := c.instance.TryP(dbCol)
		if err != nil {
			return &Create[T]{
				instance: c.instance,
				cereal:   c,
				err:      fmt.Errorf("invalid param %q: %w", dbCol, err),
			}
		}

		values[f] = p
	}
	builder = builder.Values(values)

	// Add RETURNING for all columns (to get generated PKs, defaults, etc.)
	for _, field := range c.metadata.Fields {
		dbCol := field.Tags["db"]
		if dbCol == "" || dbCol == "-" {
			continue
		}

		f, err := c.instance.TryF(dbCol)
		if err != nil {
			return &Create[T]{
				instance: c.instance,
				cereal:   c,
				err:      fmt.Errorf("invalid field %q: %w", dbCol, err),
			}
		}

		builder = builder.Returning(f)
	}

	return &Create[T]{
		instance: c.instance,
		builder:  builder,
		cereal:   c,
	}
}

// Modify returns an Update for building UPDATE queries.
// The  is pre-configured with the table for this Cereal instance
// and automatically adds RETURNING for all columns.
//
// IMPORTANT: You must add at least one WHERE condition to prevent accidental full-table updates.
//
// Example:
//
//	params := map[string]any{
//	    "new_name": "Updated Name",
//	    "new_age": 30,
//	    "user_id": 123,
//	}
//	updated, err := cereal.Modify().
//	    Set("name", "new_name").
//	    Set("age", "new_age").
//	    Where("id", "=", "user_id").
//	    Exec(ctx, params)
func (c *Cereal[T]) Modify() *Update[T] {
	t, err := c.instance.TryT(c.tableName)
	if err != nil {
		return &Update[T]{
			instance: c.instance,
			cereal:   c,
			err:      fmt.Errorf("invalid table %q: %w", c.tableName, err),
		}
	}

	builder := astql.Update(t)

	// Add RETURNING for all columns (to get updated values)
	for _, field := range c.metadata.Fields {
		dbCol := field.Tags["db"]
		if dbCol == "" || dbCol == "-" {
			continue
		}

		f, err := c.instance.TryF(dbCol)
		if err != nil {
			return &Update[T]{
				instance: c.instance,
				cereal:   c,
				err:      fmt.Errorf("invalid field %q: %w", dbCol, err),
			}
		}

		builder = builder.Returning(f)
	}

	return &Update[T]{
		instance: c.instance,
		builder:  builder,
		cereal:   c,
	}
}

// Remove returns a Delete for building DELETE queries.
// The  is pre-configured with the table for this Cereal instance.
//
// IMPORTANT: You must add at least one WHERE condition to prevent accidental full-table deletes.
//
// Example:
//
//	params := map[string]any{"user_id": 123}
//	rowsDeleted, err := cereal.Remove().
//	    Where("id", "=", "user_id").
//	    Exec(ctx, params)
func (c *Cereal[T]) Remove() *Delete[T] {
	t, err := c.instance.TryT(c.tableName)
	if err != nil {
		return &Delete[T]{
			instance: c.instance,
			cereal:   c,
			err:      fmt.Errorf("invalid table %q: %w", c.tableName, err),
		}
	}

	builder := astql.Delete(t)

	return &Delete[T]{
		instance: c.instance,
		builder:  builder,
		cereal:   c,
	}
}

// contains checks if a string contains a substring (case-insensitive).
func contains(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
