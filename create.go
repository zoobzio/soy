package soy

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql"
	"github.com/zoobzio/atom"
	"github.com/zoobzio/capitan"
)

// Create provides a focused API for building INSERT queries.
// It wraps ASTQL's INSERT functionality with a simple string-based interface.
// Use this for inserting new records into the database.
type Create[T any] struct {
	instance *astql.ASTQL
	builder  *astql.Builder
	soy      soyExecutor // interface for execution
	err      error       // stores first error encountered during building

	// Conflict tracking for fallback upsert (MSSQL)
	hasConflict     bool              // true if OnConflict was called
	conflictColumns []string          // columns for conflict detection (WHERE clause)
	updateFields    map[string]string // field -> param for UPDATE SET clause
}

// OnConflict adds an ON CONFLICT clause for handling unique constraint violations.
// Specify the column(s) that might conflict.
//
// Example:
//
//	soy.Insert().
//	    OnConflict("email").
//	    DoNothing().
//	    Exec(ctx, &user)
func (cb *Create[T]) OnConflict(columns ...string) *Conflict[T] {
	if cb.err != nil {
		return &Conflict[T]{
			create: cb,
		}
	}

	// Track conflict for fallback upsert
	cb.hasConflict = true
	cb.conflictColumns = columns
	cb.updateFields = make(map[string]string)

	// Build fields slice for conflict columns
	fields := cb.instance.Fields()
	for _, col := range columns {
		f, err := cb.instance.TryF(col)
		if err != nil {
			cb.err = fmt.Errorf("invalid conflict column %q: %w", col, err)
			return &Conflict[T]{
				create: cb,
			}
		}
		fields = append(fields, f)
	}

	astqlConflict := cb.builder.OnConflict(fields...)

	return &Conflict[T]{
		create:        cb,
		astqlConflict: astqlConflict,
	}
}

// Exec executes the INSERT query with values from the provided record.
// Returns the inserted record with all fields populated (including generated PKs, defaults).
//
// Example:
//
//	user := &User{Email: "test@example.com", Name: "Test"}
//	inserted, err := soy.Insert().Exec(ctx, user)
func (cb *Create[T]) Exec(ctx context.Context, record *T) (*T, error) {
	return cb.exec(ctx, cb.soy.execer(), record)
}

// ExecTx executes the INSERT query within a transaction.
func (cb *Create[T]) ExecTx(ctx context.Context, tx *sqlx.Tx, record *T) (*T, error) {
	return cb.exec(ctx, tx, record)
}

// ExecAtom executes the INSERT query and returns the inserted record as an Atom.
// This method enables type-erased execution where T is not known at consumption time.
//
// Example:
//
//	atom, err := soy.Insert().ExecAtom(ctx, map[string]any{"email": "test@example.com", "name": "Test"})
func (cb *Create[T]) ExecAtom(ctx context.Context, params map[string]any) (*atom.Atom, error) {
	return cb.execAtom(ctx, cb.soy.execer(), params)
}

// ExecTxAtom executes the INSERT query within a transaction and returns the inserted record as an Atom.
// This method enables type-erased execution where T is not known at consumption time.
//
// Example:
//
//	tx, _ := db.BeginTxx(ctx, nil)
//	defer tx.Rollback()
//	atom, err := soy.Insert().ExecTxAtom(ctx, tx, map[string]any{"email": "test@example.com"})
//	tx.Commit()
func (cb *Create[T]) ExecTxAtom(ctx context.Context, tx *sqlx.Tx, params map[string]any) (*atom.Atom, error) {
	return cb.execAtom(ctx, tx, params)
}

// execAtom is the internal atom execution method used by both ExecAtom and ExecTxAtom.
func (cb *Create[T]) execAtom(ctx context.Context, execer sqlx.ExtContext, params map[string]any) (*atom.Atom, error) {
	if cb.err != nil {
		return nil, fmt.Errorf("create builder has errors: %w", cb.err)
	}

	result, err := cb.builder.Render(cb.soy.renderer())
	if err != nil {
		return nil, fmt.Errorf("failed to render INSERT query: %w", err)
	}

	return execAtomSingleRow(ctx, execer, cb.soy.atomScanner(), result.SQL, params, cb.soy.getTableName(), "INSERT")
}

// ExecBatch executes the INSERT query for multiple records.
// Returns the number of records inserted.
//
// Example:
//
//	users := []*User{
//	    {Email: "user1@example.com", Name: "User 1"},
//	    {Email: "user2@example.com", Name: "User 2"},
//	}
//	count, err := soy.Insert().ExecBatch(ctx, users)
func (cb *Create[T]) ExecBatch(ctx context.Context, records []*T) (int64, error) {
	return cb.execBatch(ctx, cb.soy.execer(), records)
}

// ExecBatchTx executes the INSERT query for multiple records within a transaction.
// Returns the number of records inserted.
//
// Example:
//
//	tx, _ := db.BeginTxx(ctx, nil)
//	defer tx.Rollback()
//	count, err := soy.Insert().ExecBatchTx(ctx, tx, users)
//	tx.Commit()
func (cb *Create[T]) ExecBatchTx(ctx context.Context, tx *sqlx.Tx, records []*T) (int64, error) {
	return cb.execBatch(ctx, tx, records)
}

// execBatch is the internal batch execution method.
func (cb *Create[T]) execBatch(ctx context.Context, execer sqlx.ExtContext, records []*T) (int64, error) {
	// Check for  errors first
	if cb.err != nil {
		return 0, fmt.Errorf("create  has errors: %w", cb.err)
	}

	if len(records) == 0 {
		return 0, nil
	}

	// Render the query once
	result, err := cb.builder.Render(cb.soy.renderer())
	if err != nil {
		return 0, fmt.Errorf("failed to render INSERT query: %w", err)
	}

	// Emit query started event
	tableName := cb.soy.getTableName()
	capitan.Debug(ctx, QueryStarted,
		TableKey.Field(tableName),
		OperationKey.Field("INSERT_BATCH"),
		SQLKey.Field(result.SQL),
	)

	startTime := time.Now()

	var count int64
	for _, record := range records {
		// Execute each insert
		res, err := sqlx.NamedExecContext(ctx, execer, result.SQL, record)
		if err != nil {
			durationMs := time.Since(startTime).Milliseconds()
			capitan.Error(ctx, QueryFailed,
				TableKey.Field(tableName),
				OperationKey.Field("INSERT_BATCH"),
				DurationMsKey.Field(durationMs),
				ErrorKey.Field(err.Error()),
			)
			return count, fmt.Errorf("batch INSERT failed after %d records: %w", count, err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			durationMs := time.Since(startTime).Milliseconds()
			capitan.Error(ctx, QueryFailed,
				TableKey.Field(tableName),
				OperationKey.Field("INSERT_BATCH"),
				DurationMsKey.Field(durationMs),
				ErrorKey.Field(err.Error()),
			)
			return count, fmt.Errorf("failed to get rows affected: %w", err)
		}
		count += affected
	}

	// Emit query completed event
	durationMs := time.Since(startTime).Milliseconds()
	capitan.Info(ctx, QueryCompleted,
		TableKey.Field(tableName),
		OperationKey.Field("INSERT_BATCH"),
		DurationMsKey.Field(durationMs),
		RowsAffectedKey.Field(count),
	)

	return count, nil
}

// exec is the internal execution method used by both Exec and ExecTx.
func (cb *Create[T]) exec(ctx context.Context, execer sqlx.ExtContext, record *T) (*T, error) {
	// Check for errors first
	if cb.err != nil {
		return nil, fmt.Errorf("create builder has errors: %w", cb.err)
	}

	// Check if we need fallback upsert (MSSQL doesn't support ON CONFLICT)
	caps := cb.soy.renderer().Capabilities()
	if cb.hasConflict && !caps.Upsert {
		return cb.execUpdateThenInsert(ctx, execer, record)
	}

	// Standard path: render and execute (works for postgres, sqlite, mariadb)
	return cb.execWithUpsert(ctx, execer, record)
}

// execWithUpsert executes INSERT with ON CONFLICT support (PostgreSQL, SQLite, MariaDB).
func (cb *Create[T]) execWithUpsert(ctx context.Context, execer sqlx.ExtContext, record *T) (*T, error) {
	// Render the query
	result, err := cb.builder.Render(cb.soy.renderer())
	if err != nil {
		return nil, fmt.Errorf("failed to render INSERT query: %w", err)
	}

	// Emit query started event
	tableName := cb.soy.getTableName()
	capitan.Debug(ctx, QueryStarted,
		TableKey.Field(tableName),
		OperationKey.Field("INSERT"),
		SQLKey.Field(result.SQL),
	)

	startTime := time.Now()

	// Execute named query with RETURNING
	rows, err := sqlx.NamedQueryContext(ctx, execer, result.SQL, record)
	if err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field("INSERT"),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return nil, fmt.Errorf("INSERT failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

	// Scan returned row into a new record
	if !rows.Next() {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field("INSERT"),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field("INSERT returned no rows"),
		)
		return nil, fmt.Errorf("INSERT returned no rows")
	}

	var inserted T
	if err := rows.StructScan(&inserted); err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field("INSERT"),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return nil, fmt.Errorf("failed to scan INSERT result: %w", err)
	}

	// Emit query completed event
	durationMs := time.Since(startTime).Milliseconds()
	capitan.Info(ctx, QueryCompleted,
		TableKey.Field(tableName),
		OperationKey.Field("INSERT"),
		DurationMsKey.Field(durationMs),
		RowsAffectedKey.Field(1),
	)

	return &inserted, nil
}

// execUpdateThenInsert is the fallback upsert for dialects without ON CONFLICT (MSSQL).
// It tries UPDATE first, then INSERT if no rows were affected.
func (cb *Create[T]) execUpdateThenInsert(ctx context.Context, execer sqlx.ExtContext, record *T) (*T, error) {
	tableName := cb.soy.getTableName()
	startTime := time.Now()
	instance := cb.soy.getInstance()
	metadata := cb.soy.getMetadata()

	t, err := instance.TryT(tableName)
	if err != nil {
		return nil, fmt.Errorf("invalid table %q: %w", tableName, err)
	}

	// Build UPDATE query with SET and WHERE clauses
	updateBuilder := astql.Update(t)
	for field, param := range cb.updateFields {
		f, err := instance.TryF(field)
		if err != nil {
			return nil, fmt.Errorf("invalid field %q: %w", field, err)
		}
		p, err := instance.TryP(param)
		if err != nil {
			return nil, fmt.Errorf("invalid param %q: %w", param, err)
		}
		updateBuilder = updateBuilder.Set(f, p)
	}
	for _, col := range cb.conflictColumns {
		f, err := instance.TryF(col)
		if err != nil {
			return nil, fmt.Errorf("invalid conflict column %q: %w", col, err)
		}
		p, err := instance.TryP(col)
		if err != nil {
			return nil, fmt.Errorf("invalid conflict param %q: %w", col, err)
		}
		cond, err := instance.TryC(f, astql.EQ, p)
		if err != nil {
			return nil, fmt.Errorf("invalid condition: %w", err)
		}
		updateBuilder = updateBuilder.Where(cond)
	}

	// Try UPDATE first
	result, err := updateBuilder.Render(cb.soy.renderer())
	if err != nil {
		return nil, fmt.Errorf("failed to render UPDATE query: %w", err)
	}

	capitan.Debug(ctx, QueryStarted,
		TableKey.Field(tableName),
		OperationKey.Field("UPSERT_UPDATE"),
		SQLKey.Field(result.SQL),
	)

	res, err := sqlx.NamedExecContext(ctx, execer, result.SQL, record)
	if err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field("UPSERT_UPDATE"),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return nil, fmt.Errorf("UPDATE failed: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("failed to get rows affected: %w", err)
	}

	// If UPDATE affected rows, SELECT and return the updated record
	if rowsAffected > 0 {
		capitan.Debug(ctx, QueryCompleted,
			TableKey.Field(tableName),
			OperationKey.Field("UPSERT_UPDATE"),
			RowsAffectedKey.Field(rowsAffected),
		)
		return cb.selectByConflictColumns(ctx, execer, record)
	}

	// No rows affected, do INSERT
	insertBuilder := astql.Insert(t)
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
		f, err := instance.TryF(dbCol)
		if err != nil {
			return nil, fmt.Errorf("invalid field %q: %w", dbCol, err)
		}
		p, err := instance.TryP(dbCol)
		if err != nil {
			return nil, fmt.Errorf("invalid param %q: %w", dbCol, err)
		}
		values[f] = p
	}
	insertBuilder = insertBuilder.Values(values)

	// Add RETURNING for all columns
	for _, field := range metadata.Fields {
		dbCol := field.Tags["db"]
		if dbCol == "" || dbCol == "-" {
			continue
		}
		f, err := instance.TryF(dbCol)
		if err != nil {
			return nil, fmt.Errorf("invalid field %q: %w", dbCol, err)
		}
		insertBuilder = insertBuilder.Returning(f)
	}

	insertResult, err := insertBuilder.Render(cb.soy.renderer())
	if err != nil {
		return nil, fmt.Errorf("failed to render INSERT query: %w", err)
	}

	capitan.Debug(ctx, QueryStarted,
		TableKey.Field(tableName),
		OperationKey.Field("UPSERT_INSERT"),
		SQLKey.Field(insertResult.SQL),
	)

	rows, err := sqlx.NamedQueryContext(ctx, execer, insertResult.SQL, record)
	if err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field("UPSERT_INSERT"),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return nil, fmt.Errorf("INSERT failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		return nil, fmt.Errorf("INSERT returned no rows")
	}

	var inserted T
	if err := rows.StructScan(&inserted); err != nil {
		return nil, fmt.Errorf("failed to scan INSERT result: %w", err)
	}

	durationMs := time.Since(startTime).Milliseconds()
	capitan.Info(ctx, QueryCompleted,
		TableKey.Field(tableName),
		OperationKey.Field("UPSERT"),
		DurationMsKey.Field(durationMs),
		RowsAffectedKey.Field(1),
	)

	return &inserted, nil
}

// selectByConflictColumns fetches the record by conflict column values.
func (cb *Create[T]) selectByConflictColumns(ctx context.Context, execer sqlx.ExtContext, record *T) (*T, error) {
	tableName := cb.soy.getTableName()
	instance := cb.soy.getInstance()

	t, err := instance.TryT(tableName)
	if err != nil {
		return nil, fmt.Errorf("invalid table %q: %w", tableName, err)
	}

	selectBuilder := astql.Select(t)
	for _, col := range cb.conflictColumns {
		f, err := instance.TryF(col)
		if err != nil {
			return nil, fmt.Errorf("invalid conflict column %q: %w", col, err)
		}
		p, err := instance.TryP(col)
		if err != nil {
			return nil, fmt.Errorf("invalid conflict param %q: %w", col, err)
		}
		cond, err := instance.TryC(f, astql.EQ, p)
		if err != nil {
			return nil, fmt.Errorf("invalid condition: %w", err)
		}
		selectBuilder = selectBuilder.Where(cond)
	}

	result, err := selectBuilder.Render(cb.soy.renderer())
	if err != nil {
		return nil, fmt.Errorf("failed to render SELECT query: %w", err)
	}

	rows, err := sqlx.NamedQueryContext(ctx, execer, result.SQL, record)
	if err != nil {
		return nil, fmt.Errorf("SELECT failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		return nil, fmt.Errorf("SELECT returned no rows")
	}

	var selected T
	if err := rows.StructScan(&selected); err != nil {
		return nil, fmt.Errorf("failed to scan SELECT result: %w", err)
	}

	return &selected, nil
}

// Render builds and renders the query to SQL with parameter placeholders.
// Returns the SQL string and list of required parameters.
// Useful for inspection/debugging before execution.
func (cb *Create[T]) Render() (*astql.QueryResult, error) {
	// Check for  errors first
	if cb.err != nil {
		return nil, fmt.Errorf("create  has errors: %w", cb.err)
	}

	result, err := cb.builder.Render(cb.soy.renderer())
	if err != nil {
		return nil, fmt.Errorf("failed to render INSERT query: %w", err)
	}
	return result, nil
}

// MustRender is like Render but panics on error.
// This is intentionally preserved for cases where panicking is desired (e.g., tests, initialization).
func (cb *Create[T]) MustRender() *astql.QueryResult {
	result, err := cb.Render()
	if err != nil {
		panic(err)
	}
	return result
}

// Instance returns the underlying ASTQL instance for advanced query building.
// Use this escape hatch when you need ASTQL features not exposed by Create.
func (cb *Create[T]) Instance() *astql.ASTQL {
	return cb.instance
}

// Conflict handles ON CONFLICT resolution strategies.
type Conflict[T any] struct {
	create        *Create[T]
	astqlConflict *astql.ConflictBuilder
}

// DoNothing sets the conflict resolution to DO NOTHING.
// If a conflict occurs, the INSERT is silently skipped.
//
// Example:
//
//	soy.Insert().
//	    OnConflict("email").
//	    DoNothing().
//	    Exec(ctx, &user)
func (cfb *Conflict[T]) DoNothing() *Create[T] {
	cfb.create.builder = cfb.astqlConflict.DoNothing()
	return cfb.create
}

// DoUpdate starts building a DO UPDATE SET clause.
// Use Set() to specify which fields to update on conflict.
//
// Example:
//
//	soy.Insert().
//	    OnConflict("email").
//	    DoUpdate().
//	    Set("name", "updated_name").
//	    Exec(ctx, &user)
func (cfb *Conflict[T]) DoUpdate() *ConflictUpdate[T] {
	astqlUpdate := cfb.astqlConflict.DoUpdate()

	return &ConflictUpdate[T]{
		create:      cfb.create,
		astqlUpdate: astqlUpdate,
	}
}

// ConflictUpdate handles DO UPDATE SET clauses for ON CONFLICT.
type ConflictUpdate[T any] struct {
	create      *Create[T]
	astqlUpdate *astql.UpdateBuilder
}

// Set adds a field to update on conflict.
// The value comes from the parameter with the same name.
//
// Example:
//
//	DoUpdate().Set("name", "updated_name").Set("age", "new_age")
func (cub *ConflictUpdate[T]) Set(field, param string) *ConflictUpdate[T] {
	if cub.create.err != nil {
		return cub
	}

	// Track for fallback upsert
	cub.create.updateFields[field] = param

	f, err := cub.create.instance.TryF(field)
	if err != nil {
		cub.create.err = fmt.Errorf("invalid field %q: %w", field, err)
		return cub
	}

	p, err := cub.create.instance.TryP(param)
	if err != nil {
		cub.create.err = fmt.Errorf("invalid param %q: %w", param, err)
		return cub
	}

	cub.astqlUpdate = cub.astqlUpdate.Set(f, p)
	return cub
}

// Build finalizes the conflict update and returns the Create for execution.
func (cub *ConflictUpdate[T]) Build() *Create[T] {
	cub.create.builder = cub.astqlUpdate.Build()
	return cub.create
}

// Exec is a convenience method that calls Build() then Exec() on the Create.
func (cub *ConflictUpdate[T]) Exec(ctx context.Context, record *T) (*T, error) {
	return cub.Build().Exec(ctx, record)
}
