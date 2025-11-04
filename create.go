package cereal

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql"
	"github.com/zoobzio/capitan"
)

// Create provides a focused API for building INSERT queries.
// It wraps ASTQL's INSERT functionality with a simple string-based interface.
// Use this for inserting new records into the database.
type Create[T any] struct {
	instance *astql.ASTQL
	builder  *astql.Builder
	cereal   cerealExecutor // interface for execution
	err      error          // stores first error encountered during building
}

// OnConflict adds an ON CONFLICT clause for handling unique constraint violations.
// Specify the column(s) that might conflict.
//
// Example:
//
//	cereal.Insert().
//	    OnConflict("email").
//	    DoNothing().
//	    Exec(ctx, &user)
func (cb *Create[T]) OnConflict(columns ...string) *Conflict[T] {
	if cb.err != nil {
		return &Conflict[T]{
			create: cb,
		}
	}

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
//	inserted, err := cereal.Insert().Exec(ctx, user)
func (cb *Create[T]) Exec(ctx context.Context, record *T) (*T, error) {
	return cb.exec(ctx, cb.cereal.execer(), record)
}

// ExecTx executes the INSERT query within a transaction.
func (cb *Create[T]) ExecTx(ctx context.Context, tx *sqlx.Tx, record *T) (*T, error) {
	return cb.exec(ctx, tx, record)
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
//	count, err := cereal.Insert().ExecBatch(ctx, users)
func (cb *Create[T]) ExecBatch(ctx context.Context, records []*T) (int64, error) {
	return cb.execBatch(ctx, cb.cereal.execer(), records)
}

// ExecBatchTx executes the INSERT query for multiple records within a transaction.
// Returns the number of records inserted.
//
// Example:
//
//	tx, _ := db.BeginTxx(ctx, nil)
//	defer tx.Rollback()
//	count, err := cereal.Insert().ExecBatchTx(ctx, tx, users)
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
	result, err := cb.builder.Render()
	if err != nil {
		return 0, fmt.Errorf("failed to render INSERT query: %w", err)
	}

	// Emit query started event
	tableName := cb.cereal.getTableName()
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
	// Check for  errors first
	if cb.err != nil {
		return nil, fmt.Errorf("create  has errors: %w", cb.err)
	}

	// Render the query
	result, err := cb.builder.Render()
	if err != nil {
		return nil, fmt.Errorf("failed to render INSERT query: %w", err)
	}

	// Emit query started event
	tableName := cb.cereal.getTableName()
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
	defer rows.Close()

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

// Render builds and renders the query to SQL with parameter placeholders.
// Returns the SQL string and list of required parameters.
// Useful for inspection/debugging before execution.
func (cb *Create[T]) Render() (*astql.QueryResult, error) {
	// Check for  errors first
	if cb.err != nil {
		return nil, fmt.Errorf("create  has errors: %w", cb.err)
	}

	result, err := cb.builder.Render()
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
//	cereal.Insert().
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
//	cereal.Insert().
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
