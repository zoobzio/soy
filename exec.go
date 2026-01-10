package soy

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/atom"
	"github.com/zoobzio/capitan"
	"github.com/zoobzio/soy/internal/scanner"
)

// execMultipleRows is a shared helper for executing queries that return multiple rows.
// It handles logging, execution, scanning, and error handling in a consistent way.
func execMultipleRows[T any](
	ctx context.Context,
	execer sqlx.ExtContext,
	sql string,
	params map[string]any,
	tableName string,
	operation string,
) ([]*T, error) {
	capitan.Debug(ctx, QueryStarted,
		TableKey.Field(tableName),
		OperationKey.Field(operation),
		SQLKey.Field(sql),
	)

	startTime := time.Now()

	rows, err := sqlx.NamedQueryContext(ctx, execer, sql, params)
	if err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field(operation),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return nil, newQueryError(operation, err)
	}
	defer func() { _ = rows.Close() }()

	var records []*T
	for rows.Next() {
		var record T
		if err := rows.StructScan(&record); err != nil {
			durationMs := time.Since(startTime).Milliseconds()
			capitan.Error(ctx, QueryFailed,
				TableKey.Field(tableName),
				OperationKey.Field(operation),
				DurationMsKey.Field(durationMs),
				ErrorKey.Field(err.Error()),
			)
			return nil, newScanError(operation, err)
		}
		records = append(records, &record)
	}

	if err := rows.Err(); err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field(operation),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return nil, newIterationError(err)
	}

	durationMs := time.Since(startTime).Milliseconds()
	capitan.Info(ctx, QueryCompleted,
		TableKey.Field(tableName),
		OperationKey.Field(operation),
		DurationMsKey.Field(durationMs),
		RowsReturnedKey.Field(len(records)),
	)

	return records, nil
}

// execAtomSingleRow executes a query and scans the single result directly into an Atom.
// Returns an error if zero rows or more than one row is found.
func execAtomSingleRow(
	ctx context.Context,
	execer sqlx.ExtContext,
	sc *scanner.Scanner,
	sql string,
	params map[string]any,
	tableName string,
	operation string,
) (*atom.Atom, error) {
	capitan.Debug(ctx, QueryStarted,
		TableKey.Field(tableName),
		OperationKey.Field(operation),
		SQLKey.Field(sql),
	)

	startTime := time.Now()

	rows, err := sqlx.NamedQueryContext(ctx, execer, sql, params)
	if err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field(operation),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return nil, newQueryError(operation, err)
	}
	defer func() { _ = rows.Close() }()

	// Check for exactly one row
	if !rows.Next() {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field(operation),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field("no rows found"),
		)
		return nil, ErrNotFound
	}

	// Scan directly into atom
	result, err := sc.Scan(rows)
	if err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field(operation),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return nil, newScanError(operation, err)
	}

	// Ensure no additional rows
	if rows.Next() {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field(operation),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field("expected exactly one row, found multiple"),
		)
		return nil, ErrMultipleRows
	}

	durationMs := time.Since(startTime).Milliseconds()
	capitan.Info(ctx, QueryCompleted,
		TableKey.Field(tableName),
		OperationKey.Field(operation),
		DurationMsKey.Field(durationMs),
		RowsReturnedKey.Field(1),
	)

	return result, nil
}

// execAtomMultipleRows executes a query and scans all results directly into Atoms.
func execAtomMultipleRows(
	ctx context.Context,
	execer sqlx.ExtContext,
	sc *scanner.Scanner,
	sql string,
	params map[string]any,
	tableName string,
	operation string,
) ([]*atom.Atom, error) {
	capitan.Debug(ctx, QueryStarted,
		TableKey.Field(tableName),
		OperationKey.Field(operation),
		SQLKey.Field(sql),
	)

	startTime := time.Now()

	rows, err := sqlx.NamedQueryContext(ctx, execer, sql, params)
	if err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field(operation),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return nil, newQueryError(operation, err)
	}
	defer func() { _ = rows.Close() }()

	// Scan all rows directly into atoms
	atoms, err := sc.ScanAll(rows, rows.Next)
	if err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field(operation),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return nil, newScanError(operation, err)
	}

	if err := rows.Err(); err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field(operation),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return nil, newIterationError(err)
	}

	durationMs := time.Since(startTime).Milliseconds()
	capitan.Info(ctx, QueryCompleted,
		TableKey.Field(tableName),
		OperationKey.Field(operation),
		DurationMsKey.Field(durationMs),
		RowsReturnedKey.Field(len(atoms)),
	)

	return atoms, nil
}
