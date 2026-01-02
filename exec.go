package soy

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/atom"
	"github.com/zoobzio/capitan"
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
		return nil, fmt.Errorf("%s query failed: %w", operation, err)
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
			return nil, fmt.Errorf("failed to scan row: %w", err)
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
		return nil, fmt.Errorf("error iterating rows: %w", err)
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
	scanner *atom.Scanner,
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
		return nil, fmt.Errorf("%s query failed: %w", operation, err)
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
		return nil, fmt.Errorf("no rows found")
	}

	// Scan directly into atom
	result, err := scanner.Scan(rows)
	if err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field(operation),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return nil, fmt.Errorf("failed to scan row to atom: %w", err)
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
		return nil, fmt.Errorf("expected exactly one row, found multiple")
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
	scanner *atom.Scanner,
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
		return nil, fmt.Errorf("%s query failed: %w", operation, err)
	}
	defer func() { _ = rows.Close() }()

	// Scan all rows directly into atoms
	atoms, err := scanner.ScanAll(rows, rows.Next)
	if err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field(operation),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return nil, fmt.Errorf("failed to scan rows to atoms: %w", err)
	}

	if err := rows.Err(); err != nil {
		durationMs := time.Since(startTime).Milliseconds()
		capitan.Error(ctx, QueryFailed,
			TableKey.Field(tableName),
			OperationKey.Field(operation),
			DurationMsKey.Field(durationMs),
			ErrorKey.Field(err.Error()),
		)
		return nil, fmt.Errorf("error iterating rows: %w", err)
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
