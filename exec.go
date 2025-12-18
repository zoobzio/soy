package cereal

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
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
