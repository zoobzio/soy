package soy

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/zoobzio/astql"
	"github.com/zoobzio/capitan"
)

// executeBatch is a shared helper for batch execution logic used by Update and Delete.
// It handles rendering, logging, execution, and error reporting for batch operations.
func executeBatch(
	ctx context.Context,
	execer sqlx.ExtContext,
	batchParams []map[string]any,
	builder *astql.Builder,
	renderer astql.Renderer,
	tableName string,
	operation string,
	hasWhere bool,
	builderErr error,
) (int64, error) {
	// Check for builder errors first
	if builderErr != nil {
		return 0, fmt.Errorf("%s builder has errors: %w", operation, builderErr)
	}

	// Safety check: require WHERE clause
	if !hasWhere {
		return 0, fmt.Errorf("%s requires at least one WHERE condition to prevent accidental full-table operation", operation)
	}

	if len(batchParams) == 0 {
		return 0, nil
	}

	// Render the query once
	result, err := builder.Render(renderer)
	if err != nil {
		return 0, fmt.Errorf("failed to render %s query: %w", operation, err)
	}

	// Emit query started event
	capitan.Debug(ctx, QueryStarted,
		TableKey.Field(tableName),
		OperationKey.Field(operation+"_BATCH"),
		SQLKey.Field(result.SQL),
	)

	startTime := time.Now()

	var totalAffected int64
	for i, params := range batchParams {
		res, err := sqlx.NamedExecContext(ctx, execer, result.SQL, params)
		if err != nil {
			durationMs := time.Since(startTime).Milliseconds()
			capitan.Error(ctx, QueryFailed,
				TableKey.Field(tableName),
				OperationKey.Field(operation+"_BATCH"),
				DurationMsKey.Field(durationMs),
				ErrorKey.Field(err.Error()),
			)
			return totalAffected, fmt.Errorf("batch %s failed at index %d after %d rows: %w", operation, i, totalAffected, err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			durationMs := time.Since(startTime).Milliseconds()
			capitan.Error(ctx, QueryFailed,
				TableKey.Field(tableName),
				OperationKey.Field(operation+"_BATCH"),
				DurationMsKey.Field(durationMs),
				ErrorKey.Field(err.Error()),
			)
			return totalAffected, fmt.Errorf("failed to get rows affected: %w", err)
		}
		totalAffected += affected
	}

	// Emit query completed event
	durationMs := time.Since(startTime).Milliseconds()
	capitan.Info(ctx, QueryCompleted,
		TableKey.Field(tableName),
		OperationKey.Field(operation+"_BATCH"),
		DurationMsKey.Field(durationMs),
		RowsAffectedKey.Field(totalAffected),
	)

	return totalAffected, nil
}
