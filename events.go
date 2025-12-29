package soy

import "github.com/zoobzio/capitan"

// Query execution signals.
var (
	// QueryStarted is emitted when a database query begins execution.
	// Fields: TableKey, OperationKey, SQLKey.
	QueryStarted = capitan.NewSignal("db.query.started", "Database query execution started")

	// QueryCompleted is emitted when a query completes successfully.
	// Fields: TableKey, OperationKey, DurationMsKey, RowsAffectedKey or RowsReturnedKey.
	QueryCompleted = capitan.NewSignal("db.query.completed", "Database query completed successfully")

	// QueryFailed is emitted when a query fails with an error.
	// Fields: TableKey, OperationKey, DurationMsKey, ErrorKey.
	QueryFailed = capitan.NewSignal("db.query.failed", "Database query failed with error")
)

// Event field keys for query operations.
var (
	// TableKey identifies the database table being operated on.
	TableKey = capitan.NewStringKey("table")

	// OperationKey identifies the type of database operation (SELECT, INSERT, UPDATE, DELETE, COUNT, etc).
	OperationKey = capitan.NewStringKey("operation")

	// SQLKey contains the rendered SQL query string.
	SQLKey = capitan.NewStringKey("sql")

	// DurationMsKey contains the query execution duration in milliseconds.
	DurationMsKey = capitan.NewInt64Key("duration_ms")

	// RowsAffectedKey contains the number of rows affected by INSERT/UPDATE/DELETE operations.
	RowsAffectedKey = capitan.NewInt64Key("rows_affected")

	// RowsReturnedKey contains the number of rows returned by SELECT operations.
	RowsReturnedKey = capitan.NewIntKey("rows_returned")

	// ErrorKey contains the error message when a query fails.
	ErrorKey = capitan.NewStringKey("error")

	// FieldKey identifies the field being aggregated (for SUM, AVG, MIN, MAX operations).
	FieldKey = capitan.NewStringKey("field")

	// ResultValueKey contains the result value for COUNT and aggregate operations.
	ResultValueKey = capitan.NewFloat64Key("result_value")
)
