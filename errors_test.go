package soy

import (
	"errors"
	"testing"
)

func TestValidationError(t *testing.T) {
	t.Run("Is matches by Kind", func(t *testing.T) {
		err := newFieldError("user_id", errors.New("not found in schema"))

		if !errors.Is(err, ErrInvalidField) {
			t.Error("expected newFieldError to match ErrInvalidField")
		}
		if errors.Is(err, ErrInvalidParam) {
			t.Error("expected newFieldError NOT to match ErrInvalidParam")
		}
	})

	t.Run("Is matches param errors", func(t *testing.T) {
		err := newParamError("limit", errors.New("not found"))

		if !errors.Is(err, ErrInvalidParam) {
			t.Error("expected newParamError to match ErrInvalidParam")
		}
		if errors.Is(err, ErrInvalidField) {
			t.Error("expected newParamError NOT to match ErrInvalidField")
		}
	})

	t.Run("Is matches operator errors", func(t *testing.T) {
		err := newOperatorError("INVALID")

		if !errors.Is(err, ErrInvalidOperator) {
			t.Error("expected newOperatorError to match ErrInvalidOperator")
		}
	})

	t.Run("Is matches direction errors", func(t *testing.T) {
		err := newDirectionError("sideways")

		if !errors.Is(err, ErrInvalidDirection) {
			t.Error("expected newDirectionError to match ErrInvalidDirection")
		}
	})

	t.Run("Is matches condition errors", func(t *testing.T) {
		err := newConditionError(errors.New("bad condition"))

		if !errors.Is(err, ErrInvalidCondition) {
			t.Error("expected newConditionError to match ErrInvalidCondition")
		}
	})

	t.Run("Unwrap returns underlying error", func(t *testing.T) {
		underlying := errors.New("schema error")
		err := newFieldError("email", underlying)

		if !errors.Is(err, underlying) {
			t.Error("expected Unwrap to expose underlying error")
		}
	})

	t.Run("As extracts ValidationError details", func(t *testing.T) {
		err := newFieldError("created_at", errors.New("not in schema"))

		var valErr *ValidationError
		if !errors.As(err, &valErr) {
			t.Fatal("expected errors.As to succeed")
		}
		if valErr.Kind != "field" {
			t.Errorf("Kind = %q, want %q", valErr.Kind, "field")
		}
		if valErr.Name != "created_at" {
			t.Errorf("Name = %q, want %q", valErr.Name, "created_at")
		}
	})

	t.Run("Error message formatting", func(t *testing.T) {
		err := newFieldError("status", errors.New("unknown"))
		msg := err.Error()
		if msg != `invalid field "status": unknown` {
			t.Errorf("Error() = %q, unexpected format", msg)
		}

		err2 := newOperatorError("~~")
		msg2 := err2.Error()
		if len(msg2) == 0 {
			t.Error("expected non-empty error message")
		}
	})
}

func TestQueryError(t *testing.T) {
	t.Run("Is matches by Phase", func(t *testing.T) {
		err := newQueryError("SELECT", errors.New("connection refused"))

		if !errors.Is(err, ErrQueryFailed) {
			t.Error("expected newQueryError to match ErrQueryFailed")
		}
		if errors.Is(err, ErrScanFailed) {
			t.Error("expected newQueryError NOT to match ErrScanFailed")
		}
	})

	t.Run("Is matches scan errors", func(t *testing.T) {
		err := newScanError("INSERT", errors.New("type mismatch"))

		if !errors.Is(err, ErrScanFailed) {
			t.Error("expected newScanError to match ErrScanFailed")
		}
	})

	t.Run("Is matches iteration errors", func(t *testing.T) {
		err := newIterationError(errors.New("cursor closed"))

		if !errors.Is(err, ErrIterationFailed) {
			t.Error("expected newIterationError to match ErrIterationFailed")
		}
	})

	t.Run("Is matches render errors", func(t *testing.T) {
		err := newRenderError("UPDATE", errors.New("invalid AST"))

		if !errors.Is(err, ErrRenderFailed) {
			t.Error("expected newRenderError to match ErrRenderFailed")
		}
	})

	t.Run("Unwrap returns underlying error", func(t *testing.T) {
		underlying := errors.New("db error")
		err := newQueryError("DELETE", underlying)

		if !errors.Is(err, underlying) {
			t.Error("expected Unwrap to expose underlying error")
		}
	})

	t.Run("As extracts QueryError details", func(t *testing.T) {
		err := newQueryError("SELECT", errors.New("timeout"))

		var qErr *QueryError
		if !errors.As(err, &qErr) {
			t.Fatal("expected errors.As to succeed")
		}
		if qErr.Operation != "SELECT" {
			t.Errorf("Operation = %q, want %q", qErr.Operation, "SELECT")
		}
		if qErr.Phase != "execution" {
			t.Errorf("Phase = %q, want %q", qErr.Phase, "execution")
		}
	})
}

func TestBuilderError(t *testing.T) {
	t.Run("Is matches BuilderError", func(t *testing.T) {
		err := newBuilderError("select", errors.New("invalid field"))

		if !errors.Is(err, ErrBuilderHasErrors) {
			t.Error("expected newBuilderError to match ErrBuilderHasErrors")
		}
	})

	t.Run("Unwrap returns underlying error", func(t *testing.T) {
		underlying := newFieldError("bad", errors.New("x"))
		err := newBuilderError("update", underlying)

		if !errors.Is(err, underlying) {
			t.Error("expected Unwrap to expose underlying error")
		}
		if !errors.Is(err, ErrInvalidField) {
			t.Error("expected to match ErrInvalidField through chain")
		}
	})

	t.Run("As extracts BuilderError details", func(t *testing.T) {
		err := newBuilderError("delete", errors.New("missing where"))

		var bErr *BuilderError
		if !errors.As(err, &bErr) {
			t.Fatal("expected errors.As to succeed")
		}
		if bErr.Builder != "delete" {
			t.Errorf("Builder = %q, want %q", bErr.Builder, "delete")
		}
	})
}

func TestUnsafeOperationError(t *testing.T) {
	t.Run("Is matches ErrUnsafeUpdate", func(t *testing.T) {
		err := &UnsafeOperationError{Operation: "UPDATE"}

		if !errors.Is(err, ErrUnsafeUpdate) {
			t.Error("expected UPDATE UnsafeOperationError to match ErrUnsafeUpdate")
		}
		if errors.Is(err, ErrUnsafeDelete) {
			t.Error("expected UPDATE NOT to match ErrUnsafeDelete")
		}
	})

	t.Run("Is matches ErrUnsafeDelete", func(t *testing.T) {
		err := &UnsafeOperationError{Operation: "DELETE"}

		if !errors.Is(err, ErrUnsafeDelete) {
			t.Error("expected DELETE UnsafeOperationError to match ErrUnsafeDelete")
		}
	})

	t.Run("Is matches generic ErrUnsafeOperation", func(t *testing.T) {
		err := &UnsafeOperationError{Operation: "UPDATE"}

		if !errors.Is(err, ErrUnsafeOperation) {
			t.Error("expected to match generic ErrUnsafeOperation")
		}
	})
}

func TestSimpleSentinelErrors(t *testing.T) {
	t.Run("ErrNotFound", func(t *testing.T) {
		if !errors.Is(ErrNotFound, ErrNotFound) {
			t.Error("ErrNotFound should match itself")
		}
	})

	t.Run("ErrMultipleRows", func(t *testing.T) {
		if !errors.Is(ErrMultipleRows, ErrMultipleRows) {
			t.Error("ErrMultipleRows should match itself")
		}
	})

	t.Run("ErrEmptyTableName", func(t *testing.T) {
		if !errors.Is(ErrEmptyTableName, ErrEmptyTableName) {
			t.Error("ErrEmptyTableName should match itself")
		}
	})

	t.Run("ErrNilRenderer", func(t *testing.T) {
		if !errors.Is(ErrNilRenderer, ErrNilRenderer) {
			t.Error("ErrNilRenderer should match itself")
		}
	})
}

func TestTableError(t *testing.T) {
	t.Run("Is matches ErrInvalidTable", func(t *testing.T) {
		err := newTableError("users", errors.New("not found"))

		if !errors.Is(err, ErrInvalidTable) {
			t.Error("expected newTableError to match ErrInvalidTable")
		}
		if errors.Is(err, ErrInvalidField) {
			t.Error("expected newTableError NOT to match ErrInvalidField")
		}
	})

	t.Run("Error message formatting", func(t *testing.T) {
		err := newTableError("orders", errors.New("schema mismatch"))
		msg := err.Error()
		if msg != `invalid table "orders": schema mismatch` {
			t.Errorf("Error() = %q, unexpected format", msg)
		}
	})
}

func TestQueryError_AllPhases(t *testing.T) {
	t.Run("render phase Error message", func(t *testing.T) {
		err := newRenderError("UPDATE", errors.New("invalid AST"))
		msg := err.Error()
		if msg != "failed to render UPDATE query: invalid AST" {
			t.Errorf("Error() = %q, unexpected format", msg)
		}
	})

	t.Run("execution phase Error message", func(t *testing.T) {
		err := newQueryError("SELECT", errors.New("connection refused"))
		msg := err.Error()
		if msg != "SELECT query failed: connection refused" {
			t.Errorf("Error() = %q, unexpected format", msg)
		}
	})

	t.Run("scan phase Error message", func(t *testing.T) {
		err := newScanError("INSERT", errors.New("type mismatch"))
		msg := err.Error()
		if msg != "failed to scan INSERT result: type mismatch" {
			t.Errorf("Error() = %q, unexpected format", msg)
		}
	})

	t.Run("iteration phase Error message", func(t *testing.T) {
		err := newIterationError(errors.New("cursor closed"))
		msg := err.Error()
		if msg != "error iterating rows: cursor closed" {
			t.Errorf("Error() = %q, unexpected format", msg)
		}
	})

	t.Run("unknown phase Error message", func(t *testing.T) {
		err := &QueryError{Operation: "MERGE", Phase: "unknown", Err: errors.New("test")}
		msg := err.Error()
		if msg != "MERGE failed: test" {
			t.Errorf("Error() = %q, unexpected format", msg)
		}
	})
}

func TestBuilderError_ErrorMessage(t *testing.T) {
	t.Run("Error message formatting", func(t *testing.T) {
		err := newBuilderError("select", errors.New("invalid field"))
		msg := err.Error()
		if msg != "select builder has errors: invalid field" {
			t.Errorf("Error() = %q, unexpected format", msg)
		}
	})
}

func TestUnsafeOperationError_ErrorMessage(t *testing.T) {
	t.Run("UPDATE Error message", func(t *testing.T) {
		err := &UnsafeOperationError{Operation: "UPDATE"}
		msg := err.Error()
		if msg != "UPDATE requires at least one WHERE condition to prevent accidental full-table operation" {
			t.Errorf("Error() = %q, unexpected format", msg)
		}
	})

	t.Run("DELETE Error message", func(t *testing.T) {
		err := &UnsafeOperationError{Operation: "DELETE"}
		msg := err.Error()
		if msg != "DELETE requires at least one WHERE condition to prevent accidental full-table operation" {
			t.Errorf("Error() = %q, unexpected format", msg)
		}
	})
}

func TestValidationError_MessageField(t *testing.T) {
	t.Run("Error uses Message when set", func(t *testing.T) {
		err := &ValidationError{
			Kind:    "field",
			Name:    "test",
			Message: "custom message",
		}
		if err.Error() != "custom message" {
			t.Errorf("Error() = %q, expected custom message", err.Error())
		}
	})

	t.Run("Error uses Kind and Name when no Message or Err", func(t *testing.T) {
		err := &ValidationError{
			Kind: "operator",
			Name: "~~",
		}
		if err.Error() != `invalid operator "~~"` {
			t.Errorf("Error() = %q, unexpected format", err.Error())
		}
	})
}

func TestNullsOrderingError(t *testing.T) {
	t.Run("Is matches ErrInvalidNullsOrdering", func(t *testing.T) {
		err := newNullsOrderingError("middle")

		if !errors.Is(err, ErrInvalidNullsOrdering) {
			t.Error("expected newNullsOrderingError to match ErrInvalidNullsOrdering")
		}
	})

	t.Run("Error message formatting", func(t *testing.T) {
		err := newNullsOrderingError("middle")
		msg := err.Error()
		if msg != `invalid nulls ordering "middle", must be 'first' or 'last'` {
			t.Errorf("Error() = %q, unexpected format", msg)
		}
	})
}

func TestAggregateFuncError(t *testing.T) {
	t.Run("Is matches ErrInvalidAggregateFunc", func(t *testing.T) {
		err := newAggregateFuncError("median")

		if !errors.Is(err, ErrInvalidAggregateFunc) {
			t.Error("expected newAggregateFuncError to match ErrInvalidAggregateFunc")
		}
	})

	t.Run("Error message formatting", func(t *testing.T) {
		err := newAggregateFuncError("median")
		msg := err.Error()
		if msg != `invalid aggregate function "median", must be one of: count, sum, avg, min, max, count_distinct` {
			t.Errorf("Error() = %q, unexpected format", msg)
		}
	})
}
