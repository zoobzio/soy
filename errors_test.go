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
