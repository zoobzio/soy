package cereal

import (
	"errors"
	"testing"
)

func TestErrorConstants(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "ErrNotFound",
			err:  ErrNotFound,
			want: "resource not found",
		},
		{
			name: "ErrQueryNotFound",
			err:  ErrQueryNotFound,
			want: "query not found",
		},
		{
			name: "ErrQueryExists",
			err:  ErrQueryExists,
			want: "query already exists",
		},
		{
			name: "ErrBatchSizeMismatch",
			err:  ErrBatchSizeMismatch,
			want: "batch operation size mismatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err.Error() != tt.want {
				t.Errorf("Error message = %v, want %v", tt.err.Error(), tt.want)
			}
		})
	}
}

func TestErrorComparison(t *testing.T) {
	// Test that errors can be compared with errors.Is
	testErr := ErrNotFound

	if !errors.Is(testErr, ErrNotFound) {
		t.Error("Expected errors.Is to return true for same error")
	}

	if errors.Is(testErr, ErrQueryNotFound) {
		t.Error("Expected errors.Is to return false for different error")
	}

	// Test wrapping
	wrappedErr := errors.Join(ErrQueryExists, errors.New("additional context"))
	if !errors.Is(wrappedErr, ErrQueryExists) {
		t.Error("Expected errors.Is to work with wrapped errors")
	}
}

func TestErrorUniqueness(t *testing.T) {
	// Ensure all errors are unique
	allErrors := []error{
		ErrNotFound,
		ErrQueryNotFound,
		ErrQueryExists,
		ErrBatchSizeMismatch,
	}

	seen := make(map[error]bool)
	for _, err := range allErrors {
		if seen[err] {
			t.Errorf("Duplicate error found: %v", err)
		}
		seen[err] = true
	}
}
