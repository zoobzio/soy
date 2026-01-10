package soy

import "testing"

// TestSignalsInitialized verifies that all query signals are properly initialized.
func TestSignalsInitialized(t *testing.T) {
	signals := []struct {
		name   string
		signal any
	}{
		{"QueryStarted", QueryStarted},
		{"QueryCompleted", QueryCompleted},
		{"QueryFailed", QueryFailed},
	}

	for _, s := range signals {
		if s.signal == nil {
			t.Errorf("%s signal is nil", s.name)
		}
	}
}

// TestEventKeysInitialized verifies that all event keys are properly initialized.
func TestEventKeysInitialized(t *testing.T) {
	keys := []struct {
		name string
		key  any
	}{
		{"TableKey", TableKey},
		{"OperationKey", OperationKey},
		{"SQLKey", SQLKey},
		{"DurationMsKey", DurationMsKey},
		{"RowsAffectedKey", RowsAffectedKey},
		{"RowsReturnedKey", RowsReturnedKey},
		{"ErrorKey", ErrorKey},
		{"FieldKey", FieldKey},
		{"ResultValueKey", ResultValueKey},
	}

	for _, k := range keys {
		if k.key == nil {
			t.Errorf("%s is nil", k.name)
		}
	}
}
