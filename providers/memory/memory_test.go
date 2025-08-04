package memory_test

import (
	"testing"

	"github.com/zoobzio/cereal"
	"github.com/zoobzio/cereal/providers/memory"
)

func TestMemoryProvider(t *testing.T) {
	provider := memory.New()
	cereal.TestProvider(t, "memory", provider)
}
