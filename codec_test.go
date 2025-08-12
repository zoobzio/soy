package cereal

import (
	"encoding/json"
	"testing"
)

type testStruct struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func TestJSONCodec(t *testing.T) {
	codec := JSONCodec

	t.Run("Marshal", func(t *testing.T) {
		input := testStruct{
			ID:   "123",
			Name: "John",
			Age:  30,
		}

		data, err := codec.Marshal(input)
		if err != nil {
			t.Fatalf("Marshal failed: %v", err)
		}

		// Verify it's valid JSON
		var check map[string]interface{}
		if err := json.Unmarshal(data, &check); err != nil {
			t.Errorf("Marshal produced invalid JSON: %v", err)
		}

		if check["id"] != "123" {
			t.Errorf("Expected id=123, got %v", check["id"])
		}
	})

	t.Run("Unmarshal", func(t *testing.T) {
		data := []byte(`{"id":"456","name":"Jane","age":25}`)

		var result testStruct
		err := codec.Unmarshal(data, &result)
		if err != nil {
			t.Fatalf("Unmarshal failed: %v", err)
		}

		if result.ID != "456" {
			t.Errorf("Expected ID=456, got %s", result.ID)
		}
		if result.Name != "Jane" {
			t.Errorf("Expected Name=Jane, got %s", result.Name)
		}
		if result.Age != 25 {
			t.Errorf("Expected Age=25, got %d", result.Age)
		}
	})

	t.Run("Unmarshal array", func(t *testing.T) {
		data := []byte(`[{"id":"1","name":"A"},{"id":"2","name":"B"}]`)

		var results []testStruct
		err := codec.Unmarshal(data, &results)
		if err != nil {
			t.Fatalf("Unmarshal array failed: %v", err)
		}

		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}
	})
}

func TestMsgPackCodec(t *testing.T) {
	codec := MsgPackCodec

	t.Run("Marshal and Unmarshal", func(t *testing.T) {
		input := testStruct{
			ID:   "789",
			Name: "Bob",
			Age:  40,
		}

		data, err := codec.Marshal(input)
		if err != nil {
			t.Fatalf("Marshal failed: %v", err)
		}

		var result testStruct
		err = codec.Unmarshal(data, &result)
		if err != nil {
			t.Fatalf("Unmarshal failed: %v", err)
		}

		if result != input {
			t.Errorf("Round trip failed: got %+v, want %+v", result, input)
		}
	})
}

func TestTOMLCodec(t *testing.T) {
	codec := TOMLCodec

	t.Run("Marshal and Unmarshal", func(t *testing.T) {
		input := testStruct{
			ID:   "999",
			Name: "Alice",
			Age:  35,
		}

		data, err := codec.Marshal(input)
		if err != nil {
			t.Fatalf("Marshal failed: %v", err)
		}

		var result testStruct
		err = codec.Unmarshal(data, &result)
		if err != nil {
			t.Fatalf("Unmarshal failed: %v", err)
		}

		if result != input {
			t.Errorf("Round trip failed: got %+v, want %+v", result, input)
		}
	})
}
