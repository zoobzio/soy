package cereal

import (
	"encoding/json"

	"github.com/pelletier/go-toml/v2"
	"github.com/vmihailenco/msgpack/v5"
)

// Codec defines the serialization interface for converting between Go types and byte slices.
type Codec interface {
	Marshal(v interface{}) ([]byte, error)
	Unmarshal(data []byte, v interface{}) error
}

// jsonCodec implements Codec using standard JSON encoding.
type jsonCodec struct{}

func (jsonCodec) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonCodec) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// msgpackCodec implements Codec using MessagePack encoding.
type msgpackCodec struct{}

func (msgpackCodec) Marshal(v interface{}) ([]byte, error) {
	return msgpack.Marshal(v)
}

func (msgpackCodec) Unmarshal(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}

// tomlCodec implements Codec using TOML encoding.
type tomlCodec struct{}

func (tomlCodec) Marshal(v interface{}) ([]byte, error) {
	return toml.Marshal(v)
}

func (tomlCodec) Unmarshal(data []byte, v interface{}) error {
	return toml.Unmarshal(data, v)
}

// Built-in codec instances.
var (
	JSONCodec    Codec = jsonCodec{}
	MsgPackCodec Codec = msgpackCodec{}
	TOMLCodec    Codec = tomlCodec{}
)
