package cereal

import (
	"bytes"
	"encoding/gob"
	"encoding/json"

	"github.com/pelletier/go-toml/v2"
	"github.com/vmihailenco/msgpack/v5"
	"gopkg.in/yaml.v3"
)

// Codec defines the interface for data serialization/deserialization.
type Codec interface {
	Encode(v any) ([]byte, error)
	Decode(data []byte, v any) error
	Name() string
}

// JSONCodec implements Codec using standard library JSON.
type JSONCodec struct{}

func (*JSONCodec) Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (*JSONCodec) Decode(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func (*JSONCodec) Name() string {
	return "json"
}

// GOBCodec implements Codec using Go's gob encoding.
type GOBCodec struct{}

func (*GOBCodec) Encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (*GOBCodec) Decode(data []byte, v any) error {
	buf := bytes.NewReader(data)
	dec := gob.NewDecoder(buf)
	return dec.Decode(v)
}

func (*GOBCodec) Name() string {
	return "gob"
}

// MsgpackCodec implements Codec using MessagePack (efficient binary format).
type MsgpackCodec struct{}

func (*MsgpackCodec) Encode(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

func (*MsgpackCodec) Decode(data []byte, v any) error {
	return msgpack.Unmarshal(data, v)
}

func (*MsgpackCodec) Name() string {
	return "msgpack"
}

// YAMLCodec implements Codec using YAML.
type YAMLCodec struct{}

func (*YAMLCodec) Encode(v any) ([]byte, error) {
	return yaml.Marshal(v)
}

func (*YAMLCodec) Decode(data []byte, v any) error {
	return yaml.Unmarshal(data, v)
}

func (*YAMLCodec) Name() string {
	return "yaml"
}

// TOMLCodec implements Codec using TOML.
type TOMLCodec struct{}

func (*TOMLCodec) Encode(v any) ([]byte, error) {
	return toml.Marshal(v)
}

func (*TOMLCodec) Decode(data []byte, v any) error {
	return toml.Unmarshal(data, v)
}

func (*TOMLCodec) Name() string {
	return "toml"
}

// Registry of available codecs.
var codecRegistry = map[string]Codec{
	"json":    &JSONCodec{},
	"gob":     &GOBCodec{},
	"msgpack": &MsgpackCodec{},
	"yaml":    &YAMLCodec{},
	"toml":    &TOMLCodec{},
}

// RegisterCodec adds a new codec to the registry.
func RegisterCodec(name string, codec Codec) {
	codecRegistry[name] = codec
}

// GetCodec retrieves a codec by name.
func GetCodec(name string) (Codec, bool) {
	codec, ok := codecRegistry[name]
	return codec, ok
}

// DefaultCodecs returns the default codec fallback order.
func DefaultCodecs() []string {
	return []string{"json"}
}
