package pika

import (
	"encoding/json"

	"github.com/vmihailenco/msgpack/v5"
)

type Protocol interface {
	ContentType() string
	Marshal(any) ([]byte, error)
	Unmarshal([]byte, any) error
}

type JsonProtocol struct{}

func (p JsonProtocol) ContentType() string {
	return "application/json"
}

func (p JsonProtocol) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (p JsonProtocol) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

type MsgPackProtocol struct{}

func (p MsgPackProtocol) ContentType() string {
	return "application/x-msgpack"
}

func (p MsgPackProtocol) Marshal(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

func (p MsgPackProtocol) Unmarshal(data []byte, v any) error {
	return msgpack.Unmarshal(data, v)
}
