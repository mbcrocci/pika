package pika

import "encoding/json"

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

// TODO ProtocolBuffers
