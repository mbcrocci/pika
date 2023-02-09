package pika

type Message struct {
	protocol Protocol
	body        []byte

	// TODO add tags
}

func (m Message) Bind(v any) error {
	return m.protocol.Unmarshal(m.body, v)
}
