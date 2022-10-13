package pika

import (
	"errors"
	"fmt"
)

type PubSub struct {
	logger Logger
	topics map[string][]chan []byte
}

func NewPubSub() Connector {
	return &PubSub{
		topics: make(map[string][]chan []byte),
	}
}

func (p *PubSub) Connect(string) error {
	p.logger.Info("PubSub connected")
	return nil
}

func (p *PubSub) Disconnect() error {
	p.logger.Info("PubSub disconnected")
	return nil
}

func (p *PubSub) Channel() (Channel, error) {
	c := PubSubChannel{
		ps: p,
	}
	return &c, nil
}

func (p *PubSub) SetLogger(l Logger) { p.logger = l }
func (p *PubSub) Logger() Logger     { return p.logger }

func (p *PubSub) key(exchange, topic string) string {
	return fmt.Sprintf("%s-%s", exchange, topic)
}

func (p *PubSub) broadcast(k string, data []byte) error {
	cs, ok := p.topics[k]
	if !ok {
		return errors.New("topic not found")
	}

	for _, c := range cs {
		c <- data
	}

	return nil
}

func (p *PubSub) subscribe(k string) chan []byte {
	_, ok := p.topics[k]
	if !ok {
		p.topics[k] = make([]chan []byte, 0)
	}

	c := make(chan []byte)
	p.topics[k] = append(p.topics[k], c)

	return c
}

type PubSubChannel struct {
	ps *PubSub
}

func (p *PubSubChannel) Consume(o ConsumerOptions, out chan any) error {
	k := p.ps.key(o.Exchange, o.Topic)
	c := p.ps.subscribe(k)

	go func() {
		for msg := range c {
			out <- msg
		}
	}()

	return nil
}

func (p *PubSubChannel) Publish(o PublisherOptions, data []byte) error {
	k := p.ps.key(o.Exchange, o.Topic)
	return p.ps.broadcast(k, data)
}

func (p *PubSubChannel) Ack(uint64, bool)    {}
func (p *PubSubChannel) Reject(uint64, bool) {}

func (p *PubSubChannel) Logger() Logger { return p.ps.Logger() }
