package pika

import (
	"context"
	"errors"
	"fmt"
)

type PubSub struct {
	logger Logger
	topics map[string][]Consumer
}

func NewPubSub() Connector {
	return &PubSub{
		logger: &nullLogger{},
		topics: make(map[string][]Consumer),
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

func (p *PubSub) Consume(c Consumer, o ConsumerOptions) error {
	k := p.key(o.Exchange, o.Topic)
	p.subscribe(k, c)
	return nil
}

func (p *PubSub) Publish(data any, o PublisherOptions) error {
	k := p.key(o.Exchange, o.Topic)
	return p.broadcast(k, data)
}

func (p *PubSub) key(exchange, topic string) string {
	return fmt.Sprintf("%s-%s", exchange, topic)
}

func (p *PubSub) broadcast(k string, data any) error {
	cs, ok := p.topics[k]
	if !ok {
		return errors.New("topic not found")
	}

	for _, c := range cs {
		c.HandleMessage(context.TODO(), data)
	}

	return nil
}

func (p *PubSub) subscribe(k string, c Consumer) {
	_, ok := p.topics[k]
	if !ok {
		p.topics[k] = make([]Consumer, 0)
	}

	p.topics[k] = append(p.topics[k], c)
}
