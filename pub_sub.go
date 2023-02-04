package pika

import (
	"context"
	"errors"
	"fmt"
)

type pubSub struct {
	logger Logger
	topics map[string][]Consumer
}

func NewPubSub() Connector {
	return &pubSub{
		logger: &nullLogger{},
		topics: make(map[string][]Consumer),
	}
}

func (c *pubSub) WithLogger(l Logger) Connector {
	c.logger = l
	return c
}

func (c *pubSub) WithProtocol(p Protocol) Connector {
	return c
}

func (c *pubSub) WithConsumers(n int) Connector {
	return c
}

func (c *pubSub) WithPublishers(n int) Connector {
	return c
}

func (p *pubSub) Connect(string) error {
	p.logger.Info("PubSub connected")
	return nil
}

func (p *pubSub) Disconnect() error {
	p.logger.Info("PubSub disconnected")
	return nil
}

func (p *pubSub) Consume(c Consumer, o ConsumerOptions) error {
	k := p.key(o.Exchange, o.Topic)
	p.subscribe(k, c)
	return nil
}

func (p *pubSub) Publish(data any, o PublishOptions) error {
	k := p.key(o.Exchange, o.Topic)
	return p.broadcast(k, data)
}

func (p *pubSub) key(exchange, topic string) string {
	return fmt.Sprintf("%s-%s", exchange, topic)
}

func (p *pubSub) broadcast(k string, data any) error {
	cs, ok := p.topics[k]
	if !ok {
		return errors.New("topic not found")
	}

	for _, c := range cs {
		c.HandleMessage(context.TODO(), data)
	}

	return nil
}

func (p *pubSub) subscribe(k string, c Consumer) {
	_, ok := p.topics[k]
	if !ok {
		p.topics[k] = make([]Consumer, 0)
	}

	p.topics[k] = append(p.topics[k], c)
}
