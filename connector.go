package pika

import (
	"errors"

	"github.com/cenkalti/backoff/v4"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sourcegraph/conc/pool"
)

type Connector interface {
	Connect(string) error
	Disconnect() error

	Consume(Consumer, ConsumerOptions) error
	Publish(any, PublisherOptions) error
}

type RabbitConnector struct {
	url      string
	conn     *amqp.Connection
	channels []*AMQPChannel
	logger   Logger
	pool     *pool.Pool
}

func NewConnector() Connector {
	return &RabbitConnector{
		logger:   &nullLogger{},
		channels: make([]*AMQPChannel, 0),
		pool:     pool.New().WithMaxGoroutines(10),
	}
}

func (c *RabbitConnector) Connect(url string) error {
	c.url = url
	return c.connect()
}

func (rc *RabbitConnector) connect() error {
	rc.logger.Info("connecting to RabbitMQ")

	conn, err := amqp.Dial(rc.url)
	if err != nil {
		rc.logger.Error("unable to connect")
		return err
	}

	rc.conn = conn

	for _, c := range rc.channels {
		c.connect()
	}

	rc.logger.Info("connected to RabbitMQ")
	rc.logger.Debug("connection string", rc.url)

	rc.pool.Go(rc.handleDisconnect)

	return nil
}

func (rc *RabbitConnector) handleDisconnect() {
	closeChan := rc.conn.NotifyClose(make(chan *amqp.Error, 1))

	e := <-closeChan
	if e != nil {
		rc.logger.Warn("RabbitMQ connection was closed: " + e.Error())

		rc.logger.Warn("closing all channels")
		for _, c := range rc.channels {
			c.Close()
		}

		backoff.Retry(rc.connect, backoff.NewExponentialBackOff())
	}
}

func (c *RabbitConnector) Disconnect() error {
	if c.conn == nil {
		return errors.New("Connection is nil")
	}

	c.logger.Info("disconnecting from RabbitMQ")
	return c.conn.Close()
}

func (c *RabbitConnector) createChannel() (*AMQPChannel, error) {
	channel, err := c.conn.Channel()
	if err != nil {
		return nil, err
	}

	ch, err := NewAMQPChannel(channel, c.logger)
	if err != nil {
		return nil, err
	}

	c.channels = append(c.channels, ch)

	return ch, nil
}