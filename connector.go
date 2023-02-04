package pika

import (
	"github.com/cenkalti/backoff/v4"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sourcegraph/conc"
	"github.com/sourcegraph/conc/pool"
)

type Connector interface {
	Connect(string) error
	Disconnect() error

	Consume(Consumer, ConsumerOptions) error
	Publish(any, PublisherOptions) error

	WithLogger(Logger) Connector
	WithProtocol(Protocol) Connector
	WithConsumers(int) Connector
}

type RabbitConnector struct {
	url      string
	logger   Logger
	protocol Protocol
	conn     *amqp.Connection

	conChannels []*AMQPChannel
	conPool     *pool.Pool
	pubChannels map[uint64]*AMQPChannel
	waitGroup   *conc.WaitGroup
}

func NewConnector() Connector {
	return &RabbitConnector{
		logger:      &nullLogger{},
		protocol:    JsonProtocol{},
		conPool:     pool.New(),
		pubChannels: make(map[uint64]*AMQPChannel),
		waitGroup:   conc.NewWaitGroup(),
	}
}

func (c *RabbitConnector) WithLogger(l Logger) Connector {
	c.logger = l
	return c
}

func (c *RabbitConnector) WithProtocol(p Protocol) Connector {
	c.protocol = p
	return c
}

func (c *RabbitConnector) WithConsumers(n int) Connector {
	c.conChannels = make([]*AMQPChannel, 0)
	c.conPool = pool.New().WithMaxGoroutines(n)
	return c
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

	rc.connectConsumers()
	rc.connectPublishers()

	rc.waitGroup.Go(rc.handleDisconnect)

	rc.logger.Info("connected to RabbitMQ")
	rc.logger.Debug("connection string", rc.url)
	return nil
}

func (rc *RabbitConnector) handleDisconnect() {
	closeChan := rc.conn.NotifyClose(make(chan *amqp.Error, 1))

	e := <-closeChan
	if e != nil {
		rc.logger.Warn("RabbitMQ connection was closed: " + e.Error())

		rc.logger.Warn("closing all channels")
		rc.disconnectConsumers()
		rc.disconnectPublishers()

		backoff.Retry(rc.connect, backoff.NewExponentialBackOff())
	}
}

func (c *RabbitConnector) registerConsumer(ch *AMQPChannel) {
	c.conChannels = append(c.conChannels, ch)
}

func (c *RabbitConnector) registerPublisher(k uint64, ch *AMQPChannel) {
	c.pubChannels[k] = ch
}

func (c *RabbitConnector) connectConsumers() error {
	for _, ch := range c.conChannels {
		err := ch.connect()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *RabbitConnector) connectPublishers() error {
	for _, ch := range c.pubChannels {
		err := ch.connect()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *RabbitConnector) disconnectConsumers() {
	for _, ch := range c.conChannels {
		ch.Close()
	}

	c.conPool.Wait()
}

func (c *RabbitConnector) disconnectPublishers() {
	for _, ch := range c.pubChannels {
		ch.Close()
	}
}

func (c *RabbitConnector) Disconnect() error {
	c.logger.Info("disconnecting from RabbitMQ")

	c.disconnectConsumers()
	c.disconnectPublishers()

	return c.conn.Close()
}

func (c *RabbitConnector) createChannel() (*AMQPChannel, error) {
	ch, err := NewAMQPChannel(c.conn.Channel, c.logger, c.protocol)
	if err != nil {
		return nil, err
	}

	err = ch.connect()
	if err != nil {
		return nil, err
	}

	return ch, nil
}
