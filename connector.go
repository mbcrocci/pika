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
	Publish(any, PublishOptions) error

	WithLogger(Logger) Connector
	WithProtocol(Protocol) Connector
	WithConsumers(int) Connector
}

type rabbitConnector struct {
	url      string
	logger   Logger
	protocol Protocol
	conn     *amqp.Connection

	conChannels []*amqpChannel
	conPool     *pool.Pool
	pubChannels map[uint64]*amqpChannel
	waitGroup   *conc.WaitGroup
}

func NewRabbitConnector() Connector {
	return &rabbitConnector{
		logger:      &nullLogger{},
		protocol:    JsonProtocol{},
		conPool:     pool.New(),
		pubChannels: make(map[uint64]*amqpChannel),
		waitGroup:   conc.NewWaitGroup(),
	}
}

func (c *rabbitConnector) WithLogger(l Logger) Connector {
	c.logger = l
	return c
}

func (c *rabbitConnector) WithProtocol(p Protocol) Connector {
	c.protocol = p
	return c
}

func (c *rabbitConnector) WithConsumers(n int) Connector {
	c.conChannels = make([]*amqpChannel, 0)
	c.conPool = pool.New().WithMaxGoroutines(n)
	return c
}

func (c *rabbitConnector) Connect(url string) error {
	c.url = url
	return c.connect()
}

func (rc *rabbitConnector) connect() error {
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

func (rc *rabbitConnector) handleDisconnect() {
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

func (c *rabbitConnector) registerConsumer(ch *amqpChannel) {
	c.conChannels = append(c.conChannels, ch)
}

func (c *rabbitConnector) registerPublisher(k uint64, ch *amqpChannel) {
	c.pubChannels[k] = ch
}

func (c *rabbitConnector) connectConsumers() error {
	for _, ch := range c.conChannels {
		err := ch.connect()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *rabbitConnector) connectPublishers() error {
	for _, ch := range c.pubChannels {
		err := ch.connect()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *rabbitConnector) disconnectConsumers() {
	for _, ch := range c.conChannels {
		ch.Close()
	}

	c.conPool.Wait()
}

func (c *rabbitConnector) disconnectPublishers() {
	for _, ch := range c.pubChannels {
		ch.Close()
	}
}

func (c *rabbitConnector) Disconnect() error {
	c.logger.Info("disconnecting from RabbitMQ")

	c.disconnectConsumers()
	c.disconnectPublishers()

	return c.conn.Close()
}

func (c *rabbitConnector) createChannel() (*amqpChannel, error) {
	ch, err := newAMQPChannel(c.conn.Channel, c.logger, c.protocol)
	if err != nil {
		return nil, err
	}

	err = ch.connect()
	if err != nil {
		return nil, err
	}

	return ch, nil
}
