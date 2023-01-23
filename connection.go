package pika

import (
	"errors"
	"time"

	"github.com/cenkalti/backoff/v4"
	amqp "github.com/rabbitmq/amqp091-go"
)

type LogFunc func(string)

type Connector interface {
	Connect(string) error
	Disconnect() error

	Channel() (Channel, error)

	SetLogger(Logger)
	Logger() Logger
}

type RabbitConnector struct {
	url      string
	conn     *amqp.Connection
	channels []*AMQPChannel
	logger   Logger
}

func NewConnector() Connector {
	rc := new(RabbitConnector)
	rc.logger = nullLogger{}

	return rc
}

func (rc *RabbitConnector) SetLogger(logger Logger) { rc.logger = logger }
func (rc *RabbitConnector) Logger() Logger          { return rc.logger }

func (rc *RabbitConnector) Connect(url string) error {
	rc.url = url
	return rc.connect()
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

	go rc.handleDisconnect()

	return nil
}

func (rc RabbitConnector) Disconnect() error {
	if rc.conn == nil {
		return errors.New("Connection is nil")
	}

	rc.logger.Info("disconnecting from RabbitMQ")
	return rc.conn.Close()
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

		reconnect := func() error {
			rc.logger.Warn("attempting to reconnect")
			return rc.connect()
		}

		backoff.Retry(reconnect, backoff.NewExponentialBackOff())
	}
}

func (rc *RabbitConnector) createChannel() (*amqp.Channel, error) {
	c, err := rc.conn.Channel()
	if err != nil {
		return nil, err
	}

	return c, err
}

func (rc *RabbitConnector) registerChannel(c *AMQPChannel) {
	rc.channels = append(rc.channels, c)
}

func (rc *RabbitConnector) Channel() (Channel, error) {
	if rc.conn == nil {
		return nil, errors.New("connection is nil")
	}

	c, err := NewAMQPChannel(rc)
	if err != nil {
		return nil, err
	}

	rc.registerChannel(c)

	return c, nil
}
