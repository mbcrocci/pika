package pika

import (
	"errors"
	"time"

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
	return rc
}

func (rc *RabbitConnector) SetLogger(logger Logger) { rc.logger = logger }
func (rc *RabbitConnector) Logger() Logger          { return rc.logger }

func (rc *RabbitConnector) debug(msg string) {
	if rc.logger != nil {
		rc.logger.Debug(msg)
	}
}

func (rc *RabbitConnector) info(msg string) {

	if rc.logger != nil {
		rc.logger.Info(msg)
	}
}

func (rc *RabbitConnector) warn(msg string) {
	if rc.logger != nil {
		rc.logger.Warn(msg)
	}
}

func (rc *RabbitConnector) error(msg string) {
	if rc.logger != nil {
		rc.logger.Error(msg)
	}
}

func (rc *RabbitConnector) Connect(url string) error {
	rc.url = url
	return rc.connect()
}

func (rc *RabbitConnector) connect() error {
	conn, err := amqp.Dial(rc.url)
	if err != nil {
		return err
	}

	rc.conn = conn

	for _, c := range rc.channels {
		c.connect()
	}

	rc.info("Connected to RabbitMQ")
	rc.debug("Connection string: " + rc.url)

	go rc.handleDisconnect()

	return nil
}

func (rc RabbitConnector) Disconnect() error {
	if rc.conn == nil {
		return errors.New("Connection is nil")
	}

	rc.info("Connection closed")
	return rc.conn.Close()
}

func (rc *RabbitConnector) handleDisconnect() {
	closeChan := rc.conn.NotifyClose(make(chan *amqp.Error, 1))

	e := <-closeChan
	if e != nil {
		rc.warn("Connection was closed: " + e.Error())
		for rc.connect() != nil {
			time.Sleep(5 * time.Second)
		}
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
		return nil, errors.New("Connection is nil")
	}

	c, err := NewAMQPChannel(rc)
	if err != nil {
		return nil, err
	}

	rc.registerChannel(c)

	return c, nil
}
