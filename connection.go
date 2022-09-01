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

	SetLogger(LogFunc)
	Log(string)
}

type RabbitConnector struct {
	url      string
	conn     *amqp.Connection
	channels []*AMQPChannel
	logger   func(string)
}

func NewConnector() Connector {
	rc := new(RabbitConnector)
	return rc
}

func (rc *RabbitConnector) SetLogger(logFunc LogFunc) {
	rc.logger = logFunc
}

func (rc *RabbitConnector) Log(msg string) {
	if rc.logger != nil {
		rc.logger(msg)
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

	rc.Log("Connected to RabbitMQ")
	go rc.handleDisconnect()

	return nil
}

func (rc RabbitConnector) Disconnect() error {
	if rc.conn == nil {
		return errors.New("Connection is nil")
	}

	return rc.conn.Close()
}

func (rc *RabbitConnector) handleDisconnect() {
	closeChan := rc.conn.NotifyClose(make(chan *amqp.Error, 1))

	e := <-closeChan
	if e != nil {
		rc.Log("Connection was closed: " + e.Error())
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
