package pika

import (
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type RabbitConnector struct {
	logger   *zap.Logger
	url      string
	conn     *amqp.Connection
	channels []*Channel
}

func NewConnector(logger *zap.Logger) *RabbitConnector {
	rc := new(RabbitConnector)
	rc.logger = logger
	return rc
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

	rc.logger.Info("Connected to RabbitMQ")
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
		rc.logger.Error("Connection was closed", zap.Error(e))
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

func (rc *RabbitConnector) registerChannel(c *Channel) {
	rc.channels = append(rc.channels, c)
}

func (rc *RabbitConnector) Channel() (*Channel, error) {
	if rc.conn == nil {
		return nil, errors.New("Connection is nil")
	}

	return newChannel(rc)
}
