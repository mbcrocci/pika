package pika

import (
	"errors"

	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

type RabbitConnector struct {
	conn *amqp.Connection

	logger *zap.Logger
}

func NewConnector(logger *zap.Logger) *RabbitConnector {
	rc := new(RabbitConnector)
	rc.logger = logger
	return rc
}

func (rc *RabbitConnector) Connect(url string) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}

	rc.conn = conn
	rc.logger.Info("Connected to RabbitMQ")

	return nil
}

func (rc RabbitConnector) Disconnect() error {
	if rc.conn == nil {
		return errors.New("Connection is nil")
	}

	return rc.conn.Close()
}

func (rc RabbitConnector) Channel() (*amqp.Channel, error) {
	if rc.conn == nil {
		return nil, errors.New("Connection is nil")
	}

	return rc.conn.Channel()
}
