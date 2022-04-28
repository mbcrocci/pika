package pika

import (
	"errors"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type RabbitConnector struct {
	conn *amqp.Connection
}

func NewConnector() *RabbitConnector {
	rc := new(RabbitConnector)
	return rc
}

func (rc *RabbitConnector) Connect(url string) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}

	rc.conn = conn
	log.Info("Connected to RabbitMQ")

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
