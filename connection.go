package pika

import (
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type RabbitConnector struct {
	conn *amqp.Connection
}

func NewConnector(url string) (*RabbitConnector, error) {
	rc := &RabbitConnector{}

	return rc, rc.Connect(url)
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
	return rc.conn.Close()
}

func (rc RabbitConnector) Channel() (*amqp.Channel, error) {
	return rc.conn.Channel()
}
