package pika

import (
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

// PublisherOptions specifies where a Publisher will publish messages
type PublisherOptions struct {
	Exchange string
	Topic    string
}

// Publisher represents a specific msg that can be published
type Publisher[T any] struct {
	options PublisherOptions
	channel *amqp.Channel
}

// Publish publishes the `message` on the specified exchange and queue
func (p Publisher[T]) Publish(message T) error {
	msg, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return p.channel.Publish(
		p.options.Exchange,
		p.options.Topic,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        msg,
		},
	)
}

// CreatePublisher creates a `Publisher`
func CreatePublisher[T any](r *RabbitConnector, options PublisherOptions) (*Publisher[T], error) {
	channel, err := r.Channel()
	if err != nil {
		return nil, err
	}

	return &Publisher[T]{
		options: options,
		channel: channel,
	}, nil
}
