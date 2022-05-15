package pika

import (
	"encoding/json"

	"github.com/streadway/amqp"
)

type PublisherOptions struct {
	Exchange string
	Topic    string
}

type Publisher[T any] struct {
	options PublisherOptions
	channel *amqp.Channel
}

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
