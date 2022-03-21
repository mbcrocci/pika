package pika

import "github.com/streadway/amqp"

type PublisherOptions struct {
	Exchange string
	Topic    string
}

type Publisher struct {
	options PublisherOptions
	channel *amqp.Channel
}

func (p Publisher) Publish(message string) error {
	return p.channel.Publish(
		p.options.Exchange,
		p.options.Topic,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		},
	)
}

func (r *RabbitConnector) CreatePublisher(options PublisherOptions) (*Publisher, error) {
	channel, err := r.Channel()
	if err != nil {
		return nil, err
	}

	return &Publisher{
		options: options,
		channel: channel,
	}, nil
}
