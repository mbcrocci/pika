package pika

import (
	"context"
)

type Publisher interface {
	Publish(ctx context.Context) any
}

// PublisherOptions specifies where a Publisher will publish messages
type PublisherOptions struct {
	Exchange string
	Topic    string
}

func (r *RabbitConnector) Publish(msg any, opts PublisherOptions) error {
	// TODO doens't make sense to create a channel everytime
	//	maybe to this in a publishing pool
	channel, err := r.createChannel()
	if err != nil {
		return err
	}

	data, err := r.protocol.Marshal(msg)
	if err != nil {
		return err
	}

	return channel.Publish(opts, data)
}
