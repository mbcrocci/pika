package pika

import (
	"context"
	"github.com/mitchellh/hashstructure/v2"
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
	hash, err := hashstructure.Hash(opts, hashstructure.FormatV2, nil)
	if err != nil {
		return err
	}

	_, exists := r.pubChannels[hash]
	if !exists {
		ch, err := r.createChannel()
		if err != nil {
			return err
		}

		r.registerPublisher(hash, ch)
	}

	return r.pubChannels[hash].Publish(msg, opts)
}
