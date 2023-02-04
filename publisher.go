package pika

import (
	"github.com/mitchellh/hashstructure/v2"
)

// PublishOptions specifies where to publish messages
type PublishOptions struct {
	Exchange string
	Topic    string
}

func (r *rabbitConnector) Publish(msg any, opts PublishOptions) error {
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
