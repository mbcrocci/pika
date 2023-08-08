package pika

import (
	"context"

	"github.com/cenkalti/backoff/v4"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Consumer represents a RabbitMQ consumer for a typed `T` message
type Consumer interface {
	HandleMessage(context.Context, Message) error
}

// StartConsumer makes the necessary declarations and bindings to start a consumer.
// It will spawn 2 goroutines to receive and process (with retries) messages.
//
// Everything will be handle with the options declared in the `Consumer` method `Options`
func (c *rabbitConnector) Consume(consumer Consumer, options ConsumerOptions) error {
	channel, err := c.createChannel()
	if err != nil {
		return err
	}

	c.registerConsumer(channel)

	err = channel.SetupConsume(options)
	if err != nil {
		return err
	}

	c.conPool.Go(func() { channel.Consume(consumer, options) })

	c.logger.Info(
		"consuming on queue ", options.QueueName, ", ",
		"connected to ", options.Exchange, " exchange ",
		"with topic ", options.Topic,
	)

	return nil
}

func (c *amqpChannel) setupQueue(opts ConsumerOptions) (<-chan amqp.Delivery, error) {
	// Declare ensures the queue exists, ie. it either creates or check if parameters match.
	// So, the only way it can fail is if parameters do not match or is impossible
	// to create queue.
	// Therefore we can simply error out
	_, err := c.channel.QueueDeclare(opts.QueueName, opts.durableQueue, opts.autoDeleteQueue, false, false, nil)
	if err != nil {
		return nil, err
	}

	err = c.channel.Qos(c.options.prefetch, 0, false)
	if err != nil {
		return nil, err
	}

	// QueueBind only fails if there is a mismatch between the queue and  exchange
	err = c.channel.QueueBind(opts.QueueName, opts.Topic, opts.Exchange, false, nil)
	if err != nil {
		return nil, err
	}

	return c.channel.Consume(opts.QueueName, opts.consumerName, false, false, false, false, nil)
}

func (c *amqpChannel) SetupConsume(opts ConsumerOptions) error {
	c.logger.Info("creating consumer")
	defer c.logger.Info("consumer created")

	c.consuming = true
	c.options = opts

	msgs, err := c.setupQueue(opts)
	if err != nil {
		return err
	}

	c.delivery = msgs

	return nil
}

func (c *amqpChannel) Consume(consumer Consumer, opts ConsumerOptions) {
	for msg := range c.delivery {
		msg := msg

		c.pool.Go(func(ctx context.Context) error {
			msg := msg

			err := c.Ack(msg.DeliveryTag, false)
			if err != nil {
				c.logger.Error("failed to ack message", err)
				return err
			}

			msgSize := len(msg.Body)

			data := Message{
				protocol: c.protocol,
				body:     make([]byte, msgSize),
			}

			n := copy(data.body, msg.Body[:])
			if n != msgSize {
				c.logger.Warn("message size mismatch")
			}

			err = consumer.HandleMessage(ctx, data)
			if err != nil {
				if opts.HasRetry() {
					c.logger.Info("retrying message", err)

					c.pool.Go(func(ctx context.Context) error {
						data := data

						err := backoff.Retry(
							func() error { return consumer.HandleMessage(ctx, data) },
							backoff.NewExponentialBackOff(),
						)
						if err != nil {
							c.logger.Error("unable to handle after retries", err)
						}
						return err
					})
				} else {
					c.logger.Error("unable to handle message", err)
					return err
				}
			}

			return nil
		})
	}
}
