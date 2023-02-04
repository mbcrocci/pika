package pika

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Consumer represents a RabbitMQ consumer for a typed `T` message
type Consumer interface {
	HandleMessage(context.Context, any) error
}

// StartConsumer makes the necessary declarations and bindings to start a consumer.
// It will spawn 2 goroutines to receive and process (with retries) messages.
//
// Everything will be handle with the options declared in the `Consumer` method `Options`
func (c *RabbitConnector) Consume(consumer Consumer, options ConsumerOptions) error {
	channel, err := c.createChannel()
	if err != nil {
		return err
	}

	err = channel.SetupConsume(options)
	if err != nil {
		return err
	}

	c.pool.Go(func() { channel.Consume(consumer, options) })

	c.logger.Info(
		"consuming on queue ", options.QueueName, ", ",
		"connected to ", options.Exchange, " exchange ",
		"with topic ", options.Topic,
	)

	return nil
}

func (c *AMQPChannel) setupQueue(opts ConsumerOptions) (<-chan amqp.Delivery, error) {
	// Declare ensures the queue exists, ie. it either creates or check if parameters match.
	// So, the only way it can fail is if parameters do not match or is impossible
	// to create queue.
	// Therefore we can simply error out
	_, err := c.channel.QueueDeclare(opts.QueueName, opts.durableQueue, opts.autoDeleteQueue, false, false, nil)
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

func (c *AMQPChannel) SetupConsume(opts ConsumerOptions) error {
	c.logger.Info("creating consumer")
	defer c.logger.Info("consumer created")

	c.consumer.consuming = true
	c.consumer.options = opts

	msgs, err := c.setupQueue(opts)
	if err != nil {
		return err
	}

	c.consumer.delivery = msgs

	return nil
}

func (c *AMQPChannel) Consume(consumer Consumer, opts ConsumerOptions) {
	for msg := range c.consumer.delivery {
		msg := msg

		c.pool.Go(func() {
			// TODO extract to protocol plugin system
			var data any
			err := json.Unmarshal(msg.Body, &data)
			if err != nil {
				c.logger.Error(err)
				c.Reject(msg.DeliveryTag, false)
				return
			}

			err = consumer.HandleMessage(context.TODO(), data)
			if err != nil {
				c.logger.Error(err)
				c.Reject(msg.DeliveryTag, opts.HasRetry())
				return
			}

			c.Ack(msg.DeliveryTag, false)
		})
	}
}
