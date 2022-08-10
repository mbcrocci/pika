package pika

import (
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Consumer represents a RabbitMQ consumer for a typed `T` message
type Consumer[T any] interface {
	Options() ConsumerOptions
	HandleMessage(T) error
}

func consume[T any](msg amqp.Delivery) (T, error) {
	var e T
	err := json.Unmarshal(msg.Body, &e)

	return e, err
}

func handle[T any](c *Channel, outterMsgs chan amqp.Delivery, innerMsgs chan Msg[T]) {
	for msg := range outterMsgs {
		e, err := consume[T](msg)
		if err != nil {
			// Is not a type of message we want
			c.channel.Reject(msg.DeliveryTag, msg.Redelivered)
			continue
		}

		innerMsgs <- Msg[T]{msg: e}
		c.channel.Ack(msg.DeliveryTag, false)
	}
}

func process[T any](msgs chan Msg[T], c Consumer[T]) {
	opts := c.Options()

	for msg := range msgs {
		err := c.HandleMessage(msg.msg)
		if err != nil {
			if opts.HasRetry() && msg.ShouldRetry(opts.retries) {
				go msg.Retry(opts.retryInterval, msgs)
			}
		}
	}
}

// StartConsumer makes the necessary declarations and bindings to start a consumer.
// It will spawn 2 goroutines to receive and process (with retries) messages.
//
// Everything will be handle with the options declared in the `Consumer` method `Options`
func StartConsumer[T any](r *RabbitConnector, consumer Consumer[T]) error {
	channel, err := r.Channel()
	if err != nil {
		return err
	}

	opts := consumer.Options()

	outterMsgs := make(chan amqp.Delivery)
	innerMsgs := make(chan Msg[T])

	channel.consume(opts, outterMsgs)

	go handle[T](channel, outterMsgs, innerMsgs)
	go process(innerMsgs, consumer)

	return nil
}
