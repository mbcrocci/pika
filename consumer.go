package pika

import (
	"encoding/json"
	"time"
)

type ConsumerOptions struct {
	Exchange  string
	Topic     string
	QueueName string

	durableQueue    bool
	autoDeleteQueue bool

	retries       int // 0 -> will not queue for retry
	retryInterval time.Duration
}

type Msg[T any] struct {
	retryCount int
	msg        T
}

func NewConsumerOptions(exchange, topic, queue string) ConsumerOptions {
	co := ConsumerOptions{}
	co.Exchange = exchange
	co.Topic = topic
	co.QueueName = queue

	co.durableQueue = false
	co.autoDeleteQueue = true

	return co
}

func (co ConsumerOptions) SetDurable() ConsumerOptions {
	co.durableQueue = true
	co.autoDeleteQueue = false
	return co
}

func (co ConsumerOptions) WithRetry(retries int, interval time.Duration) ConsumerOptions {
	co.retries = retries
	co.retryInterval = interval
	return co
}

func (co ConsumerOptions) HasRetry() bool {
	return co.retries > 0
}

type Consumer[T any] interface {
	Options() ConsumerOptions
	HandleMessage(T) error
}

func StartConsumer[T any](r *RabbitConnector, consumer Consumer[T]) error {
	channel, err := r.Channel()
	if err != nil {
		return err
	}

	opts := consumer.Options()

	_, err = channel.QueueDeclare(opts.QueueName, opts.durableQueue, opts.autoDeleteQueue, false, false, nil)
	if err != nil {
		return err
	}

	err = channel.QueueBind(opts.QueueName, opts.Topic, opts.Exchange, false, nil)
	if err != nil {
		return err
	}

	msgs, err := channel.Consume(opts.QueueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	sugar := r.logger.Sugar()
	innerMsgs := make(chan Msg[T])

	// receiver
	go func() {
		for msg := range msgs {
			var e T
			if err := json.Unmarshal(msg.Body, &e); err != nil {
				// Is not a type of message we want
				channel.Reject(msg.DeliveryTag, msg.Redelivered)
				sugar.Errorw("Failed to parse msg", "error", err)
				continue
			}

			innerMsgs <- Msg[T]{msg: e}
			channel.Ack(msg.DeliveryTag, false)
		}
	}()

	// processor

	go func() {
		for msg := range innerMsgs {
			err := consumer.HandleMessage(msg.msg)
			if err != nil {
				sugar.Errorw("Couldn't handle msg", "error", err)

				if opts.HasRetry() {
					msg.retryCount += 1

					if msg.retryCount <= opts.retries {
						go func(msg Msg[T]) {
							time.Sleep(opts.retryInterval)
							innerMsgs <- msg
						}(msg)
					}
				}
			}
		}
	}()

	return nil
}
