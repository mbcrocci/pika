package pika

import (
	"encoding/json"
	"strings"
	"time"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// ConsumerOptions represents a queue binding for a Consumer
type ConsumerOptions struct {
	Exchange  string
	Topic     string
	QueueName string

	consumerName    string
	durableQueue    bool
	autoDeleteQueue bool

	retries       int // 0 -> will not queue for retry
	retryInterval time.Duration
}

// Msg holds a RabbitMQ Delivery content's (after parse)
// alonside any attacked state needed. ex: number of retries
type Msg[T any] struct {
	retryCount int
	msg        T
}

// ShouldRetry reports if a msg can still be retried
func (msg Msg[T]) ShouldRetry(retries int) bool {
	return msg.retryCount < retries
}

// Retry increases inner counter and sleeps before sending itself back through msgs.
// Use a goroutine!
func (msg Msg[T]) Retry(backoff time.Duration, msgs chan Msg[T]) {
	msg.retryCount += 1

	time.Sleep(backoff)
	msgs <- msg
}

func defaultName(topic string) string {
	words := strings.ReplaceAll(topic, ".", " ")

	caser := cases.Title(language.English)
	sentence := caser.String(words)

	name := strings.ReplaceAll(sentence, " ", "")
	return name + "Consumer"
}

// NewConsumerOptions creates a ConsumerOptions object with default configurations
func NewConsumerOptions(exchange, topic, queue string) ConsumerOptions {
	co := ConsumerOptions{}
	co.Exchange = exchange
	co.Topic = topic
	co.QueueName = queue

	co.consumerName = defaultName(topic)
	co.durableQueue = false
	co.autoDeleteQueue = true

	return co
}

// SetDurable configures the queue to be persist if the consumer disconnects
func (co ConsumerOptions) SetDurable() ConsumerOptions {
	co.durableQueue = true
	co.autoDeleteQueue = false
	return co
}

// WithRetry enables in memory retries of unhandled messages.
// It will retry `retries` times waiting `interval` each time.
func (co ConsumerOptions) WithRetry(retries int, interval time.Duration) ConsumerOptions {
	co.retries = retries
	co.retryInterval = interval
	return co
}

func (co ConsumerOptions) HasRetry() bool {
	return co.retries > 0
}

// SetName sets the consumer name
func (co ConsumerOptions) SetName(name string) ConsumerOptions {
	co.consumerName = name
	return co
}

// Consumer represents a RabbitMQ consumer for a typed `T` message
type Consumer[T any] interface {
	Options() ConsumerOptions
	HandleMessage(T) error
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

	_, err = channel.QueueDeclare(opts.QueueName, opts.durableQueue, opts.autoDeleteQueue, false, false, nil)
	if err != nil {
		return err
	}

	err = channel.QueueBind(opts.QueueName, opts.Topic, opts.Exchange, false, nil)
	if err != nil {
		return err
	}

	msgs, err := channel.Consume(opts.QueueName, opts.consumerName, false, false, false, false, nil)
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

				if opts.HasRetry() && msg.ShouldRetry(opts.retries) {
					go msg.Retry(opts.retryInterval, innerMsgs)
				}
			}
		}
	}()

	return nil
}
