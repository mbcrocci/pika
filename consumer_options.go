package pika

import (
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
