package pika

// ConsumerOptions represents a queue binding for a Consumer
type ConsumerOptions struct {
	Exchange  string
	Topic     string
	QueueName string

	consumerName    string
	durableQueue    bool
	autoDeleteQueue bool
	autoACK         bool

	prefetch int // 0 -> unlimited

	retry bool
}

// NewConsumerOptions creates a ConsumerOptions object with default configurations
func NewConsumerOptions(exchange, topic, queue string) ConsumerOptions {
	co := ConsumerOptions{}
	co.Exchange = exchange
	co.Topic = topic
	co.QueueName = queue

	co.consumerName = ""
	co.durableQueue = false
	co.autoDeleteQueue = true
	co.autoACK = false

	return co
}

// SetDurable configures the queue to be persist if the consumer disconnects
func (co ConsumerOptions) SetDurable() ConsumerOptions {
	co.durableQueue = true
	co.autoDeleteQueue = false
	return co
}

// SetPrefetch configures the prefetch count for the consumer.
// Default is 0, which means unlimited.
func (co ConsumerOptions) SetPrefetch(n int) ConsumerOptions {
	co.prefetch = n
	return co
}

// WithRetry enables in memory retries of unhandled messages.
// It will exponationally backoff for a max of 15min
func (co ConsumerOptions) WithRetry() ConsumerOptions {
	co.retry = true
	return co
}

func (co ConsumerOptions) HasRetry() bool {
	return co.retry
}

// SetName sets the consumer name
func (co ConsumerOptions) SetName(name string) ConsumerOptions {
	co.consumerName = name
	return co
}
