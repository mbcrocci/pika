package pika

import (
	"github.com/cenkalti/backoff/v4"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sourcegraph/conc/pool"
)

// Wraps an amqp.Channel to handle reconnects
type AMQPChannel struct {
	channel  *amqp.Channel
	logger   Logger
	protocol Protocol
	closing  bool

	pool *pool.Pool

	consuming bool
	options   ConsumerOptions
	delivery  <-chan amqp.Delivery
}

func NewAMQPChannel(ch *amqp.Channel, l Logger, p Protocol) (*AMQPChannel, error) {
	c := &AMQPChannel{
		channel:  ch,
		logger:   l,
		protocol: p,
		pool:     pool.New().WithMaxGoroutines(10),
	}

	err := c.connect()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *AMQPChannel) connect() error {
	c.pool.Go(c.handleDisconnect)

	return nil
}

func (c *AMQPChannel) handleDisconnect() {
	closeChan := c.channel.NotifyClose(make(chan *amqp.Error, 1))
	//cancelChan := c.channel.NotifyCancel(make(chan string, 1))

	e := <-closeChan
	if e != nil {
		c.logger.Warn("channel was closed: " + e.Error())

		// If the connection was closed channels will be notified of closing
		// in that case no reconnect should happen
		if c.closing {
			return
		}

		c.logger.Warn("attempting to reconnect: " + e.Error())
		backoff.Retry(c.connect, backoff.NewExponentialBackOff())
	}
}

func (c *AMQPChannel) Publish(opts PublisherOptions, msg []byte) error {
	c.channel.Publish(
		opts.Exchange,
		opts.Topic,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        msg,
		},
	)

	return nil
}

func (c *AMQPChannel) Ack(tag uint64, multiple bool) {
	c.channel.Ack(tag, multiple)
}

func (c *AMQPChannel) Reject(tag uint64, requeue bool) {
	c.channel.Reject(tag, requeue)
}

func (c *AMQPChannel) Close() error {
	c.closing = true
	c.pool.Wait()
	return nil
}
