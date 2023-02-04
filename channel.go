package pika

import (
	"github.com/cenkalti/backoff/v4"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sourcegraph/conc/pool"
)

type ChannelFactory func() (*amqp.Channel, error)

// Wraps an amqp.Channel to handle reconnects
type AMQPChannel struct {
	channelF ChannelFactory
	channel  *amqp.Channel
	logger   Logger
	protocol Protocol
	closing  bool

	pool *pool.Pool

	consuming bool
	options   ConsumerOptions
	delivery  <-chan amqp.Delivery
}

func NewAMQPChannel(chf ChannelFactory, l Logger, p Protocol) (*AMQPChannel, error) {
	c := &AMQPChannel{
		channelF: chf,
		logger:   l,
		protocol: p,
		pool:     pool.New().WithMaxGoroutines(10),
	}

	return c, nil
}

func (c *AMQPChannel) connect() error {
	ch, err := c.channelF()
	if err != nil {
		return err
	}

	c.channel = ch

	c.pool.Go(c.handleDisconnect)

	return nil
}

func (c *AMQPChannel) handleDisconnect() {
	closeChan := c.channel.NotifyClose(make(chan *amqp.Error, 1))
	//cancelChan := c.channel.NotifyCancel(make(chan string, 1))

	e := <-closeChan
	if e != nil {
		c.logger.Warn("channel was closed:", e.Error())

		// If the connection was closed channels will be notified of closing
		// in that case no reconnect should happen
		if c.closing {
			return
		}

		c.logger.Warn("attempting to reconnect...")
		backoff.Retry(c.connect, backoff.NewExponentialBackOff())
	}
}

func (c *AMQPChannel) Publish(msg any, opts PublisherOptions) error {
	data, err := c.protocol.Marshal(msg)
	if err != nil {
		return err
	}

	c.channel.Publish(
		opts.Exchange,
		opts.Topic,
		false,
		false,
		amqp.Publishing{
			ContentType: c.protocol.ContentType(),
			Body:        data,
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
	if !c.channel.IsClosed() {
		err := c.channel.Close()
		if err != nil {
			return err
		}
	}

	c.closing = true
	c.pool.Wait()
	return nil
}
