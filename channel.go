package pika

import (
	"time"

	"github.com/cenkalti/backoff/v4"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Channel interface {
	Consume(ConsumerOptions, chan any) error
	Publish(PublisherOptions, []byte) error

	Ack(uint64, bool)
	Reject(uint64, bool)

	Logger() Logger
}

// Wraps an amqp.Channel to handle reconnects
type AMQPChannel struct {
	conn    *RabbitConnector
	channel *amqp.Channel
	closing bool

	consumer struct {
		consuming bool
		options   ConsumerOptions
		delivery  chan any
	}
	isPublisher bool
}

func NewAMQPChannel(conn *RabbitConnector) (*AMQPChannel, error) {
	c := &AMQPChannel{conn: conn}

	err := c.connect()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *AMQPChannel) Logger() Logger { return c.conn.Logger() }

func (c *AMQPChannel) connect() error {
	ch, err := c.conn.createChannel()
	if err != nil {
		return err
	}

	c.channel = ch
	go c.handleDisconnect()

	if c.consumer.consuming {
		c.Consume(c.consumer.options, c.consumer.delivery)
	}

	return nil
}

func (c *AMQPChannel) handleDisconnect() {
	closeChan := c.channel.NotifyClose(make(chan *amqp.Error, 1))
	//cancelChan := c.channel.NotifyCancel(make(chan string, 1))

	e := <-closeChan
	if e != nil {
		c.Logger().Warn("channel was closed: " + e.Error())

		// If the connection was closed channels will be notified of closing
		// in that case no reconnect should happen
		if c.closing {
			return
		}

		c.Logger().Warn("attempting to reconnect: " + e.Error())
		reconnect := func() error {
			return c.connect()
		}
		backoff.Retry(reconnect, backoff.NewExponentialBackOff())
	}
}

func (c *AMQPChannel) Consume(opts ConsumerOptions, outMsgs chan any) error {
	c.Logger().Info("creating consumer")
	defer c.Logger().Info("consumer created")

	c.consumer.consuming = true
	c.consumer.options = opts
	c.consumer.delivery = outMsgs

	// Declare ensures the queue exists, ie. it either creates or check if parameters match.
	// So, the only way it can fail is if parameters do not match or is impossible
	// to create queue.
	// Therefore we can simply error out
	_, err := c.channel.QueueDeclare(opts.QueueName, opts.durableQueue, opts.autoDeleteQueue, false, false, nil)
	if err != nil {
		return err
	}

	// QueueBind only fails if there is a mismatch between the queue and  exchange
	err = c.channel.QueueBind(opts.QueueName, opts.Topic, opts.Exchange, false, nil)
	if err != nil {
		return err
	}

	msgs, err := c.channel.Consume(opts.QueueName, opts.consumerName, false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		c.Logger().Info("consuming on", opts.QueueName)

		for msg := range msgs {
			outMsgs <- msg
		}
	}()

	return nil
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
	return nil
}
