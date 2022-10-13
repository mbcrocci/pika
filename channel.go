package pika

import (
	"time"

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
		c.conn.warn("channel closed: " + e.Error())
		for c.connect() != nil {
			time.Sleep(5 * time.Second)
		}
	}
}

func (c *AMQPChannel) Consume(opts ConsumerOptions, outMsgs chan any) error {
	c.consumer.consuming = true
	c.consumer.options = opts
	c.consumer.delivery = outMsgs

	_, err := c.channel.QueueDeclare(opts.QueueName, opts.durableQueue, opts.autoDeleteQueue, false, false, nil)
	if err != nil {
		return err
	}

	err = c.channel.QueueBind(opts.QueueName, opts.Topic, opts.Exchange, false, nil)
	if err != nil {
		return err
	}

	msgs, err := c.channel.Consume(opts.QueueName, opts.consumerName, false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
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
