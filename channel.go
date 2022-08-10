package pika

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Wraps an amqp.Channel to handle reconnects
type Channel struct {
	conn    *RabbitConnector
	channel *amqp.Channel

	consumer struct {
		consuming bool
		options   ConsumerOptions
		delivery  chan amqp.Delivery
	}
	isPublisher bool
}

func newChannel(conn *RabbitConnector) (*Channel, error) {
	c := &Channel{conn: conn}

	conn.registerChannel(c)

	err := c.connect()

	return c, err
}

func (c *Channel) connect() error {
	ch, err := c.conn.createChannel()
	if err != nil {
		return err
	}

	c.channel = ch
	go c.handleDisconnect()
	if c.consumer.consuming {
		c.consume(c.consumer.options, c.consumer.delivery)
	}

	return nil
}

func (c *Channel) handleDisconnect() {
	closeChan := c.channel.NotifyClose(make(chan *amqp.Error, 1))
	//cancelChan := c.channel.NotifyCancel(make(chan string, 1))

	e := <-closeChan
	if e != nil {
		for c.connect() != nil {
			time.Sleep(5 * time.Second)
		}
	}
}

func (c *Channel) consume(opts ConsumerOptions, outMsgs chan amqp.Delivery) error {
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
