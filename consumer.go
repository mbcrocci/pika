package pika

import (
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type ConsumerOptions struct {
	Exchange  string
	Topic     string
	QueueName string

	retries       int // 0 -> will not queue for retry
	retryInterval time.Duration
}

func (co *ConsumerOptions) WithRetry(retries int, interval time.Duration) {
	co.retries = retries
	co.retryInterval = interval
}

func (co ConsumerOptions) HasRetry() bool {
	return co.retries > 0
}

type Consumer interface {
	Options() ConsumerOptions
	HandleMessage([]byte) error
}

func (r *RabbitConnector) StartConsumer(consumer Consumer) error {
	channel, err := r.Channel()
	if err != nil {
		return err
	}

	opts := consumer.Options()

	_, err = channel.QueueDeclare(opts.QueueName, false, true, false, false, nil)
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

	go r.consumerHandler(consumer, channel, msgs)

	log.WithFields(log.Fields{
		"Exchange": opts.Exchange,
		"Topic":    opts.Topic,
	}).Info("Started Consumer")
	return nil
}

func (r *RabbitConnector) consumerHandler(consumer Consumer, channel *amqp.Channel, msgs <-chan amqp.Delivery) {
	opts := consumer.Options()

	for d := range msgs {
		err := consumer.HandleMessage(d.Body)
		if err != nil && !opts.HasRetry() {
			log.Error(err)
			channel.Reject(d.DeliveryTag, !d.Redelivered)
			continue
		}

		if err != nil && opts.HasRetry() {
			log.Error(err)
			r.retrier.Retry(consumer, d.Body)
			// Because the retrying is gonna be handled locally, tecnically the message is accepted instead of rejected
		}

		channel.Ack(d.DeliveryTag, false)
	}
}
