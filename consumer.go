package pika

import (
	log "github.com/sirupsen/logrus"
)

type ConsumerOptions struct {
	Exchange  string
	Topic     string
	QueueName string
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

	msgs, err := channel.Consume(
		opts.QueueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	go func() {
		for d := range msgs {
			if err := consumer.HandleMessage(d.Body); err != nil {
				log.Error(err)
				channel.Reject(d.DeliveryTag, !d.Redelivered)
			} else {
				channel.Ack(d.DeliveryTag, false)
			}
		}
	}()

	log.WithFields(log.Fields{
		"Exchange": opts.Exchange,
		"Topic":    opts.Topic,
	}).Info("Started Consumer")
	return nil
}
