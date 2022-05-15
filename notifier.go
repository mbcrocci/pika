package pika

import (
	"encoding/json"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type NotificationOptions struct {
	Exchange string
	Topic    string
	Interval time.Duration
}

type Notifier[T any] interface {
	Options() NotificationOptions
	Stop() bool
	Notify() (T, error)
}

func StartNotifier[T any](r *RabbitConnector, notifier Notifier[T]) error {
	channel, err := r.Channel()
	if err != nil {
		return err
	}

	notify(channel, notifier)

	go notifyLoop(channel, notifier)

	options := notifier.Options()
	log.WithFields(log.Fields{
		"Exchange": options.Exchange,
		"Topic":    options.Topic,
	}).Info("Started Notifier")

	return nil
}

func notify[T any](channel *amqp.Channel, notifier Notifier[T]) {
	options := notifier.Options()

	msg, err := notifier.Notify()
	if err != nil {
		log.Error(err)
		return
	}

	body, err := json.Marshal(msg)
	if err != nil {
		log.Error(err)
		return
	}

	err = channel.Publish(
		options.Exchange,
		options.Topic,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		},
	)
	if err != nil {
		log.Error(err)
	}
}

func notifyLoop[T any](channel *amqp.Channel, notifier Notifier[T]) {
	options := notifier.Options()

	t := time.NewTicker(options.Interval)
	defer t.Stop()

	for range t.C {
		if notifier.Stop() {
			break
		}

		notify(channel, notifier)
	}
}
