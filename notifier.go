package pika

import (
	"encoding/json"
	"time"

	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

// NotificationOptions represents the behaviour of the `Notifier`
type NotificationOptions struct {
	Exchange string
	Topic    string
	Interval time.Duration
}

// Notifier is a `Publisher` that publish on a regular interval
type Notifier[T any] interface {
	Options() NotificationOptions
	Stop() bool
	Notify() (T, error)
}

// StartNotifier initiates the `Notifier` that will run in a goroutine
func StartNotifier[T any](r *RabbitConnector, notifier Notifier[T]) error {
	channel, err := r.Channel()
	if err != nil {
		return err
	}

	sugar := r.logger.Sugar()

	notify(channel, notifier, sugar)

	go notifyLoop(channel, notifier, sugar)

	options := notifier.Options()
	sugar.Infow("Started Notifier",
		"Exchange", options.Exchange,
		"Topic", options.Topic,
	)

	return nil
}

func notify[T any](channel *amqp.Channel, notifier Notifier[T], sugar *zap.SugaredLogger) {
	options := notifier.Options()

	msg, err := notifier.Notify()
	if err != nil {
		sugar.Error(err)
		return
	}

	body, err := json.Marshal(msg)
	if err != nil {
		sugar.Error(err)
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
		sugar.Error(err)
	}
}

func notifyLoop[T any](channel *amqp.Channel, notifier Notifier[T], sugar *zap.SugaredLogger) {
	options := notifier.Options()

	t := time.NewTicker(options.Interval)
	defer t.Stop()

	for range t.C {
		if notifier.Stop() {
			break
		}

		notify(channel, notifier, sugar)
	}
}
