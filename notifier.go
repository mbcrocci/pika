package pika

import (
	"encoding/json"
	"time"
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
func StartNotifier[T any](r Connector, notifier Notifier[T]) error {
	channel, err := r.Channel()
	if err != nil {
		return err
	}

	err = notify(channel, notifier)
	if err != nil {
		return err
	}

	go notifyLoop(channel, notifier)

	return nil
}

func notify[T any](c Channel, notifier Notifier[T]) error {
	options := notifier.Options()

	msg, err := notifier.Notify()
	if err != nil {
		return err
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	opts := PublisherOptions{options.Exchange, options.Topic}

	return c.Publish(opts, body)
}

func notifyLoop[T any](c Channel, notifier Notifier[T]) {
	options := notifier.Options()

	t := time.NewTicker(options.Interval)
	defer t.Stop()

	for range t.C {
		if notifier.Stop() {
			break
		}

		err := notify(c, notifier)
		if err != nil {
			// c.Error("Unable to notify: " + err.Error())
		}
	}
}
