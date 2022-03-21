package pika

import (
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type NotificationOptions struct {
	Exchange string
	Topic    string
	Interval time.Duration
}

type Notifier interface {
	Options() NotificationOptions
	Stop() bool
	Notify() (string, error)
}

func (r *RabbitConnector) StartNotifier(notifier Notifier) error {
	channel, err := r.Channel()
	if err != nil {
		return err
	}

	options := notifier.Options()

	notify := func() {
		msg, err := notifier.Notify()
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
				Body:        []byte(msg),
			},
		)
		if err != nil {
			log.Error(err)
		}

	}

	notify()

	go func() {
		t := time.NewTicker(options.Interval)
		defer t.Stop()

		for range t.C {
			if notifier.Stop() {
				break
			}

			notify()
		}
	}()

	log.WithFields(log.Fields{
		"Exchange": options.Exchange,
		"Topic":    options.Topic,
	}).Info("Started Notifier")
	return nil
}
