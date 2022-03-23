package pika

import (
	"time"

	"github.com/streadway/amqp"
)

type RetryMessage struct {
	Exchange string
	Topic    string
	Message  string

	Retries  int
	Interval time.Duration

	count      int
	last_retry time.Time
}

type Retrier struct {
	channel *amqp.Channel
	queue   *Queue[RetryMessage]
}

func NewRetrier(c *amqp.Channel) *Retrier {
	r := &Retrier{
		channel: c,
		queue:   NewQueue[RetryMessage](),
	}

	go r.retryLoop()

	return r
}

func (r *Retrier) Retry(rm *RetryMessage) {
	r.queue.Queue(rm)
}

func (r *Retrier) retryLoop() {
	for {
		rm, err := r.queue.Dequeue()
		if err != nil {
			// should only error out if it's empty
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if err := r.resendMessage(rm); err != nil {
			rm.count++
			if rm.count != rm.Retries {
				r.queue.Queue(rm)
			}
		}
	}
}

func (r *Retrier) resendMessage(rm *RetryMessage) error {
	return r.channel.Publish(
		rm.Exchange,
		rm.Topic,
		false, false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(rm.Message),
		},
	)
}
