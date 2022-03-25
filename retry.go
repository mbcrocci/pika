package pika

import (
	"time"

	log "github.com/sirupsen/logrus"
)

type Entry struct {
	consumer Consumer

	retries  int
	count    int
	interval time.Duration

	body []byte
}

type Retrier struct {
	queue *Queue[Entry]
}

func NewRetrier() *Retrier {
	r := new(Retrier)
	r.queue = NewQueue[Entry]()

	return r
}

func (r *Retrier) Retry(consumer Consumer, msg []byte, retries int, interval time.Duration) {
	e := new(Entry)
	e.consumer = consumer
	e.retries = retries
	e.interval = interval
	e.body = msg

	go func() {
		for {
			r.retryNext()
		}
	}()

	r.retry(e)
}

func (r *Retrier) retry(entry *Entry) {
	// TODO this should not be done as "sleeping in a goroutine" to avoid polluting the heap/scheduler
	go func() {
		time.Sleep(entry.interval)
		r.queue.Queue(entry)
	}()
}

func (r *Retrier) retryNext() {
	entry := r.queue.Dequeue()
	if entry == nil {
		return
	}

	err := entry.consumer.HandleMessage(entry.body)
	// Everthing went well so no need to retry again
	if err == nil {
		return
	}

	log.Error(err)
	entry.count++

	// Done with the retries
	if entry.count == entry.retries {
		return
	}

	// Requeue for further retries
	r.retry(entry)
}
