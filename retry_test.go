package pika

import (
	"errors"
	"testing"
	"time"
)

type RetryTestConsumer struct{}

func (RetryTestConsumer) Options() ConsumerOptions {
	return NewConsumerOptions("", "", "").WithRetry(10, 10*time.Millisecond)
}

func (RetryTestConsumer) HandleMessage(body []byte) error {
	return errors.New("Test error")
}

func TestRetry(t *testing.T) {
	c := RetryTestConsumer{}

	r := new(Retrier)
	r.queue = NewQueue[Entry]()

	r.Retry(c, []byte("hello world"))

	if r.queue.Length() > 0 {
		t.Fatal("Queue should be waiting while interval hasn't passed")
	}

	time.Sleep(20 * time.Millisecond)

	if r.queue.Length() != 1 {
		t.Fatal("Queue should have entry")
	}

	e := r.queue.Peek()
	if e.count != 0 {
		t.Fatal("Retries count should start at 0")
	}
	if e.retries != 10 {
		t.Fatal("Retries should be 10")
	}

	r.retryNext()
	if r.queue.Length() > 0 {
		t.Fatal("Queue should be waiting while interval hasn't passed (2)")
	}
	time.Sleep(20 * time.Millisecond)
	if r.queue.Length() != 1 {
		t.Fatal("Queue should have entry (2)")
	}
	e = r.queue.Peek()
	if e.count != 1 {
		t.Fatal("Retries count should be at 1")
	}

	for i := 0; i < 8; i++ {
		r.retryNext()
	}

	time.Sleep(20 * time.Millisecond)
	e = r.queue.Peek()
	if e.count != 9 {
		t.Fatal("Retries count should be at 9")
	}

	r.retryNext()
	if r.queue.Length() != 0 {
		t.Fatal("Queue should be empty")
	}

	time.Sleep(20 * time.Millisecond)
	if r.queue.Length() != 0 {
		t.Fatal("Queue should be empty")
	}
}
