package pika

import "time"

// Msg holds a RabbitMQ Delivery content's (after parse)
// alonside any attacked state needed. ex: number of retries
type Msg[T any] struct {
	retryCount int
	msg        T
}

// ShouldRetry reports if a msg can still be retried
func (msg Msg[T]) ShouldRetry(retries int) bool {
	return msg.retryCount < retries
}

// Retry increases inner counter and sleeps before sending itself back through msgs.
// Use a goroutine!
func (msg Msg[T]) Retry(backoff time.Duration, msgs chan Msg[T]) {
	msg.retryCount += 1

	time.Sleep(backoff)
	msgs <- msg
}
