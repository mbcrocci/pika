package pika

import (
	"errors"
)

var minLength = 10

type Queue[T any] struct {
	buf               []*T
	head, tail, count int
}

func NewQueue[T any]() *Queue[T] {
	return &Queue[T]{
		buf: make([]*T, minLength),
	}
}

func (q *Queue[T]) Length() int {
	return q.count
}

func (q *Queue[T]) resize() {
	newBuff := make([]*T, q.count<<1)

	if q.tail > q.head {
		copy(newBuff, q.buf[q.head:q.tail])
	} else {
		n := copy(newBuff, q.buf[q.head:])
		copy(newBuff[n:], q.buf[:q.tail])
	}

	q.head = 0
	q.tail = q.count
	q.buf = newBuff
}

func (q *Queue[T]) Queue(e *T) {
	if q.count == len(q.buf) {
		q.resize()
	}

	q.buf[q.tail] = e
	q.tail = (q.tail + 1) & (len(q.buf) - 1)
	q.count++
}

func (q *Queue[T]) Peek() (*T, error) {
	if q.count <= 0 {
		return nil, errors.New("Can't peek on empty queue")
	}

	return q.buf[q.head], nil
}

func (q *Queue[T]) PeakAt(i int) (*T, error) {
	if i < 0 {
		i += q.count
	}

	if i < 0 || i >= q.count {
		return nil, errors.New("i outside of bounds")
	}

	j := (q.head+i)&len(q.buf) - 1
	return q.buf[j], nil
}

func (q *Queue[T]) Dequeue() (*T, error) {
	if q.count <= 0 {
		return nil, errors.New("Queue is empty")
	}

	ret := q.buf[q.head]
	q.buf[q.head] = nil
	q.head = (q.head + 1) & (len(q.buf) - 1)
	q.count--

	if len(q.buf) > minLength && (q.count<<2) == len(q.buf) {
		q.resize()
	}

	return ret, nil
}
