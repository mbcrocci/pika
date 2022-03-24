package pika

import	"sync"

var minLength = 10

type Queue[T any] struct {
	buf               []*T
	head, tail, count int

	m sync.Mutex
	c sync.Cond
}

func NewQueue[T any]() *Queue[T] {
	queue := new(Queue[T])
	queue.buf = make([]*T, minLength)
	queue.c = sync.Cond{L: &queue.m}
	return queue
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
	q.c.L.Lock()

	if q.count == len(q.buf) {
		q.resize()
	}

	q.buf[q.tail] = e
	q.tail = (q.tail + 1) & (len(q.buf) - 1)
	q.count++

	q.c.Signal()
	q.c.L.Unlock()
}

func (q *Queue[T]) Peek() *T {
	if q.count <= 0 {
		return nil
	}

	return q.buf[q.head]
}

func (q *Queue[T]) PeakAt(i int) *T {
	if i < 0 {
		i += q.count
	}

	if i < 0 || i >= q.count {
		return nil
	}

	j := (q.head+i)&len(q.buf) - 1
	return q.buf[j]
}

func (q *Queue[T]) Dequeue() *T {
	q.c.L.Lock()

	if q.count <= 0 {
		q.c.Wait()
	}

	ret := q.buf[q.head]
	q.buf[q.head] = nil
	q.head = (q.head + 1) & (len(q.buf) - 1)
	q.count--

	if len(q.buf) > minLength && (q.count<<2) == len(q.buf) {
		q.resize()
	}

	q.c.L.Unlock()
	return ret
}
