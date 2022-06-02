package pika

import (
	"testing"
	"time"
)

func TestQueue(t *testing.T) {
	queue := NewQueue[int]()
	done := make(chan bool)

	first, second := 1, 2
	queue.Queue(&first)
	queue.Queue(&second)

	// Test length
	if queue.Length() != 2 {
		t.Fail()
	}

	// Test FIFO
	t_first, t_second := queue.Dequeue(), queue.Dequeue()
	if first != *t_first || second != *t_second {
		t.Fail()
	}

	// Test concurrency
	go func() {
		a, b, c := 1, 2, 3

		queue.Queue(&a)
		queue.Queue(&b)
		time.Sleep(10 * time.Millisecond)
		queue.Queue(&c)
	}()

	go func() {
		a, b, c := queue.Dequeue(), queue.Dequeue(), queue.Dequeue()
		if a == nil || b == nil || c == nil {
			t.Fail()
		}

		done <- true
	}()

	<-done
}

func BenchmarkEmptyDequeue(b *testing.B) {
	queue := NewQueue[int]()

	for n := 0; n < b.N; n++ {
		queue.DequeueNoWait()
	}
}

func setupQueue(n int) *Queue[int] {
	queue := NewQueue[int]()
	queue.buf = make([]*int, n)
	queue.count = n

	return queue
}

func BenchmarkQueueNoWait(b *testing.B) {
	queue := setupQueue(b.N)
	for n := 0; n < b.N; n++ {
		queue.DequeueNoWait()
	}
}

func BenchmarkQueue(b *testing.B) {
	queue := setupQueue(b.N)
	for n := 0; n < b.N; n++ {
		queue.Dequeue()
	}
}
