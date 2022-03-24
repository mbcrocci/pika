package pika

import (
	"testing"
	"time"
)

func TestQueue(t *testing.T) {
	queue := NewQueue[int]()
	done := make(chan bool)

	go func() {
		a := 1
		b := 2
		c := 3

		queue.Queue(&a)
		queue.Queue(&b)
		time.Sleep(100 * time.Millisecond)
		queue.Queue(&c)
	}()

	go func() {
		a := queue.Dequeue()
		if a == nil {
			t.Log("A is nill")
			t.Fail()
		}

		b := queue.Dequeue()
		if b == nil {
			t.Log("B is nill")
			t.Fail()
		}

		c := queue.Dequeue()
		if c == nil {
			t.Log("C is nill")
			t.Fail()
		}

		done <- true
	}()

	<-done
}
