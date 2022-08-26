package pika

import (
	"testing"
	"time"
)

func TestDefaultName(t *testing.T) {
	topic := "integration.resource.created"
	expected := "IntegrationResourceCreatedConsumer"
	t.Log("Expected: " + expected)

	actual := defaultName(topic)
	t.Log("Actual: " + actual)

	if actual != expected {
		t.Fail()
	}
}

func TestRetries(t *testing.T) {
	opts := NewConsumerOptions("", "", "")
	msg := Msg[int]{retryCount: 0}

	if msg.ShouldRetry(opts.retries) {
		t.Log("Shouldn't retry if retries weren't configured (= 0)")
		t.FailNow()
	}

	opts = opts.WithRetry(1, time.Millisecond)
	if !msg.ShouldRetry(opts.retries) {
		t.Log("Should retry once")
		t.Log("Retries: ", opts.retries)
		t.Log("Retry Count: ", msg.retryCount)
		t.FailNow()
	}

	msgs := make(chan Msg[int])
	go msg.Retry(opts.retryInterval, msgs)

	rmsg := <-msgs

	if rmsg.retryCount != 1 {
		t.Log("Retry Count: ", rmsg.retryCount)
	}
}

type TestConsumer struct{}

func (c TestConsumer) Options() ConsumerOptions {
	return NewConsumerOptions("", "", "")
}

func (c TestConsumer) HandleMessage(msg TestEvent) error {
	return nil
}

func TestConsumerHandler(t *testing.T) {
	c := &TestConnector{}
	c.SetLogger(func(s string) { t.Log(s) })

	tc := TestConsumer{}

	err := StartConsumer[TestEvent](c, tc)
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
}
