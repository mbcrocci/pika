package pika

import (
	"testing"
	"time"
)

type testPubSubEvent struct {
	Msg string
}

type testPubsubConsumerA struct {
	lastMsg  string
	msgsRead int
}

func (c *testPubsubConsumerA) Options() ConsumerOptions {
	return NewConsumerOptions("exchange", "topicA", "")
}
func (c *testPubsubConsumerA) HandleMessage(e testPubSubEvent) error {
	c.lastMsg = e.Msg
	c.msgsRead++
	return nil
}

type testPubsubConsumerB struct {
	lastMsg string
	logger  LogFunc
}

func (c *testPubsubConsumerB) SetLogger(l LogFunc) { c.logger = l }

func (c *testPubsubConsumerB) Options() ConsumerOptions {
	return NewConsumerOptions("exchange", "topicB", "")
}
func (c *testPubsubConsumerB) HandleMessage(e testPubSubEvent) error {
	c.lastMsg = e.Msg
	return nil
}

type testPubsubPublisherA struct{}
type testPubsubPublisherB struct{}

func TestPubSub(t *testing.T) {
	pubsub := NewPubSub()
	pubsub.SetLogger(testLogger{t})

	pubsub.Connect("")
	defer pubsub.Disconnect()

	ca := &testPubsubConsumerA{}
	cb := &testPubsubConsumerB{}

	pa, _ := CreatePublisher[testPubSubEvent](pubsub, PublisherOptions{"exchange", "topicA"})
	pb, _ := CreatePublisher[testPubSubEvent](pubsub, PublisherOptions{"exchange", "topicB"})

	StartConsumer[testPubSubEvent](pubsub, ca)
	StartConsumer[testPubSubEvent](pubsub, cb)

	pa.Publish(testPubSubEvent{"AAAAA"})
	pa.Publish(testPubSubEvent{"AAAAA"})
	pa.Publish(testPubSubEvent{"AAAAA"})

	pb.Publish(testPubSubEvent{"BBBBB"})
	pb.Publish(testPubSubEvent{"BBBBB"})
	pb.Publish(testPubSubEvent{"BBBBB"})

	// Because consumers do their job in a coroutine, we need to sleep a tiny bit to allow
	// it to consume all events
	time.Sleep(time.Millisecond)

	if ca.lastMsg != "AAAAA" {
		t.Log(ca.lastMsg)
		t.Fail()
	}

	if cb.lastMsg != "BBBBB" {
		t.Log(cb.lastMsg)
		t.Fail()
	}
}

func BenchmarkPubSub(b *testing.B) {
	pubsub := NewPubSub()
	ca := &testPubsubConsumerA{}
	pa, _ := CreatePublisher[testPubSubEvent](pubsub, PublisherOptions{"exchange", "topicA"})
	StartConsumer[testPubSubEvent](pubsub, ca)

	for i := 0; i < b.N; i++ {
		pa.Publish(testPubSubEvent{Msg: "static"})
	}

	// for ca.msgsRead < b.N {
	// }
}
