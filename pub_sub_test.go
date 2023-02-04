package pika_test

import (
	"context"
	"testing"

	"github.com/mbcrocci/pika/v2"
)

type testPubSubEvent struct {
	Msg string
}

type testPubsubConsumerA struct {
	lastMsg  string
	msgsRead int
}

func (c *testPubsubConsumerA) HandleMessage(ctx context.Context, data any) error {
	c.lastMsg = data.(testPubSubEvent).Msg
	c.msgsRead++
	return nil
}

type testPubsubConsumerB struct {
	lastMsg string
}

func (c *testPubsubConsumerB) HandleMessage(ctx context.Context, data any) error {
	c.lastMsg = data.(testPubSubEvent).Msg
	return nil
}

type testPubsubPublisherA struct{}
type testPubsubPublisherB struct{}

func TestPubSub(t *testing.T) {
	pubsub := pika.NewPubSub()

	pubsub.Connect("")
	defer pubsub.Disconnect()

	ca := &testPubsubConsumerA{}
	cb := &testPubsubConsumerB{}

	pubsub.Consume(ca, pika.ConsumerOptions{Exchange: "exchange", Topic: "topicA"})
	pubsub.Consume(cb, pika.ConsumerOptions{Exchange: "exchange", Topic: "topicB"})

	pa := pika.PublisherOptions{Exchange: "exchange", Topic: "topicA"}
	pb := pika.PublisherOptions{Exchange: "exchange", Topic: "topicB"}

	pubsub.Publish(testPubSubEvent{"AAAAA"}, pa)
	pubsub.Publish(testPubSubEvent{"AAAAA"}, pa)
	pubsub.Publish(testPubSubEvent{"AAAAA"}, pa)

	pubsub.Publish(testPubSubEvent{"BBBBB"}, pb)
	pubsub.Publish(testPubSubEvent{"BBBBB"}, pb)
	pubsub.Publish(testPubSubEvent{"BBBBB"}, pb)

	if ca.lastMsg != "AAAAA" {
		t.Log(ca.lastMsg)
		t.Fail()
	}

	if cb.lastMsg != "BBBBB" {
		t.Log(cb.lastMsg)
		t.Fail()
	}
}
