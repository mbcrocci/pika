package pika

import (
	"testing"
)

type TestChannel struct {
	MsgWatcher    func(any)
	PublishedMsgs [][]byte
}

func (c *TestChannel) Consume(opts ConsumerOptions, msgs chan any) error {
	go func() {
		for msg := range msgs {
			c.MsgWatcher(msg)
		}
	}()

	return nil
}

func (c *TestChannel) Publish(opts PublisherOptions, msg []byte) error {
	c.PublishedMsgs = append(c.PublishedMsgs, msg)
	return nil
}

func (c *TestChannel) Ack(uint64, bool)    {}
func (c *TestChannel) Reject(uint64, bool) {}
func (c *TestChannel) Logger() Logger      { return nil }

func TestChannelConsume(t *testing.T) {
	type TMsg struct {
		a int
		b bool
	}
	wants := []TMsg{{0, true}, {1, false}, {1, true}, {2, false}, {2, true}}
	gots := make([]TMsg, 0)

	chann := &TestChannel{
		MsgWatcher: func(a any) {
			m := a.(TMsg)
			gots = append(gots, m)
		},
	}

	msgs := make(chan any)
	chann.Consume(ConsumerOptions{}, msgs)

	for _, w := range wants {
		msgs <- w
	}

	for i := range gots {
		if gots[i].a != wants[i].a || gots[i].b != wants[i].b {
			t.Logf("%d: wanted %v  got %v", i, wants[i], gots[i])
			t.Fail()
		}
	}
}

func TestChannelPublish(t *testing.T) {
	chann := &TestChannel{}

	wants := []string{"1", "2", "3", "4", "5"}

	for _, w := range wants {
		chann.Publish(PublisherOptions{}, []byte(w))
	}

	for i := range wants {
		if wants[i] != string(chann.PublishedMsgs[i]) {
			t.Logf("%d: wanted %s, got %s", i, wants[i], string(chann.PublishedMsgs[i]))
		}
	}
}
