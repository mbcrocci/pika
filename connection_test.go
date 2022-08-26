package pika

import (
	"testing"
)

type TestConnector struct {
	logF LogFunc
}

func (c *TestConnector) Connect(url string) error  { return nil }
func (c *TestConnector) Disconnect() error         { return nil }
func (c *TestConnector) Channel() (Channel, error) { return &TestChannel{}, nil }
func (c *TestConnector) SetLogger(f LogFunc)       { c.logF = f }
func (c *TestConnector) Log(msg string)            { c.logF(msg) }

type TestEvent struct{}

func TestConnectorMock(t *testing.T) {
	c := &TestConnector{}
	c.SetLogger(func(s string) {
		t.Log(s)
	})

	consumer := TestConsumer{}

	err := StartConsumer[TestEvent](c, consumer)
	if err != nil {
		t.Log(err)
		t.Fail()
	}

	_, err = CreatePublisher[TestEvent](c, PublisherOptions{})
	if err != nil {
		t.Log(err)
		t.Fail()
	}
}
