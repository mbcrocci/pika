package pika

import (
	"testing"
)

type TestConnector struct {
	logF Logger
}

func (c *TestConnector) Connect(url string) error  { return nil }
func (c *TestConnector) Disconnect() error         { return nil }
func (c *TestConnector) Channel() (Channel, error) { return &TestChannel{}, nil }
func (c *TestConnector) SetLogger(f Logger)        { c.logF = f }
func (c *TestConnector) Logger() Logger            { return c.logF }

type TestEvent struct{}

func TestConnectorMock(t *testing.T) {
	c := &TestConnector{}
	c.SetLogger(testLogger{t: t})

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
