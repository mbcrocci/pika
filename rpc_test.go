package pika_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mbcrocci/pika/v2"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

type Prefix string

const PrefixKey Prefix = "prefix"

type testLogger struct {
	t      *testing.T
	prefix string
}

func (l testLogger) Debug(args ...any) { l.t.Log("[debug]", l.prefix, args) }
func (l testLogger) Info(args ...any)  { l.t.Log("[info]", l.prefix, args) }
func (l testLogger) Warn(args ...any)  { l.t.Log("[warn]", l.prefix, args) }
func (l testLogger) Error(args ...any) { l.t.Log("[error]", l.prefix, args) }

func newPikaConnection(t *testing.T, ctx context.Context, url string) (pika.Connector, func()) {
	prefix, ok := ctx.Value(PrefixKey).(string)
	if !ok {
		prefix = "[pika]"
	}
	l := testLogger{t, prefix}
	conn := pika.NewRabbitConnector().WithLogger(l).WithContext(ctx)
	err := conn.Connect(url)
	if err != nil {
		t.Logf("failed to connect: %s", err)
		t.FailNow()
	}
	disconnect := func() {
		if err := conn.Disconnect(); err != nil {
			t.Logf("failed to disconnect: %s", err)
			t.FailNow()
		}
	}

	return conn, disconnect
}

type testRPCMessage struct {
	Message string
	A, B    int
}

type testRPCConsumer struct{}

func (c testRPCConsumer) HandleMessage(ctx context.Context, msg pika.Message) (any, error) {
	var m testRPCMessage
	if err := msg.Bind(&m); err != nil {
		return pika.Message{}, err
	}

	return testRPCMessage{
		Message: m.Message,
		A:       m.A + 1,
		B:       m.B + 1,
	}, nil
}

func TestRPC(t *testing.T) {
	ctx := context.Background()

	rabbitmqContainer, err := rabbitmq.Run(ctx,
		"rabbitmq:4.0.3-management-alpine",
		rabbitmq.WithAdminUsername("guest"),
		rabbitmq.WithAdminPassword("guest"),
	)
	defer func() {
		if err := testcontainers.TerminateContainer(rabbitmqContainer); err != nil {
			t.Logf("failed to terminate container: %s", err)
		}
	}()
	if err != nil {
		t.Logf("failed to start container: %s", err)
		t.FailNow()
	}

	err = rabbitmqContainer.Start(ctx)
	if err != nil {
		t.Logf("failed to start container: %s", err)
		t.FailNow()
	}

	url, err := rabbitmqContainer.AmqpURL(ctx)
	if err != nil {
		t.Logf("failed to get url: %s", err)
		t.FailNow()
	}

	t.Log("connecting to rabbitmq")

	conn1, _ := newPikaConnection(t, context.WithValue(ctx, PrefixKey, "[recv]"), url)
	conn2, _ := newPikaConnection(t, context.WithValue(ctx, PrefixKey, "[call]"), url)

	t.Log("pikas connected")

	queue := "test.rpc"

	err = conn1.RPCRegister("test", queue, testRPCConsumer{})
	if err != nil {
		t.Logf("unable to register consumer: %s", err)
		t.FailNow()
	}

	// Wait a bit to make sure the consumer is ready
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 10; i++ {
		result, err := conn2.RPCCall(queue, testRPCMessage{fmt.Sprint("some message", i), i, i + 1})
		if err != nil {
			t.Logf("failed to call rpc: %s", err)
			t.FailNow()
		}

		expected := testRPCMessage{fmt.Sprint("some message", i), i + 1, i + 2}
		got := testRPCMessage{}
		if err := result.Bind(&got); err != nil {
			t.Logf("failed to bind result: %s", err)
			t.FailNow()
		}

		if got != expected {
			t.Logf("expected: %v, got: %v", expected, got)
			t.FailNow()
		}
	}
}
