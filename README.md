# pika
A small RabbitMQ utility wrapper for go services

It provides a simpler abstraction to ease setting up services that communicate with RabbitMQ.
To keep simplicity it makes a few assumptions:
- There is only need for `Topic Exchanges`
- Message body contains JSON
- A `Consumer` only receives a message type

If you need more features consider a mixed used of the library or simply an alternative.

## Connection

To connect to RabbitMQ create a new `RabbitConnector`

```go
func main() {
  logger, err := zap.NewProduction()
  if err != nil { }
  
  conn := pika.NewConnector(logger)
  if err := conn.Connect(RABBIT_URL); err != nil {
    // TODO
  }
}
```

This connection will be reused for each  Consumer, Publisher and Notifier (1 channel each).

If you have a more complex use-case you can still reuse the connection asking it for a new channel and setting up bindings yourself.
```go
  channel, err := conn.Channel()
```

## Consumer

To receive messages from RabbitMQ create a `Consumer` of the underlying event type.

```go
type MsgEvent struct {
  Name string `json:"name"`
  Age  int    `json:"age"`
}

type MsgConsumer struct {}

func (c MsgConsumer) Options() pika.ConsumerOptions {
  // TODO
}

func (c MsgConsumer) HandleMessage(e MsgEvent) error {
  return nil
}
```

Then start the consumer in you main file.
```go
func main() {
  //.. setup logger and connection
  consumer := MsgConsumer{}
  err := pika.StartConsumer[MsgEvent](conn, consumer)
  if err != nil {
    // TODO
  }
  
}
```

### Consumer Options

The connector knows how to setup the connection based on the `ConsumerOptions` returned by the `Consumer`.
At a minimum it needs an `exchange`, `topic` and `queue`.

```go
func (c MsgConsumer) Options() pika.ConsumerOptions {
  return pika.NewConsumerOptions(
    "", // Exchange
    "", // Topic / Routing-Key
    "", // Queue name (leave empty for random name)
  )
}
```

To setup the queue to persist if the application crashes.
```go
  return pika.NewConsumerOptions("", "", "").SetDurable()
```
This allows messages to queue-up and start consuming as soon as it starts

You can also retry messages. It will be done in memory instead of using a dead-letter exchange.
```go
  return pika.NewConsumerOptions("", "", "").WithRetry(1, time.Second)
```

## Publisher

A publisher is a simple abstraction to conform the event type. It only holds a channel.
```go
publisher, err := pika.CreatePublisher[MsgEvent](conn, pika.PublisherOptions{
  Exchange: "",
  Topic: "",
})

// To use
publisher.Publish(MsgEvent{})
```

## Notifier
Sometimes you want to perform operations on regular intervals and them publish the result.

```go
type Notifier[T any] interface {
	Options() NotificationOptions
	Stop() bool
	Notify() (T, error)
}
```
