# pika
A small RabbitMQ utility wrapper for go services

It provides a simpler abstraction to ease setting up services that communicate with RabbitMQ.
To keep simplicity it makes a few assumptions:
- There is only need for `Topic Exchanges`
- Message body has a format (JSON by default)

Features:
- Consume messages from RabbitMQ
- Publish messages to RabbitMQ
- RPC calls using Direct Reply-to
- PubSub using in-memory communication
- Retry (in memory) for unhandled messages
- Configurable logger
- Configurable protocol (JSON by default)
- Configurable concurrency
- Maintains the connection and channels for you

If you need more features consider a mixed used of the library or simply an alternative.

## Connectiong to RabbitMQ

To connect to RabbitMQ create a new `RabbitConnector`

```go
// Create a connector with a background context and a logger
conn := pika.NewRabbitConnector().
  WithContext(context.Background()).
  WithLogger(logger)

// Connect and handle error if any
if err := conn.Connect(cfg.Rabbit.URL); err != nil {
  return err
}
```

This connection will be reused for all consumers and publishers.

If you have a more complex use-case you can still reuse the connection asking it for a new channel and setting up bindings yourself.
```go
channel, err := conn.Channel()
```

## Consuming messages

To receive messages from RabbitMQ create a `Consumer` of the underlying event type.

```go
type MsgEvent struct {
  Name string `json:"name"`
  Age  int    `json:"age"`
}


func Consumer(ctx context.Context, msg pika.Message) error {
  var event MsgEvent
  if err := msg.Bind(&event); err != nil {
    return err
  }

  // Do something with the event
}

// You can also use a method if you prefer

type MsgConsumer struct{}

func (c MsgConsumer) HandleMessage(ctx context.Context, msg pika.Message) error {
  var event MsgEvent
  if err := msg.Bind(&event); err != nil {
    return err
  }

  // Do something with the event
}
```

Then start the consumer in you main file.
```go

conn.Consume(Consumer, pika.ConsumerOptions{...})

c := Consumer{}
conn.Consume(c.HandleMessage, pika.ConsumerOptions{...})
```

### Consumer Options

The connector knows how to setup the connection based on the passed `ConsumerOptions` returned by the `Consumer`.
At a minimum it needs an `exchange`, `topic` and `queue`.

```go
opts := pika.NewConsumerOptions("exchange", "topic", "queue").
  SetDurable(). // The queue will persist and message will still be able to be queued up in RabbitMQ 
  WithRetry(5, time.Second) // Setup a retry mechanism for messages. It will retry up to 5 time with exponential backoff. It will be done in memory instead of using a dead-letter exchange.
```

## Publishing

To publish messages all you need to do is call the `Publish` method on the `RabbitConnector`.
```go
conn.Publish(MsgEvent{}, pika.PublishOptions{"exchange", "topic"})
```

Internally the connector will create a channel if it doesn't exist yet and bind the exchange and topic to the channel.

## PubSub
If for testing or other use-cases you don't want to connect to a rabbitMq cluster,
You can a PubSub which will handle all communication in memory.

```go
pubsub := pika.NewPubSub()

pubsub.Consume(YourConsumer, ConsumerOptions{"exchange", "topic"})
pubsub.Publish(MsgEvent{}, PublisherOptions{"exchange", "topic"})
```
