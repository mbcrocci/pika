package pika

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// RPCConsumer is an interface for consumers that handle RPC requests using Direct Reply-to functionality
type RPCConsumer interface {
	HandleMessage(context.Context, Message) (any, error)
}

// RPCContext is a context for RPC calls
type rpcContext struct {
	// channel used to send rpc calls and recive response
	channel       *amqpChannel
	correlationID string
	replyQueue    chan Message
}

type rpcReplyConsumer struct {
	ctx *rpcContext
}

func (c rpcReplyConsumer) HandleMessage(ctx context.Context, msg Message) error {
	log.Println("got reply", msg)
	// only handle replies that match the correlationID
	if msg.correlationID == c.ctx.correlationID {
		c.ctx.replyQueue <- msg
	}
	return nil
}

func (c *rabbitConnector) newRPCContext() error {
	callChannel, err := c.createChannel()
	if err != nil {
		return err
	}

	ctx := new(rpcContext)
	ctx.replyQueue = make(chan Message)
	ctx.channel = callChannel
	ctx.correlationID = generateCollectionID()

	// Setup a consumer to handle replies that only match the correlationID
	// It auto-acks the message
	// consuming on queue: "amq.rabbitmq.reply-to"
	// send message in a (go) channel for the RPCcall
	opts := ConsumerOptions{
		QueueName:    "amq.rabbitmq.reply-to",
		consumerName: "rpc-reply-" + ctx.correlationID}

	// setup consumer channel manually to avoid setting up the queue
	ctx.channel.consuming = true
	ctx.channel.options = opts
	msgs, err := ctx.channel.channel.Consume(
		opts.QueueName,
		opts.consumerName,
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return err
	}

	ctx.channel.delivery = msgs
	c.conPool.Go(func() {
		c.logger.Debug("consuming responses")
		time.Sleep(100 * time.Millisecond)
		ctx.channel.Consume(rpcReplyConsumer{ctx}, opts)
	})

	c.rpc = ctx
	c.logger.Debug("rpc context created", c.rpc.correlationID)
	return nil
}

// RPCCall makes a call to a RPC consumer
func (c *rabbitConnector) RPCCall(topic string, msg any) (Message, error) {
	if c.rpc == nil {
		if err := c.newRPCContext(); err != nil {
			return Message{}, err
		}
	}

	data, err := c.protocol.Marshal(msg)
	if err != nil {
		return Message{}, err
	}

	go func() {
		r := make(chan amqp.Return)
		rr := c.rpc.channel.channel.NotifyReturn(r)

		select {
		case ret1 := <-r:
			c.logger.Debug("return 1", ret1)
		case ret2 := <-rr:
			c.logger.Debug("return 2", ret2)
		}
	}()

	c.logger.Debug("sending rpc call", topic)
	err = c.rpc.channel.channel.PublishWithContext(
		c.ctx,
		"",    // exchange
		topic, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:   c.protocol.ContentType(),
			Body:          data,
			CorrelationId: c.rpc.correlationID,
			ReplyTo:       "amq.rabbitmq.reply-to",
		},
	)
	if err != nil {
		return Message{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c.logger.Debug("waiting for rpc reply")
	select {
	case resp := <-c.rpc.replyQueue:
		return resp, nil
	case <-ctx.Done():
		return Message{}, ctx.Err()
	}
}

func generateCollectionID() string {
	uid, err := uuid.NewV7()
	if err != nil {
		return ""
	}

	return uid.String()
}

type rpcConsumerWrapper struct {
	channel  *amqpChannel
	consumer RPCConsumer
}

func (c rpcConsumerWrapper) HandleMessage(ctx context.Context, msg Message) error {
	c.channel.logger.Debug("got rpc message")
	reply, err := c.consumer.HandleMessage(ctx, msg)
	if err != nil {
		return err
	}

	data, err := c.channel.protocol.Marshal(reply)
	if err != nil {
		return err
	}

	c.channel.logger.Debug("replying to rpc call")
	return c.channel.channel.PublishWithContext(
		c.channel.ctx,
		"", // on purpose
		msg.replyTo,
		false,
		false,
		amqp.Publishing{
			ContentType:   c.channel.protocol.ContentType(),
			Body:          data,
			CorrelationId: msg.correlationID,
		},
	)
}

// RPCRegister registers a consumer to handle RPC requests
func (c *rabbitConnector) RPCRegister(consumer RPCConsumer, opts ConsumerOptions) error {
	c.logger.Debug("registering consumer")
	channel, err := c.prepareConsumer(opts)
	if err != nil {
		return err
	}
	wrapper := rpcConsumerWrapper{consumer: consumer, channel: channel}
	return c.consume(channel, wrapper, opts)
}
