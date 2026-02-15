package rabbitmq

import (
	"fmt"
	"time"

	"github.com/israelio/rabbit-go-client/internal/frame"
	"github.com/israelio/rabbit-go-client/internal/protocol"
)

// ConsumerCallback is a callback-based consumer interface (matching Java's Consumer)
type ConsumerCallback interface {
	HandleConsumeOk(consumerTag string)
	HandleCancelOk(consumerTag string)
	HandleCancel(consumerTag string) error
	HandleDelivery(consumerTag string, delivery Delivery) error
	HandleShutdown(consumerTag string, cause *Error)
	HandleRecoverOk(consumerTag string)
}

// DefaultConsumer provides default no-op implementations of ConsumerCallback
type DefaultConsumer struct{}

// HandleConsumeOk is called when the consumer is successfully registered
func (dc *DefaultConsumer) HandleConsumeOk(consumerTag string) {}

// HandleCancelOk is called when the consumer is successfully cancelled
func (dc *DefaultConsumer) HandleCancelOk(consumerTag string) {}

// HandleCancel is called when the server cancels the consumer
func (dc *DefaultConsumer) HandleCancel(consumerTag string) error {
	return nil
}

// HandleDelivery is called when a message is delivered
func (dc *DefaultConsumer) HandleDelivery(consumerTag string, delivery Delivery) error {
	return nil
}

// HandleShutdown is called when the channel shuts down
func (dc *DefaultConsumer) HandleShutdown(consumerTag string, cause *Error) {}

// HandleRecoverOk is called after successful recovery
func (dc *DefaultConsumer) HandleRecoverOk(consumerTag string) {}

// DeliveryHandlerFunc is a function-based delivery handler (like Java's DeliverCallback)
type DeliveryHandlerFunc func(consumerTag string, delivery Delivery) error

// CancelHandlerFunc handles consumer cancellation
type CancelHandlerFunc func(consumerTag string) error

// ConsumeWithCallback starts a consumer with a callback interface
func (ch *Channel) ConsumeWithCallback(queue, consumerTag string, opts ConsumeOptions, callback ConsumerCallback) error {
	if ch.GetState() != ChannelStateOpen {
		return ErrChannelClosed
	}

	// Generate consumer tag if not provided
	if consumerTag == "" {
		consumerTag = generateConsumerTag(queue, ch.id)
	}

	// Register consumer
	consumer := &consumerState{
		tag:       consumerTag,
		queue:     queue,
		callback:  callback,
		cancelChan: make(chan struct{}),
		autoAck:   opts.AutoAck,
		exclusive: opts.Exclusive,
		noLocal:   opts.NoLocal,
		args:      opts.Args,
	}

	ch.consumerMux.Lock()
	ch.consumers[consumerTag] = consumer
	ch.consumerMux.Unlock()

	// Send Basic.Consume
	builder := frame.NewMethodArgsBuilder()
	builder.WriteUint16(0) // ticket (deprecated, always 0)
	builder.WriteShortString(queue)
	builder.WriteShortString(consumerTag)
	// Pack flags: no-local, no-ack, exclusive, no-wait
	builder.WriteFlags(opts.NoLocal, opts.AutoAck, opts.Exclusive, opts.NoWait)
	builder.WriteTable(opts.Args)

	if opts.NoWait {
		methodFrame := frame.NewMethodFrame(ch.id, protocol.ClassBasic, protocol.MethodBasicConsume, builder.Bytes())
		if err := ch.sendFrame(methodFrame); err != nil {
			ch.consumerMux.Lock()
			delete(ch.consumers, consumerTag)
			ch.consumerMux.Unlock()
			return err
		}
		// Notify callback
		callback.HandleConsumeOk(consumerTag)
	} else {
		method, err := ch.rpcCall(protocol.ClassBasic, protocol.MethodBasicConsume, builder.Bytes())
		if err != nil {
			ch.consumerMux.Lock()
			delete(ch.consumers, consumerTag)
			ch.consumerMux.Unlock()
			return err
		}

		if method.MethodID != protocol.MethodBasicConsumeOk {
			ch.consumerMux.Lock()
			delete(ch.consumers, consumerTag)
			ch.consumerMux.Unlock()
			return ErrCommandInvalid
		}

		// Parse response and notify callback
		args := frame.NewMethodArgs(method.Args)
		returnedTag, _ := args.ReadShortString()
		callback.HandleConsumeOk(returnedTag)
	}

	return nil
}

// ConsumeWithHandler starts a consumer with a simple function handler
func (ch *Channel) ConsumeWithHandler(queue, consumerTag string, opts ConsumeOptions, handler DeliveryHandlerFunc) error {
	// Wrap handler in a consumer callback
	consumer := &handlerConsumer{
		DefaultConsumer: DefaultConsumer{},
		handler:         handler,
	}

	return ch.ConsumeWithCallback(queue, consumerTag, opts, consumer)
}

// handlerConsumer wraps a DeliveryHandlerFunc
type handlerConsumer struct {
	DefaultConsumer
	handler DeliveryHandlerFunc
}

// HandleDelivery delegates to the handler function
func (hc *handlerConsumer) HandleDelivery(consumerTag string, delivery Delivery) error {
	return hc.handler(consumerTag, delivery)
}

// generateConsumerTag generates a unique consumer tag
func generateConsumerTag(queue string, channelID uint16) string {
	return fmt.Sprintf("ctag-%s-%d-%d", queue, channelID, time.Now().UnixNano())
}
