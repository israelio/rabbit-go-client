package rabbitmq

import (
	"fmt"

	"github.com/israelio/rabbit-go-client/internal/frame"
	"github.com/israelio/rabbit-go-client/internal/protocol"
)

// ExchangeDeclareOptions configures exchange declaration
type ExchangeDeclareOptions struct {
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       Table
}

// ExchangeDeleteOptions configures exchange deletion
type ExchangeDeleteOptions struct {
	IfUnused bool
	NoWait   bool
}

// QueueDeclareOptions configures queue declaration
type QueueDeclareOptions struct {
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       Table
}

// QueueDeleteOptions configures queue deletion
type QueueDeleteOptions struct {
	IfUnused bool
	IfEmpty  bool
	NoWait   bool
}

// ExchangeDeclare declares an exchange
func (ch *Channel) ExchangeDeclare(name, kind string, opts ExchangeDeclareOptions) error {
	if ch.GetState() != ChannelStateOpen {
		return ErrChannelClosed
	}

	builder := frame.NewMethodArgsBuilder()
	builder.WriteUint16(0) // ticket (deprecated, always 0)
	builder.WriteShortString(name)
	builder.WriteShortString(kind)
	// Pack flags: passive, durable, auto-delete, internal, no-wait
	builder.WriteFlags(false, opts.Durable, opts.AutoDelete, opts.Internal, opts.NoWait)
	builder.WriteTable(opts.Args)

	if opts.NoWait {
		methodFrame := frame.NewMethodFrame(ch.id, protocol.ClassExchange, protocol.MethodExchangeDeclare, builder.Bytes())
		return ch.sendFrame(methodFrame)
	}

	method, err := ch.rpcCall(protocol.ClassExchange, protocol.MethodExchangeDeclare, builder.Bytes())
	if err != nil {
		return err
	}

	if method.MethodID != protocol.MethodExchangeDeclareOk {
		return fmt.Errorf("unexpected response to Exchange.Declare: %d", method.MethodID)
	}

	// Record topology for recovery (skip if we're recovering to avoid duplicates)
	if ch.conn.GetState() != StateRecovering {
		ch.conn.recovery.recordExchange(name, kind, opts)
	}

	return nil
}

// ExchangeDeclarePassive checks if an exchange exists
func (ch *Channel) ExchangeDeclarePassive(name, kind string) error {
	if ch.GetState() != ChannelStateOpen {
		return ErrChannelClosed
	}

	builder := frame.NewMethodArgsBuilder()
	builder.WriteUint16(0) // ticket (deprecated, always 0)
	builder.WriteShortString(name)
	builder.WriteShortString(kind)
	// Pack flags: passive, durable, auto-delete, internal, no-wait
	builder.WriteFlags(true, false, false, false, false)
	builder.WriteTable(nil) // args

	method, err := ch.rpcCall(protocol.ClassExchange, protocol.MethodExchangeDeclare, builder.Bytes())
	if err != nil {
		return err
	}

	if method.MethodID != protocol.MethodExchangeDeclareOk {
		return fmt.Errorf("unexpected response to Exchange.DeclarePassive: %d", method.MethodID)
	}

	return nil
}

// ExchangeDelete deletes an exchange
func (ch *Channel) ExchangeDelete(name string, opts ExchangeDeleteOptions) error {
	if ch.GetState() != ChannelStateOpen {
		return ErrChannelClosed
	}

	builder := frame.NewMethodArgsBuilder()
	builder.WriteUint16(0) // ticket (deprecated, always 0)
	builder.WriteShortString(name)
	// Pack flags: if-unused, no-wait
	builder.WriteFlags(opts.IfUnused, opts.NoWait)

	if opts.NoWait {
		methodFrame := frame.NewMethodFrame(ch.id, protocol.ClassExchange, protocol.MethodExchangeDelete, builder.Bytes())
		return ch.sendFrame(methodFrame)
	}

	method, err := ch.rpcCall(protocol.ClassExchange, protocol.MethodExchangeDelete, builder.Bytes())
	if err != nil {
		return err
	}

	if method.MethodID != protocol.MethodExchangeDeleteOk {
		return fmt.Errorf("unexpected response to Exchange.Delete: %d", method.MethodID)
	}

	return nil
}

// ExchangeBind binds an exchange to another exchange
func (ch *Channel) ExchangeBind(destination, source, routingKey string, args Table) error {
	if ch.GetState() != ChannelStateOpen {
		return ErrChannelClosed
	}

	builder := frame.NewMethodArgsBuilder()
	builder.WriteUint16(0) // ticket (deprecated, always 0)
	builder.WriteShortString(destination)
	builder.WriteShortString(source)
	builder.WriteShortString(routingKey)
	builder.WriteFlags(false) // no-wait
	builder.WriteTable(args)

	method, err := ch.rpcCall(protocol.ClassExchange, protocol.MethodExchangeBind, builder.Bytes())
	if err != nil {
		return err
	}

	if method.MethodID != protocol.MethodExchangeBindOk {
		return fmt.Errorf("unexpected response to Exchange.Bind: %d", method.MethodID)
	}

	return nil
}

// ExchangeUnbind unbinds an exchange from another exchange
func (ch *Channel) ExchangeUnbind(destination, source, routingKey string, args Table) error {
	if ch.GetState() != ChannelStateOpen {
		return ErrChannelClosed
	}

	builder := frame.NewMethodArgsBuilder()
	builder.WriteUint16(0) // ticket (deprecated, always 0)
	builder.WriteShortString(destination)
	builder.WriteShortString(source)
	builder.WriteShortString(routingKey)
	builder.WriteFlags(false) // no-wait
	builder.WriteTable(args)

	method, err := ch.rpcCall(protocol.ClassExchange, protocol.MethodExchangeUnbind, builder.Bytes())
	if err != nil {
		return err
	}

	if method.MethodID != protocol.MethodExchangeUnbindOk {
		return fmt.Errorf("unexpected response to Exchange.Unbind: %d", method.MethodID)
	}

	return nil
}

// QueueDeclare declares a queue
func (ch *Channel) QueueDeclare(name string, opts QueueDeclareOptions) (Queue, error) {
	if ch.GetState() != ChannelStateOpen {
		return Queue{}, ErrChannelClosed
	}

	builder := frame.NewMethodArgsBuilder()
	builder.WriteUint16(0)        // ticket (deprecated, always 0)
	builder.WriteShortString(name)
	// Pack flags: passive, durable, exclusive, auto-delete, no-wait
	builder.WriteFlags(false, opts.Durable, opts.Exclusive, opts.AutoDelete, opts.NoWait)
	builder.WriteTable(opts.Args)

	if opts.NoWait {
		methodFrame := frame.NewMethodFrame(ch.id, protocol.ClassQueue, protocol.MethodQueueDeclare, builder.Bytes())
		return Queue{Name: name}, ch.sendFrame(methodFrame)
	}

	method, err := ch.rpcCall(protocol.ClassQueue, protocol.MethodQueueDeclare, builder.Bytes())
	if err != nil {
		return Queue{}, err
	}

	if method.MethodID != protocol.MethodQueueDeclareOk {
		return Queue{}, fmt.Errorf("unexpected response to Queue.Declare: %d", method.MethodID)
	}

	// Parse response
	args := frame.NewMethodArgs(method.Args)
	queueName, _ := args.ReadShortString()
	messageCount, _ := args.ReadUint32()
	consumerCount, _ := args.ReadUint32()

	// Record topology for recovery (skip if we're recovering to avoid duplicates)
	if ch.conn.GetState() != StateRecovering {
		ch.conn.recovery.recordQueue(queueName, opts)
	}

	return Queue{
		Name:      queueName,
		Messages:  int(messageCount),
		Consumers: int(consumerCount),
	}, nil
}

// QueueDeclarePassive checks if a queue exists
func (ch *Channel) QueueDeclarePassive(name string) (Queue, error) {
	if ch.GetState() != ChannelStateOpen {
		return Queue{}, ErrChannelClosed
	}

	builder := frame.NewMethodArgsBuilder()
	builder.WriteUint16(0)        // ticket (deprecated, always 0)
	builder.WriteShortString(name)
	// Pack flags: passive, durable, exclusive, auto-delete, no-wait
	builder.WriteFlags(true, false, false, false, false)
	builder.WriteTable(nil)  // args

	method, err := ch.rpcCall(protocol.ClassQueue, protocol.MethodQueueDeclare, builder.Bytes())
	if err != nil {
		return Queue{}, err
	}

	if method.MethodID != protocol.MethodQueueDeclareOk {
		return Queue{}, fmt.Errorf("unexpected response to Queue.DeclarePassive: %d", method.MethodID)
	}

	// Parse response
	args := frame.NewMethodArgs(method.Args)
	queueName, _ := args.ReadShortString()
	messageCount, _ := args.ReadUint32()
	consumerCount, _ := args.ReadUint32()

	return Queue{
		Name:      queueName,
		Messages:  int(messageCount),
		Consumers: int(consumerCount),
	}, nil
}

// QueueDelete deletes a queue
func (ch *Channel) QueueDelete(name string, opts QueueDeleteOptions) (int, error) {
	if ch.GetState() != ChannelStateOpen {
		return 0, ErrChannelClosed
	}

	builder := frame.NewMethodArgsBuilder()
	builder.WriteUint16(0)        // ticket (deprecated, always 0)
	builder.WriteShortString(name)
	// Pack flags: if-unused, if-empty, no-wait
	builder.WriteFlags(opts.IfUnused, opts.IfEmpty, opts.NoWait)

	if opts.NoWait {
		methodFrame := frame.NewMethodFrame(ch.id, protocol.ClassQueue, protocol.MethodQueueDelete, builder.Bytes())
		return 0, ch.sendFrame(methodFrame)
	}

	method, err := ch.rpcCall(protocol.ClassQueue, protocol.MethodQueueDelete, builder.Bytes())
	if err != nil {
		return 0, err
	}

	if method.MethodID != protocol.MethodQueueDeleteOk {
		return 0, fmt.Errorf("unexpected response to Queue.Delete: %d", method.MethodID)
	}

	// Parse response
	args := frame.NewMethodArgs(method.Args)
	messageCount, _ := args.ReadUint32()

	return int(messageCount), nil
}

// QueueBind binds a queue to an exchange
func (ch *Channel) QueueBind(name, exchange, routingKey string, args Table) error {
	if ch.GetState() != ChannelStateOpen {
		return ErrChannelClosed
	}

	builder := frame.NewMethodArgsBuilder()
	builder.WriteUint16(0)        // ticket (deprecated, always 0)
	builder.WriteShortString(name)
	builder.WriteShortString(exchange)
	builder.WriteShortString(routingKey)
	builder.WriteFlags(false) // no-wait
	builder.WriteTable(args)

	method, err := ch.rpcCall(protocol.ClassQueue, protocol.MethodQueueBind, builder.Bytes())
	if err != nil {
		return err
	}

	if method.MethodID != protocol.MethodQueueBindOk {
		return fmt.Errorf("unexpected response to Queue.Bind: %d", method.MethodID)
	}

	// Record topology for recovery (skip if we're recovering to avoid duplicates)
	if ch.conn.GetState() != StateRecovering {
		ch.conn.recovery.recordBinding(name, exchange, routingKey, args)
	}

	return nil
}

// QueueUnbind unbinds a queue from an exchange
func (ch *Channel) QueueUnbind(name, exchange, routingKey string, args Table) error {
	if ch.GetState() != ChannelStateOpen {
		return ErrChannelClosed
	}

	builder := frame.NewMethodArgsBuilder()
	builder.WriteUint16(0)        // ticket (deprecated, always 0)
	builder.WriteShortString(name)
	builder.WriteShortString(exchange)
	builder.WriteShortString(routingKey)
	builder.WriteTable(args)

	method, err := ch.rpcCall(protocol.ClassQueue, protocol.MethodQueueUnbind, builder.Bytes())
	if err != nil {
		return err
	}

	if method.MethodID != protocol.MethodQueueUnbindOk {
		return fmt.Errorf("unexpected response to Queue.Unbind: %d", method.MethodID)
	}

	return nil
}

// QueuePurge purges all messages from a queue
func (ch *Channel) QueuePurge(name string, noWait bool) (int, error) {
	if ch.GetState() != ChannelStateOpen {
		return 0, ErrChannelClosed
	}

	builder := frame.NewMethodArgsBuilder()
	builder.WriteUint16(0)        // ticket (deprecated, always 0)
	builder.WriteShortString(name)
	builder.WriteFlags(noWait) // no-wait flag

	if noWait {
		methodFrame := frame.NewMethodFrame(ch.id, protocol.ClassQueue, protocol.MethodQueuePurge, builder.Bytes())
		return 0, ch.sendFrame(methodFrame)
	}

	method, err := ch.rpcCall(protocol.ClassQueue, protocol.MethodQueuePurge, builder.Bytes())
	if err != nil {
		return 0, err
	}

	if method.MethodID != protocol.MethodQueuePurgeOk {
		return 0, fmt.Errorf("unexpected response to Queue.Purge: %d", method.MethodID)
	}

	// Parse response
	args := frame.NewMethodArgs(method.Args)
	messageCount, _ := args.ReadUint32()

	return int(messageCount), nil
}
