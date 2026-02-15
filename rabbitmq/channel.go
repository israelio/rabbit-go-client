package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/israelio/rabbit-go-client/internal/frame"
	"github.com/israelio/rabbit-go-client/internal/protocol"
)

// ChannelState represents the state of a channel
type ChannelState int32

const (
	ChannelStateOpen ChannelState = iota
	ChannelStateClosing
	ChannelStateClosed
)

// Channel represents an AMQP channel
type Channel struct {
	conn *Connection
	id   uint16

	// State
	state     atomic.Int32
	closeOnce sync.Once
	closeChan chan *Error
	closed    chan struct{}

	// Frame handling
	incomingFrames chan *frame.Frame
	frameMux       sync.Mutex

	// RPC calls
	rpcMux    sync.Mutex
	rpcWaiters map[uint32]chan *frame.Method
	rpcSeq    uint32

	// Flow control
	flow     atomic.Bool
	flowChan chan bool

	// Publisher confirms
	confirmMux    sync.RWMutex
	confirms      *confirmManager
	nextPublishSeq atomic.Uint64

	// Returns (unroutable messages)
	returnMux     sync.RWMutex
	returnChans   []chan Return
	returnListeners []ReturnListener

	// Consumers
	consumerMux sync.RWMutex
	consumers   map[string]*consumerState

	// QoS settings
	prefetchCount int
	prefetchSize  int
	globalQos     bool

	// Transaction mode
	txMode atomic.Bool
}

// consumerState tracks an active consumer
type consumerState struct {
	tag          string
	queue        string
	callback     ConsumerCallback
	deliveryChan chan Delivery
	cancelChan   chan struct{}
	autoAck      bool
	exclusive    bool
	noLocal      bool
	args         Table
}

// ConsumeOptions configures consumer behavior
type ConsumeOptions struct {
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      Table
}

// open opens the channel
func (ch *Channel) open(ctx context.Context) error {
	// Start frame processor
	go ch.frameProcessor()

	// Send Channel.Open
	builder := frame.NewMethodArgsBuilder()
	builder.WriteShortString("") // reserved

	method, err := ch.rpcCall(protocol.ClassChannel, protocol.MethodChannelOpen, builder.Bytes())
	if err != nil {
		return fmt.Errorf("channel open: %w", err)
	}

	if method.MethodID != protocol.MethodChannelOpenOk {
		return fmt.Errorf("unexpected response to Channel.Open: %d", method.MethodID)
	}

	ch.state.Store(int32(ChannelStateOpen))
	return nil
}

// frameProcessor processes incoming frames for this channel
func (ch *Channel) frameProcessor() {
	for {
		select {
		case <-ch.closed:
			return
		case f := <-ch.incomingFrames:
			if err := ch.handleFrame(f); err != nil {
				ch.forceClose()
				return
			}
		}
	}
}

// handleFrame handles a single frame
func (ch *Channel) handleFrame(f *frame.Frame) error {
	switch f.Type {
	case protocol.FrameMethod:
		return ch.handleMethodFrame(f)
	case protocol.FrameHeader:
		return ch.handleHeaderFrame(f)
	case protocol.FrameBody:
		return ch.handleBodyFrame(f)
	default:
		return fmt.Errorf("unexpected frame type: %d", f.Type)
	}
}

// handleMethodFrame handles method frames
func (ch *Channel) handleMethodFrame(f *frame.Frame) error {
	method, err := f.ParseMethod()
	if err != nil {
		return err
	}

	switch method.ClassID {
	case protocol.ClassChannel:
		return ch.handleChannelMethod(method)
	case protocol.ClassBasic:
		return ch.handleBasicMethod(method)
	default:
		// Check if this is a response to an RPC call
		return ch.deliverRPCResponse(method)
	}
}

// handleChannelMethod handles channel class methods
func (ch *Channel) handleChannelMethod(method *frame.Method) error {
	switch method.MethodID {
	case protocol.MethodChannelClose:
		return ch.handleChannelClose(method)
	case protocol.MethodChannelFlow:
		return ch.handleChannelFlow(method)
	default:
		return ch.deliverRPCResponse(method)
	}
}

// handleChannelClose processes Channel.Close
func (ch *Channel) handleChannelClose(method *frame.Method) error {
	args := frame.NewMethodArgs(method.Args)
	replyCode, _ := args.ReadUint16()
	replyText, _ := args.ReadShortString()

	// Send Channel.CloseOk
	builder := frame.NewMethodArgsBuilder()
	closeOkFrame := frame.NewMethodFrame(ch.id, protocol.ClassChannel, protocol.MethodChannelCloseOk, builder.Bytes())
	ch.sendFrame(closeOkFrame)

	// Close channel
	err := NewError(int(replyCode), replyText, true)
	ch.closeWithError(err)

	return nil
}

// handleChannelFlow processes Channel.Flow
func (ch *Channel) handleChannelFlow(method *frame.Method) error {
	args := frame.NewMethodArgs(method.Args)
	active, _ := args.ReadBool()

	ch.flow.Store(active)

	// Send Channel.FlowOk
	builder := frame.NewMethodArgsBuilder()
	builder.WriteBool(active)
	flowOkFrame := frame.NewMethodFrame(ch.id, protocol.ClassChannel, protocol.MethodChannelFlowOk, builder.Bytes())
	ch.sendFrame(flowOkFrame)

	// Notify flow channel
	select {
	case ch.flowChan <- active:
	default:
	}

	return nil
}

// handleBasicMethod handles basic class methods
func (ch *Channel) handleBasicMethod(method *frame.Method) error {
	switch method.MethodID {
	case protocol.MethodBasicDeliver:
		return ch.handleBasicDeliver(method)
	case protocol.MethodBasicReturn:
		return ch.handleBasicReturn(method)
	case protocol.MethodBasicAck:
		return ch.handleBasicAck(method)
	case protocol.MethodBasicNack:
		return ch.handleBasicNack(method)
	case protocol.MethodBasicCancel:
		return ch.handleBasicCancel(method)
	default:
		return ch.deliverRPCResponse(method)
	}
}

// handleBasicDeliver processes Basic.Deliver (message delivery to consumer)
func (ch *Channel) handleBasicDeliver(method *frame.Method) error {
	// Parse delivery info
	args := frame.NewMethodArgs(method.Args)
	consumerTag, _ := args.ReadShortString()
	deliveryTag, _ := args.ReadUint64()
	redelivered, _ := args.ReadBool()
	exchange, _ := args.ReadShortString()
	routingKey, _ := args.ReadShortString()

	// Read content header and body
	properties, body, err := ch.readContent()
	if err != nil {
		return err
	}

	// Create delivery
	delivery := Delivery{
		ConsumerTag: consumerTag,
		DeliveryTag: deliveryTag,
		Redelivered: redelivered,
		Exchange:    exchange,
		RoutingKey:  routingKey,
		Properties:  properties,
		Body:        body,
		channel:     ch,
	}

	// Deliver to consumer
	ch.consumerMux.RLock()
	consumer, exists := ch.consumers[consumerTag]
	ch.consumerMux.RUnlock()

	if !exists {
		return fmt.Errorf("delivery for unknown consumer: %s", consumerTag)
	}

	// Note: If consumer.autoAck is true, we already told RabbitMQ to auto-ack
	// by setting no-ack=true in Basic.Consume, so we don't need to manually ack here

	// Dispatch delivery
	if consumer.callback != nil {
		// Callback-based consumer
		go func() {
			if err := consumer.callback.HandleDelivery(consumerTag, delivery); err != nil {
				if ch.conn.factory.ErrorHandler != nil {
					ch.conn.factory.ErrorHandler.HandleConsumerError(ch, consumerTag, err)
				}
			}
		}()
	} else if consumer.deliveryChan != nil {
		// Channel-based consumer
		select {
		case consumer.deliveryChan <- delivery:
		case <-consumer.cancelChan:
		case <-ch.closed:
		}
	}

	return nil
}

// handleBasicReturn processes Basic.Return (unroutable message)
func (ch *Channel) handleBasicReturn(method *frame.Method) error {
	// Parse return info
	args := frame.NewMethodArgs(method.Args)
	replyCode, _ := args.ReadUint16()
	replyText, _ := args.ReadShortString()
	exchange, _ := args.ReadShortString()
	routingKey, _ := args.ReadShortString()

	// Read content header and body
	properties, body, err := ch.readContent()
	if err != nil {
		return err
	}

	// Create return
	ret := Return{
		ReplyCode:  replyCode,
		ReplyText:  replyText,
		Exchange:   exchange,
		RoutingKey: routingKey,
		Properties: properties,
		Body:       body,
	}

	// Notify return channels
	ch.returnMux.RLock()
	defer ch.returnMux.RUnlock()

	for _, returnChan := range ch.returnChans {
		select {
		case returnChan <- ret:
		default:
		}
	}

	// Notify return listeners
	for _, listener := range ch.returnListeners {
		go func(l ReturnListener) {
			l.HandleReturn(ret)
		}(listener)
	}

	return nil
}

// handleBasicAck processes Basic.Ack (publisher confirm)
func (ch *Channel) handleBasicAck(method *frame.Method) error {
	args := frame.NewMethodArgs(method.Args)
	deliveryTag, _ := args.ReadUint64()
	multiple, _ := args.ReadBool()

	if ch.confirms != nil {
		ch.confirms.handleAck(deliveryTag, multiple)
	}

	return nil
}

// handleBasicNack processes Basic.Nack (publisher negative confirm)
func (ch *Channel) handleBasicNack(method *frame.Method) error {
	args := frame.NewMethodArgs(method.Args)
	deliveryTag, _ := args.ReadUint64()
	multiple, _ := args.ReadBool()

	if ch.confirms != nil {
		ch.confirms.handleNack(deliveryTag, multiple)
	}

	return nil
}

// handleBasicCancel processes Basic.Cancel (server-side consumer cancellation)
func (ch *Channel) handleBasicCancel(method *frame.Method) error {
	args := frame.NewMethodArgs(method.Args)
	consumerTag, _ := args.ReadShortString()

	ch.consumerMux.Lock()
	consumer, exists := ch.consumers[consumerTag]
	if exists {
		delete(ch.consumers, consumerTag)
	}
	ch.consumerMux.Unlock()

	if exists && consumer.callback != nil {
		go consumer.callback.HandleCancel(consumerTag)
	}

	return nil
}

// handleHeaderFrame handles content header frames
func (ch *Channel) handleHeaderFrame(f *frame.Frame) error {
	// Header frames are handled by readContent()
	return nil
}

// handleBodyFrame handles content body frames
func (ch *Channel) handleBodyFrame(f *frame.Frame) error {
	// Body frames are handled by readContent()
	return nil
}

// readContent reads content header and body frames
func (ch *Channel) readContent() (Properties, []byte, error) {
	// Read header frame
	headerFrame := <-ch.incomingFrames
	if headerFrame.Type != protocol.FrameHeader {
		return Properties{}, nil, fmt.Errorf("expected header frame, got %d", headerFrame.Type)
	}

	header, err := headerFrame.ParseHeader()
	if err != nil {
		return Properties{}, nil, err
	}

	// Decode properties
	properties, err := DecodeProperties(header.Properties)
	if err != nil {
		return Properties{}, nil, err
	}

	// Read body frames
	bodySize := header.BodySize
	body := make([]byte, 0, bodySize)

	for uint64(len(body)) < bodySize {
		bodyFrame := <-ch.incomingFrames
		if bodyFrame.Type != protocol.FrameBody {
			return Properties{}, nil, fmt.Errorf("expected body frame, got %d", bodyFrame.Type)
		}

		bodyContent, err := bodyFrame.ParseBody()
		if err != nil {
			return Properties{}, nil, err
		}

		body = append(body, bodyContent.Data...)
	}

	return properties, body, nil
}

// Publish publishes a message to an exchange
func (ch *Channel) Publish(exchange, routingKey string, mandatory, immediate bool, msg Publishing) error {
	_, err := ch.publishInternal(context.Background(), exchange, routingKey, mandatory, immediate, msg)
	return err
}

// PublishWithContext publishes a message with context support
func (ch *Channel) PublishWithContext(ctx context.Context, exchange, routingKey string, mandatory, immediate bool, msg Publishing) error {
	_, err := ch.publishInternal(ctx, exchange, routingKey, mandatory, immediate, msg)
	return err
}

// publishInternal is the internal publish implementation that returns the sequence number
func (ch *Channel) publishInternal(ctx context.Context, exchange, routingKey string, mandatory, immediate bool, msg Publishing) (uint64, error) {
	if ch.GetState() != ChannelStateOpen {
		return 0, ErrChannelClosed
	}

	// Encode properties
	propData, err := EncodeProperties(msg.Properties)
	if err != nil {
		return 0, fmt.Errorf("encode properties: %w", err)
	}

	// Build Basic.Publish method
	builder := frame.NewMethodArgsBuilder()
	builder.WriteUint16(0) // ticket (deprecated, always 0)
	builder.WriteShortString(exchange)
	builder.WriteShortString(routingKey)
	// Pack flags: mandatory, immediate
	builder.WriteFlags(mandatory, immediate)

	methodFrame := frame.NewMethodFrame(ch.id, protocol.ClassBasic, protocol.MethodBasicPublish, builder.Bytes())

	// Build content header frame
	headerFrame := frame.NewHeaderFrame(ch.id, protocol.ClassBasic, uint64(len(msg.Body)), propData)

	// Build body frames
	bodyFrames := ch.splitBody(msg.Body)

	// Track publish sequence for confirms (before sending)
	// This must be done before sending to ensure proper sequencing
	var seqNo uint64
	if ch.confirms != nil && ch.confirms.enabled {
		seqNo = ch.nextPublishSeq.Add(1)
	}

	// Send frames
	ch.frameMux.Lock()
	defer ch.frameMux.Unlock()

	if err := ch.sendFrame(methodFrame); err != nil {
		return seqNo, err
	}

	if err := ch.sendFrame(headerFrame); err != nil {
		return seqNo, err
	}

	for _, bodyFrame := range bodyFrames {
		if err := ch.sendFrame(bodyFrame); err != nil {
			return seqNo, err
		}
	}

	return seqNo, nil
}

// splitBody splits message body into frames
func (ch *Channel) splitBody(body []byte) []*frame.Frame {
	if len(body) == 0 {
		return []*frame.Frame{}
	}

	maxPayload := int(ch.conn.frameMax - protocol.FrameHeaderSize - protocol.FrameEndSize)
	frameCount := (len(body) + maxPayload - 1) / maxPayload

	frames := make([]*frame.Frame, frameCount)
	offset := 0

	for i := 0; i < frameCount; i++ {
		end := offset + maxPayload
		if end > len(body) {
			end = len(body)
		}

		frames[i] = frame.NewBodyFrame(ch.id, body[offset:end])
		offset = end
	}

	return frames
}

// Consume starts consuming messages from a queue
func (ch *Channel) Consume(queue, consumerTag string, opts ConsumeOptions) (<-chan Delivery, error) {
	if ch.GetState() != ChannelStateOpen {
		return nil, ErrChannelClosed
	}

	// Generate consumer tag if not provided
	if consumerTag == "" {
		consumerTag = fmt.Sprintf("ctag-%s-%d", queue, ch.id)
	}

	// Create delivery channel
	deliveryChan := make(chan Delivery, 100)

	// Register consumer
	consumer := &consumerState{
		tag:          consumerTag,
		queue:        queue,
		deliveryChan: deliveryChan,
		cancelChan:   make(chan struct{}),
		autoAck:      opts.AutoAck,
		exclusive:    opts.Exclusive,
		noLocal:      opts.NoLocal,
		args:         opts.Args,
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
			return nil, err
		}
	} else {
		method, err := ch.rpcCall(protocol.ClassBasic, protocol.MethodBasicConsume, builder.Bytes())
		if err != nil {
			ch.consumerMux.Lock()
			delete(ch.consumers, consumerTag)
			ch.consumerMux.Unlock()
			return nil, err
		}

		if method.MethodID != protocol.MethodBasicConsumeOk {
			ch.consumerMux.Lock()
			delete(ch.consumers, consumerTag)
			ch.consumerMux.Unlock()
			return nil, fmt.Errorf("unexpected response to Basic.Consume: %d", method.MethodID)
		}
	}

	return deliveryChan, nil
}

// BasicGet polls a message from a queue
func (ch *Channel) BasicGet(queue string, autoAck bool) (*GetResponse, bool, error) {
	if ch.GetState() != ChannelStateOpen {
		return nil, false, ErrChannelClosed
	}

	// Send Basic.Get
	builder := frame.NewMethodArgsBuilder()
	builder.WriteUint16(0) // ticket (deprecated, always 0)
	builder.WriteShortString(queue)
	builder.WriteFlags(autoAck) // no-ack flag

	method, err := ch.rpcCall(protocol.ClassBasic, protocol.MethodBasicGet, builder.Bytes())
	if err != nil {
		return nil, false, err
	}

	if method.MethodID == protocol.MethodBasicGetEmpty {
		return nil, false, nil
	}

	if method.MethodID != protocol.MethodBasicGetOk {
		return nil, false, fmt.Errorf("unexpected response to Basic.Get: %d", method.MethodID)
	}

	// Parse GetOk
	args := frame.NewMethodArgs(method.Args)
	deliveryTag, _ := args.ReadUint64()
	redelivered, _ := args.ReadBool()
	exchange, _ := args.ReadShortString()
	routingKey, _ := args.ReadShortString()
	messageCount, _ := args.ReadUint32()

	// Read content
	properties, body, err := ch.readContent()
	if err != nil {
		return nil, false, err
	}

	response := &GetResponse{
		DeliveryTag:  deliveryTag,
		Redelivered:  redelivered,
		Exchange:     exchange,
		RoutingKey:   routingKey,
		MessageCount: int(messageCount),
		Properties:   properties,
		Body:         body,
		channel:      ch,
	}

	return response, true, nil
}

// BasicAck acknowledges a delivery
func (ch *Channel) BasicAck(deliveryTag uint64, multiple bool) error {
	if ch.GetState() != ChannelStateOpen {
		return ErrChannelClosed
	}

	builder := frame.NewMethodArgsBuilder()
	builder.WriteUint64(deliveryTag)
	builder.WriteFlags(multiple) // multiple flag

	methodFrame := frame.NewMethodFrame(ch.id, protocol.ClassBasic, protocol.MethodBasicAck, builder.Bytes())
	return ch.sendFrame(methodFrame)
}

// BasicNack negatively acknowledges a delivery
func (ch *Channel) BasicNack(deliveryTag uint64, multiple, requeue bool) error {
	if ch.GetState() != ChannelStateOpen {
		return ErrChannelClosed
	}

	builder := frame.NewMethodArgsBuilder()
	builder.WriteUint64(deliveryTag)
	// Pack flags: multiple, requeue
	builder.WriteFlags(multiple, requeue)

	methodFrame := frame.NewMethodFrame(ch.id, protocol.ClassBasic, protocol.MethodBasicNack, builder.Bytes())
	return ch.sendFrame(methodFrame)
}

// BasicReject rejects a delivery
func (ch *Channel) BasicReject(deliveryTag uint64, requeue bool) error {
	if ch.GetState() != ChannelStateOpen {
		return ErrChannelClosed
	}

	builder := frame.NewMethodArgsBuilder()
	builder.WriteUint64(deliveryTag)
	builder.WriteFlags(requeue) // requeue flag

	methodFrame := frame.NewMethodFrame(ch.id, protocol.ClassBasic, protocol.MethodBasicReject, builder.Bytes())
	return ch.sendFrame(methodFrame)
}

// BasicCancel cancels a consumer
func (ch *Channel) BasicCancel(consumerTag string, noWait bool) error {
	if ch.GetState() != ChannelStateOpen {
		return ErrChannelClosed
	}

	builder := frame.NewMethodArgsBuilder()
	builder.WriteShortString(consumerTag)
	builder.WriteFlags(noWait) // no-wait flag

	if noWait {
		methodFrame := frame.NewMethodFrame(ch.id, protocol.ClassBasic, protocol.MethodBasicCancel, builder.Bytes())
		return ch.sendFrame(methodFrame)
	}

	method, err := ch.rpcCall(protocol.ClassBasic, protocol.MethodBasicCancel, builder.Bytes())
	if err != nil {
		return err
	}

	if method.MethodID != protocol.MethodBasicCancelOk {
		return fmt.Errorf("unexpected response to Basic.Cancel: %d", method.MethodID)
	}

	// Remove consumer
	ch.consumerMux.Lock()
	delete(ch.consumers, consumerTag)
	ch.consumerMux.Unlock()

	return nil
}

// Qos sets the quality of service (prefetch)
func (ch *Channel) Qos(prefetchCount, prefetchSize int, global bool) error {
	if ch.GetState() != ChannelStateOpen {
		return ErrChannelClosed
	}

	builder := frame.NewMethodArgsBuilder()
	builder.WriteUint32(uint32(prefetchSize))
	builder.WriteUint16(uint16(prefetchCount))
	builder.WriteFlags(global) // global flag

	method, err := ch.rpcCall(protocol.ClassBasic, protocol.MethodBasicQos, builder.Bytes())
	if err != nil {
		return err
	}

	if method.MethodID != protocol.MethodBasicQosOk {
		return fmt.Errorf("unexpected response to Basic.Qos: %d", method.MethodID)
	}

	ch.prefetchCount = prefetchCount
	ch.prefetchSize = prefetchSize
	ch.globalQos = global

	return nil
}

// Close closes the channel
func (ch *Channel) Close() error {
	return ch.CloseWithCode(protocol.ReplySuccess, "channel closed")
}

// GetChannelID returns the channel ID (channel number)
func (ch *Channel) GetChannelID() uint16 {
	return ch.id
}

// CloseWithCode closes the channel with a specific reply code
func (ch *Channel) CloseWithCode(code int, text string) error {
	if ch.GetState() != ChannelStateOpen {
		return nil
	}

	ch.state.Store(int32(ChannelStateClosing))

	// Send Channel.Close
	builder := frame.NewMethodArgsBuilder()
	builder.WriteUint16(uint16(code))
	builder.WriteShortString(text)
	builder.WriteUint16(0) // class-id
	builder.WriteUint16(0) // method-id

	method, err := ch.rpcCall(protocol.ClassChannel, protocol.MethodChannelClose, builder.Bytes())
	if err != nil {
		ch.forceClose()
		return err
	}

	if method.MethodID != protocol.MethodChannelCloseOk {
		ch.forceClose()
		return fmt.Errorf("unexpected response to Channel.Close: %d", method.MethodID)
	}

	ch.cleanup()
	return nil
}

// closeWithError closes the channel with an error
func (ch *Channel) closeWithError(err *Error) {
	ch.closeOnce.Do(func() {
		ch.state.Store(int32(ChannelStateClosed))

		select {
		case ch.closeChan <- err:
		default:
		}

		if ch.conn.factory.ErrorHandler != nil {
			ch.conn.factory.ErrorHandler.HandleChannelError(ch, err)
		}

		close(ch.closed)
		ch.cleanup()
	})
}

// forceClose forcefully closes the channel
func (ch *Channel) forceClose() {
	ch.closeWithError(ErrChannelClosed)
}

// cleanup releases channel resources
func (ch *Channel) cleanup() {
	ch.cleanupConsumers()
	ch.removeFromConnection()
}

// cleanupConsumers cancels all consumers and closes their channels
func (ch *Channel) cleanupConsumers() {
	ch.consumerMux.Lock()
	defer ch.consumerMux.Unlock()

	for tag, consumer := range ch.consumers {
		close(consumer.cancelChan)
		if consumer.callback != nil {
			consumer.callback.HandleShutdown(tag, ErrChannelClosed)
		}
		if consumer.deliveryChan != nil {
			close(consumer.deliveryChan)
		}
	}
	ch.consumers = make(map[string]*consumerState)
}

// removeFromConnection removes the channel from the connection's channel map
func (ch *Channel) removeFromConnection() {
	ch.conn.channelMux.Lock()
	delete(ch.conn.channels, ch.id)
	ch.conn.channelMux.Unlock()
}

// GetState returns the current channel state
func (ch *Channel) GetState() ChannelState {
	return ChannelState(ch.state.Load())
}

// IsClosed returns whether the channel is closed
func (ch *Channel) IsClosed() bool {
	return ch.GetState() == ChannelStateClosed
}

// NotifyClose registers a listener for channel closure
func (ch *Channel) NotifyClose(notifyChan chan *Error) chan *Error {
	go func() {
		err := <-ch.closeChan
		notifyChan <- err
	}()
	return notifyChan
}

// NotifyFlow registers a listener for flow control
func (ch *Channel) NotifyFlow(notifyChan chan bool) chan bool {
	ch.flowChan = notifyChan
	return notifyChan
}

// sendFrame sends a frame on this channel
func (ch *Channel) sendFrame(f *frame.Frame) error {
	return ch.conn.frameWriter.WriteFrame(f)
}

// rpcCall performs an RPC-style method call
func (ch *Channel) rpcCall(classID, methodID uint16, args []byte) (*frame.Method, error) {
	ch.rpcMux.Lock()
	seq := ch.rpcSeq
	ch.rpcSeq++
	waiter := make(chan *frame.Method, 1)
	ch.rpcWaiters[seq] = waiter
	ch.rpcMux.Unlock()

	defer func() {
		ch.rpcMux.Lock()
		delete(ch.rpcWaiters, seq)
		ch.rpcMux.Unlock()
	}()

	// Send method frame
	methodFrame := frame.NewMethodFrame(ch.id, classID, methodID, args)
	if err := ch.sendFrame(methodFrame); err != nil {
		return nil, err
	}

	// Wait for response with timeout
	select {
	case response := <-waiter:
		return response, nil
	case <-ch.closed:
		return nil, ErrChannelClosed
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("RPC call timeout: %d.%d", classID, methodID)
	}
}

// deliverRPCResponse delivers a method response to an RPC waiter
func (ch *Channel) deliverRPCResponse(method *frame.Method) error {
	ch.rpcMux.Lock()
	defer ch.rpcMux.Unlock()

	// Deliver to any waiting RPC call (FIFO order)
	if len(ch.rpcWaiters) > 0 {
		// Get the first waiter (map iteration gives us one)
		for seq, waiter := range ch.rpcWaiters {
			waiter <- method
			delete(ch.rpcWaiters, seq)
			return nil
		}
	}

	return fmt.Errorf("unexpected method: %d.%d with no waiters", method.ClassID, method.MethodID)
}
