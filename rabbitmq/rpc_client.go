package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// RpcClient provides a simple RPC (Remote Procedure Call) pattern implementation
type RpcClient struct {
	channel        *Channel
	replyQueue     string
	consumerTag    string
	replies        <-chan Delivery
	pending        map[string]chan *Delivery
	mu             sync.RWMutex
	closed         atomic.Bool
	closeChan      chan struct{}
	dispatchDone   chan struct{}
	correlationSeq atomic.Uint64
}

// NewRpcClient creates a new RPC client using the given channel
func NewRpcClient(ch *Channel) (*RpcClient, error) {
	// Declare a temporary queue for replies
	// Note: Not exclusive so other connections (RPC servers) can publish to it
	q, err := ch.QueueDeclare("", QueueDeclareOptions{
		Exclusive:  false,
		AutoDelete: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to declare reply queue: %w", err)
	}

	// Generate consumer tag
	consumerTag := fmt.Sprintf("rpc-client-%d", time.Now().UnixNano())

	// Start consuming from reply queue
	replies, err := ch.Consume(q.Name, consumerTag, ConsumeOptions{
		AutoAck:   true,
		Exclusive: false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to consume reply queue: %w", err)
	}

	client := &RpcClient{
		channel:      ch,
		replyQueue:   q.Name,
		consumerTag:  consumerTag,
		replies:      replies,
		pending:      make(map[string]chan *Delivery),
		closeChan:    make(chan struct{}),
		dispatchDone: make(chan struct{}),
	}

	// Start reply dispatcher
	go client.dispatchReplies()

	return client, nil
}

// Call performs a synchronous RPC call and waits for the response
func (c *RpcClient) Call(ctx context.Context, exchange, routingKey string, msg Publishing) (*Delivery, error) {
	if c.closed.Load() {
		return nil, fmt.Errorf("RPC client is closed")
	}

	// Generate unique correlation ID
	correlationId := fmt.Sprintf("%d-%d", time.Now().UnixNano(), c.correlationSeq.Add(1))

	// Create channel to receive reply
	replyChan := make(chan *Delivery, 1)

	// Register pending request
	c.mu.Lock()
	c.pending[correlationId] = replyChan
	c.mu.Unlock()

	// Cleanup on exit
	defer func() {
		c.mu.Lock()
		delete(c.pending, correlationId)
		c.mu.Unlock()
		close(replyChan)
	}()

	// Prepare message with reply-to and correlation ID
	msg.Properties.ReplyTo = c.replyQueue
	msg.Properties.CorrelationId = correlationId

	// Publish request
	if err := c.channel.Publish(exchange, routingKey, false, false, msg); err != nil {
		return nil, fmt.Errorf("failed to publish RPC request: %w", err)
	}

	// Wait for reply or timeout
	select {
	case reply := <-replyChan:
		if reply == nil {
			return nil, fmt.Errorf("RPC client closed while waiting for reply")
		}
		return reply, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closeChan:
		return nil, fmt.Errorf("RPC client closed")
	}
}

// dispatchReplies routes incoming replies to the correct pending request
func (c *RpcClient) dispatchReplies() {
	defer close(c.dispatchDone)

	for delivery := range c.replies {
		correlationId := delivery.Properties.CorrelationId
		if correlationId == "" {
			continue
		}

		c.mu.RLock()
		replyChan, ok := c.pending[correlationId]
		c.mu.RUnlock()

		if ok {
			// Copy delivery and clear channel reference to prevent accidental acks
			// (reply queue has AutoAck: true, so manual acks would cause errors)
			deliveryCopy := delivery
			deliveryCopy.channel = nil

			// Non-blocking send
			select {
			case replyChan <- &deliveryCopy:
			default:
				// Reply channel is full or closed, skip
			}
		}
	}

	// Notify all pending requests that we're closing
	// Send nil to signal closure, but don't close the channels
	// (they will be closed by their respective Call() defer statements)
	c.mu.Lock()
	for _, ch := range c.pending {
		select {
		case ch <- nil:
		default:
			// Channel is full, skip
		}
	}
	c.pending = make(map[string]chan *Delivery)
	c.mu.Unlock()
}

// Close closes the RPC client and cleans up resources
func (c *RpcClient) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	// Cancel the consumer to stop receiving messages
	if err := c.channel.BasicCancel(c.consumerTag, false); err != nil {
		// Best effort - consumer might already be cancelled
	}

	// Close the close channel to signal any waiting Call() operations
	close(c.closeChan)

	// Wait for dispatch goroutine to finish (with timeout)
	select {
	case <-c.dispatchDone:
		// Dispatch goroutine finished cleanly
	case <-time.After(2 * time.Second):
		// Timeout - dispatch goroutine didn't finish
	}

	return nil
}
