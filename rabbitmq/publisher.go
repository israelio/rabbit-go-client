package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/israelio/rabbit-go-client/internal/frame"
	"github.com/israelio/rabbit-go-client/internal/protocol"
)

// Confirmation represents a publish confirmation (ack or nack)
type Confirmation struct {
	DeliveryTag uint64
	Ack         bool // true for ack, false for nack
}

// ConfirmListener provides a callback-based confirm interface
type ConfirmListener interface {
	HandleAck(deliveryTag uint64, multiple bool)
	HandleNack(deliveryTag uint64, multiple bool)
}

// confirmManager manages publisher confirms
type confirmManager struct {
	enabled bool
	mu      sync.RWMutex

	// Pending confirmations indexed by delivery tag
	pending map[uint64]chan Confirmation

	// Notification channels
	listeners []chan Confirmation

	// Callback listeners
	callbacks []ConfirmListener

	// Last confirmed delivery tag (for tracking multiple confirmations)
	lastConfirmed uint64
}

// newConfirmManager creates a new confirm manager
func newConfirmManager() *confirmManager {
	return &confirmManager{
		enabled:       false,
		pending:       make(map[uint64]chan Confirmation),
		listeners:     make([]chan Confirmation, 0),
		callbacks:     make([]ConfirmListener, 0),
		lastConfirmed: 0,
	}
}

// handleAck processes a Basic.Ack confirmation
func (cm *confirmManager) handleAck(deliveryTag uint64, multiple bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if multiple {
		// Confirm all messages from lastConfirmed+1 to deliveryTag
		// First, handle pending waiters
		for tag, waiter := range cm.pending {
			if tag > cm.lastConfirmed && tag <= deliveryTag {
				select {
				case waiter <- Confirmation{DeliveryTag: tag, Ack: true}:
				default:
				}
				delete(cm.pending, tag)
			}
		}

		// Notify listeners for ALL tags in range (not just pending ones)
		for tag := cm.lastConfirmed + 1; tag <= deliveryTag; tag++ {
			for _, listener := range cm.listeners {
				select {
				case listener <- Confirmation{DeliveryTag: tag, Ack: true}:
				default:
				}
			}
		}

		cm.lastConfirmed = deliveryTag
	} else {
		// Confirm single message
		if waiter, exists := cm.pending[deliveryTag]; exists {
			select {
			case waiter <- Confirmation{DeliveryTag: deliveryTag, Ack: true}:
			default:
			}
			delete(cm.pending, deliveryTag)
		}

		// Notify listeners
		for _, listener := range cm.listeners {
			select {
			case listener <- Confirmation{DeliveryTag: deliveryTag, Ack: true}:
			default:
			}
		}

		// Update lastConfirmed if this is the highest tag so far
		if deliveryTag > cm.lastConfirmed {
			cm.lastConfirmed = deliveryTag
		}
	}

	// Notify callbacks
	for _, callback := range cm.callbacks {
		go callback.HandleAck(deliveryTag, multiple)
	}
}

// handleNack processes a Basic.Nack confirmation
func (cm *confirmManager) handleNack(deliveryTag uint64, multiple bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if multiple {
		// Nack all messages from lastConfirmed+1 to deliveryTag
		// First, handle pending waiters
		for tag, waiter := range cm.pending {
			if tag > cm.lastConfirmed && tag <= deliveryTag {
				select {
				case waiter <- Confirmation{DeliveryTag: tag, Ack: false}:
				default:
				}
				delete(cm.pending, tag)
			}
		}

		// Notify listeners for ALL tags in range (not just pending ones)
		for tag := cm.lastConfirmed + 1; tag <= deliveryTag; tag++ {
			for _, listener := range cm.listeners {
				select {
				case listener <- Confirmation{DeliveryTag: tag, Ack: false}:
				default:
				}
			}
		}

		cm.lastConfirmed = deliveryTag
	} else {
		// Nack single message
		if waiter, exists := cm.pending[deliveryTag]; exists {
			select {
			case waiter <- Confirmation{DeliveryTag: deliveryTag, Ack: false}:
			default:
			}
			delete(cm.pending, deliveryTag)
		}

		// Notify listeners
		for _, listener := range cm.listeners {
			select {
			case listener <- Confirmation{DeliveryTag: deliveryTag, Ack: false}:
			default:
			}
		}

		// Update lastConfirmed if this is the highest tag so far
		if deliveryTag > cm.lastConfirmed {
			cm.lastConfirmed = deliveryTag
		}
	}

	// Notify callbacks
	for _, callback := range cm.callbacks {
		go callback.HandleNack(deliveryTag, multiple)
	}
}

// registerPending registers a pending confirmation
func (cm *confirmManager) registerPending(deliveryTag uint64) chan Confirmation {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	waiter := make(chan Confirmation, 1)
	cm.pending[deliveryTag] = waiter
	return waiter
}

// addListener adds a notification channel
func (cm *confirmManager) addListener(listener chan Confirmation) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.listeners = append(cm.listeners, listener)
}

// addCallback adds a callback listener
func (cm *confirmManager) addCallback(callback ConfirmListener) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.callbacks = append(cm.callbacks, callback)
}

// ConfirmSelect enables publisher confirms on this channel
func (ch *Channel) ConfirmSelect(noWait bool) error {
	if ch.GetState() != ChannelStateOpen {
		return ErrChannelClosed
	}

	// Initialize confirm manager if needed
	if ch.confirms == nil {
		ch.confirms = newConfirmManager()
	}

	builder := frame.NewMethodArgsBuilder()
	builder.WriteFlags(noWait) // no-wait flag

	if noWait {
		methodFrame := frame.NewMethodFrame(ch.id, protocol.ClassConfirm, protocol.MethodConfirmSelect, builder.Bytes())
		if err := ch.sendFrame(methodFrame); err != nil {
			return err
		}
		ch.confirms.enabled = true
		ch.nextPublishSeq.Store(0) // Start at 0, first publish will increment to 1
		return nil
	}

	method, err := ch.rpcCall(protocol.ClassConfirm, protocol.MethodConfirmSelect, builder.Bytes())
	if err != nil {
		return err
	}

	if method.MethodID != protocol.MethodConfirmSelectOk {
		return fmt.Errorf("unexpected response to Confirm.Select: %d", method.MethodID)
	}

	ch.confirms.enabled = true
	ch.nextPublishSeq.Store(0) // Start at 0, first publish will increment to 1

	return nil
}

// NotifyPublish registers a channel to receive publish confirmations
func (ch *Channel) NotifyPublish(confirmChan chan Confirmation) chan Confirmation {
	if ch.confirms == nil {
		ch.confirms = newConfirmManager()
	}

	ch.confirms.addListener(confirmChan)
	return confirmChan
}

// AddConfirmListener adds a callback-based confirm listener
func (ch *Channel) AddConfirmListener(listener ConfirmListener) {
	if ch.confirms == nil {
		ch.confirms = newConfirmManager()
	}

	ch.confirms.addCallback(listener)
}

// WaitForConfirms waits for all outstanding confirmations with a timeout
func (ch *Channel) WaitForConfirms(timeout time.Duration) error {
	if ch.confirms == nil || !ch.confirms.enabled {
		return fmt.Errorf("publisher confirms not enabled")
	}

	deadline := time.After(timeout)

	for {
		ch.confirms.mu.RLock()
		pendingCount := len(ch.confirms.pending)
		ch.confirms.mu.RUnlock()

		if pendingCount == 0 {
			return nil
		}

		select {
		case <-deadline:
			return fmt.Errorf("timeout waiting for confirmations: %d pending", pendingCount)
		case <-time.After(10 * time.Millisecond):
			// Continue waiting
		}
	}
}

// WaitForConfirmsOrDie waits for confirmations and panics on timeout
func (ch *Channel) WaitForConfirmsOrDie(timeout time.Duration) {
	if err := ch.WaitForConfirms(timeout); err != nil {
		panic(err)
	}
}

// PublishWithConfirm publishes a message and waits for confirmation
func (ch *Channel) PublishWithConfirm(exchange, routingKey string, mandatory, immediate bool, msg Publishing, timeout time.Duration) error {
	if ch.confirms == nil || !ch.confirms.enabled {
		return fmt.Errorf("publisher confirms not enabled")
	}

	// Create waiter channel
	waiter := make(chan Confirmation, 1)

	// We need to atomically: get next seq, register waiter, and publish
	// Use the confirm manager lock to ensure atomicity
	ch.confirms.mu.Lock()

	// Get next sequence (peek at what it will be)
	seqNo := ch.nextPublishSeq.Load() + 1

	// Register waiter for this sequence
	ch.confirms.pending[seqNo] = waiter

	ch.confirms.mu.Unlock()

	// Publish message (this will atomically increment sequence)
	actualSeq, err := ch.publishInternal(context.Background(), exchange, routingKey, mandatory, immediate, msg)
	if err != nil {
		ch.confirms.mu.Lock()
		delete(ch.confirms.pending, seqNo)
		ch.confirms.mu.Unlock()
		return err
	}

	// Verify sequence matches (should always be true with proper locking)
	if actualSeq != seqNo {
		// Unexpected - this shouldn't happen
		ch.confirms.mu.Lock()
		// Move waiter to correct sequence
		delete(ch.confirms.pending, seqNo)
		ch.confirms.pending[actualSeq] = waiter
		seqNo = actualSeq
		ch.confirms.mu.Unlock()
	}

	// Wait for confirmation
	select {
	case conf := <-waiter:
		if !conf.Ack {
			return fmt.Errorf("message nacked by broker")
		}
		return nil
	case <-time.After(timeout):
		ch.confirms.mu.Lock()
		delete(ch.confirms.pending, seqNo)
		ch.confirms.mu.Unlock()
		return fmt.Errorf("confirmation timeout")
	}
}
