package rabbitmq

import (
	"fmt"
	"sync"
	"time"
)

// recoveryManager handles automatic connection and topology recovery
type recoveryManager struct {
	enabled  bool
	topology bool
	interval time.Duration
	attempts int

	mu sync.RWMutex

	// Recorded topology for recovery
	exchanges []exchangeDeclaration
	queues    []queueDeclaration
	bindings  []bindingDeclaration
	consumers []consumerDeclaration
}

// exchangeDeclaration records an exchange declaration
type exchangeDeclaration struct {
	name string
	kind string
	opts ExchangeDeclareOptions
}

// queueDeclaration records a queue declaration
type queueDeclaration struct {
	name string
	opts QueueDeclareOptions
}

// bindingDeclaration records a binding
type bindingDeclaration struct {
	queue      string
	exchange   string
	routingKey string
	args       Table
}

// consumerDeclaration records a consumer
type consumerDeclaration struct {
	queue    string
	tag      string
	callback ConsumerCallback
	opts     ConsumeOptions
}

// channelSnapshot captures the state of a channel for recovery
type channelSnapshot struct {
	ch            *Channel // Reference to the actual channel object (will be reused)
	id            uint16
	prefetchCount int
	prefetchSize  int
	globalQos     bool
	confirmMode   bool
	consumers     []consumerSnapshot
}

// consumerSnapshot captures the state of a consumer for recovery
type consumerSnapshot struct {
	queue    string
	tag      string
	callback ConsumerCallback
	opts     ConsumeOptions
}

// newRecoveryManager creates a new recovery manager
func newRecoveryManager(enabled, topology bool, interval time.Duration, attempts int) *recoveryManager {
	return &recoveryManager{
		enabled:   enabled,
		topology:  topology,
		interval:  interval,
		attempts:  attempts,
		exchanges: make([]exchangeDeclaration, 0),
		queues:    make([]queueDeclaration, 0),
		bindings:  make([]bindingDeclaration, 0),
		consumers: make([]consumerDeclaration, 0),
	}
}

// recordExchange records an exchange declaration for recovery
func (rm *recoveryManager) recordExchange(name, kind string, opts ExchangeDeclareOptions) {
	if !rm.topology {
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Check if already recorded
	for i, ex := range rm.exchanges {
		if ex.name == name {
			rm.exchanges[i] = exchangeDeclaration{name, kind, opts}
			return
		}
	}

	rm.exchanges = append(rm.exchanges, exchangeDeclaration{name, kind, opts})
}

// recordQueue records a queue declaration for recovery
func (rm *recoveryManager) recordQueue(name string, opts QueueDeclareOptions) {
	if !rm.topology {
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Check if already recorded
	for i, q := range rm.queues {
		if q.name == name {
			rm.queues[i] = queueDeclaration{name, opts}
			return
		}
	}

	rm.queues = append(rm.queues, queueDeclaration{name, opts})
}

// recordBinding records a binding for recovery
func (rm *recoveryManager) recordBinding(queue, exchange, routingKey string, args Table) {
	if !rm.topology {
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.bindings = append(rm.bindings, bindingDeclaration{queue, exchange, routingKey, args})
}

// recordConsumer records a consumer for recovery
func (rm *recoveryManager) recordConsumer(queue, tag string, callback ConsumerCallback, opts ConsumeOptions) {
	if !rm.topology {
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.consumers = append(rm.consumers, consumerDeclaration{queue, tag, callback, opts})
}

// recover attempts to recover the connection and topology
func (rm *recoveryManager) recover(factory *ConnectionFactory) (*Connection, error) {
	if !rm.enabled {
		return nil, nil
	}

	var lastErr error

	for attempt := 0; attempt < rm.attempts; attempt++ {
		if attempt > 0 {
			time.Sleep(rm.interval)
		}

		// Attempt to reconnect
		conn, err := factory.NewConnection()
		if err != nil {
			lastErr = err
			continue
		}

		// Recover topology if enabled
		if rm.topology {
			if err := rm.recoverTopology(conn); err != nil {
				conn.Close()
				lastErr = err
				continue
			}
		}

		return conn, nil
	}

	return nil, lastErr
}

// recoverTopology recovers exchanges, queues, bindings, and consumers
func (rm *recoveryManager) recoverTopology(conn *Connection) error {
	// Copy topology lists while holding lock, then release BEFORE doing recovery operations
	// This prevents deadlock when ExchangeDeclare/etc try to record topology
	rm.mu.RLock()
	exchanges := make([]exchangeDeclaration, len(rm.exchanges))
	queues := make([]queueDeclaration, len(rm.queues))
	bindings := make([]bindingDeclaration, len(rm.bindings))
	copy(exchanges, rm.exchanges)
	copy(queues, rm.queues)
	copy(bindings, rm.bindings)
	rm.mu.RUnlock()

	// Open a channel for recovery
	ch, err := conn.NewChannel()
	if err != nil {
		return fmt.Errorf("create recovery channel: %w", err)
	}
	defer ch.Close()

	// Recover exchanges
	for i, ex := range exchanges {
		if err := ch.ExchangeDeclare(ex.name, ex.kind, ex.opts); err != nil {
			return fmt.Errorf("recover exchange %d/%d (%s): %w", i+1, len(exchanges), ex.name, err)
		}
	}

	// Recover queues
	for i, q := range queues {
		if _, err := ch.QueueDeclare(q.name, q.opts); err != nil {
			return fmt.Errorf("recover queue %d/%d (%s): %w", i+1, len(queues), q.name, err)
		}
	}

	// Recover bindings
	for i, b := range bindings {
		if err := ch.QueueBind(b.queue, b.exchange, b.routingKey, b.args); err != nil {
			return fmt.Errorf("recover binding %d/%d (%s->%s): %w", i+1, len(bindings), b.exchange, b.queue, err)
		}
	}

	// NOTE: Consumers are NOT recovered here! They are recovered in recoverChannels()
	// on the application's actual channels, not on this temporary recovery channel.

	return nil
}

// clear clears all recorded topology
func (rm *recoveryManager) clear() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.exchanges = make([]exchangeDeclaration, 0)
	rm.queues = make([]queueDeclaration, 0)
	rm.bindings = make([]bindingDeclaration, 0)
	rm.consumers = make([]consumerDeclaration, 0)
}
