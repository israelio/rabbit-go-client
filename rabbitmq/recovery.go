package rabbitmq

import (
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
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	// Open a channel for recovery
	ch, err := conn.NewChannel()
	if err != nil {
		return err
	}
	defer ch.Close()

	// Recover exchanges
	for _, ex := range rm.exchanges {
		if err := ch.ExchangeDeclare(ex.name, ex.kind, ex.opts); err != nil {
			return err
		}
	}

	// Recover queues
	for _, q := range rm.queues {
		if _, err := ch.QueueDeclare(q.name, q.opts); err != nil {
			return err
		}
	}

	// Recover bindings
	for _, b := range rm.bindings {
		if err := ch.QueueBind(b.queue, b.exchange, b.routingKey, b.args); err != nil {
			return err
		}
	}

	// Recover consumers
	for _, c := range rm.consumers {
		if err := ch.ConsumeWithCallback(c.queue, c.tag, c.opts, c.callback); err != nil {
			return err
		}
	}

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
