package rabbitmq

import (
	"sync/atomic"
)

// MetricsCollector collects metrics for RabbitMQ client operations
type MetricsCollector interface {
	// Connection metrics
	ConnectionCreated()
	ConnectionClosed()
	ConnectionError(err error)

	// Channel metrics
	ChannelCreated()
	ChannelClosed()
	ChannelError(err error)

	// Message metrics
	MessagePublished()
	MessageConsumed()
	MessageAcked()
	MessageNacked()
	MessageRejected()
	MessageReturned()

	// Publisher confirm metrics
	ConfirmReceived(ack bool)
}

// StandardMetricsCollector provides a thread-safe metrics collector
type StandardMetricsCollector struct {
	connectionsCreated  atomic.Int64
	connectionsClosed   atomic.Int64
	connectionErrors    atomic.Int64

	channelsCreated     atomic.Int64
	channelsClosed      atomic.Int64
	channelErrors       atomic.Int64

	messagesPublished   atomic.Int64
	messagesConsumed    atomic.Int64
	messagesAcked       atomic.Int64
	messagesNacked      atomic.Int64
	messagesRejected    atomic.Int64
	messagesReturned    atomic.Int64

	confirmsAcked       atomic.Int64
	confirmsNacked      atomic.Int64
}

// NewStandardMetricsCollector creates a new standard metrics collector
func NewStandardMetricsCollector() *StandardMetricsCollector {
	return &StandardMetricsCollector{}
}

// Connection metrics
func (m *StandardMetricsCollector) ConnectionCreated() {
	m.connectionsCreated.Add(1)
}

func (m *StandardMetricsCollector) ConnectionClosed() {
	m.connectionsClosed.Add(1)
}

func (m *StandardMetricsCollector) ConnectionError(err error) {
	m.connectionErrors.Add(1)
}

// Channel metrics
func (m *StandardMetricsCollector) ChannelCreated() {
	m.channelsCreated.Add(1)
}

func (m *StandardMetricsCollector) ChannelClosed() {
	m.channelsClosed.Add(1)
}

func (m *StandardMetricsCollector) ChannelError(err error) {
	m.channelErrors.Add(1)
}

// Message metrics
func (m *StandardMetricsCollector) MessagePublished() {
	m.messagesPublished.Add(1)
}

func (m *StandardMetricsCollector) MessageConsumed() {
	m.messagesConsumed.Add(1)
}

func (m *StandardMetricsCollector) MessageAcked() {
	m.messagesAcked.Add(1)
}

func (m *StandardMetricsCollector) MessageNacked() {
	m.messagesNacked.Add(1)
}

func (m *StandardMetricsCollector) MessageRejected() {
	m.messagesRejected.Add(1)
}

func (m *StandardMetricsCollector) MessageReturned() {
	m.messagesReturned.Add(1)
}

// Confirm metrics
func (m *StandardMetricsCollector) ConfirmReceived(ack bool) {
	if ack {
		m.confirmsAcked.Add(1)
	} else {
		m.confirmsNacked.Add(1)
	}
}

// Getters for metrics
func (m *StandardMetricsCollector) GetConnectionsCreated() int64 {
	return m.connectionsCreated.Load()
}

func (m *StandardMetricsCollector) GetConnectionsClosed() int64 {
	return m.connectionsClosed.Load()
}

func (m *StandardMetricsCollector) GetConnectionErrors() int64 {
	return m.connectionErrors.Load()
}

func (m *StandardMetricsCollector) GetChannelsCreated() int64 {
	return m.channelsCreated.Load()
}

func (m *StandardMetricsCollector) GetChannelsClosed() int64 {
	return m.channelsClosed.Load()
}

func (m *StandardMetricsCollector) GetChannelErrors() int64 {
	return m.channelErrors.Load()
}

func (m *StandardMetricsCollector) GetMessagesPublished() int64 {
	return m.messagesPublished.Load()
}

func (m *StandardMetricsCollector) GetMessagesConsumed() int64 {
	return m.messagesConsumed.Load()
}

func (m *StandardMetricsCollector) GetMessagesAcked() int64 {
	return m.messagesAcked.Load()
}

func (m *StandardMetricsCollector) GetMessagesNacked() int64 {
	return m.messagesNacked.Load()
}

func (m *StandardMetricsCollector) GetMessagesRejected() int64 {
	return m.messagesRejected.Load()
}

func (m *StandardMetricsCollector) GetMessagesReturned() int64 {
	return m.messagesReturned.Load()
}

func (m *StandardMetricsCollector) GetConfirmsAcked() int64 {
	return m.confirmsAcked.Load()
}

func (m *StandardMetricsCollector) GetConfirmsNacked() int64 {
	return m.confirmsNacked.Load()
}

// NoOpMetricsCollector is a metrics collector that does nothing
type NoOpMetricsCollector struct{}

func (n *NoOpMetricsCollector) ConnectionCreated()          {}
func (n *NoOpMetricsCollector) ConnectionClosed()           {}
func (n *NoOpMetricsCollector) ConnectionError(err error)   {}
func (n *NoOpMetricsCollector) ChannelCreated()             {}
func (n *NoOpMetricsCollector) ChannelClosed()              {}
func (n *NoOpMetricsCollector) ChannelError(err error)      {}
func (n *NoOpMetricsCollector) MessagePublished()           {}
func (n *NoOpMetricsCollector) MessageConsumed()            {}
func (n *NoOpMetricsCollector) MessageAcked()               {}
func (n *NoOpMetricsCollector) MessageNacked()              {}
func (n *NoOpMetricsCollector) MessageRejected()            {}
func (n *NoOpMetricsCollector) MessageReturned()            {}
func (n *NoOpMetricsCollector) ConfirmReceived(ack bool)    {}

// NewNoOpMetricsCollector creates a no-op metrics collector
func NewNoOpMetricsCollector() *NoOpMetricsCollector {
	return &NoOpMetricsCollector{}
}
