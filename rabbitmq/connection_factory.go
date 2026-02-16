package rabbitmq

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"
)

// ConnectionFactory creates and configures AMQP connections
type ConnectionFactory struct {
	// Connection settings
	Host     string
	Port     int
	VHost    string
	Username string
	Password string

	// TLS configuration
	TLS *tls.Config

	// Timeouts
	ConnectionTimeout time.Duration
	HandshakeTimeout  time.Duration

	// AMQP parameters
	ChannelMax uint16
	FrameMax   uint32
	Heartbeat  time.Duration

	// Recovery settings
	AutomaticRecovery       bool
	TopologyRecovery        bool
	RecoveryInterval        time.Duration
	ConnectionRetryAttempts int

	// Client properties sent to server
	ClientProperties map[string]interface{}

	// Custom handlers
	ErrorHandler    ErrorHandler
	RecoveryHandler RecoveryHandler
	BlockedHandler  BlockedHandler

	// Logger
	Logger Logger
}

// RecoveryHandler receives recovery events
type RecoveryHandler interface {
	OnRecoveryStarted(conn *Connection)
	OnRecoveryCompleted(conn *Connection)
	OnRecoveryFailed(conn *Connection, err error)
	OnTopologyRecoveryStarted(conn *Connection)
	OnTopologyRecoveryCompleted(conn *Connection)
}

// BlockedHandler receives connection blocked/unblocked events
type BlockedHandler interface {
	OnBlocked(conn *Connection, reason string)
	OnUnblocked(conn *Connection)
}

// NewConnectionFactory creates a new ConnectionFactory with sensible defaults
func NewConnectionFactory(opts ...FactoryOption) *ConnectionFactory {
	cf := &ConnectionFactory{
		Host:                    "localhost",
		Port:                    5672,
		VHost:                   "/",
		Username:                "guest",
		Password:                "guest",
		ConnectionTimeout:       60 * time.Second,
		HandshakeTimeout:        10 * time.Second,
		Heartbeat:               10 * time.Second,
		ChannelMax:              0, // 0 = no limit (server decides)
		FrameMax:                0, // 0 = no limit (server decides)
		AutomaticRecovery:       false,
		TopologyRecovery:        true,
		RecoveryInterval:        5 * time.Second,
		ConnectionRetryAttempts: 3,
		ClientProperties:        defaultClientProperties(),
	}

	// Apply options
	for _, opt := range opts {
		opt(cf)
	}

	// Set default error handler if not provided
	if cf.ErrorHandler == nil {
		cf.ErrorHandler = &DefaultErrorHandler{Logger: cf.Logger}
	}

	return cf
}

// NewConnection creates a new connection using the factory settings
func (cf *ConnectionFactory) NewConnection() (*Connection, error) {
	return cf.NewConnectionWithContext(context.Background())
}

// NewConnectionWithContext creates a new connection with context support
func (cf *ConnectionFactory) NewConnectionWithContext(ctx context.Context) (*Connection, error) {
	// Create connection with timeout
	connCtx, cancel := context.WithTimeout(ctx, cf.ConnectionTimeout)
	defer cancel()

	// Establish TCP or TLS connection
	netConn, err := cf.dial(connCtx)
	if err != nil {
		return nil, fmt.Errorf("dial failed: %w", err)
	}

	// Create connection object
	conn := &Connection{
		factory:       cf,
		conn:          netConn,
		channels:      make(map[uint16]*Channel),
		nextChannelID: 1,
		closeChan:     make(chan *Error, 1),
		blockedChan:   make(chan BlockedNotification, 1),
		recovery:      newRecoveryManager(cf.AutomaticRecovery, cf.TopologyRecovery, cf.RecoveryInterval, cf.ConnectionRetryAttempts),
	}
	conn.state.Store(int32(StateConnecting))

	// Perform AMQP handshake
	if err := conn.handshake(ctx); err != nil {
		netConn.Close()
		return nil, fmt.Errorf("handshake failed: %w", err)
	}

	// Start background goroutines
	conn.start()

	return conn, nil
}

// dial establishes a network connection (TCP or TLS)
func (cf *ConnectionFactory) dial(ctx context.Context) (net.Conn, error) {
	addr := fmt.Sprintf("%s:%d", cf.Host, cf.Port)

	// Create dialer
	dialer := &net.Dialer{
		Timeout: cf.ConnectionTimeout,
	}

	// Establish connection
	if cf.TLS != nil {
		return tls.DialWithDialer(dialer, "tcp", addr, cf.TLS)
	}

	return dialer.DialContext(ctx, "tcp", addr)
}

// Validate validates the ConnectionFactory configuration
func (cf *ConnectionFactory) Validate() error {
	// Validate host
	if cf.Host == "" {
		return fmt.Errorf("host cannot be empty")
	}

	// Validate port
	if cf.Port <= 0 || cf.Port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535, got %d", cf.Port)
	}

	// Validate VHost
	if cf.VHost == "" {
		return fmt.Errorf("vhost cannot be empty")
	}

	// Validate username
	if cf.Username == "" {
		return fmt.Errorf("username cannot be empty")
	}

	// Validate timeouts
	if cf.ConnectionTimeout < 0 {
		return fmt.Errorf("connection timeout cannot be negative, got %v", cf.ConnectionTimeout)
	}
	if cf.HandshakeTimeout < 0 {
		return fmt.Errorf("handshake timeout cannot be negative, got %v", cf.HandshakeTimeout)
	}

	// Validate heartbeat (0 means disabled, which is valid)
	if cf.Heartbeat < 0 {
		return fmt.Errorf("heartbeat cannot be negative, got %v", cf.Heartbeat)
	}

	// Validate frame max (0 means server decides, 4096 is minimum per AMQP spec)
	if cf.FrameMax != 0 && cf.FrameMax < 4096 {
		return fmt.Errorf("frame max must be 0 or >= 4096, got %d", cf.FrameMax)
	}

	// Validate recovery interval
	if cf.RecoveryInterval < 0 {
		return fmt.Errorf("recovery interval cannot be negative, got %v", cf.RecoveryInterval)
	}

	// Validate retry attempts
	if cf.ConnectionRetryAttempts < 0 {
		return fmt.Errorf("connection retry attempts cannot be negative, got %d", cf.ConnectionRetryAttempts)
	}

	return nil
}

// defaultClientProperties returns default client properties
func defaultClientProperties() map[string]interface{} {
	// Use flat structure to avoid nested map encoding issues
	return map[string]interface{}{
		"product":  "RabbitMQ Go Client",
		"version":  "1.0.0",
		"platform": "Go",
		// Capabilities as flat properties
		"capabilities.publisher_confirms":           true,
		"capabilities.exchange_exchange_bindings":   true,
		"capabilities.basic.nack":                   true,
		"capabilities.consumer_cancel_notify":       true,
		"capabilities.connection.blocked":           true,
		"capabilities.authentication_failure_close": true,
	}
}
