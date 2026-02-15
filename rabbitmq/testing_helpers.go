package rabbitmq

import (
	"net"
	"testing"
	"time"
)

// requireRabbitMQ skips the test if RabbitMQ is not available on localhost:5672
func requireRabbitMQ(t *testing.T) *ConnectionFactory {
	t.Helper()

	// Try to connect to RabbitMQ
	conn, err := net.DialTimeout("tcp", "localhost:5672", 2*time.Second)
	if err != nil {
		t.Skipf("RabbitMQ not available on localhost:5672: %v", err)
		return nil
	}
	conn.Close()

	// Create factory with test defaults
	factory := NewConnectionFactory(
		WithHost("localhost"),
		WithPort(5672),
		WithCredentials("guest", "guest"),
		WithConnectionTimeout(10*time.Second),
	)

	return factory
}

// mustConnect creates a connection or fails the test
func mustConnect(t *testing.T, factory *ConnectionFactory) *Connection {
	t.Helper()

	conn, err := factory.NewConnection()
	if err != nil {
		t.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	return conn
}

// mustCreateChannel creates a channel or fails the test
func mustCreateChannel(t *testing.T, conn *Connection) *Channel {
	t.Helper()

	ch, err := conn.NewChannel()
	if err != nil {
		t.Fatalf("Failed to create channel: %v", err)
	}

	return ch
}
