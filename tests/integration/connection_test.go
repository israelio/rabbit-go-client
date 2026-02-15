package integration

import (
	"testing"
	"time"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

// TestConnectionLifecycle tests basic connection open/close
func TestConnectionLifecycle(t *testing.T) {
	RequireRabbitMQ(t)

	factory := NewTestConnectionFactory(t)

	// Create connection
	conn, err := factory.NewConnection()
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Verify connection is open
	if conn.IsClosed() {
		t.Error("Connection should be open")
	}

	if conn.GetState() != rabbitmq.StateOpen {
		t.Errorf("Connection state: got %v, want StateOpen", conn.GetState())
	}

	// Close connection
	err = conn.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Verify connection is closed
	if !conn.IsClosed() {
		t.Error("Connection should be closed")
	}
}

// TestConnectionParameters tests connection parameter negotiation
func TestConnectionParameters(t *testing.T) {
	RequireRabbitMQ(t)

	conn := NewTestConnection(t)
	defer conn.Close()

	// Verify negotiated parameters
	if conn.GetChannelMax() == 0 {
		t.Error("ChannelMax should be negotiated")
	}

	if conn.GetFrameMax() == 0 {
		t.Error("FrameMax should be negotiated")
	}

	if conn.GetHeartbeat() < 0 {
		t.Error("Heartbeat should be non-negative")
	}

	t.Logf("Negotiated parameters: ChannelMax=%d, FrameMax=%d, Heartbeat=%v",
		conn.GetChannelMax(), conn.GetFrameMax(), conn.GetHeartbeat())
}

// TestMultipleChannels tests creating multiple channels
func TestMultipleChannels(t *testing.T) {
	RequireRabbitMQ(t)

	conn := NewTestConnection(t)
	defer conn.Close()

	// Create multiple channels
	numChannels := 5
	channels := make([]*rabbitmq.Channel, numChannels)

	for i := 0; i < numChannels; i++ {
		ch, err := conn.NewChannel()
		if err != nil {
			t.Fatalf("Failed to create channel %d: %v", i, err)
		}
		channels[i] = ch
	}

	// Close all channels
	for i, ch := range channels {
		if err := ch.Close(); err != nil {
			t.Errorf("Failed to close channel %d: %v", i, err)
		}
	}
}

// TestConnectionCloseNotification tests close notification
func TestConnectionCloseNotification(t *testing.T) {
	RequireRabbitMQ(t)

	conn := NewTestConnection(t)

	// Register close listener
	closeChan := make(chan *rabbitmq.Error, 1)
	conn.NotifyClose(closeChan)

	// Close connection
	conn.Close()

	// Wait for notification
	select {
	case err := <-closeChan:
		if err == nil {
			t.Error("Expected error in close notification")
		}
		t.Logf("Received close notification: %v", err)
	case <-time.After(2 * time.Second):
		t.Error("Did not receive close notification")
	}
}

// TestConnectionHeartbeat tests heartbeat mechanism
func TestConnectionHeartbeat(t *testing.T) {
	RequireRabbitMQ(t)

	factory := NewTestConnectionFactory(t)

	// Create connection with short heartbeat
	factory = rabbitmq.NewConnectionFactory(
		rabbitmq.WithHost(GetTestConfig().Host),
		rabbitmq.WithPort(GetTestConfig().Port),
		rabbitmq.WithCredentials(GetTestConfig().Username, GetTestConfig().Password),
		rabbitmq.WithHeartbeat(2*time.Second),
	)

	conn, err := factory.NewConnection()
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Connection should stay alive with heartbeats
	time.Sleep(5 * time.Second)

	if conn.IsClosed() {
		t.Error("Connection should still be alive with heartbeats")
	}
}

// TestConnectionWithTLS tests TLS connection (if TLS is configured)
func TestConnectionWithTLS(t *testing.T) {
	t.Skip("TLS test requires TLS-enabled RabbitMQ and certificates")

	// This test would be enabled when TLS is properly configured
	// factory := rabbitmq.NewConnectionFactory(
	// 	rabbitmq.WithHost(GetTestConfig().Host),
	// 	rabbitmq.WithPort(5671), // TLS port
	// 	rabbitmq.WithCredentials(GetTestConfig().Username, GetTestConfig().Password),
	// 	rabbitmq.WithTLS(tlsConfig),
	// )
	//
	// conn, err := factory.NewConnection()
	// if err != nil {
	// 	t.Fatalf("TLS connection failed: %v", err)
	// }
	// defer conn.Close()
}
// TestChannelLifecycle tests channel open/close
func TestChannelLifecycle(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()

	// Verify channel is open
	if ch.IsClosed() {
		t.Error("Channel should be open")
	}

	// Close channel
	err := ch.Close()
	if err != nil {
		t.Errorf("Channel close failed: %v", err)
	}

	// Verify channel is closed
	if !ch.IsClosed() {
		t.Error("Channel should be closed")
	}

	// Verify cannot perform operations on closed channel
	_, err = ch.QueueDeclare("test", rabbitmq.QueueDeclareOptions{})
	if err == nil {
		t.Error("Should not be able to declare queue on closed channel")
	}
}

// TestChannelCloseNotification tests channel close notification
func TestChannelCloseNotification(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()

	// Register close listener
	closeChan := make(chan *rabbitmq.Error, 1)
	ch.NotifyClose(closeChan)

	// Close channel
	ch.Close()

	// Wait for notification
	select {
	case err := <-closeChan:
		if err == nil {
			t.Error("Expected error in close notification")
		}
		t.Logf("Received close notification: %v", err)
	case <-time.After(2 * time.Second):
		t.Error("Did not receive close notification")
	}
}

// TestConnectionRecovery tests automatic connection recovery
func TestConnectionRecovery(t *testing.T) {
	t.Skip("Recovery test requires killing connection - run manually")

	RequireRabbitMQ(t)

	// Create connection with auto-recovery
	factory := rabbitmq.NewConnectionFactory(
		rabbitmq.WithHost(GetTestConfig().Host),
		rabbitmq.WithPort(GetTestConfig().Port),
		rabbitmq.WithCredentials(GetTestConfig().Username, GetTestConfig().Password),
		rabbitmq.WithAutomaticRecovery(true),
		rabbitmq.WithRecoveryInterval(2*time.Second),
	)

	conn, err := factory.NewConnection()
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// For manual testing:
	// 1. Note the connection name in management UI
	// 2. Kill the connection: rabbitmqctl close_connection "<name>" "test"
	// 3. Observe automatic recovery
	// 4. Connection should reconnect

	t.Log("Manual test: kill connection to test automatic recovery")
}
