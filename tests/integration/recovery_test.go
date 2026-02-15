package integration

import (
	"testing"
	"time"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

// TestAutomaticRecovery tests basic automatic recovery
func TestAutomaticRecovery(t *testing.T) {
	t.Skip("Requires manual broker restart - see documentation")

	// To test:
	// 1. Start broker
	// 2. Run test
	// 3. Kill broker when prompted
	// 4. Restart broker when prompted
	// 5. Verify recovery

	RequireRabbitMQ(t)

	factory := NewTestConnectionFactory(t)
	factory.AutomaticRecovery = true
	factory.RecoveryInterval = 2 * time.Second

	conn, err := factory.NewConnection()
	if err != nil {
		t.Fatalf("NewConnection failed: %v", err)
	}
	defer conn.Close()

	ch, err := conn.NewChannel()
	if err != nil {
		t.Fatalf("NewChannel failed: %v", err)
	}
	defer ch.Close()

	queueName := GenerateQueueName(t)

	// Declare queue
	_, err = ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Publish initial message
	msg := rabbitmq.Publishing{Body: []byte("before restart")}
	err = ch.Publish("", queueName, false, false, msg)
	if err != nil {
		t.Fatalf("Initial publish failed: %v", err)
	}

	t.Log("Kill broker now...")
	time.Sleep(10 * time.Second)

	t.Log("Restart broker now...")
	time.Sleep(10 * time.Second)

	// Connection should recover
	t.Log("Waiting for recovery...")
	time.Sleep(5 * time.Second)

	// Try publishing after recovery
	msg2 := rabbitmq.Publishing{Body: []byte("after restart")}
	err = ch.Publish("", queueName, false, false, msg2)
	if err != nil {
		t.Fatalf("Publish after recovery failed: %v", err)
	}

	t.Log("Successfully published after recovery")

	// Cleanup
	CleanupQueue(t, ch, queueName)
}

// TestTopologyRecovery tests topology recovery
func TestTopologyRecovery(t *testing.T) {
	t.Skip("Requires manual broker restart")

	RequireRabbitMQ(t)

	factory := NewTestConnectionFactory(t)
	factory.AutomaticRecovery = true
	factory.TopologyRecovery = true
	factory.RecoveryInterval = 2 * time.Second

	conn, err := factory.NewConnection()
	if err != nil {
		t.Fatalf("NewConnection failed: %v", err)
	}
	defer conn.Close()

	ch, err := conn.NewChannel()
	if err != nil {
		t.Fatalf("NewChannel failed: %v", err)
	}
	defer ch.Close()

	exchangeName := GenerateExchangeName(t)
	queueName := GenerateQueueName(t)

	// Declare topology
	err = ch.ExchangeDeclare(exchangeName, "direct", rabbitmq.ExchangeDeclareOptions{})
	if err != nil {
		t.Fatalf("ExchangeDeclare failed: %v", err)
	}

	_, err = ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	err = ch.QueueBind(queueName, exchangeName, "key", nil)
	if err != nil {
		t.Fatalf("QueueBind failed: %v", err)
	}

	t.Log("Topology declared. Kill broker now...")
	time.Sleep(10 * time.Second)

	t.Log("Restart broker now...")
	time.Sleep(10 * time.Second)

	t.Log("Waiting for topology recovery...")
	time.Sleep(5 * time.Second)

	// Topology should be recovered - test it
	msg := rabbitmq.Publishing{Body: []byte("test")}
	err = ch.Publish(exchangeName, "key", false, false, msg)
	if err != nil {
		t.Fatalf("Publish after recovery failed: %v", err)
	}

	response, ok, err := ch.BasicGet(queueName, true)
	if err != nil {
		t.Fatalf("BasicGet after recovery failed: %v", err)
	}
	if !ok {
		t.Fatal("Message should be in recovered queue")
	}
	if string(response.Body) != "test" {
		t.Errorf("Message body: got %q, want test", response.Body)
	}

	t.Log("Topology successfully recovered")

	// Cleanup
	CleanupQueue(t, ch, queueName)
	CleanupExchange(t, ch, exchangeName)
}

// TestConsumerRecovery tests consumer recovery
func TestConsumerRecovery(t *testing.T) {
	t.Skip("Requires manual broker restart")

	RequireRabbitMQ(t)

	factory := NewTestConnectionFactory(t)
	factory.AutomaticRecovery = true
	factory.TopologyRecovery = true
	factory.RecoveryInterval = 2 * time.Second

	conn, err := factory.NewConnection()
	if err != nil {
		t.Fatalf("NewConnection failed: %v", err)
	}
	defer conn.Close()

	ch, err := conn.NewChannel()
	if err != nil {
		t.Fatalf("NewChannel failed: %v", err)
	}
	defer ch.Close()

	queueName := GenerateQueueName(t)
	_, err = ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Start consumer
	deliveries, err := ch.Consume(queueName, "consumer1", rabbitmq.ConsumeOptions{})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	// Publish message before restart
	msg1 := rabbitmq.Publishing{Body: []byte("before")}
	ch.Publish("", queueName, false, false, msg1)

	// Receive it
	select {
	case delivery := <-deliveries:
		t.Logf("Received before restart: %s", delivery.Body)
		delivery.Ack(false)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout before restart")
	}

	t.Log("Kill broker now...")
	time.Sleep(10 * time.Second)

	t.Log("Restart broker now...")
	time.Sleep(10 * time.Second)

	t.Log("Waiting for consumer recovery...")
	time.Sleep(5 * time.Second)

	// Publish message after restart
	msg2 := rabbitmq.Publishing{Body: []byte("after")}
	err = ch.Publish("", queueName, false, false, msg2)
	if err != nil {
		t.Fatalf("Publish after recovery failed: %v", err)
	}

	// Consumer should receive it
	select {
	case delivery := <-deliveries:
		t.Logf("Received after restart: %s", delivery.Body)
		if string(delivery.Body) != "after" {
			t.Errorf("Body: got %q, want after", delivery.Body)
		}
		delivery.Ack(false)
	case <-time.After(5 * time.Second):
		t.Fatal("Consumer did not recover - timeout")
	}

	t.Log("Consumer successfully recovered")

	CleanupQueue(t, ch, queueName)
}

// TestRecoveryNotification tests recovery notifications
func TestRecoveryNotification(t *testing.T) {
	t.Skip("Requires manual broker restart")

	RequireRabbitMQ(t)

	factory := NewTestConnectionFactory(t)
	factory.AutomaticRecovery = true
	factory.RecoveryInterval = 2 * time.Second

	conn, err := factory.NewConnection()
	if err != nil {
		t.Fatalf("NewConnection failed: %v", err)
	}
	defer conn.Close()

	// Register for recovery notifications
	recoveryStarted := make(chan struct{}, 1)
	recoveryCompleted := make(chan struct{}, 1)

	conn.NotifyRecoveryStarted(recoveryStarted)
	conn.NotifyRecoveryCompleted(recoveryCompleted)

	t.Log("Kill broker now...")
	time.Sleep(10 * time.Second)

	t.Log("Restart broker now...")

	// Wait for recovery notifications
	select {
	case <-recoveryStarted:
		t.Log("Recovery started notification received")
	case <-time.After(30 * time.Second):
		t.Fatal("Timeout waiting for recovery started")
	}

	select {
	case <-recoveryCompleted:
		t.Log("Recovery completed notification received")
	case <-time.After(30 * time.Second):
		t.Fatal("Timeout waiting for recovery completed")
	}
}

// TestRecoveryFailure tests recovery failure handling
func TestRecoveryFailure(t *testing.T) {
	t.Skip("Requires broker that stays down")

	RequireRabbitMQ(t)

	factory := NewTestConnectionFactory(t)
	factory.AutomaticRecovery = true
	factory.RecoveryInterval = 2 * time.Second

	conn, err := factory.NewConnection()
	if err != nil {
		t.Fatalf("NewConnection failed: %v", err)
	}
	defer conn.Close()

	recoveryFailed := make(chan error, 10)
	conn.NotifyRecoveryFailed(recoveryFailed)

	t.Log("Kill broker and keep it down...")
	time.Sleep(5 * time.Second)

	// Should get recovery failure notifications
	select {
	case err := <-recoveryFailed:
		t.Logf("Recovery failed as expected: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("Timeout waiting for recovery failure notification")
	}
}

// TestRecoveryWithMultipleChannels tests recovery with many channels
func TestRecoveryWithMultipleChannels(t *testing.T) {
	t.Skip("Requires manual broker restart")

	RequireRabbitMQ(t)

	factory := NewTestConnectionFactory(t)
	factory.AutomaticRecovery = true
	factory.TopologyRecovery = true

	conn, err := factory.NewConnection()
	if err != nil {
		t.Fatalf("NewConnection failed: %v", err)
	}
	defer conn.Close()

	// Create multiple channels
	channels := make([]*rabbitmq.Channel, 5)
	for i := 0; i < 5; i++ {
		ch, err := conn.NewChannel()
		if err != nil {
			t.Fatalf("NewChannel %d failed: %v", i, err)
		}
		channels[i] = ch
		defer ch.Close()
	}

	// Declare queues on each channel
	queues := make([]string, 5)
	for i, ch := range channels {
		queues[i] = GenerateQueueName(t) + string(rune('0'+i))
		_, err := ch.QueueDeclare(queues[i], rabbitmq.QueueDeclareOptions{})
		if err != nil {
			t.Fatalf("QueueDeclare %d failed: %v", i, err)
		}
	}

	t.Log("Kill broker now...")
	time.Sleep(10 * time.Second)

	t.Log("Restart broker now...")
	time.Sleep(10 * time.Second)

	t.Log("Waiting for recovery...")
	time.Sleep(5 * time.Second)

	// Test all channels work after recovery
	for i, ch := range channels {
		msg := rabbitmq.Publishing{Body: []byte("test")}
		err := ch.Publish("", queues[i], false, false, msg)
		if err != nil {
			t.Errorf("Channel %d publish after recovery failed: %v", i, err)
		}

		response, ok, err := ch.BasicGet(queues[i], true)
		if err != nil {
			t.Errorf("Channel %d get after recovery failed: %v", i, err)
		}
		if ok && string(response.Body) == "test" {
			t.Logf("Channel %d recovered successfully", i)
		}
	}

	// Cleanup
	for i, ch := range channels {
		CleanupQueue(t, ch, queues[i])
	}
}

// TestNoRecoveryWhenDisabled tests that recovery doesn't happen when disabled
func TestNoRecoveryWhenDisabled(t *testing.T) {
	t.Skip("Requires manual broker restart")

	RequireRabbitMQ(t)

	factory := NewTestConnectionFactory(t)
	factory.AutomaticRecovery = false // Disabled

	conn, err := factory.NewConnection()
	if err != nil {
		t.Fatalf("NewConnection failed: %v", err)
	}
	defer conn.Close()

	ch, err := conn.NewChannel()
	if err != nil {
		t.Fatalf("NewChannel failed: %v", err)
	}
	defer ch.Close()

	t.Log("Kill broker now...")
	time.Sleep(10 * time.Second)

	t.Log("Restart broker now...")
	time.Sleep(10 * time.Second)

	// Connection should NOT recover
	queueName := GenerateQueueName(t)
	_, err = ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{})
	if err == nil {
		t.Error("Operation should fail - connection not recovered")
		CleanupQueue(t, ch, queueName)
	} else {
		t.Logf("Operation correctly failed (no recovery): %v", err)
	}
}

// Note: Recovery notification methods (NotifyRecoveryStarted, NotifyRecoveryCompleted, NotifyRecoveryFailed)
// are not yet implemented in the rabbitmq package. These tests are placeholders for future functionality.
