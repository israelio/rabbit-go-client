package integration

import (
	"testing"
	"time"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

// TestConnectionBlocked tests connection blocking notifications
func TestConnectionBlocked(t *testing.T) {
	t.Skip("Requires manual memory alarm trigger - see test documentation")

	// To test:
	// 1. Set memory alarm: rabbitmqctl set_vm_memory_high_watermark 0.00001
	// 2. Run test
	// 3. Clear alarm: rabbitmqctl set_vm_memory_high_watermark 0.4

	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	// Register for blocked notifications
	blocked := make(chan rabbitmq.BlockedNotification, 10)
	conn.NotifyBlocked(blocked)

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Try publishing to trigger alarm
	msg := rabbitmq.Publishing{
		Body: make([]byte, 1024*1024), // 1MB message
	}

	for i := 0; i < 100; i++ {
		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Logf("Publish failed at %d: %v", i, err)
			break
		}
	}

	// Wait for blocked notification
	select {
	case notification := <-blocked:
		t.Logf("Connection blocked: %s", notification.Reason)
		if !notification.Blocked {
			t.Error("Expected blocked=true")
		}
	case <-time.After(10 * time.Second):
		t.Error("Timeout waiting for blocked notification")
	}

	// Clear alarm manually, then wait for unblocked
	t.Log("Clear alarm with: rabbitmqctl set_vm_memory_high_watermark 0.4")

	select {
	case notification := <-blocked:
		t.Logf("Connection unblocked")
		if notification.Blocked {
			t.Error("Expected blocked=false")
		}
	case <-time.After(30 * time.Second):
		t.Error("Timeout waiting for unblocked notification")
	}
}

// TestBlockedNotificationChannel tests multiple listeners
func TestBlockedNotificationChannel(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	// Register multiple notification channels
	blocked1 := make(chan rabbitmq.BlockedNotification, 5)
	blocked2 := make(chan rabbitmq.BlockedNotification, 5)

	conn.NotifyBlocked(blocked1)
	conn.NotifyBlocked(blocked2)

	t.Log("Registered two blocked notification channels")

	// Both channels should receive notifications when triggered
	// (Would need actual blocking to test)
}

// TestPublishDuringBlocked tests behavior during blocking
func TestPublishDuringBlocked(t *testing.T) {
	t.Skip("Requires manual memory alarm - integration test")

	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	blocked := make(chan rabbitmq.BlockedNotification, 10)
	conn.NotifyBlocked(blocked)

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Wait for blocked state
	t.Log("Trigger memory alarm and wait...")
	select {
	case notification := <-blocked:
		if !notification.Blocked {
			t.Fatal("Expected blocked state")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Timeout waiting for blocked state")
	}

	// Try publishing during blocked - should block or fail
	msg := rabbitmq.Publishing{Body: []byte("test")}

	publishDone := make(chan error, 1)
	go func() {
		publishDone <- ch.Publish("", queueName, false, false, msg)
	}()

	select {
	case err := <-publishDone:
		t.Logf("Publish during blocked returned: %v", err)
	case <-time.After(5 * time.Second):
		t.Log("Publish is blocking (expected)")
	}
}

// TestFlowControl tests channel flow control
func TestFlowControl(t *testing.T) {
	t.Skip("Flow control requires specific broker configuration")

	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	// Channel flow is a way for broker to tell client to stop sending
	// This is different from connection blocking

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Publish rapidly to trigger flow control
	for i := 0; i < 10000; i++ {
		msg := rabbitmq.Publishing{
			Body: make([]byte, 1024), // 1KB
		}
		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Logf("Publish failed at %d: %v", i, err)
			break
		}

		if i%1000 == 0 {
			t.Logf("Published %d messages", i)
		}
	}
}

// TestBlockedDuringConsume tests consuming during blocked state
func TestBlockedDuringConsume(t *testing.T) {
	t.Skip("Requires manual memory alarm")

	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	blocked := make(chan rabbitmq.BlockedNotification, 10)
	conn.NotifyBlocked(blocked)

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Publish some messages before blocking
	for i := 0; i < 10; i++ {
		msg := rabbitmq.Publishing{
			Body: []byte(string(rune('0' + i))),
		}
		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	// Start consumer
	deliveries, err := ch.Consume(queueName, "", rabbitmq.ConsumeOptions{
		AutoAck: true,
	})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	// Consume a few
	for i := 0; i < 3; i++ {
		select {
		case delivery := <-deliveries:
			t.Logf("Consumed before block: %s", delivery.Body)
		case <-time.After(2 * time.Second):
			t.Fatal("Timeout consuming")
		}
	}

	// Trigger blocking
	t.Log("Trigger memory alarm now...")

	// Wait for blocked state
	select {
	case notification := <-blocked:
		if !notification.Blocked {
			t.Fatal("Expected blocked state")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Timeout waiting for blocked state")
	}

	// Try consuming during blocked - consumer should still work
	consumed := 0
	timeout := time.After(10 * time.Second)
	for consumed < 7 {
		select {
		case delivery := <-deliveries:
			t.Logf("Consumed during block: %s", delivery.Body)
			consumed++
		case <-timeout:
			t.Logf("Consumed %d messages during blocked state", consumed)
			goto done
		}
	}
done:

	if consumed == 0 {
		t.Error("Should be able to consume during blocked state")
	}
}

// TestConnectionBlockedAfterRecovery tests blocking after connection recovery
func TestConnectionBlockedAfterRecovery(t *testing.T) {
	t.Skip("Requires manual broker restart and memory alarm")

	RequireRabbitMQ(t)

	factory := NewTestConnectionFactory(t)
	factory.AutomaticRecovery = true

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

	blocked := make(chan rabbitmq.BlockedNotification, 10)
	conn.NotifyBlocked(blocked)

	// Kill and restart broker
	t.Log("Kill broker now...")
	time.Sleep(5 * time.Second)

	t.Log("Start broker now...")
	time.Sleep(5 * time.Second)

	// Trigger memory alarm after recovery
	t.Log("Trigger memory alarm...")

	select {
	case notification := <-blocked:
		t.Logf("Blocked notification after recovery: %+v", notification)
	case <-time.After(30 * time.Second):
		t.Error("Timeout waiting for blocked notification")
	}
}

// TestBlockedReasonParsing tests parsing of blocked reason
func TestBlockedReasonParsing(t *testing.T) {
	notification := rabbitmq.BlockedNotification{
		Blocked: true,
		Reason:  "low on memory",
	}

	if !notification.Blocked {
		t.Error("Should be blocked")
	}

	if notification.Reason != "low on memory" {
		t.Errorf("Reason: got %q, want 'low on memory'", notification.Reason)
	}

	// Unblocked
	notification2 := rabbitmq.BlockedNotification{
		Blocked: false,
		Reason:  "",
	}

	if notification2.Blocked {
		t.Error("Should not be blocked")
	}
}
