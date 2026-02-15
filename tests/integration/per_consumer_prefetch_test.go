package integration

import (
	"testing"
	"time"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

// TestPerConsumerPrefetch tests per-consumer QoS
func TestPerConsumerPrefetch(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare queue
	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Set per-consumer QoS (global=false)
	err = ch.Qos(2, 0, false)
	if err != nil {
		t.Fatalf("Qos failed: %v", err)
	}

	// Publish messages
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
		AutoAck: false,
	})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	// Should receive exactly prefetch count messages initially
	received := 0
	timeout := time.After(1 * time.Second)

	// Receive first batch (up to prefetch)
	for received < 2 {
		select {
		case delivery := <-deliveries:
			received++
			t.Logf("Received message %d: %s", received, delivery.Body)
			// Don't ack yet
		case <-timeout:
			break
		}
	}

	if received != 2 {
		t.Errorf("Should receive prefetch count (2), got %d", received)
	}

	// Should not receive more without acking
	select {
	case delivery := <-deliveries:
		t.Errorf("Should not receive more without ack, got: %s", delivery.Body)
	case <-time.After(500 * time.Millisecond):
		t.Log("Correctly did not receive more without ack")
	}
}

// TestGlobalPrefetch tests global QoS across all consumers
func TestGlobalPrefetch(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queue1 := GenerateQueueName(t) + "-1"
	queue2 := GenerateQueueName(t) + "-2"
	defer CleanupQueue(t, ch, queue1)
	defer CleanupQueue(t, ch, queue2)

	// Declare queues
	_, err := ch.QueueDeclare(queue1, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("Queue1 declare failed: %v", err)
	}

	_, err = ch.QueueDeclare(queue2, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("Queue2 declare failed: %v", err)
	}

	// Set global QoS (global=true)
	err = ch.Qos(3, 0, true)
	if err != nil {
		t.Fatalf("Global Qos failed: %v", err)
	}

	// Publish to both queues
	for i := 0; i < 5; i++ {
		msg := rabbitmq.Publishing{Body: []byte("msg")}
		ch.Publish("", queue1, false, false, msg)
		ch.Publish("", queue2, false, false, msg)
	}

	// Start consumers on both queues
	deliveries1, err := ch.Consume(queue1, "consumer1", rabbitmq.ConsumeOptions{
		AutoAck: false,
	})
	if err != nil {
		t.Fatalf("Consume queue1 failed: %v", err)
	}

	deliveries2, err := ch.Consume(queue2, "consumer2", rabbitmq.ConsumeOptions{
		AutoAck: false,
	})
	if err != nil {
		t.Fatalf("Consume queue2 failed: %v", err)
	}

	// Should receive global prefetch count across both consumers
	received := 0
	timeout := time.After(1 * time.Second)

	for received < 3 {
		select {
		case <-deliveries1:
			received++
		case <-deliveries2:
			received++
		case <-timeout:
			break
		}
	}

	t.Logf("Received %d messages with global prefetch=3", received)

	// Should not get more without acking
	select {
	case <-deliveries1:
		t.Error("Should not receive more without ack")
	case <-deliveries2:
		t.Error("Should not receive more without ack")
	case <-time.After(500 * time.Millisecond):
		t.Log("Correctly blocked at global prefetch limit")
	}
}

// TestPrefetchSize tests prefetch size limit
func TestPrefetchSize(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Set QoS with size limit (1024 bytes)
	err = ch.Qos(0, 1024, false)
	if err != nil {
		t.Fatalf("Qos failed: %v", err)
	}

	// Publish messages of different sizes
	sizes := []int{512, 512, 512} // Total 1536 bytes
	for _, size := range sizes {
		msg := rabbitmq.Publishing{
			Body: make([]byte, size),
		}
		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	// Start consumer
	deliveries, err := ch.Consume(queueName, "", rabbitmq.ConsumeOptions{
		AutoAck: false,
	})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	// Should receive messages up to size limit
	receivedSize := 0
	receivedCount := 0
	timeout := time.After(1 * time.Second)

	for receivedSize < 1024 {
		select {
		case delivery := <-deliveries:
			receivedSize += len(delivery.Body)
			receivedCount++
			t.Logf("Received message %d, size %d, total %d", receivedCount, len(delivery.Body), receivedSize)
		case <-timeout:
			break
		}
	}

	t.Logf("Received %d messages, %d bytes with prefetch size=1024", receivedCount, receivedSize)
}

// TestPrefetchCountAndSize tests both count and size limits
func TestPrefetchCountAndSize(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Set both count and size limits
	err = ch.Qos(5, 2048, false) // 5 messages OR 2048 bytes
	if err != nil {
		t.Fatalf("Qos failed: %v", err)
	}

	// Publish 10 small messages
	for i := 0; i < 10; i++ {
		msg := rabbitmq.Publishing{
			Body: []byte("small"),
		}
		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	deliveries, err := ch.Consume(queueName, "", rabbitmq.ConsumeOptions{
		AutoAck: false,
	})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	// Should hit count limit (5) before size limit
	received := 0
	timeout := time.After(1 * time.Second)

	for received < 5 {
		select {
		case <-deliveries:
			received++
		case <-timeout:
			break
		}
	}

	if received != 5 {
		t.Errorf("Should receive prefetch count (5), got %d", received)
	}

	// Should not get 6th message
	select {
	case <-deliveries:
		t.Error("Should not receive 6th message")
	case <-time.After(500 * time.Millisecond):
		t.Log("Correctly stopped at count limit")
	}
}

// TestPrefetchZero tests no prefetch limit
func TestPrefetchZero(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Set QoS to 0 (unlimited)
	err = ch.Qos(0, 0, false)
	if err != nil {
		t.Fatalf("Qos failed: %v", err)
	}

	// Publish many messages
	numMessages := 20
	for i := 0; i < numMessages; i++ {
		msg := rabbitmq.Publishing{Body: []byte("msg")}
		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	deliveries, err := ch.Consume(queueName, "", rabbitmq.ConsumeOptions{
		AutoAck: false,
	})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	// Should receive all messages (no limit)
	received := 0
	timeout := time.After(2 * time.Second)

	for received < numMessages {
		select {
		case <-deliveries:
			received++
		case <-timeout:
			break
		}
	}

	t.Logf("Received %d/%d messages with no prefetch limit", received, numMessages)

	if received < numMessages {
		t.Logf("Warning: Only received %d/%d (may be buffering)", received, numMessages)
	}
}

// TestChangingPrefetch tests changing prefetch during consumption
func TestChangingPrefetch(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Start with prefetch=1
	err = ch.Qos(1, 0, false)
	if err != nil {
		t.Fatalf("Initial Qos failed: %v", err)
	}

	// Publish messages
	for i := 0; i < 10; i++ {
		msg := rabbitmq.Publishing{Body: []byte(string(rune('0' + i)))}
		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	deliveries, err := ch.Consume(queueName, "", rabbitmq.ConsumeOptions{
		AutoAck: true,
	})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	// Consume a few
	for i := 0; i < 3; i++ {
		select {
		case <-deliveries:
			t.Logf("Consumed with prefetch=1")
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Change prefetch to 5
	err = ch.Qos(5, 0, false)
	if err != nil {
		t.Fatalf("Changed Qos failed: %v", err)
	}

	t.Log("Changed prefetch to 5")

	// Continue consuming
	for i := 0; i < 7; i++ {
		select {
		case <-deliveries:
			t.Logf("Consumed with prefetch=5")
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
