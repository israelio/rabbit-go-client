package integration

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

// TestBasicPublishConsume tests basic publish and consume
func TestBasicPublishConsume(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare queue
	queue, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Durable:    false,
		AutoDelete: true,
		Exclusive:  false,
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Publish message
	messageBody := []byte("Hello, RabbitMQ!")
	err = ch.Publish("", queue.Name, false, false, rabbitmq.Publishing{
		Properties: rabbitmq.TextPlain,
		Body:       messageBody,
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Consume message
	deliveries, err := ch.Consume(queue.Name, "", rabbitmq.ConsumeOptions{
		AutoAck: true,
	})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	// Wait for message
	select {
	case delivery := <-deliveries:
		if string(delivery.Body) != string(messageBody) {
			t.Errorf("Message body mismatch: got %q, want %q", delivery.Body, messageBody)
		}
		if delivery.Exchange != "" {
			t.Errorf("Exchange: got %q, want empty", delivery.Exchange)
		}
		if delivery.RoutingKey != queue.Name {
			t.Errorf("RoutingKey: got %q, want %q", delivery.RoutingKey, queue.Name)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
}

// TestBasicGet tests synchronous message retrieval
func TestBasicGet(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare queue
	queue, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Durable: false,
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Publish messages
	numMessages := 5
	for i := 0; i < numMessages; i++ {
		body := []byte(fmt.Sprintf("Message %d", i))
		err = ch.Publish("", queue.Name, false, false, rabbitmq.Publishing{
			Body: body,
		})
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	// Get messages
	for i := 0; i < numMessages; i++ {
		response, ok, err := ch.BasicGet(queue.Name, true)
		if err != nil {
			t.Fatalf("BasicGet failed: %v", err)
		}
		if !ok {
			t.Fatalf("No message available at index %d", i)
		}

		expectedBody := fmt.Sprintf("Message %d", i)
		if string(response.Body) != expectedBody {
			t.Errorf("Message %d: got %q, want %q", i, response.Body, expectedBody)
		}
	}

	// Verify queue is empty
	response, ok, err := ch.BasicGet(queue.Name, true)
	if err != nil {
		t.Fatalf("BasicGet failed: %v", err)
	}
	if ok {
		t.Errorf("Expected empty queue, got message: %s", response.Body)
	}
}

// TestManualAcknowledgment tests manual message acknowledgment
func TestManualAcknowledgment(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare queue
	queue, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Durable: false,
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Publish message
	err = ch.Publish("", queue.Name, false, false, rabbitmq.Publishing{
		Body: []byte("test message"),
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Consume with manual ack
	deliveries, err := ch.Consume(queue.Name, "", rabbitmq.ConsumeOptions{
		AutoAck: false,
	})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	// Receive and acknowledge
	select {
	case delivery := <-deliveries:
		// Acknowledge message
		err = delivery.Ack(false)
		if err != nil {
			t.Errorf("Ack failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message")
	}

	// Verify queue is empty
	response, ok, _ := ch.BasicGet(queue.Name, true)
	if ok {
		t.Errorf("Queue should be empty after ack, got: %s", response.Body)
	}
}

// TestNegativeAcknowledgment tests message rejection with requeue
func TestNegativeAcknowledgment(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare queue
	queue, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Durable: false,
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Publish message
	messageBody := []byte("test message")
	err = ch.Publish("", queue.Name, false, false, rabbitmq.Publishing{
		Body: messageBody,
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Consume with manual ack
	deliveries, err := ch.Consume(queue.Name, "", rabbitmq.ConsumeOptions{
		AutoAck: false,
	})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	// Receive and reject with requeue
	select {
	case delivery := <-deliveries:
		// Reject and requeue
		err = delivery.Nack(false, true)
		if err != nil {
			t.Errorf("Nack failed: %v", err)
		}

		// Check redelivered flag on next delivery
		select {
		case delivery2 := <-deliveries:
			if !delivery2.Redelivered {
				t.Error("Message should be marked as redelivered")
			}
			if string(delivery2.Body) != string(messageBody) {
				t.Error("Redelivered message body mismatch")
			}
			delivery2.Ack(false)
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for redelivered message")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
}

// TestQoS tests quality of service (prefetch)
func TestQoS(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare queue
	queue, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Durable: false,
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Set QoS
	prefetchCount := 2
	err = ch.Qos(prefetchCount, 0, false)
	if err != nil {
		t.Fatalf("Qos failed: %v", err)
	}

	// Publish multiple messages
	numMessages := 10
	for i := 0; i < numMessages; i++ {
		err = ch.Publish("", queue.Name, false, false, rabbitmq.Publishing{
			Body: []byte(fmt.Sprintf("Message %d", i)),
		})
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	// Consume with manual ack
	deliveries, err := ch.Consume(queue.Name, "", rabbitmq.ConsumeOptions{
		AutoAck: false,
	})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	// Receive messages (should be limited by QoS)
	received := 0
	for i := 0; i < numMessages; i++ {
		select {
		case delivery := <-deliveries:
			received++
			// Acknowledge after processing
			time.Sleep(100 * time.Millisecond)
			delivery.Ack(false)
		case <-time.After(2 * time.Second):
			break
		}
	}

	if received != numMessages {
		t.Logf("Received %d messages (prefetch=%d)", received, prefetchCount)
	}
}

// TestMultipleConsumers tests multiple consumers on same queue
func TestMultipleConsumers(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare queue
	queue, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Durable: false,
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Create multiple consumers
	numConsumers := 3
	consumers := make([]<-chan rabbitmq.Delivery, numConsumers)

	for i := 0; i < numConsumers; i++ {
		deliveries, err := ch.Consume(queue.Name, fmt.Sprintf("consumer-%d", i), rabbitmq.ConsumeOptions{
			AutoAck: true,
		})
		if err != nil {
			t.Fatalf("Consume failed for consumer %d: %v", i, err)
		}
		consumers[i] = deliveries
	}

	// Publish messages
	numMessages := 30
	for i := 0; i < numMessages; i++ {
		err = ch.Publish("", queue.Name, false, false, rabbitmq.Publishing{
			Body: []byte(fmt.Sprintf("Message %d", i)),
		})
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	// Count messages received by each consumer
	counts := make([]int, numConsumers)
	var wg sync.WaitGroup
	wg.Add(numConsumers)

	for i := 0; i < numConsumers; i++ {
		go func(idx int, deliveries <-chan rabbitmq.Delivery) {
			defer wg.Done()
			timeout := time.After(5 * time.Second)
			for {
				select {
				case _, ok := <-deliveries:
					if !ok {
						return
					}
					counts[idx]++
				case <-timeout:
					return
				}
			}
		}(i, consumers[i])
	}

	wg.Wait()

	// Verify messages were distributed
	totalReceived := 0
	for i, count := range counts {
		t.Logf("Consumer %d received %d messages", i, count)
		totalReceived += count
	}

	if totalReceived != numMessages {
		t.Errorf("Total received: got %d, want %d", totalReceived, numMessages)
	}

	// Verify fair distribution (each consumer should get some messages)
	for i, count := range counts {
		if count == 0 {
			t.Errorf("Consumer %d received no messages", i)
		}
	}
}

// TestMessageProperties tests message properties
func TestMessageProperties(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare queue
	queue, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Durable: false,
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Publish message with properties
	timestamp := time.Now()
	err = ch.Publish("", queue.Name, false, false, rabbitmq.Publishing{
		Properties: rabbitmq.Properties{
			ContentType:     "application/json",
			ContentEncoding: "utf-8",
			DeliveryMode:    2,
			Priority:        5,
			CorrelationId:   "corr-123",
			ReplyTo:         "reply-queue",
			MessageId:       "msg-456",
			Timestamp:       timestamp,
			Type:            "user.created",
			UserId:          "guest",
			AppId:           "test-app",
			Headers: rabbitmq.Table{
				"x-custom": "value",
			},
		},
		Body: []byte(`{"name":"test"}`),
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Consume message
	deliveries, err := ch.Consume(queue.Name, "", rabbitmq.ConsumeOptions{
		AutoAck: true,
	})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	// Verify properties
	select {
	case delivery := <-deliveries:
		props := delivery.Properties

		if props.ContentType != "application/json" {
			t.Errorf("ContentType: got %q, want %q", props.ContentType, "application/json")
		}
		if props.ContentEncoding != "utf-8" {
			t.Errorf("ContentEncoding: got %q, want %q", props.ContentEncoding, "utf-8")
		}
		if props.DeliveryMode != 2 {
			t.Errorf("DeliveryMode: got %d, want 2", props.DeliveryMode)
		}
		if props.Priority != 5 {
			t.Errorf("Priority: got %d, want 5", props.Priority)
		}
		if props.CorrelationId != "corr-123" {
			t.Errorf("CorrelationId: got %q, want %q", props.CorrelationId, "corr-123")
		}
		if props.ReplyTo != "reply-queue" {
			t.Errorf("ReplyTo: got %q, want %q", props.ReplyTo, "reply-queue")
		}
		if props.MessageId != "msg-456" {
			t.Errorf("MessageId: got %q, want %q", props.MessageId, "msg-456")
		}
		if props.Type != "user.created" {
			t.Errorf("Type: got %q, want %q", props.Type, "user.created")
		}

		// Verify headers
		if len(props.Headers) == 0 {
			t.Error("Headers should not be empty")
		}

	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
}
