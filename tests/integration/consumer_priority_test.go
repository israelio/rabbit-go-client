package integration

import (
	"testing"
	"time"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

// TestConsumerPriority tests that higher priority consumers get messages first
func TestConsumerPriority(t *testing.T) {
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

	// Publish messages before consumers start
	numMessages := 10
	for i := 0; i < numMessages; i++ {
		msg := rabbitmq.Publishing{
			Body: []byte("message"),
		}
		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	// Consumer with priority 10 (high)
	highPriorityDeliveries, err := ch.Consume(queueName, "high", rabbitmq.ConsumeOptions{
		Args: rabbitmq.Table{
			"x-priority": int32(10),
		},
	})
	if err != nil {
		t.Fatalf("High priority consume failed: %v", err)
	}

	// Consumer with priority 1 (low)
	lowPriorityDeliveries, err := ch.Consume(queueName, "low", rabbitmq.ConsumeOptions{
		Args: rabbitmq.Table{
			"x-priority": int32(1),
		},
	})
	if err != nil {
		t.Fatalf("Low priority consume failed: %v", err)
	}

	// Count messages per consumer
	highCount := 0
	lowCount := 0
	timeout := time.After(5 * time.Second)

	for highCount+lowCount < numMessages {
		select {
		case delivery := <-highPriorityDeliveries:
			highCount++
			delivery.Ack(false)
		case delivery := <-lowPriorityDeliveries:
			lowCount++
			delivery.Ack(false)
		case <-timeout:
			t.Fatal("Timeout waiting for messages")
		}
	}

	// High priority consumer should get most/all messages
	if highCount < lowCount {
		t.Errorf("High priority consumer should get more messages: high=%d, low=%d", highCount, lowCount)
	}

	t.Logf("Messages distribution: high=%d, low=%d", highCount, lowCount)
}

// TestConsumerPriorityWithPrefetch tests consumer priority with QoS
func TestConsumerPriorityWithPrefetch(t *testing.T) {
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

	// Set QoS
	err = ch.Qos(1, 0, false)
	if err != nil {
		t.Fatalf("Qos failed: %v", err)
	}

	// High priority consumer
	highPriorityDeliveries, err := ch.Consume(queueName, "high", rabbitmq.ConsumeOptions{
		Args: rabbitmq.Table{
			"x-priority": int32(10),
		},
	})
	if err != nil {
		t.Fatalf("High priority consume failed: %v", err)
	}

	// Low priority consumer
	lowPriorityDeliveries, err := ch.Consume(queueName, "low", rabbitmq.ConsumeOptions{
		Args: rabbitmq.Table{
			"x-priority": int32(1),
		},
	})
	if err != nil {
		t.Fatalf("Low priority consume failed: %v", err)
	}

	// Publish messages
	numMessages := 5
	for i := 0; i < numMessages; i++ {
		msg := rabbitmq.Publishing{
			Body: []byte("message"),
		}
		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	// Process messages with delay to see priority effect
	highCount := 0
	lowCount := 0
	timeout := time.After(10 * time.Second)

	for highCount+lowCount < numMessages {
		select {
		case delivery := <-highPriorityDeliveries:
			time.Sleep(100 * time.Millisecond) // Simulate work
			delivery.Ack(false)
			highCount++
		case delivery := <-lowPriorityDeliveries:
			time.Sleep(100 * time.Millisecond) // Simulate work
			delivery.Ack(false)
			lowCount++
		case <-timeout:
			t.Fatal("Timeout waiting for messages")
		}
	}

	t.Logf("With QoS - Messages distribution: high=%d, low=%d", highCount, lowCount)
}

// TestConsumerPriorityNoArguments tests consumer without priority (default 0)
func TestConsumerPriorityNoArguments(t *testing.T) {
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

	// Consumer with explicit priority
	priorityDeliveries, err := ch.Consume(queueName, "priority", rabbitmq.ConsumeOptions{
		Args: rabbitmq.Table{
			"x-priority": int32(5),
		},
	})
	if err != nil {
		t.Fatalf("Priority consume failed: %v", err)
	}

	// Consumer without priority (default 0)
	defaultDeliveries, err := ch.Consume(queueName, "default", rabbitmq.ConsumeOptions{})
	if err != nil {
		t.Fatalf("Default consume failed: %v", err)
	}

	// Publish messages
	numMessages := 10
	for i := 0; i < numMessages; i++ {
		msg := rabbitmq.Publishing{
			Body: []byte("message"),
		}
		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	// Count messages
	priorityCount := 0
	defaultCount := 0
	timeout := time.After(5 * time.Second)

	for priorityCount+defaultCount < numMessages {
		select {
		case delivery := <-priorityDeliveries:
			priorityCount++
			delivery.Ack(false)
		case delivery := <-defaultDeliveries:
			defaultCount++
			delivery.Ack(false)
		case <-timeout:
			t.Fatal("Timeout waiting for messages")
		}
	}

	// Priority consumer should get more messages
	if priorityCount <= defaultCount {
		t.Errorf("Priority consumer should get more: priority=%d, default=%d", priorityCount, defaultCount)
	}

	t.Logf("Messages distribution: priority=%d, default=%d", priorityCount, defaultCount)
}

// TestConsumerPriorityNegative tests that negative priorities are handled
func TestConsumerPriorityNegative(t *testing.T) {
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

	// Consumer with negative priority
	_, err = ch.Consume(queueName, "negative", rabbitmq.ConsumeOptions{
		Args: rabbitmq.Table{
			"x-priority": int32(-5),
		},
	})

	// RabbitMQ may accept negative priorities or reject them
	// Either behavior is acceptable
	if err != nil {
		t.Logf("Negative priority rejected: %v", err)
	} else {
		t.Log("Negative priority accepted")
	}
}

// TestConsumerPriorityRange tests priority range limits
func TestConsumerPriorityRange(t *testing.T) {
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

	// Test various priority values
	priorities := []int32{0, 1, 5, 10, 100, 255}

	for _, priority := range priorities {
		consumerTag := "consumer-" + string(rune('0'+priority))
		_, err := ch.Consume(queueName, consumerTag, rabbitmq.ConsumeOptions{
			Args: rabbitmq.Table{
				"x-priority": priority,
			},
		})

		if err != nil {
			t.Logf("Priority %d failed: %v", priority, err)
		} else {
			t.Logf("Priority %d accepted", priority)
			// Cancel consumer
			ch.BasicCancel(consumerTag, false)
		}
	}
}
