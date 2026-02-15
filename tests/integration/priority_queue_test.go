package integration

import (
	"testing"
	"time"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

// TestPriorityQueue tests priority queue functionality
func TestPriorityQueue(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare priority queue with max priority 10
	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Args: rabbitmq.Table{
			"x-max-priority": int32(10),
		},
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Publish messages with different priorities
	priorities := []uint8{5, 1, 9, 3, 7}
	for i, pri := range priorities {
		msg := rabbitmq.Publishing{
			Body: []byte(string(rune('A' + i))),
			Properties: rabbitmq.Properties{
				Priority: pri,
			},
		}
		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	// Wait for messages to be prioritized
	time.Sleep(200 * time.Millisecond)

	// Consume messages - should be in priority order (high to low)
	// Expected order: 9(C), 7(E), 5(A), 3(D), 1(B)
	expected := []uint8{9, 7, 5, 3, 1}
	received := make([]uint8, 0)

	for i := 0; i < 5; i++ {
		response, ok, err := ch.BasicGet(queueName, true)
		if err != nil {
			t.Fatalf("BasicGet %d failed: %v", i, err)
		}
		if !ok {
			t.Fatalf("Should have message %d", i)
		}
		received = append(received, response.Properties.Priority)
		t.Logf("Message %d: %s, priority %d", i, response.Body, response.Properties.Priority)
	}

	// Verify priority order
	for i, pri := range expected {
		if received[i] != pri {
			t.Errorf("Position %d: got priority %d, want %d", i, received[i], pri)
		}
	}
}

// TestPriorityQueueWithConsumer tests priority with consumer
func TestPriorityQueueWithConsumer(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare priority queue
	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Args: rabbitmq.Table{
			"x-max-priority": int32(5),
		},
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Publish messages
	for i := 0; i < 10; i++ {
		priority := uint8(i % 6) // 0-5
		msg := rabbitmq.Publishing{
			Body: []byte(string(rune('0' + i))),
			Properties: rabbitmq.Properties{
				Priority: priority,
			},
		}
		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	// Consume messages
	deliveries, err := ch.Consume(queueName, "", rabbitmq.ConsumeOptions{
		AutoAck: true,
	})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	received := make([]uint8, 0)
	timeout := time.After(3 * time.Second)

	for len(received) < 10 {
		select {
		case delivery := <-deliveries:
			received = append(received, delivery.Properties.Priority)
			t.Logf("Consumed: %s, priority %d", delivery.Body, delivery.Properties.Priority)
		case <-timeout:
			t.Fatalf("Timeout, received %d messages", len(received))
		}
	}

	// Verify high priority messages came first
	// First few should have higher priorities than last few
	firstHalf := received[:5]
	secondHalf := received[5:]

	firstAvg := average(firstHalf)
	secondAvg := average(secondHalf)

	if firstAvg <= secondAvg {
		t.Errorf("First half avg priority (%v) should be > second half (%v)", firstAvg, secondAvg)
	}
}

func average(nums []uint8) float64 {
	sum := 0
	for _, n := range nums {
		sum += int(n)
	}
	return float64(sum) / float64(len(nums))
}

// TestPriorityQueueDefaultPriority tests default priority (0)
func TestPriorityQueueDefaultPriority(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare priority queue
	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Args: rabbitmq.Table{
			"x-max-priority": int32(10),
		},
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Publish message without priority (default 0)
	msg1 := rabbitmq.Publishing{
		Body: []byte("default priority"),
	}
	err = ch.Publish("", queueName, false, false, msg1)
	if err != nil {
		t.Fatalf("Publish 1 failed: %v", err)
	}

	// Publish message with priority 1
	msg2 := rabbitmq.Publishing{
		Body: []byte("priority 1"),
		Properties: rabbitmq.Properties{
			Priority: 1,
		},
	}
	err = ch.Publish("", queueName, false, false, msg2)
	if err != nil {
		t.Fatalf("Publish 2 failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Priority 1 should come first
	response, ok, err := ch.BasicGet(queueName, true)
	if err != nil {
		t.Fatalf("BasicGet 1 failed: %v", err)
	}
	if !ok {
		t.Fatal("Should have message 1")
	}
	if string(response.Body) != "priority 1" {
		t.Errorf("First message: got %q, want 'priority 1'", response.Body)
	}

	// Default priority comes second
	response, ok, err = ch.BasicGet(queueName, true)
	if err != nil {
		t.Fatalf("BasicGet 2 failed: %v", err)
	}
	if !ok {
		t.Fatal("Should have message 2")
	}
	if string(response.Body) != "default priority" {
		t.Errorf("Second message: got %q, want 'default priority'", response.Body)
	}
}

// TestPriorityQueueMaxPriorityLimit tests priority clamping
func TestPriorityQueueMaxPriorityLimit(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare queue with max priority 5
	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Args: rabbitmq.Table{
			"x-max-priority": int32(5),
		},
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Publish message with priority exceeding max (10 > 5)
	msg := rabbitmq.Publishing{
		Body: []byte("high priority"),
		Properties: rabbitmq.Properties{
			Priority: 10, // Exceeds max
		},
	}
	err = ch.Publish("", queueName, false, false, msg)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Publish message with priority 5
	msg2 := rabbitmq.Publishing{
		Body: []byte("max priority"),
		Properties: rabbitmq.Properties{
			Priority: 5,
		},
	}
	err = ch.Publish("", queueName, false, false, msg2)
	if err != nil {
		t.Fatalf("Publish 2 failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Both should be treated as priority 5
	// Order may vary
	for i := 0; i < 2; i++ {
		response, ok, err := ch.BasicGet(queueName, true)
		if err != nil {
			t.Fatalf("BasicGet %d failed: %v", i, err)
		}
		if !ok {
			t.Fatalf("Should have message %d", i)
		}
		t.Logf("Message %d: %s, priority %d", i, response.Body, response.Properties.Priority)
	}
}

// TestPriorityQueueWithQoS tests priority queue with prefetch
func TestPriorityQueueWithQoS(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare priority queue
	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Args: rabbitmq.Table{
			"x-max-priority": int32(10),
		},
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Set QoS
	err = ch.Qos(2, 0, false)
	if err != nil {
		t.Fatalf("Qos failed: %v", err)
	}

	// Publish messages with different priorities
	for i := 0; i < 5; i++ {
		msg := rabbitmq.Publishing{
			Body: []byte(string(rune('0' + i))),
			Properties: rabbitmq.Properties{
				Priority: uint8(i),
			},
		}
		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	// Consume with manual ack
	deliveries, err := ch.Consume(queueName, "", rabbitmq.ConsumeOptions{
		AutoAck: false,
	})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	// Receive and ack messages
	received := make([]uint8, 0)
	timeout := time.After(3 * time.Second)

	for len(received) < 5 {
		select {
		case delivery := <-deliveries:
			received = append(received, delivery.Properties.Priority)
			t.Logf("Received priority %d", delivery.Properties.Priority)
			delivery.Ack(false)
			time.Sleep(100 * time.Millisecond) // Simulate processing
		case <-timeout:
			t.Fatalf("Timeout, received %d", len(received))
		}
	}

	// Should receive in priority order (4, 3, 2, 1, 0)
	for i := 0; i < len(received)-1; i++ {
		if received[i] < received[i+1] {
			t.Errorf("Priority order violated at %d: %v", i, received)
			break
		}
	}
}

// TestNormalQueueWithPriority tests priority on non-priority queue
func TestNormalQueueWithPriority(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare normal queue (no max-priority)
	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Publish messages with priorities
	for i := 0; i < 3; i++ {
		msg := rabbitmq.Publishing{
			Body: []byte(string(rune('0' + i))),
			Properties: rabbitmq.Properties{
				Priority: uint8(2 - i), // 2, 1, 0
			},
		}
		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	// Should be FIFO order (priorities ignored)
	expected := []string{"0", "1", "2"}
	for i, exp := range expected {
		response, ok, err := ch.BasicGet(queueName, true)
		if err != nil {
			t.Fatalf("BasicGet %d failed: %v", i, err)
		}
		if !ok {
			t.Fatalf("Should have message %d", i)
		}
		if string(response.Body) != exp {
			t.Errorf("Message %d: got %q, want %q (FIFO order)", i, response.Body, exp)
		}
	}
}
