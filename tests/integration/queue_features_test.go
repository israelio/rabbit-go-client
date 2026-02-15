package integration

import (
	"testing"
	"time"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

// TestQueueMaxLength tests queue maximum length
func TestQueueMaxLength(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare queue with max length of 3
	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Args: rabbitmq.Table{
			"x-max-length": int32(3),
		},
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Publish 5 messages
	for i := 1; i <= 5; i++ {
		msg := rabbitmq.Publishing{
			Body: []byte(string(rune('0' + i))),
		}
		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	// Should only have 3 messages (oldest 2 dropped)
	time.Sleep(100 * time.Millisecond)

	// Get first message - should be "3"
	response, ok, err := ch.BasicGet(queueName, true)
	if err != nil {
		t.Fatalf("BasicGet 1 failed: %v", err)
	}
	if !ok {
		t.Fatal("Should have first message")
	}
	if string(response.Body) != "3" {
		t.Errorf("First message: got %q, want %q", response.Body, "3")
	}

	// Get remaining messages
	for i := 4; i <= 5; i++ {
		response, ok, err := ch.BasicGet(queueName, true)
		if err != nil {
			t.Fatalf("BasicGet %d failed: %v", i, err)
		}
		if !ok {
			t.Fatalf("Should have message %d", i)
		}
		expected := string(rune('0' + i))
		if string(response.Body) != expected {
			t.Errorf("Message %d: got %q, want %q", i, response.Body, expected)
		}
	}

	// No more messages
	_, ok, err = ch.BasicGet(queueName, true)
	if err != nil {
		t.Fatalf("Final BasicGet failed: %v", err)
	}
	if ok {
		t.Error("Should not have more messages")
	}
}

// TestQueueMaxLengthBytes tests queue maximum length in bytes
func TestQueueMaxLengthBytes(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare queue with max length of 100 bytes
	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Args: rabbitmq.Table{
			"x-max-length-bytes": int32(100),
		},
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Publish messages until we exceed limit
	for i := 1; i <= 10; i++ {
		msg := rabbitmq.Publishing{
			Body: []byte("message with some content"),
		}
		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	// Count how many messages remain
	count := 0
	for {
		_, ok, err := ch.BasicGet(queueName, true)
		if err != nil {
			t.Fatalf("BasicGet failed: %v", err)
		}
		if !ok {
			break
		}
		count++
		if count > 10 {
			t.Fatal("Too many messages in queue")
		}
	}

	// Should have fewer than 10 messages due to byte limit
	if count >= 10 {
		t.Errorf("Should have dropped some messages, got %d", count)
	}
	t.Logf("Queue retained %d messages within byte limit", count)
}

// TestQueueOverflowDropHead tests drop-head overflow behavior
func TestQueueOverflowDropHead(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare queue with max length and drop-head
	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Args: rabbitmq.Table{
			"x-max-length":     int32(3),
			"x-overflow":       "drop-head",
		},
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Publish 5 messages
	for i := 1; i <= 5; i++ {
		msg := rabbitmq.Publishing{
			Body: []byte(string(rune('0' + i))),
		}
		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	// Should have newest 3 messages (3, 4, 5)
	expected := []string{"3", "4", "5"}
	for i, exp := range expected {
		response, ok, err := ch.BasicGet(queueName, true)
		if err != nil {
			t.Fatalf("BasicGet %d failed: %v", i, err)
		}
		if !ok {
			t.Fatalf("Should have message %d", i)
		}
		if string(response.Body) != exp {
			t.Errorf("Message %d: got %q, want %q", i, response.Body, exp)
		}
	}
}

// TestQueueOverflowRejectPublish tests reject-publish overflow behavior
func TestQueueOverflowRejectPublish(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare queue with max length and reject-publish
	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Args: rabbitmq.Table{
			"x-max-length": int32(3),
			"x-overflow":   "reject-publish",
		},
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Enable publisher confirms
	err = ch.ConfirmSelect(false)
	if err != nil {
		t.Fatalf("ConfirmSelect failed: %v", err)
	}

	confirms := make(chan rabbitmq.Confirmation, 10)
	ch.NotifyPublish(confirms)

	// Publish messages
	for i := 1; i <= 5; i++ {
		msg := rabbitmq.Publishing{
			Body: []byte(string(rune('0' + i))),
		}
		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	// Check confirmations - some should be nacks
	acks := 0
	nacks := 0
	timeout := time.After(2 * time.Second)

	for acks+nacks < 5 {
		select {
		case conf := <-confirms:
			if conf.Ack {
				acks++
			} else {
				nacks++
			}
		case <-timeout:
			t.Fatalf("Timeout waiting for confirmations: got %d acks, %d nacks", acks, nacks)
		}
	}

	t.Logf("Confirmations: %d acks, %d nacks", acks, nacks)

	// Should have some nacks due to queue full
	if nacks == 0 {
		t.Error("Expected some nacks for rejected publishes")
	}
}

// TestQueueSingleActiveConsumer tests single active consumer feature
func TestQueueSingleActiveConsumer(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare queue with single active consumer
	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Args: rabbitmq.Table{
			"x-single-active-consumer": true,
		},
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Start two consumers
	consumer1, err := ch.Consume(queueName, "consumer1", rabbitmq.ConsumeOptions{})
	if err != nil {
		t.Fatalf("Consumer 1 failed: %v", err)
	}

	consumer2, err := ch.Consume(queueName, "consumer2", rabbitmq.ConsumeOptions{})
	if err != nil {
		t.Fatalf("Consumer 2 failed: %v", err)
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

	// Only one consumer should receive messages
	count1 := 0
	count2 := 0
	timeout := time.After(3 * time.Second)

	for count1+count2 < numMessages {
		select {
		case delivery := <-consumer1:
			count1++
			delivery.Ack(false)
		case delivery := <-consumer2:
			count2++
			delivery.Ack(false)
		case <-timeout:
			t.Fatal("Timeout waiting for messages")
		}
	}

	// One consumer should get all messages, other should get none
	if count1 == 0 && count2 == 0 {
		t.Error("No consumer received messages")
	} else if count1 > 0 && count2 > 0 {
		t.Errorf("Both consumers received messages: consumer1=%d, consumer2=%d", count1, count2)
	} else {
		t.Logf("Single active consumer working: consumer1=%d, consumer2=%d", count1, count2)
	}
}

// TestQueueExclusivity tests exclusive queue behavior
func TestQueueExclusivity(t *testing.T) {
	RequireRabbitMQ(t)

	conn1, ch1 := NewTestChannel(t)
	defer conn1.Close()
	defer ch1.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch1, queueName)

	// Declare exclusive queue
	_, err := ch1.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Exclusive: true,
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Try to access from another channel - should fail
	conn2, ch2 := NewTestChannel(t)
	defer conn2.Close()
	defer ch2.Close()

	_, err = ch2.QueueDeclarePassive(queueName)
	if err == nil {
		t.Error("Should not be able to access exclusive queue from another connection")
	}
}

// TestQueueMessageCount tests message count reporting
func TestQueueMessageCount(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare queue
	q, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	if q.Messages != 0 {
		t.Errorf("Initial message count: got %d, want 0", q.Messages)
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

	// Passive declare to get updated count
	q, err = ch.QueueDeclarePassive(queueName)
	if err != nil {
		t.Fatalf("QueueDeclarePassive failed: %v", err)
	}

	if q.Messages != numMessages {
		t.Errorf("Message count: got %d, want %d", q.Messages, numMessages)
	}
}
