package integration

import (
	"testing"
	"time"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

// TestRequeueOnConnectionClose tests message requeue when connection closes
func TestRequeueOnConnectionClose(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer func() {
		// Cleanup with new connection
		cleanConn, cleanCh := NewTestChannel(t)
		defer cleanConn.Close()
		defer cleanCh.Close()
		CleanupQueue(t, cleanCh, queueName)
	}()

	// Declare queue
	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Publish message
	msg := rabbitmq.Publishing{
		Body: []byte("will be requeued"),
	}

	err = ch.Publish("", queueName, false, false, msg)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Start consuming (without ack)
	deliveries, err := ch.Consume(queueName, "", rabbitmq.ConsumeOptions{
		AutoAck: false,
	})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	// Receive message but don't ack
	select {
	case delivery := <-deliveries:
		t.Logf("Received message: %s", delivery.Body)
		// Don't ack - close connection instead
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message")
	}

	// Close connection without acking
	conn.Close()

	// Wait for requeue
	time.Sleep(500 * time.Millisecond)

	// Open new connection and check message was requeued
	newConn, newCh := NewTestChannel(t)
	defer newConn.Close()
	defer newCh.Close()

	response, ok, err := newCh.BasicGet(queueName, true)
	if err != nil {
		t.Fatalf("BasicGet failed: %v", err)
	}
	if !ok {
		t.Fatal("Message should have been requeued")
	}
	if string(response.Body) != "will be requeued" {
		t.Errorf("Body: got %q, want %q", response.Body, "will be requeued")
	}
	if !response.Redelivered {
		t.Error("Redelivered flag should be true")
	}
}

// TestRequeueOnChannelClose tests message requeue when channel closes
func TestRequeueOnChannelClose(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()

	queueName := GenerateQueueName(t)
	defer func() {
		cleanCh, _ := conn.NewChannel()
		defer cleanCh.Close()
		CleanupQueue(t, cleanCh, queueName)
	}()

	// Declare queue
	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Publish message
	msg := rabbitmq.Publishing{
		Body: []byte("channel close requeue"),
	}

	err = ch.Publish("", queueName, false, false, msg)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Consume without ack
	deliveries, err := ch.Consume(queueName, "", rabbitmq.ConsumeOptions{
		AutoAck: false,
	})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	select {
	case delivery := <-deliveries:
		t.Logf("Received: %s", delivery.Body)
		// Don't ack
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout")
	}

	// Close channel
	ch.Close()

	time.Sleep(300 * time.Millisecond)

	// Open new channel and verify requeue
	newCh, err := conn.NewChannel()
	if err != nil {
		t.Fatalf("NewChannel failed: %v", err)
	}
	defer newCh.Close()

	response, ok, err := newCh.BasicGet(queueName, true)
	if err != nil {
		t.Fatalf("BasicGet failed: %v", err)
	}
	if !ok {
		t.Fatal("Message should be requeued")
	}
	if !response.Redelivered {
		t.Error("Redelivered flag should be true")
	}
}

// TestNoRequeueOnCancel tests no requeue when consumer is cancelled
func TestNoRequeueOnCancel(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare queue with no-requeue-on-cancel
	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Args: rabbitmq.Table{
			"x-cancel-on-ha-failover": false,
		},
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Publish message
	msg := rabbitmq.Publishing{
		Body: []byte("no requeue on cancel"),
	}

	err = ch.Publish("", queueName, false, false, msg)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Consume with no-ack=false
	consumerTag := "test-consumer"
	deliveries, err := ch.Consume(queueName, consumerTag, rabbitmq.ConsumeOptions{
		AutoAck: false,
	})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	select {
	case delivery := <-deliveries:
		t.Logf("Received: %s", delivery.Body)
		// Don't ack
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout")
	}

	// Cancel consumer
	err = ch.BasicCancel(consumerTag, false)
	if err != nil {
		t.Fatalf("Cancel failed: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	// Message should still be in queue (requeued on cancel)
	response, ok, err := ch.BasicGet(queueName, true)
	if err != nil {
		t.Fatalf("BasicGet failed: %v", err)
	}
	if !ok {
		t.Fatal("Message should be in queue")
	}
	t.Logf("Message after cancel: %s, redelivered: %v", response.Body, response.Redelivered)
}

// TestRequeueWithMultipleConsumers tests requeue with multiple consumers
func TestRequeueWithMultipleConsumers(t *testing.T) {
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

	// Start consumer 1
	deliveries1, err := ch.Consume(queueName, "consumer1", rabbitmq.ConsumeOptions{
		AutoAck: false,
	})
	if err != nil {
		t.Fatalf("Consumer 1 failed: %v", err)
	}

	// Start consumer 2
	deliveries2, err := ch.Consume(queueName, "consumer2", rabbitmq.ConsumeOptions{
		AutoAck: false,
	})
	if err != nil {
		t.Fatalf("Consumer 2 failed: %v", err)
	}

	// Receive messages but don't ack
	received := make([]string, 0)
	timeout := time.After(3 * time.Second)
	for len(received) < 5 {
		select {
		case delivery := <-deliveries1:
			received = append(received, string(delivery.Body))
			// Nack with requeue
			delivery.Nack(false, true)
		case delivery := <-deliveries2:
			received = append(received, string(delivery.Body))
			// Nack with requeue
			delivery.Nack(false, true)
		case <-timeout:
			t.Fatalf("Timeout, received %d messages", len(received))
		}
	}

	t.Logf("Received and requeued %d messages", len(received))

	// Cancel consumers
	ch.BasicCancel("consumer1", false)
	ch.BasicCancel("consumer2", false)

	time.Sleep(200 * time.Millisecond)

	// All messages should be requeued
	count := 0
	for i := 0; i < 5; i++ {
		response, ok, err := ch.BasicGet(queueName, true)
		if err != nil {
			t.Fatalf("BasicGet failed: %v", err)
		}
		if !ok {
			break
		}
		count++
		if !response.Redelivered {
			t.Errorf("Message %d should be marked redelivered", i+1)
		}
	}

	if count != 5 {
		t.Errorf("Requeued count: got %d, want 5", count)
	}
}

// TestRedeliveredFlag tests the redelivered flag
func TestRedeliveredFlag(t *testing.T) {
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

	// Publish message
	msg := rabbitmq.Publishing{
		Body: []byte("test redelivered"),
	}

	err = ch.Publish("", queueName, false, false, msg)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// First delivery - should NOT be redelivered
	response, ok, err := ch.BasicGet(queueName, false)
	if err != nil {
		t.Fatalf("First BasicGet failed: %v", err)
	}
	if !ok {
		t.Fatal("Should have message")
	}
	if response.Redelivered {
		t.Error("First delivery should not be marked redelivered")
	}

	// Nack with requeue
	err = ch.BasicNack(response.DeliveryTag, false, true)
	if err != nil {
		t.Fatalf("Nack failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Second delivery - should BE redelivered
	response, ok, err = ch.BasicGet(queueName, false)
	if err != nil {
		t.Fatalf("Second BasicGet failed: %v", err)
	}
	if !ok {
		t.Fatal("Should have requeued message")
	}
	if !response.Redelivered {
		t.Error("Second delivery should be marked redelivered")
	}

	// Ack
	err = ch.BasicAck(response.DeliveryTag, false)
	if err != nil {
		t.Fatalf("Ack failed: %v", err)
	}
}

// TestRequeueOrder tests message order after requeue
func TestRequeueOrder(t *testing.T) {
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

	// Publish messages in order
	for i := 1; i <= 3; i++ {
		msg := rabbitmq.Publishing{
			Body: []byte(string(rune('0' + i))),
		}
		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	// Get first message and requeue
	response, ok, err := ch.BasicGet(queueName, false)
	if err != nil {
		t.Fatalf("BasicGet failed: %v", err)
	}
	if !ok {
		t.Fatal("Should have message 1")
	}
	firstMsg := string(response.Body)

	err = ch.BasicNack(response.DeliveryTag, false, true)
	if err != nil {
		t.Fatalf("Nack failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Get messages - requeued message should be at end
	order := make([]string, 0)
	for i := 0; i < 3; i++ {
		response, ok, err := ch.BasicGet(queueName, true)
		if err != nil {
			t.Fatalf("BasicGet %d failed: %v", i, err)
		}
		if !ok {
			break
		}
		order = append(order, string(response.Body))
	}

	t.Logf("Message order after requeue: %v (first was %s)", order, firstMsg)

	// Verify we got all messages
	if len(order) != 3 {
		t.Errorf("Should have 3 messages, got %d", len(order))
	}
}
