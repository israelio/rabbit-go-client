package integration

import (
	"bytes"
	"testing"
	"time"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

// TestLargeMessage tests publishing and consuming large messages
func TestLargeMessage(t *testing.T) {
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

	// Create 1MB message
	largeBody := make([]byte, 1024*1024)
	for i := range largeBody {
		largeBody[i] = byte(i % 256)
	}

	msg := rabbitmq.Publishing{
		Body: largeBody,
		Properties: rabbitmq.Properties{
			ContentType: "application/octet-stream",
		},
	}

	err = ch.Publish("", queueName, false, false, msg)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	response, ok, err := ch.BasicGet(queueName, true)
	if err != nil {
		t.Fatalf("BasicGet failed: %v", err)
	}
	if !ok {
		t.Fatal("Should have message")
	}

	if len(response.Body) != len(largeBody) {
		t.Errorf("Message size: got %d, want %d", len(response.Body), len(largeBody))
	}

	if !bytes.Equal(response.Body, largeBody) {
		t.Error("Message body corrupted")
	}

	t.Logf("Successfully sent and received %d byte message", len(largeBody))
}

// TestEmptyMessage tests publishing empty messages
func TestEmptyMessage(t *testing.T) {
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

	// Empty body
	msg := rabbitmq.Publishing{
		Body: []byte{},
		Properties: rabbitmq.Properties{
			MessageId: "empty-msg",
		},
	}

	err = ch.Publish("", queueName, false, false, msg)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	response, ok, err := ch.BasicGet(queueName, true)
	if err != nil {
		t.Fatalf("BasicGet failed: %v", err)
	}
	if !ok {
		t.Fatal("Should have message")
	}

	if len(response.Body) != 0 {
		t.Errorf("Body length: got %d, want 0", len(response.Body))
	}
	if response.Properties.MessageId != "empty-msg" {
		t.Errorf("MessageId: got %q, want empty-msg", response.Properties.MessageId)
	}
}

// TestSpecialCharactersInRoutingKey tests special characters
func TestSpecialCharactersInRoutingKey(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	exchangeName := GenerateExchangeName(t)
	queueName := GenerateQueueName(t)
	defer CleanupExchange(t, ch, exchangeName)
	defer CleanupQueue(t, ch, queueName)

	err := ch.ExchangeDeclare(exchangeName, "direct", rabbitmq.ExchangeDeclareOptions{})
	if err != nil {
		t.Fatalf("ExchangeDeclare failed: %v", err)
	}

	_, err = ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Test various special characters
	specialKeys := []string{
		"key-with-dash",
		"key.with.dots",
		"key_with_underscore",
		"key:with:colon",
		"key@with@at",
	}

	for _, key := range specialKeys {
		err = ch.QueueBind(queueName, exchangeName, key, nil)
		if err != nil {
			t.Errorf("Bind with key %q failed: %v", key, err)
			continue
		}

		msg := rabbitmq.Publishing{
			Body: []byte(key),
		}
		err = ch.Publish(exchangeName, key, false, false, msg)
		if err != nil {
			t.Errorf("Publish with key %q failed: %v", key, err)
			continue
		}

		response, ok, err := ch.BasicGet(queueName, true)
		if err != nil {
			t.Errorf("Get with key %q failed: %v", key, err)
			continue
		}
		if !ok {
			t.Errorf("Should have message for key %q", key)
			continue
		}
		if string(response.Body) != key {
			t.Errorf("Body for key %q: got %q", key, response.Body)
		}

		// Unbind
		err = ch.QueueUnbind(queueName, exchangeName, key, nil)
		if err != nil {
			t.Errorf("Unbind key %q failed: %v", key, err)
		}
	}
}

// TestMessageOrdering tests message ordering
func TestMessageOrdering(t *testing.T) {
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

	// Publish messages in order
	numMessages := 100
	for i := 0; i < numMessages; i++ {
		msg := rabbitmq.Publishing{
			Body: []byte(string(rune(i))),
			Properties: rabbitmq.Properties{
				MessageId: string(rune('0' + i)),
			},
		}
		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	// Consume and verify order
	deliveries, err := ch.Consume(queueName, "", rabbitmq.ConsumeOptions{
		AutoAck: true,
	})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	orderOK := true
	for i := 0; i < numMessages; i++ {
		select {
		case delivery := <-deliveries:
			if int(delivery.Body[0]) != i {
				orderOK = false
				t.Logf("Order violation at %d: got %d", i, int(delivery.Body[0]))
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout at message %d", i)
		}
	}

	if orderOK {
		t.Log("All messages received in correct order")
	} else {
		t.Error("Message ordering violated")
	}
}

// TestContentEncoding tests various content encodings
func TestContentEncoding(t *testing.T) {
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

	encodings := []string{
		"gzip",
		"deflate",
		"identity",
		"utf-8",
		"base64",
	}

	for _, encoding := range encodings {
		msg := rabbitmq.Publishing{
			Body: []byte("test"),
			Properties: rabbitmq.Properties{
				ContentEncoding: encoding,
			},
		}

		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Publish with encoding %q failed: %v", encoding, err)
		}

		response, ok, err := ch.BasicGet(queueName, true)
		if err != nil {
			t.Fatalf("BasicGet failed: %v", err)
		}
		if !ok {
			t.Fatal("Should have message")
		}

		if response.Properties.ContentEncoding != encoding {
			t.Errorf("ContentEncoding: got %q, want %q", response.Properties.ContentEncoding, encoding)
		}
	}
}

// TestMultipleVHosts tests connecting to different vhosts
func TestMultipleVHosts(t *testing.T) {
	t.Skip("Requires multiple vhosts configured - manual test")

	// Would test:
	// 1. Connect to default vhost (/)
	// 2. Connect to custom vhost (/test)
	// 3. Verify isolation between vhosts
}

// TestSlowConsumer tests slow consumer behavior
func TestSlowConsumer(t *testing.T) {
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

	// Set QoS
	err = ch.Qos(1, 0, false)
	if err != nil {
		t.Fatalf("Qos failed: %v", err)
	}

	// Publish messages
	for i := 0; i < 10; i++ {
		msg := rabbitmq.Publishing{Body: []byte("msg")}
		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	// Slow consumer
	deliveries, err := ch.Consume(queueName, "", rabbitmq.ConsumeOptions{
		AutoAck: false,
	})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	consumed := 0
	for consumed < 10 {
		select {
		case delivery := <-deliveries:
			time.Sleep(200 * time.Millisecond) // Slow processing
			delivery.Ack(false)
			consumed++
			t.Logf("Processed message %d slowly", consumed)
		case <-time.After(30 * time.Second):
			t.Fatalf("Timeout, consumed %d", consumed)
		}
	}

	t.Log("Slow consumer completed successfully")
}

// TestQueuePassiveDeclareNonExistent tests passive declare of non-existent queue
func TestQueuePassiveDeclareNonExistent(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t) + "-nonexistent"

	// Passive declare should fail
	_, err := ch.QueueDeclarePassive(queueName)
	if err == nil {
		t.Error("Passive declare of non-existent queue should fail")
		CleanupQueue(t, ch, queueName)
	} else {
		t.Logf("Correctly failed: %v", err)
	}
}

// TestTransactionWithReject tests transaction rollback with reject
func TestTransactionWithReject(t *testing.T) {
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

	// Enable transactions
	err = ch.TxSelect()
	if err != nil {
		t.Fatalf("TxSelect failed: %v", err)
	}

	// Publish in transaction
	for i := 0; i < 5; i++ {
		msg := rabbitmq.Publishing{Body: []byte(string(rune('0' + i)))}
		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	// Rollback
	err = ch.TxRollback()
	if err != nil {
		t.Fatalf("TxRollback failed: %v", err)
	}

	// Messages should not be in queue
	response, ok, err := ch.BasicGet(queueName, true)
	if err != nil {
		t.Fatalf("BasicGet failed: %v", err)
	}
	if ok {
		t.Errorf("Queue should be empty after rollback, got: %s", response.Body)
	}

	t.Log("Transaction rollback worked correctly")
}

// TestExclusiveConsumer tests exclusive consumer
func TestExclusiveConsumer(t *testing.T) {
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

	// Start exclusive consumer
	_, err = ch.Consume(queueName, "exclusive", rabbitmq.ConsumeOptions{
		Exclusive: true,
	})
	if err != nil {
		t.Fatalf("Exclusive consume failed: %v", err)
	}

	// Try to start another consumer - should fail
	_, err = ch.Consume(queueName, "second", rabbitmq.ConsumeOptions{})
	if err == nil {
		t.Error("Second consumer should fail when queue has exclusive consumer")
	} else {
		t.Logf("Second consumer correctly rejected: %v", err)
	}
}

// TestNoLocalConsumer tests no-local consumer flag
func TestNoLocalConsumer(t *testing.T) {
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

	// Start no-local consumer
	deliveries, err := ch.Consume(queueName, "", rabbitmq.ConsumeOptions{
		NoLocal: true,
	})
	if err != nil {
		t.Fatalf("NoLocal consume failed: %v", err)
	}

	// Publish from same connection
	msg := rabbitmq.Publishing{Body: []byte("local message")}
	err = ch.Publish("", queueName, false, false, msg)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Consumer should NOT receive it (no-local)
	select {
	case delivery := <-deliveries:
		t.Errorf("No-local consumer should not receive own messages: %s", delivery.Body)
	case <-time.After(1 * time.Second):
		t.Log("No-local working correctly - did not receive own message")
	}
}
