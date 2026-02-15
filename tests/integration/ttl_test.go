package integration

import (
	"testing"
	"time"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

// TestPerMessageTTL tests per-message TTL (time-to-live)
func TestPerMessageTTL(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare queue without TTL
	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Publish message with 100ms TTL
	msg := rabbitmq.Publishing{
		Body: []byte("expiring message"),
		Properties: rabbitmq.Properties{
			Expiration: "100", // 100 milliseconds
		},
	}

	err = ch.Publish("", queueName, false, false, msg)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Wait for message to expire
	time.Sleep(200 * time.Millisecond)

	// Try to get message - should be expired
	response, ok, err := ch.BasicGet(queueName, false)
	if err != nil {
		t.Fatalf("BasicGet failed: %v", err)
	}
	if ok {
		t.Errorf("Message should have expired, but got: %s", response.Body)
	}
}

// TestPerQueueTTL tests queue-level TTL
func TestPerQueueTTL(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare queue with 100ms TTL for all messages
	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Args: rabbitmq.Table{
			"x-message-ttl": int32(100),
		},
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Publish message without TTL (will use queue TTL)
	msg := rabbitmq.Publishing{
		Body: []byte("expiring message"),
	}

	err = ch.Publish("", queueName, false, false, msg)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Wait for message to expire
	time.Sleep(200 * time.Millisecond)

	// Try to get message - should be expired
	response, ok, err := ch.BasicGet(queueName, false)
	if err != nil {
		t.Fatalf("BasicGet failed: %v", err)
	}
	if ok {
		t.Errorf("Message should have expired, but got: %s", response.Body)
	}
}

// TestQueueTTL tests queue expiration
func TestQueueTTL(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)

	// Declare queue with 200ms expiration (queue itself expires if unused)
	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Args: rabbitmq.Table{
			"x-expires": int32(200),
		},
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Wait for queue to expire
	time.Sleep(300 * time.Millisecond)

	// Try passive declare - should fail because queue expired
	_, err = ch.QueueDeclarePassive(queueName)
	if err == nil {
		t.Error("Queue should have expired")
		CleanupQueue(t, ch, queueName)
	}
}

// TestMessageTTLPriority tests that per-message TTL takes precedence
func TestMessageTTLPriority(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Queue TTL: 1000ms
	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Args: rabbitmq.Table{
			"x-message-ttl": int32(1000),
		},
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Message with shorter TTL (100ms)
	msg := rabbitmq.Publishing{
		Body: []byte("quick expiry"),
		Properties: rabbitmq.Properties{
			Expiration: "100",
		},
	}

	err = ch.Publish("", queueName, false, false, msg)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Wait 200ms - message should be expired (100ms < 1000ms)
	time.Sleep(200 * time.Millisecond)

	response, ok, err := ch.BasicGet(queueName, false)
	if err != nil {
		t.Fatalf("BasicGet failed: %v", err)
	}
	if ok {
		t.Errorf("Message should have expired, but got: %s", response.Body)
	}
}

// TestDeadLetterExchangeWithTTL tests DLX with expired messages
func TestDeadLetterExchangeWithTTL(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	dlxQueue := queueName + "-dlx"
	dlxExchange := queueName + "-dlx-exchange"

	defer CleanupQueue(t, ch, queueName)
	defer CleanupQueue(t, ch, dlxQueue)
	defer CleanupExchange(t, ch, dlxExchange)

	// Declare DLX exchange
	err := ch.ExchangeDeclare(dlxExchange, "direct", rabbitmq.ExchangeDeclareOptions{})
	if err != nil {
		t.Fatalf("DLX ExchangeDeclare failed: %v", err)
	}

	// Declare DLX queue
	_, err = ch.QueueDeclare(dlxQueue, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("DLX QueueDeclare failed: %v", err)
	}

	// Bind DLX queue
	err = ch.QueueBind(dlxQueue, dlxExchange, "", nil)
	if err != nil {
		t.Fatalf("DLX QueueBind failed: %v", err)
	}

	// Declare main queue with TTL and DLX
	_, err = ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Args: rabbitmq.Table{
			"x-message-ttl":             int32(100),
			"x-dead-letter-exchange":    dlxExchange,
			"x-dead-letter-routing-key": "",
		},
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Publish message
	msg := rabbitmq.Publishing{
		Body: []byte("will be dead-lettered"),
	}

	err = ch.Publish("", queueName, false, false, msg)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Wait for message to expire and be dead-lettered
	time.Sleep(300 * time.Millisecond)

	// Check main queue - should be empty
	response, ok, err := ch.BasicGet(queueName, true)
	if err != nil {
		t.Fatalf("BasicGet on main queue failed: %v", err)
	}
	if ok {
		t.Errorf("Main queue should be empty, but got: %s", response.Body)
	}

	// Check DLX queue - should have the message
	response, ok, err = ch.BasicGet(dlxQueue, true)
	if err != nil {
		t.Fatalf("BasicGet on DLX queue failed: %v", err)
	}
	if !ok {
		t.Fatal("DLX queue should have the dead-lettered message")
	}
	if string(response.Body) != "will be dead-lettered" {
		t.Errorf("Body: got %q, want %q", response.Body, "will be dead-lettered")
	}

	// Verify death headers
	if response.Properties.Headers == nil {
		t.Fatal("Headers should contain death information")
	}
}

// TestMaxLengthWithTTL tests interaction between max-length and TTL
func TestMaxLengthWithTTL(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare queue with max length 2 and TTL 1 second
	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Args: rabbitmq.Table{
			"x-max-length":  int32(2),
			"x-message-ttl": int32(1000),
		},
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Publish 3 messages
	for i := 1; i <= 3; i++ {
		msg := rabbitmq.Publishing{
			Body: []byte(string(rune('0' + i))),
		}
		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	// Should only have 2 messages (first dropped due to max-length)
	response, ok, err := ch.BasicGet(queueName, true)
	if err != nil {
		t.Fatalf("BasicGet 1 failed: %v", err)
	}
	if !ok {
		t.Fatal("Should have first message")
	}
	if string(response.Body) != "2" {
		t.Errorf("First message: got %q, want %q", response.Body, "2")
	}

	response, ok, err = ch.BasicGet(queueName, true)
	if err != nil {
		t.Fatalf("BasicGet 2 failed: %v", err)
	}
	if !ok {
		t.Fatal("Should have second message")
	}
	if string(response.Body) != "3" {
		t.Errorf("Second message: got %q, want %q", response.Body, "3")
	}

	// No more messages
	_, ok, err = ch.BasicGet(queueName, true)
	if err != nil {
		t.Fatalf("BasicGet 3 failed: %v", err)
	}
	if ok {
		t.Error("Should not have third message")
	}
}
