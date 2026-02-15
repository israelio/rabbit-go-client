package integration

import (
	"testing"
	"time"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

// TestAlternateExchange tests alternate exchange for unroutable messages
func TestAlternateExchange(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	mainExchange := GenerateExchangeName(t)
	altExchange := mainExchange + "-ae"
	altQueue := mainExchange + "-ae-queue"

	defer CleanupExchange(t, ch, mainExchange)
	defer CleanupExchange(t, ch, altExchange)
	defer CleanupQueue(t, ch, altQueue)

	// Declare alternate exchange
	err := ch.ExchangeDeclare(altExchange, "fanout", rabbitmq.ExchangeDeclareOptions{})
	if err != nil {
		t.Fatalf("AE ExchangeDeclare failed: %v", err)
	}

	// Declare main exchange with alternate exchange
	err = ch.ExchangeDeclare(mainExchange, "direct", rabbitmq.ExchangeDeclareOptions{
		Args: rabbitmq.Table{
			"alternate-exchange": altExchange,
		},
	})
	if err != nil {
		t.Fatalf("Main ExchangeDeclare failed: %v", err)
	}

	// Declare and bind alternate queue
	_, err = ch.QueueDeclare(altQueue, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("AE QueueDeclare failed: %v", err)
	}

	err = ch.QueueBind(altQueue, altExchange, "", nil)
	if err != nil {
		t.Fatalf("AE QueueBind failed: %v", err)
	}

	// Publish unroutable message to main exchange
	msg := rabbitmq.Publishing{
		Body: []byte("unroutable message"),
	}

	err = ch.Publish(mainExchange, "no-such-route", false, false, msg)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Wait for message to be routed to alternate exchange
	time.Sleep(200 * time.Millisecond)

	// Check alternate queue
	response, ok, err := ch.BasicGet(altQueue, true)
	if err != nil {
		t.Fatalf("BasicGet failed: %v", err)
	}
	if !ok {
		t.Fatal("Message should be in alternate queue")
	}
	if string(response.Body) != "unroutable message" {
		t.Errorf("Body: got %q, want %q", response.Body, "unroutable message")
	}
}

// TestInternalExchange tests internal exchange (cannot publish directly)
func TestInternalExchange(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	exchangeName := GenerateExchangeName(t)
	defer CleanupExchange(t, ch, exchangeName)

	// Declare internal exchange
	err := ch.ExchangeDeclare(exchangeName, "direct", rabbitmq.ExchangeDeclareOptions{
		Internal: true,
	})
	if err != nil {
		t.Fatalf("ExchangeDeclare failed: %v", err)
	}

	// Try to publish to internal exchange - should fail or be ignored
	msg := rabbitmq.Publishing{
		Body: []byte("test"),
	}

	err = ch.Publish(exchangeName, "test", false, false, msg)
	// Behavior may vary - either error or silent failure
	if err != nil {
		t.Logf("Publish to internal exchange rejected: %v", err)
	} else {
		t.Log("Publish to internal exchange accepted (will be dropped)")
	}
}

// TestExchangeToExchangeBinding tests routing between exchanges
func TestExchangeToExchangeBinding(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	sourceExchange := GenerateExchangeName(t) + "-source"
	destExchange := GenerateExchangeName(t) + "-dest"
	queueName := GenerateQueueName(t)

	defer CleanupExchange(t, ch, sourceExchange)
	defer CleanupExchange(t, ch, destExchange)
	defer CleanupQueue(t, ch, queueName)

	// Declare exchanges
	err := ch.ExchangeDeclare(sourceExchange, "direct", rabbitmq.ExchangeDeclareOptions{})
	if err != nil {
		t.Fatalf("Source ExchangeDeclare failed: %v", err)
	}

	err = ch.ExchangeDeclare(destExchange, "fanout", rabbitmq.ExchangeDeclareOptions{})
	if err != nil {
		t.Fatalf("Dest ExchangeDeclare failed: %v", err)
	}

	// Bind exchanges
	err = ch.ExchangeBind(destExchange, sourceExchange, "test-key", nil)
	if err != nil {
		t.Fatalf("ExchangeBind failed: %v", err)
	}

	// Declare and bind queue to destination exchange
	_, err = ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	err = ch.QueueBind(queueName, destExchange, "", nil)
	if err != nil {
		t.Fatalf("QueueBind failed: %v", err)
	}

	// Publish to source exchange
	msg := rabbitmq.Publishing{
		Body: []byte("routed message"),
	}

	err = ch.Publish(sourceExchange, "test-key", false, false, msg)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Wait for routing
	time.Sleep(200 * time.Millisecond)

	// Check queue
	response, ok, err := ch.BasicGet(queueName, true)
	if err != nil {
		t.Fatalf("BasicGet failed: %v", err)
	}
	if !ok {
		t.Fatal("Message should have been routed through exchanges")
	}
	if string(response.Body) != "routed message" {
		t.Errorf("Body: got %q, want %q", response.Body, "routed message")
	}
}

// TestHeadersExchange tests headers exchange matching
func TestHeadersExchange(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	exchangeName := GenerateExchangeName(t)
	queue1 := GenerateQueueName(t) + "-1"
	queue2 := GenerateQueueName(t) + "-2"

	defer CleanupExchange(t, ch, exchangeName)
	defer CleanupQueue(t, ch, queue1)
	defer CleanupQueue(t, ch, queue2)

	// Declare headers exchange
	err := ch.ExchangeDeclare(exchangeName, "headers", rabbitmq.ExchangeDeclareOptions{})
	if err != nil {
		t.Fatalf("ExchangeDeclare failed: %v", err)
	}

	// Declare queues
	_, err = ch.QueueDeclare(queue1, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("Queue1 declare failed: %v", err)
	}

	_, err = ch.QueueDeclare(queue2, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("Queue2 declare failed: %v", err)
	}

	// Bind queue1 with x-match: all
	err = ch.QueueBind(queue1, exchangeName, "", rabbitmq.Table{
		"x-match": "all",
		"format":  "pdf",
		"type":    "report",
	})
	if err != nil {
		t.Fatalf("Queue1 bind failed: %v", err)
	}

	// Bind queue2 with x-match: any
	err = ch.QueueBind(queue2, exchangeName, "", rabbitmq.Table{
		"x-match": "any",
		"format":  "pdf",
		"type":    "log",
	})
	if err != nil {
		t.Fatalf("Queue2 bind failed: %v", err)
	}

	// Test 1: Publish message matching all headers for queue1
	msg1 := rabbitmq.Publishing{
		Body: []byte("message 1"),
		Properties: rabbitmq.Properties{
			Headers: rabbitmq.Table{
				"format": "pdf",
				"type":   "report",
			},
		},
	}

	err = ch.Publish(exchangeName, "", false, false, msg1)
	if err != nil {
		t.Fatalf("Publish 1 failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Check queue1 - should have message
	_, ok, err := ch.BasicGet(queue1, true)
	if err != nil {
		t.Fatalf("BasicGet queue1 failed: %v", err)
	}
	if !ok {
		t.Error("Queue1 should have message 1")
	}

	// Check queue2 - should also have message (matches "format")
	_, ok, err = ch.BasicGet(queue2, true)
	if err != nil {
		t.Fatalf("BasicGet queue2 failed: %v", err)
	}
	if !ok {
		t.Error("Queue2 should have message 1")
	}

	// Test 2: Publish message matching only one header
	msg2 := rabbitmq.Publishing{
		Body: []byte("message 2"),
		Properties: rabbitmq.Properties{
			Headers: rabbitmq.Table{
				"format": "pdf",
			},
		},
	}

	err = ch.Publish(exchangeName, "", false, false, msg2)
	if err != nil {
		t.Fatalf("Publish 2 failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Queue1 should not have message (needs all headers)
	_, ok, err = ch.BasicGet(queue1, true)
	if err != nil {
		t.Fatalf("BasicGet queue1 failed: %v", err)
	}
	if ok {
		t.Error("Queue1 should not have message 2 (missing header)")
	}

	// Queue2 should have message (needs any header)
	_, ok, err = ch.BasicGet(queue2, true)
	if err != nil {
		t.Fatalf("BasicGet queue2 failed: %v", err)
	}
	if !ok {
		t.Error("Queue2 should have message 2")
	}
}

// TestExchangeEquivalence tests declaring same exchange with same parameters
func TestExchangeEquivalence(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	exchangeName := GenerateExchangeName(t)
	defer CleanupExchange(t, ch, exchangeName)

	// First declaration
	err := ch.ExchangeDeclare(exchangeName, "direct", rabbitmq.ExchangeDeclareOptions{
		Durable: true,
	})
	if err != nil {
		t.Fatalf("First ExchangeDeclare failed: %v", err)
	}

	// Second declaration with same parameters - should succeed
	err = ch.ExchangeDeclare(exchangeName, "direct", rabbitmq.ExchangeDeclareOptions{
		Durable: true,
	})
	if err != nil {
		t.Errorf("Equivalent declaration failed: %v", err)
	}

	// Declaration with different parameters - should fail
	err = ch.ExchangeDeclare(exchangeName, "fanout", rabbitmq.ExchangeDeclareOptions{
		Durable: true,
	})
	if err == nil {
		t.Error("Non-equivalent declaration should fail")
	}
}

// TestExchangePassiveDeclare tests passive exchange declaration
func TestExchangePassiveDeclare(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	exchangeName := GenerateExchangeName(t)

	// Passive declare non-existent exchange - should fail
	err := ch.ExchangeDeclarePassive(exchangeName, "direct")
	if err == nil {
		t.Error("Passive declare of non-existent exchange should fail")
	}

	// Declare exchange
	err = ch.ExchangeDeclare(exchangeName, "direct", rabbitmq.ExchangeDeclareOptions{})
	if err != nil {
		t.Fatalf("ExchangeDeclare failed: %v", err)
	}
	defer CleanupExchange(t, ch, exchangeName)

	// Passive declare existing exchange - should succeed
	err = ch.ExchangeDeclarePassive(exchangeName, "direct")
	if err != nil {
		t.Errorf("Passive declare of existing exchange failed: %v", err)
	}
}

// TestExchangeAutoDelete tests auto-delete exchange
func TestExchangeAutoDelete(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	exchangeName := GenerateExchangeName(t)
	queueName := GenerateQueueName(t)

	// Declare auto-delete exchange
	err := ch.ExchangeDeclare(exchangeName, "direct", rabbitmq.ExchangeDeclareOptions{
		AutoDelete: true,
	})
	if err != nil {
		t.Fatalf("ExchangeDeclare failed: %v", err)
	}

	// Declare and bind queue
	_, err = ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}
	defer CleanupQueue(t, ch, queueName)

	err = ch.QueueBind(queueName, exchangeName, "", nil)
	if err != nil {
		t.Fatalf("QueueBind failed: %v", err)
	}

	// Unbind queue - exchange should auto-delete
	err = ch.QueueUnbind(queueName, exchangeName, "", nil)
	if err != nil {
		t.Fatalf("QueueUnbind failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Exchange should be gone
	err = ch.ExchangeDeclarePassive(exchangeName, "direct")
	if err == nil {
		t.Error("Auto-delete exchange should have been deleted")
		CleanupExchange(t, ch, exchangeName)
	}
}
