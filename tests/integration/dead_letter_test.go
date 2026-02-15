package integration

import (
	"testing"
	"time"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

// TestDeadLetterExchange tests basic dead letter exchange functionality
func TestDeadLetterExchange(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	mainQueue := GenerateQueueName(t)
	dlxQueue := mainQueue + "-dlx"
	dlxExchange := mainQueue + "-dlx-exchange"

	defer CleanupQueue(t, ch, mainQueue)
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

	// Declare main queue with DLX
	_, err = ch.QueueDeclare(mainQueue, rabbitmq.QueueDeclareOptions{
		Args: rabbitmq.Table{
			"x-dead-letter-exchange": dlxExchange,
		},
	})
	if err != nil {
		t.Fatalf("Main QueueDeclare failed: %v", err)
	}

	// Publish message
	msg := rabbitmq.Publishing{
		Body: []byte("test message"),
	}

	err = ch.Publish("", mainQueue, false, false, msg)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Consume and reject message
	deliveries, err := ch.Consume(mainQueue, "", rabbitmq.ConsumeOptions{})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	select {
	case delivery := <-deliveries:
		// Reject without requeue - should go to DLX
		err = delivery.Reject(false)
		if err != nil {
			t.Fatalf("Reject failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message")
	}

	// Wait for dead-lettering
	time.Sleep(200 * time.Millisecond)

	// Check DLX queue
	response, ok, err := ch.BasicGet(dlxQueue, true)
	if err != nil {
		t.Fatalf("BasicGet on DLX failed: %v", err)
	}
	if !ok {
		t.Fatal("Message should be in DLX queue")
	}
	if string(response.Body) != "test message" {
		t.Errorf("Body: got %q, want %q", response.Body, "test message")
	}

	// Check for x-death header
	if response.Properties.Headers == nil {
		t.Error("Headers should contain death information")
	}
}

// TestDeadLetterWithRoutingKey tests DLX with custom routing key
func TestDeadLetterWithRoutingKey(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	mainQueue := GenerateQueueName(t)
	dlxQueue := mainQueue + "-dlx"
	dlxExchange := mainQueue + "-dlx-exchange"
	routingKey := "dead.letters"

	defer CleanupQueue(t, ch, mainQueue)
	defer CleanupQueue(t, ch, dlxQueue)
	defer CleanupExchange(t, ch, dlxExchange)

	// Declare DLX exchange
	err := ch.ExchangeDeclare(dlxExchange, "direct", rabbitmq.ExchangeDeclareOptions{})
	if err != nil {
		t.Fatalf("DLX ExchangeDeclare failed: %v", err)
	}

	// Declare DLX queue with routing key
	_, err = ch.QueueDeclare(dlxQueue, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("DLX QueueDeclare failed: %v", err)
	}

	err = ch.QueueBind(dlxQueue, dlxExchange, routingKey, nil)
	if err != nil {
		t.Fatalf("DLX QueueBind failed: %v", err)
	}

	// Declare main queue with DLX and routing key
	_, err = ch.QueueDeclare(mainQueue, rabbitmq.QueueDeclareOptions{
		Args: rabbitmq.Table{
			"x-dead-letter-exchange":    dlxExchange,
			"x-dead-letter-routing-key": routingKey,
		},
	})
	if err != nil {
		t.Fatalf("Main QueueDeclare failed: %v", err)
	}

	// Publish and reject message
	msg := rabbitmq.Publishing{Body: []byte("routed message")}
	err = ch.Publish("", mainQueue, false, false, msg)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	deliveries, err := ch.Consume(mainQueue, "", rabbitmq.ConsumeOptions{})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	select {
	case delivery := <-deliveries:
		delivery.Reject(false)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout")
	}

	time.Sleep(200 * time.Millisecond)

	// Message should be in DLX queue with custom routing key
	response, ok, err := ch.BasicGet(dlxQueue, true)
	if err != nil {
		t.Fatalf("BasicGet failed: %v", err)
	}
	if !ok {
		t.Fatal("Message should be in DLX queue")
	}
	if string(response.Body) != "routed message" {
		t.Errorf("Body: got %q, want %q", response.Body, "routed message")
	}
}

// TestDeadLetterMultipleRejects tests message rejected multiple times
func TestDeadLetterMultipleRejects(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queue1 := GenerateQueueName(t) + "-1"
	queue2 := GenerateQueueName(t) + "-2"
	dlxQueue := GenerateQueueName(t) + "-dlx"
	dlxExchange1 := GenerateExchangeName(t) + "-1"
	dlxExchange2 := GenerateExchangeName(t) + "-2"

	defer CleanupQueue(t, ch, queue1)
	defer CleanupQueue(t, ch, queue2)
	defer CleanupQueue(t, ch, dlxQueue)
	defer CleanupExchange(t, ch, dlxExchange1)
	defer CleanupExchange(t, ch, dlxExchange2)

	// Setup DLX chain: queue1 -> queue2 -> dlxQueue
	err := ch.ExchangeDeclare(dlxExchange1, "direct", rabbitmq.ExchangeDeclareOptions{})
	if err != nil {
		t.Fatalf("DLX1 declare failed: %v", err)
	}

	err = ch.ExchangeDeclare(dlxExchange2, "direct", rabbitmq.ExchangeDeclareOptions{})
	if err != nil {
		t.Fatalf("DLX2 declare failed: %v", err)
	}

	_, err = ch.QueueDeclare(queue2, rabbitmq.QueueDeclareOptions{
		Args: rabbitmq.Table{
			"x-dead-letter-exchange": dlxExchange2,
		},
	})
	if err != nil {
		t.Fatalf("Queue2 declare failed: %v", err)
	}

	err = ch.QueueBind(queue2, dlxExchange1, "", nil)
	if err != nil {
		t.Fatalf("Queue2 bind failed: %v", err)
	}

	_, err = ch.QueueDeclare(dlxQueue, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("DLX queue declare failed: %v", err)
	}

	err = ch.QueueBind(dlxQueue, dlxExchange2, "", nil)
	if err != nil {
		t.Fatalf("DLX queue bind failed: %v", err)
	}

	_, err = ch.QueueDeclare(queue1, rabbitmq.QueueDeclareOptions{
		Args: rabbitmq.Table{
			"x-dead-letter-exchange": dlxExchange1,
		},
	})
	if err != nil {
		t.Fatalf("Queue1 declare failed: %v", err)
	}

	// Publish message
	msg := rabbitmq.Publishing{Body: []byte("multi-reject")}
	err = ch.Publish("", queue1, false, false, msg)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Reject from queue1
	deliveries1, err := ch.Consume(queue1, "consumer1", rabbitmq.ConsumeOptions{})
	if err != nil {
		t.Fatalf("Consume queue1 failed: %v", err)
	}

	select {
	case delivery := <-deliveries1:
		delivery.Reject(false)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout on queue1")
	}

	time.Sleep(200 * time.Millisecond)

	// Reject from queue2
	deliveries2, err := ch.Consume(queue2, "consumer2", rabbitmq.ConsumeOptions{})
	if err != nil {
		t.Fatalf("Consume queue2 failed: %v", err)
	}

	select {
	case delivery := <-deliveries2:
		delivery.Reject(false)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout on queue2")
	}

	time.Sleep(200 * time.Millisecond)

	// Should be in final DLX queue
	response, ok, err := ch.BasicGet(dlxQueue, true)
	if err != nil {
		t.Fatalf("BasicGet failed: %v", err)
	}
	if !ok {
		t.Fatal("Message should be in final DLX queue")
	}

	// Check x-death header shows multiple deaths
	if response.Properties.Headers == nil {
		t.Fatal("Headers should contain death information")
	}
	t.Logf("Message rejected multiple times, headers: %+v", response.Properties.Headers)
}

// TestDeadLetterOnMaxLength tests DLX when queue max length exceeded
func TestDeadLetterOnMaxLength(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	mainQueue := GenerateQueueName(t)
	dlxQueue := mainQueue + "-dlx"
	dlxExchange := mainQueue + "-dlx-exchange"

	defer CleanupQueue(t, ch, mainQueue)
	defer CleanupQueue(t, ch, dlxQueue)
	defer CleanupExchange(t, ch, dlxExchange)

	// Setup DLX
	err := ch.ExchangeDeclare(dlxExchange, "direct", rabbitmq.ExchangeDeclareOptions{})
	if err != nil {
		t.Fatalf("DLX declare failed: %v", err)
	}

	_, err = ch.QueueDeclare(dlxQueue, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("DLX queue declare failed: %v", err)
	}

	err = ch.QueueBind(dlxQueue, dlxExchange, "", nil)
	if err != nil {
		t.Fatalf("DLX bind failed: %v", err)
	}

	// Declare queue with max length and DLX
	_, err = ch.QueueDeclare(mainQueue, rabbitmq.QueueDeclareOptions{
		Args: rabbitmq.Table{
			"x-max-length":           int32(2),
			"x-dead-letter-exchange": dlxExchange,
		},
	})
	if err != nil {
		t.Fatalf("Main queue declare failed: %v", err)
	}

	// Publish 3 messages
	for i := 1; i <= 3; i++ {
		msg := rabbitmq.Publishing{
			Body: []byte(string(rune('0' + i))),
		}
		err = ch.Publish("", mainQueue, false, false, msg)
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	// First message should be dead-lettered
	response, ok, err := ch.BasicGet(dlxQueue, true)
	if err != nil {
		t.Fatalf("BasicGet DLX failed: %v", err)
	}
	if !ok {
		t.Fatal("First message should be dead-lettered")
	}
	if string(response.Body) != "1" {
		t.Errorf("Dead-lettered message: got %q, want 1", response.Body)
	}

	// Main queue should have messages 2 and 3
	response, ok, err = ch.BasicGet(mainQueue, true)
	if err != nil {
		t.Fatalf("BasicGet main failed: %v", err)
	}
	if !ok {
		t.Fatal("Should have message 2")
	}
	if string(response.Body) != "2" {
		t.Errorf("Message: got %q, want 2", response.Body)
	}
}

// TestDeadLetterWithNack tests nack with multiple flag
func TestDeadLetterWithNack(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	mainQueue := GenerateQueueName(t)
	dlxQueue := mainQueue + "-dlx"
	dlxExchange := mainQueue + "-dlx-exchange"

	defer CleanupQueue(t, ch, mainQueue)
	defer CleanupQueue(t, ch, dlxQueue)
	defer CleanupExchange(t, ch, dlxExchange)

	// Setup DLX
	err := ch.ExchangeDeclare(dlxExchange, "direct", rabbitmq.ExchangeDeclareOptions{})
	if err != nil {
		t.Fatalf("DLX declare failed: %v", err)
	}

	_, err = ch.QueueDeclare(dlxQueue, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("DLX queue declare failed: %v", err)
	}

	err = ch.QueueBind(dlxQueue, dlxExchange, "", nil)
	if err != nil {
		t.Fatalf("DLX bind failed: %v", err)
	}

	_, err = ch.QueueDeclare(mainQueue, rabbitmq.QueueDeclareOptions{
		Args: rabbitmq.Table{
			"x-dead-letter-exchange": dlxExchange,
		},
	})
	if err != nil {
		t.Fatalf("Main queue declare failed: %v", err)
	}

	// Publish multiple messages
	for i := 1; i <= 3; i++ {
		msg := rabbitmq.Publishing{
			Body: []byte(string(rune('0' + i))),
		}
		err = ch.Publish("", mainQueue, false, false, msg)
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	// Consume and nack all with multiple=true
	deliveries, err := ch.Consume(mainQueue, "", rabbitmq.ConsumeOptions{})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	var lastTag uint64
	for i := 0; i < 3; i++ {
		select {
		case delivery := <-deliveries:
			lastTag = delivery.DeliveryTag
		case <-time.After(2 * time.Second):
			t.Fatal("Timeout")
		}
	}

	// Nack all messages
	err = ch.BasicNack(lastTag, true, false)
	if err != nil {
		t.Fatalf("Nack failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// All should be in DLX
	for i := 1; i <= 3; i++ {
		response, ok, err := ch.BasicGet(dlxQueue, true)
		if err != nil {
			t.Fatalf("BasicGet %d failed: %v", i, err)
		}
		if !ok {
			t.Fatalf("Message %d should be in DLX", i)
		}
		t.Logf("Dead-lettered message %d: %s", i, response.Body)
	}
}
