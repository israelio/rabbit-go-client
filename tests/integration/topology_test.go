package integration

import (
	"testing"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

// TestQueueDeclare tests queue declaration
func TestQueueDeclare(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)
	defer CleanupQueue(t, ch, queueName)

	// Declare queue
	queue, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	if queue.Name != queueName {
		t.Errorf("Queue name: got %q, want %q", queue.Name, queueName)
	}
}

// TestQueueDeclareAutoDelete tests auto-delete queue
func TestQueueDeclareAutoDelete(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)

	// Declare auto-delete queue
	queue, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Durable:    false,
		AutoDelete: true,
		Exclusive:  false,
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	t.Logf("Created auto-delete queue: %s", queue.Name)
	// Queue will be automatically deleted when last consumer disconnects
}

// TestQueueDeclareExclusive tests exclusive queue
func TestQueueDeclareExclusive(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	// Declare exclusive queue with empty name (server-generated)
	queue, err := ch.QueueDeclare("", rabbitmq.QueueDeclareOptions{
		Durable:    false,
		AutoDelete: true,
		Exclusive:  true,
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	if queue.Name == "" {
		t.Error("Expected server-generated queue name")
	}

	t.Logf("Created exclusive queue: %s", queue.Name)
}

// TestQueueDelete tests queue deletion
func TestQueueDelete(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	queueName := GenerateQueueName(t)

	// Declare queue
	_, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Durable: false,
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Delete queue
	messageCount, err := ch.QueueDelete(queueName, rabbitmq.QueueDeleteOptions{
		IfUnused: false,
		IfEmpty:  false,
	})
	if err != nil {
		t.Fatalf("QueueDelete failed: %v", err)
	}

	t.Logf("Deleted queue with %d messages", messageCount)

	// Verify queue is deleted (passive declare should fail)
	_, err = ch.QueueDeclarePassive(queueName)
	if err == nil {
		t.Error("Queue should not exist after deletion")
	}
}

// TestQueuePurge tests queue purging
func TestQueuePurge(t *testing.T) {
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
	numMessages := 10
	for i := 0; i < numMessages; i++ {
		ch.Publish("", queue.Name, false, false, rabbitmq.Publishing{
			Body: []byte("test"),
		})
	}

	// Purge queue
	purgedCount, err := ch.QueuePurge(queue.Name, false)
	if err != nil {
		t.Fatalf("QueuePurge failed: %v", err)
	}

	if purgedCount != numMessages {
		t.Errorf("Purged count: got %d, want %d", purgedCount, numMessages)
	}

	// Verify queue is empty
	response, ok, _ := ch.BasicGet(queue.Name, true)
	if ok {
		t.Errorf("Queue should be empty after purge, got: %s", response.Body)
	}
}

// TestExchangeDeclare tests exchange declaration
func TestExchangeDeclare(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	exchangeTypes := []string{"direct", "fanout", "topic", "headers"}

	for _, exchangeType := range exchangeTypes {
		t.Run(exchangeType, func(t *testing.T) {
			exchangeName := GenerateExchangeName(t)
			defer CleanupExchange(t, ch, exchangeName)

			err := ch.ExchangeDeclare(exchangeName, exchangeType, rabbitmq.ExchangeDeclareOptions{
				Durable:    false,
				AutoDelete: true,
				Internal:   false,
			})
			if err != nil {
				t.Fatalf("ExchangeDeclare failed: %v", err)
			}

			t.Logf("Created %s exchange: %s", exchangeType, exchangeName)
		})
	}
}

// TestExchangeDelete tests exchange deletion
func TestExchangeDelete(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	exchangeName := GenerateExchangeName(t)

	// Declare exchange
	err := ch.ExchangeDeclare(exchangeName, "direct", rabbitmq.ExchangeDeclareOptions{
		Durable: false,
	})
	if err != nil {
		t.Fatalf("ExchangeDeclare failed: %v", err)
	}

	// Delete exchange
	err = ch.ExchangeDelete(exchangeName, rabbitmq.ExchangeDeleteOptions{
		IfUnused: false,
	})
	if err != nil {
		t.Fatalf("ExchangeDelete failed: %v", err)
	}

	// Verify exchange is deleted
	err = ch.ExchangeDeclarePassive(exchangeName, "direct")
	if err == nil {
		t.Error("Exchange should not exist after deletion")
	}
}

// TestQueueBind tests binding queue to exchange
func TestQueueBind(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	exchangeName := GenerateExchangeName(t)
	queueName := GenerateQueueName(t)
	defer CleanupExchange(t, ch, exchangeName)
	defer CleanupQueue(t, ch, queueName)

	// Declare exchange
	err := ch.ExchangeDeclare(exchangeName, "direct", rabbitmq.ExchangeDeclareOptions{
		Durable:    false,
		AutoDelete: true,
	})
	if err != nil {
		t.Fatalf("ExchangeDeclare failed: %v", err)
	}

	// Declare queue
	queue, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Durable:    false,
		AutoDelete: true,
	})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Bind queue to exchange
	routingKey := "test.key"
	err = ch.QueueBind(queue.Name, exchangeName, routingKey, nil)
	if err != nil {
		t.Fatalf("QueueBind failed: %v", err)
	}

	// Publish to exchange
	err = ch.Publish(exchangeName, routingKey, false, false, rabbitmq.Publishing{
		Body: []byte("routed message"),
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Consume from queue
	response, ok, err := ch.BasicGet(queue.Name, true)
	if err != nil {
		t.Fatalf("BasicGet failed: %v", err)
	}
	if !ok {
		t.Fatal("Expected message in queue")
	}

	if string(response.Body) != "routed message" {
		t.Errorf("Message body: got %q, want %q", response.Body, "routed message")
	}
}

// TestTopicExchangeRouting tests topic exchange routing
func TestTopicExchangeRouting(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	exchangeName := GenerateExchangeName(t)
	defer CleanupExchange(t, ch, exchangeName)

	// Declare topic exchange
	err := ch.ExchangeDeclare(exchangeName, "topic", rabbitmq.ExchangeDeclareOptions{
		Durable:    false,
		AutoDelete: true,
	})
	if err != nil {
		t.Fatalf("ExchangeDeclare failed: %v", err)
	}

	// Create queues with different binding patterns
	patterns := []struct {
		pattern string
		matches []string
	}{
		{
			pattern: "user.#",
			matches: []string{"user.created", "user.updated", "user.deleted.soft"},
		},
		{
			pattern: "*.created",
			matches: []string{"user.created", "order.created"},
		},
		{
			pattern: "user.created",
			matches: []string{"user.created"},
		},
	}

	for i, p := range patterns {
		queueName := GenerateQueueName(t)
		defer CleanupQueue(t, ch, queueName)

		// Declare queue
		queue, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
			Durable:    false,
			AutoDelete: true,
		})
		if err != nil {
			t.Fatalf("QueueDeclare failed: %v", err)
		}

		// Bind with pattern
		err = ch.QueueBind(queue.Name, exchangeName, p.pattern, nil)
		if err != nil {
			t.Fatalf("QueueBind failed: %v", err)
		}

		// Test matching routing keys
		for _, routingKey := range p.matches {
			// Publish
			err = ch.Publish(exchangeName, routingKey, false, false, rabbitmq.Publishing{
				Body: []byte(routingKey),
			})
			if err != nil {
				t.Fatalf("Publish failed: %v", err)
			}

			// Verify delivery
			_, ok, err := ch.BasicGet(queue.Name, true)
			if err != nil {
				t.Fatalf("BasicGet failed: %v", err)
			}
			if !ok {
				t.Errorf("Pattern %d (%q): expected message for routing key %q", i, p.pattern, routingKey)
			} else {
				t.Logf("Pattern %q matched routing key %q", p.pattern, routingKey)
			}
		}
	}
}

// TestFanoutExchange tests fanout exchange
func TestFanoutExchange(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	exchangeName := GenerateExchangeName(t)
	defer CleanupExchange(t, ch, exchangeName)

	// Declare fanout exchange
	err := ch.ExchangeDeclare(exchangeName, "fanout", rabbitmq.ExchangeDeclareOptions{
		Durable:    false,
		AutoDelete: true,
	})
	if err != nil {
		t.Fatalf("ExchangeDeclare failed: %v", err)
	}

	// Create multiple queues
	numQueues := 3
	queues := make([]string, numQueues)

	for i := 0; i < numQueues; i++ {
		queueName := GenerateQueueName(t)
		defer CleanupQueue(t, ch, queueName)

		queue, err := ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
			Durable:    false,
			AutoDelete: true,
		})
		if err != nil {
			t.Fatalf("QueueDeclare failed: %v", err)
		}

		// Bind to fanout exchange (routing key ignored)
		err = ch.QueueBind(queue.Name, exchangeName, "", nil)
		if err != nil {
			t.Fatalf("QueueBind failed: %v", err)
		}

		queues[i] = queue.Name
	}

	// Publish one message
	err = ch.Publish(exchangeName, "", false, false, rabbitmq.Publishing{
		Body: []byte("fanout message"),
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Verify all queues received the message
	for i, queueName := range queues {
		response, ok, err := ch.BasicGet(queueName, true)
		if err != nil {
			t.Fatalf("BasicGet failed for queue %d: %v", i, err)
		}
		if !ok {
			t.Errorf("Queue %d did not receive message", i)
		} else if string(response.Body) != "fanout message" {
			t.Errorf("Queue %d: wrong message body", i)
		}
	}
}
