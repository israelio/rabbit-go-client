package integration

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

// TestPublisherConfirms tests basic publisher confirms
func TestPublisherConfirms(t *testing.T) {
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

	// Enable publisher confirms
	err = ch.ConfirmSelect(false)
	if err != nil {
		t.Fatalf("ConfirmSelect failed: %v", err)
	}

	// Set up confirm listener
	confirms := make(chan rabbitmq.Confirmation, 10)
	ch.NotifyPublish(confirms)

	// Publish messages
	numMessages := 10
	for i := 0; i < numMessages; i++ {
		err = ch.Publish("", queue.Name, false, false, rabbitmq.Publishing{
			Body: []byte(fmt.Sprintf("Message %d", i)),
		})
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	// Wait for confirmations
	confirmed := 0
	timeout := time.After(10 * time.Second)

	for confirmed < numMessages {
		select {
		case conf := <-confirms:
			if !conf.Ack {
				t.Errorf("Message %d was nacked", conf.DeliveryTag)
			}
			confirmed++
		case <-timeout:
			t.Fatalf("Timeout waiting for confirmations: %d/%d confirmed", confirmed, numMessages)
		}
	}

	t.Logf("All %d messages confirmed", confirmed)
}

// TestPublisherConfirmsSynchronous tests synchronous confirms with WaitForConfirms
func TestPublisherConfirmsSynchronous(t *testing.T) {
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

	// Enable publisher confirms
	err = ch.ConfirmSelect(false)
	if err != nil {
		t.Fatalf("ConfirmSelect failed: %v", err)
	}

	// Publish messages
	numMessages := 5
	for i := 0; i < numMessages; i++ {
		err = ch.Publish("", queue.Name, false, false, rabbitmq.Publishing{
			Body: []byte(fmt.Sprintf("Message %d", i)),
		})
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	// Wait for all confirmations
	err = ch.WaitForConfirms(10 * time.Second)
	if err != nil {
		t.Fatalf("WaitForConfirms failed: %v", err)
	}

	t.Logf("All messages confirmed via WaitForConfirms")
}

// TestPublisherConfirmsSingleMessage tests per-message confirmation
func TestPublisherConfirmsSingleMessage(t *testing.T) {
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

	// Enable publisher confirms
	err = ch.ConfirmSelect(false)
	if err != nil {
		t.Fatalf("ConfirmSelect failed: %v", err)
	}

	// Publish with synchronous confirmation
	err = ch.PublishWithConfirm("", queue.Name, false, false, rabbitmq.Publishing{
		Body: []byte("confirmed message"),
	}, 5*time.Second)
	if err != nil {
		t.Fatalf("PublishWithConfirm failed: %v", err)
	}

	t.Log("Message confirmed synchronously")
}

// TestPublisherConfirmsConcurrent tests concurrent publishing with confirms
func TestPublisherConfirmsConcurrent(t *testing.T) {
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

	// Enable publisher confirms
	err = ch.ConfirmSelect(false)
	if err != nil {
		t.Fatalf("ConfirmSelect failed: %v", err)
	}

	// Set up confirm listener
	confirms := make(chan rabbitmq.Confirmation, 100)
	ch.NotifyPublish(confirms)

	// Publish messages concurrently
	numWorkers := 5
	messagesPerWorker := 20
	totalMessages := numWorkers * messagesPerWorker

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func(workerId int) {
			defer wg.Done()
			for j := 0; j < messagesPerWorker; j++ {
				body := []byte(fmt.Sprintf("Worker %d Message %d", workerId, j))
				err := ch.Publish("", queue.Name, false, false, rabbitmq.Publishing{
					Body: body,
				})
				if err != nil {
					t.Errorf("Publish failed: %v", err)
				}
			}
		}(i)
	}

	// Wait for confirmations
	confirmed := 0
	nacked := 0
	timeout := time.After(30 * time.Second)

	confirmDone := make(chan struct{})
	go func() {
		for confirmed+nacked < totalMessages {
			select {
			case conf := <-confirms:
				if conf.Ack {
					confirmed++
				} else {
					nacked++
				}
			case <-timeout:
				return
			}
		}
		close(confirmDone)
	}()

	wg.Wait()

	select {
	case <-confirmDone:
		if nacked > 0 {
			t.Errorf("Some messages were nacked: %d", nacked)
		}
		t.Logf("Confirmed %d/%d messages concurrently", confirmed, totalMessages)
	case <-timeout:
		t.Fatalf("Timeout waiting for confirmations: %d confirmed, %d nacked, %d total",
			confirmed, nacked, totalMessages)
	}
}

// TestPublisherConfirmsWithMandatory tests confirms with mandatory flag
func TestPublisherConfirmsWithMandatory(t *testing.T) {
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

	// Enable publisher confirms
	err = ch.ConfirmSelect(false)
	if err != nil {
		t.Fatalf("ConfirmSelect failed: %v", err)
	}

	// Set up return listener
	returns := make(chan rabbitmq.Return, 10)
	ch.NotifyReturn(returns)

	// Set up confirm listener
	confirms := make(chan rabbitmq.Confirmation, 10)
	ch.NotifyPublish(confirms)

	// Publish to routable queue (should be confirmed)
	err = ch.Publish("", queue.Name, true, false, rabbitmq.Publishing{
		Body: []byte("routable message"),
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Wait for confirmation
	select {
	case conf := <-confirms:
		if !conf.Ack {
			t.Error("Routable message was nacked")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for confirmation")
	}

	// Publish to non-existent queue (should be returned and confirmed)
	err = ch.Publish("", "non-existent-queue", true, false, rabbitmq.Publishing{
		Body: []byte("unroutable message"),
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Wait for return
	select {
	case ret := <-returns:
		t.Logf("Message returned: code=%d, text=%q", ret.ReplyCode, ret.ReplyText)
		if ret.ReplyCode != 312 { // NO_ROUTE
			t.Errorf("Expected NO_ROUTE (312), got %d", ret.ReplyCode)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for return")
	}

	// Should still be confirmed (even if returned)
	select {
	case conf := <-confirms:
		if !conf.Ack {
			t.Error("Returned message should still be confirmed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for confirmation")
	}
}

// TestReturnListener tests return listener for unroutable messages
func TestReturnListener(t *testing.T) {
	RequireRabbitMQ(t)

	conn, ch := NewTestChannel(t)
	defer conn.Close()
	defer ch.Close()

	// Set up return listener
	returns := make(chan rabbitmq.Return, 10)
	ch.NotifyReturn(returns)

	// Publish to non-existent queue with mandatory flag
	messageBody := []byte("unroutable message")
	err := ch.Publish("", "non-existent-queue-123", true, false, rabbitmq.Publishing{
		Properties: rabbitmq.Properties{
			MessageId: "test-msg-1",
		},
		Body: messageBody,
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Wait for return
	select {
	case ret := <-returns:
		t.Logf("Received return: code=%d, text=%q, exchange=%q, routingKey=%q",
			ret.ReplyCode, ret.ReplyText, ret.Exchange, ret.RoutingKey)

		if ret.ReplyCode != 312 { // NO_ROUTE
			t.Errorf("ReplyCode: got %d, want 312 (NO_ROUTE)", ret.ReplyCode)
		}
		if ret.RoutingKey != "non-existent-queue-123" {
			t.Errorf("RoutingKey: got %q, want %q", ret.RoutingKey, "non-existent-queue-123")
		}
		if string(ret.Body) != string(messageBody) {
			t.Errorf("Body: got %q, want %q", ret.Body, messageBody)
		}
		if ret.Properties.MessageId != "test-msg-1" {
			t.Errorf("MessageId: got %q, want %q", ret.Properties.MessageId, "test-msg-1")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for return notification")
	}
}

// TestTransactions tests AMQP transactions
func TestTransactions(t *testing.T) {
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

	// Start transaction mode
	err = ch.TxSelect()
	if err != nil {
		t.Fatalf("TxSelect failed: %v", err)
	}

	// Publish messages in transaction
	err = ch.Publish("", queue.Name, false, false, rabbitmq.Publishing{
		Body: []byte("tx message 1"),
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	err = ch.Publish("", queue.Name, false, false, rabbitmq.Publishing{
		Body: []byte("tx message 2"),
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Verify messages not yet visible (transaction not committed)
	response, ok, _ := ch.BasicGet(queue.Name, false)
	if ok {
		t.Errorf("Message should not be visible before commit: %s", response.Body)
	}

	// Commit transaction
	err = ch.TxCommit()
	if err != nil {
		t.Fatalf("TxCommit failed: %v", err)
	}

	// Verify messages are now visible
	response, ok, err = ch.BasicGet(queue.Name, true)
	if err != nil {
		t.Fatalf("BasicGet failed: %v", err)
	}
	if !ok {
		t.Fatal("Expected message after commit")
	}
	if string(response.Body) != "tx message 1" {
		t.Errorf("First message: got %q, want %q", response.Body, "tx message 1")
	}

	response, ok, err = ch.BasicGet(queue.Name, true)
	if err != nil {
		t.Fatalf("BasicGet failed: %v", err)
	}
	if !ok {
		t.Fatal("Expected second message after commit")
	}
	if string(response.Body) != "tx message 2" {
		t.Errorf("Second message: got %q, want %q", response.Body, "tx message 2")
	}
}

// TestTransactionRollback tests transaction rollback
func TestTransactionRollback(t *testing.T) {
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

	// Start transaction mode
	err = ch.TxSelect()
	if err != nil {
		t.Fatalf("TxSelect failed: %v", err)
	}

	// Publish message in transaction
	err = ch.Publish("", queue.Name, false, false, rabbitmq.Publishing{
		Body: []byte("rollback message"),
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Rollback transaction
	err = ch.TxRollback()
	if err != nil {
		t.Fatalf("TxRollback failed: %v", err)
	}

	// Verify message was not published
	response, ok, _ := ch.BasicGet(queue.Name, true)
	if ok {
		t.Errorf("Message should not exist after rollback: %s", response.Body)
	}
}
