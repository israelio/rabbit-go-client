package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

func main() {
	fmt.Println("=== Automatic Recovery Example ===")
	fmt.Println("This example demonstrates automatic connection recovery")
	fmt.Println("Try restarting RabbitMQ to see recovery in action:")
	fmt.Println("  docker restart rabbitmq")
	fmt.Println("")
	fmt.Println("The application will automatically reconnect and restore topology!")
	fmt.Println("")

	// Create connection factory with automatic recovery enabled
	factory := rabbitmq.NewConnectionFactory(
		rabbitmq.WithHost("localhost"),
		rabbitmq.WithPort(5672),
		rabbitmq.WithCredentials("guest", "guest"),
		rabbitmq.WithAutomaticRecovery(true),        // Enable automatic recovery
		rabbitmq.WithTopologyRecovery(true),         // Restore exchanges, queues, bindings
		rabbitmq.WithRecoveryInterval(5*time.Second), // Retry every 5 seconds
		rabbitmq.WithConnectionRetryAttempts(10),    // Try 10 times before giving up
	)

	// Connect
	conn, err := factory.NewConnection()
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	log.Println("✓ Connected to RabbitMQ")

	// Register for recovery notifications
	recoveryStarted := make(chan struct{}, 1)
	recoveryCompleted := make(chan struct{}, 1)
	recoveryFailed := make(chan error, 10)

	conn.NotifyRecoveryStarted(recoveryStarted)
	conn.NotifyRecoveryCompleted(recoveryCompleted)
	conn.NotifyRecoveryFailed(recoveryFailed)

	// Monitor recovery events
	go func() {
		for {
			select {
			case <-recoveryStarted:
				log.Println("🔄 Recovery started - connection lost, attempting to reconnect...")
			case <-recoveryCompleted:
				log.Println("✅ Recovery completed - connection and topology restored!")
			case err := <-recoveryFailed:
				log.Printf("❌ Recovery attempt failed: %v", err)
			}
		}
	}()

	// Create channel
	ch, err := conn.NewChannel()
	if err != nil {
		log.Fatalf("Failed to create channel: %v", err)
	}
	defer ch.Close()

	// Declare queue (durable for persistence)
	queueName := "recovery_test_queue"
	_, err = ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Durable: true,
	})
	if err != nil {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	log.Printf("✓ Declared queue: %s", queueName)

	// Message counter
	var messageCount atomic.Int32

	// Start publisher in background
	var publisherWg sync.WaitGroup
	publisherWg.Add(1)
	publisherStop := make(chan struct{})

	go func() {
		defer publisherWg.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				count := messageCount.Add(1)

				msg := rabbitmq.Publishing{
					Properties: rabbitmq.Properties{
						DeliveryMode: 2, // Persistent
					},
					Body: []byte(fmt.Sprintf("Message %d", count)),
				}

				err := ch.Publish("", queueName, false, false, msg)
				if err != nil {
					log.Printf("⚠ Publish error: %v (will retry)", err)
					// With automatic recovery, the connection will recover
					// and we can retry publishing
					continue
				}
				fmt.Printf("→ Published: Message %d\n", count)

			case <-publisherStop:
				return
			}
		}
	}()

	// Start consumer with callback (callback-based consumers are auto-recovered!)
	consumer := &RecoveryConsumer{}
	err = ch.ConsumeWithCallback(queueName, "recovery-consumer", rabbitmq.ConsumeOptions{
		AutoAck: false,
	}, consumer)
	if err != nil {
		log.Fatalf("Failed to start consuming: %v", err)
	}

	log.Println("✓ Consumer started (will be auto-recovered after connection failure)")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	log.Println("\nShutting down...")

	// Stop publisher
	close(publisherStop)
	publisherWg.Wait()

	log.Println("Goodbye!")
}

// RecoveryConsumer is a callback-based consumer that will be automatically
// re-established after connection recovery
type RecoveryConsumer struct {
	rabbitmq.DefaultConsumer
	deliveryCount atomic.Int32
}

// HandleConsumeOk is called when the consumer is successfully registered
func (rc *RecoveryConsumer) HandleConsumeOk(consumerTag string) {
	log.Printf("✓ Consumer registered: %s", consumerTag)
}

// HandleDelivery is called when a message is delivered
func (rc *RecoveryConsumer) HandleDelivery(consumerTag string, delivery rabbitmq.Delivery) error {
	count := rc.deliveryCount.Add(1)
	fmt.Printf("← Received (#%d): %s\n", count, delivery.Body)

	// Simulate processing
	time.Sleep(100 * time.Millisecond)

	// Acknowledge
	return delivery.Ack(false)
}

// HandleCancel is called when the server cancels the consumer
func (rc *RecoveryConsumer) HandleCancel(consumerTag string) error {
	log.Printf("⚠ Consumer cancelled by server: %s", consumerTag)
	return nil
}

// HandleShutdown is called when the channel shuts down
func (rc *RecoveryConsumer) HandleShutdown(consumerTag string, cause *rabbitmq.Error) {
	log.Printf("⚠ Consumer shutdown: %s, cause: %v", consumerTag, cause)
}

// HandleRecoverOk is called after successful recovery
func (rc *RecoveryConsumer) HandleRecoverOk(consumerTag string) {
	log.Printf("✅ Consumer recovered: %s - continuing from delivery #%d",
		consumerTag, rc.deliveryCount.Load())
}
