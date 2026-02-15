package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

func main() {
	fmt.Println("=== Automatic Recovery Example ===")
	fmt.Println("This example demonstrates connection recovery")
	fmt.Println("Try restarting RabbitMQ to see recovery in action:")
	fmt.Println("  docker restart rabbitmq")
	fmt.Println("")

	// Message counter (shared across reconnects)
	var messageCount int
	var mu sync.Mutex

	// Run forever with automatic reconnection
	for {
		err := runWithRecovery(&messageCount, &mu)
		if err != nil {
			log.Printf("Connection lost: %v", err)
			log.Printf("Reconnecting in 5 seconds...")
			time.Sleep(5 * time.Second)
		}
	}
}

func runWithRecovery(messageCount *int, mu *sync.Mutex) error {
	// Create connection factory
	factory := rabbitmq.NewConnectionFactory(
		rabbitmq.WithHost("localhost"),
		rabbitmq.WithPort(5672),
		rabbitmq.WithCredentials("guest", "guest"),
	)

	// Connect
	conn, err := factory.NewConnection()
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	log.Println("✓ Connected to RabbitMQ")

	// Create channel
	ch, err := conn.NewChannel()
	if err != nil {
		return fmt.Errorf("failed to create channel: %w", err)
	}
	defer ch.Close()

	// Declare queue (durable for persistence)
	queueName := "recovery_test_queue"
	_, err = ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Durable: true,
	})
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	log.Printf("✓ Declared queue: %s", queueName)

	// Start consumer
	deliveries, err := ch.Consume(queueName, "", rabbitmq.ConsumeOptions{
		AutoAck: false,
	})
	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	log.Println("✓ Consumer started")

	// Monitor connection errors
	connErrors := make(chan *rabbitmq.Error, 1)
	conn.NotifyClose(connErrors)

	// Monitor channel errors
	channelErrors := make(chan *rabbitmq.Error, 1)
	ch.NotifyClose(channelErrors)

	// Start publisher in background
	publisherDone := make(chan struct{})
	go func() {
		defer close(publisherDone)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				mu.Lock()
				currentCount := *messageCount
				*messageCount++
				mu.Unlock()

				msg := rabbitmq.Publishing{
					Properties: rabbitmq.Properties{
						DeliveryMode: 2, // Persistent
					},
					Body: []byte(fmt.Sprintf("Message %d", currentCount)),
				}

				err := ch.Publish("", queueName, false, false, msg)
				if err != nil {
					log.Printf("⚠ Publish error: %v (connection lost)", err)
					return
				}
				fmt.Printf("→ Published: Message %d\n", currentCount)

			case <-connErrors:
				log.Println("⚠ Connection closed signal received in publisher")
				return
			case <-channelErrors:
				log.Println("⚠ Channel closed signal received in publisher")
				return
			}
		}
	}()

	// Consume messages
	for {
		select {
		case delivery, ok := <-deliveries:
			if !ok {
				log.Println("⚠ Delivery channel closed")
				<-publisherDone // Wait for publisher to stop
				return fmt.Errorf("delivery channel closed")
			}

			fmt.Printf("← Received: %s\n", delivery.Body)

			// Simulate processing
			time.Sleep(100 * time.Millisecond)

			// Acknowledge
			err = delivery.Ack(false)
			if err != nil {
				log.Printf("⚠ Ack error: %v", err)
			}

		case err := <-connErrors:
			log.Printf("⚠ Connection error: %v", err)
			<-publisherDone // Wait for publisher to stop
			return fmt.Errorf("connection closed: %w", err)

		case err := <-channelErrors:
			log.Printf("⚠ Channel error: %v", err)
			<-publisherDone // Wait for publisher to stop
			return fmt.Errorf("channel closed: %w", err)
		}
	}
}
