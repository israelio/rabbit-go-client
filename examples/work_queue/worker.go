package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

func main() {
	// Create connection factory
	factory := rabbitmq.NewConnectionFactory(
		rabbitmq.WithHost("localhost"),
		rabbitmq.WithPort(5672),
		rabbitmq.WithCredentials("guest", "guest"),
	)

	// Connect
	conn, err := factory.NewConnection()
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Create channel
	ch, err := conn.NewChannel()
	if err != nil {
		log.Fatalf("Failed to create channel: %v", err)
	}
	defer ch.Close()

	// Declare durable queue
	queueName := "task_queue"
	_, err = ch.QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{
		Durable: true, // Queue survives broker restart
	})
	if err != nil {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	// Set prefetch count - only process 1 message at a time
	// This ensures fair dispatch across multiple workers
	err = ch.Qos(1, 0, false)
	if err != nil {
		log.Fatalf("Failed to set QoS: %v", err)
	}

	// Start consuming with manual acknowledgments
	deliveries, err := ch.Consume(queueName, "", rabbitmq.ConsumeOptions{
		AutoAck: false, // Manual ack for reliability
	})
	if err != nil {
		log.Fatalf("Failed to start consuming: %v", err)
	}

	fmt.Println("Worker started. Waiting for tasks...")
	fmt.Println("To exit press CTRL+C")

	// Process tasks
	for delivery := range deliveries {
		task := string(delivery.Body)
		fmt.Printf("Received task: %s\n", task)

		// Simulate work - dots indicate duration
		dots := strings.Count(task, ".")
		duration := time.Duration(dots) * time.Second
		fmt.Printf("Processing for %v...\n", duration)
		time.Sleep(duration)

		fmt.Println("Done")

		// Acknowledge task completion
		err = delivery.Ack(false)
		if err != nil {
			log.Printf("Failed to ack message: %v", err)
		}
	}
}
