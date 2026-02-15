package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

func main() {
	// Create factory
	factory := rabbitmq.NewConnectionFactory(
		rabbitmq.WithHost("localhost"),
		rabbitmq.WithPort(5672),
		rabbitmq.WithCredentials("guest", "guest"),
	)

	// Create connection
	conn, err := factory.NewConnection()
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer conn.Close()

	log.Println("Connected to RabbitMQ")

	// Create channel
	ch, err := conn.NewChannel()
	if err != nil {
		log.Fatal("Failed to open channel:", err)
	}
	defer ch.Close()

	// Declare queue
	queue, err := ch.QueueDeclare("hello", rabbitmq.QueueDeclareOptions{
		Durable: false,
	})
	if err != nil {
		log.Fatal("Failed to declare queue:", err)
	}

	log.Printf("Queue declared: %s", queue.Name)

	// Set QoS (prefetch 1 message at a time)
	err = ch.Qos(1, 0, false)
	if err != nil {
		log.Fatal("Failed to set QoS:", err)
	}

	// Start consuming
	deliveries, err := ch.Consume(queue.Name, "", rabbitmq.ConsumeOptions{
		AutoAck: false,
	})
	if err != nil {
		log.Fatal("Failed to start consuming:", err)
	}

	log.Println("Waiting for messages. Press CTRL+C to exit.")

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	// Process messages
	go func() {
		for delivery := range deliveries {
			log.Printf("Received: %s", delivery.Body)

			// Acknowledge message
			if err := delivery.Ack(false); err != nil {
				log.Printf("Failed to ack: %v", err)
			}
		}
	}()

	// Wait for interrupt
	<-sigChan
	log.Println("Shutting down...")
}
