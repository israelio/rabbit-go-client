package main

import (
	"log"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

func main() {
	// Create factory with default settings
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

	// Publish message
	msg := rabbitmq.Publishing{
		Properties: rabbitmq.Properties{
			ContentType:  "text/plain",
			DeliveryMode: 1, // Non-persistent
		},
		Body: []byte("Hello, RabbitMQ!"),
	}

	err = ch.Publish("", queue.Name, false, false, msg)
	if err != nil {
		log.Fatal("Failed to publish:", err)
	}

	log.Println("Message published successfully!")
}
