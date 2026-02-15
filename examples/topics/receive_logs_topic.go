package main

import (
	"fmt"
	"log"
	"os"

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

	// Declare topic exchange
	exchangeName := "topic_logs"
	err = ch.ExchangeDeclare(exchangeName, "topic", rabbitmq.ExchangeDeclareOptions{})
	if err != nil {
		log.Fatalf("Failed to declare exchange: %v", err)
	}

	// Declare exclusive queue
	q, err := ch.QueueDeclare("", rabbitmq.QueueDeclareOptions{
		Exclusive:  true,
		AutoDelete: true,
	})
	if err != nil {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	// Get binding keys from command line
	bindingKeys := []string{"#"} // Default: all messages
	if len(os.Args) > 1 {
		bindingKeys = os.Args[1:]
	}

	// Bind queue for each routing key pattern
	for _, key := range bindingKeys {
		err = ch.QueueBind(q.Name, exchangeName, key, nil)
		if err != nil {
			log.Fatalf("Failed to bind queue: %v", err)
		}
		fmt.Printf("Binding queue %s to exchange %s with routing key '%s'\n", q.Name, exchangeName, key)
	}

	// Start consuming
	deliveries, err := ch.Consume(q.Name, "", rabbitmq.ConsumeOptions{
		AutoAck: true,
	})
	if err != nil {
		log.Fatalf("Failed to start consuming: %v", err)
	}

	fmt.Println("Waiting for logs. To exit press CTRL+C")

	// Process messages
	for delivery := range deliveries {
		fmt.Printf("[%s] %s\n", delivery.RoutingKey, delivery.Body)
	}
}
