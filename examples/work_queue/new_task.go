package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/israelio/rabbit-go-client/rabbitmq"
	"github.com/israelio/rabbit-go-client/internal/protocol"
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

	// Get task from command line args
	task := bodyFrom(os.Args)

	// Create persistent message
	msg := rabbitmq.Publishing{
		Body: []byte(task),
		Properties: rabbitmq.Properties{
			DeliveryMode: protocol.DeliveryModePersistent, // Survive broker restart
			ContentType:  "text/plain",
		},
	}

	// Publish task
	err = ch.Publish("", queueName, false, false, msg)
	if err != nil {
		log.Fatalf("Failed to publish task: %v", err)
	}

	fmt.Printf("Sent task: %s\n", task)
}

func bodyFrom(args []string) string {
	if len(args) < 2 || args[1] == "" {
		return "Hello World."
	}
	return strings.Join(args[1:], " ")
}
