package main

import (
	"fmt"
	"log"
	"os"
	"strings"

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

	// Get routing key and message from command line
	routingKey := "anonymous.info"
	if len(os.Args) > 1 {
		routingKey = os.Args[1]
	}

	body := bodyFrom(os.Args)

	// Publish message
	msg := rabbitmq.Publishing{
		Body: []byte(body),
		Properties: rabbitmq.Properties{
			ContentType: "text/plain",
		},
	}

	err = ch.Publish(exchangeName, routingKey, false, false, msg)
	if err != nil {
		log.Fatalf("Failed to publish message: %v", err)
	}

	fmt.Printf("Sent [%s] '%s'\n", routingKey, body)
}

func bodyFrom(args []string) string {
	if len(args) < 3 || args[2] == "" {
		return "Hello World!"
	}
	return strings.Join(args[2:], " ")
}
