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

	// Declare RPC queue
	rpcQueue := "rpc_queue"
	_, err = ch.QueueDeclare(rpcQueue, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	// Set QoS for fair dispatch
	err = ch.Qos(1, 0, false)
	if err != nil {
		log.Fatalf("Failed to set QoS: %v", err)
	}

	// Start consuming requests
	requests, err := ch.Consume(rpcQueue, "", rabbitmq.ConsumeOptions{
		AutoAck: false,
	})
	if err != nil {
		log.Fatalf("Failed to start consuming: %v", err)
	}

	fmt.Println("RPC Server started. Waiting for requests...\n")

	// Process requests
	for request := range requests {
		// Process the request
		requestBody := string(request.Body)
		fmt.Printf("Received request: %s (correlation: %s)\n", requestBody, request.Properties.CorrelationId)

		// Simulate processing
		time.Sleep(500 * time.Millisecond)

		// Generate response
		response := processRequest(requestBody)

		// Send reply
		reply := rabbitmq.Publishing{
			Properties: rabbitmq.Properties{
				CorrelationId: request.Properties.CorrelationId,
			},
			Body: []byte(response),
		}

		err = ch.Publish("", request.Properties.ReplyTo, false, false, reply)
		if err != nil {
			log.Printf("Failed to send reply: %v", err)
		} else {
			fmt.Printf("Sent reply: %s\n", response)
		}

		// Acknowledge request
		err = request.Ack(false)
		if err != nil {
			log.Printf("Failed to ack message: %v", err)
		}
	}
}

// processRequest processes the RPC request and returns a response
func processRequest(request string) string {
	// Simple processing: convert to uppercase and add timestamp
	return fmt.Sprintf("%s (processed at %s)",
		strings.ToUpper(request),
		time.Now().Format("15:04:05"))
}
