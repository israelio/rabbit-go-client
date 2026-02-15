package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

func main() {
	// Get number from command line (default 5)
	n := 5
	if len(os.Args) > 1 {
		if val, err := strconv.Atoi(os.Args[1]); err == nil {
			n = val
		}
	}

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

	// Create RPC client using the helper
	client, err := rabbitmq.NewRpcClient(ch)
	if err != nil {
		log.Fatalf("Failed to create RPC client: %v", err)
	}
	defer client.Close()

	fmt.Printf("RPC Client ready. Making %d calls...\n\n", n)

	// Make RPC calls
	for i := 1; i <= n; i++ {
		// Create request
		request := rabbitmq.Publishing{
			Body: []byte(fmt.Sprintf("Request %d", i)),
		}

		// Call with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		reply, err := client.Call(ctx, "", "rpc_queue", request)
		cancel()

		if err != nil {
			log.Printf("✗ RPC call %d failed: %v", i, err)
			continue
		}

		fmt.Printf("✓ RPC call %d: %s\n", i, string(reply.Body))
	}

	fmt.Println("\nAll RPC calls completed successfully!")
}
