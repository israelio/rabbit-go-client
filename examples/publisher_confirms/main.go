package main

import (
	"fmt"
	"log"
	"time"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

func main() {
	// Create factory
	factory := rabbitmq.NewConnectionFactory(
		rabbitmq.WithHost("localhost"),
		rabbitmq.WithCredentials("guest", "guest"),
	)

	// Connect
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
	queue, err := ch.QueueDeclare("confirm-test", rabbitmq.QueueDeclareOptions{
		Durable: true,
	})
	if err != nil {
		log.Fatal("Failed to declare queue:", err)
	}

	// Enable publisher confirms
	err = ch.ConfirmSelect(false)
	if err != nil {
		log.Fatal("Failed to enable confirms:", err)
	}

	log.Println("Publisher confirms enabled")

	// Method 1: Synchronous confirmation
	log.Println("\n--- Method 1: Synchronous Confirmation ---")
	msg := rabbitmq.Publishing{
		Properties: rabbitmq.PersistentTextPlain,
		Body:       []byte("Message with sync confirm"),
	}

	err = ch.PublishWithConfirm("", queue.Name, false, false, msg, 5*time.Second)
	if err != nil {
		log.Fatal("Publish failed:", err)
	}
	log.Println("✓ Message confirmed synchronously")

	// Method 2: Async confirmation with channel
	log.Println("\n--- Method 2: Async Confirmation (Channel) ---")
	confirms := make(chan rabbitmq.Confirmation, 10)
	ch.NotifyPublish(confirms)

	// Publish multiple messages
	for i := 1; i <= 5; i++ {
		msg := rabbitmq.Publishing{
			Properties: rabbitmq.Properties{
				ContentType:  "text/plain",
				DeliveryMode: 2,
				MessageId:    fmt.Sprintf("msg-%d", i),
			},
			Body: []byte(fmt.Sprintf("Message %d", i)),
		}

		err = ch.Publish("", queue.Name, false, false, msg)
		if err != nil {
			log.Printf("Failed to publish message %d: %v", i, err)
			continue
		}
		log.Printf("Published message %d", i)
	}

	// Wait for confirmations
	log.Println("Waiting for confirmations...")
	timeout := time.After(10 * time.Second)
	confirmed := 0

	for confirmed < 5 {
		select {
		case conf := <-confirms:
			if conf.Ack {
				confirmed++
				log.Printf("✓ Confirmed delivery tag %d (%d/%d)", conf.DeliveryTag, confirmed, 5)
			} else {
				log.Printf("✗ Nacked delivery tag %d", conf.DeliveryTag)
			}
		case <-timeout:
			log.Printf("Timeout waiting for confirmations (%d/%d confirmed)", confirmed, 5)
			return
		}
	}

	log.Println("\n✓ All messages confirmed!")
}
