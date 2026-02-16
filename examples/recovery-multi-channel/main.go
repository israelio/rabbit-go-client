package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

// This example demonstrates recovery with multiple channels
// Each channel has different configurations that are preserved:
// - Different QoS settings
// - Different purposes (publish, consume, RPC)
// - Channel IDs are preserved after recovery

func main() {
	fmt.Println("=== Multi-Channel Recovery Example ===")
	fmt.Println("Demonstrates recovery of multiple channels with preserved IDs and settings")
	fmt.Println("")

	// Create factory with recovery enabled
	factory := rabbitmq.NewConnectionFactory(
		rabbitmq.WithHost("localhost"),
		rabbitmq.WithAutomaticRecovery(true),
		rabbitmq.WithTopologyRecovery(true),
		rabbitmq.WithRecoveryInterval(3*time.Second),
		rabbitmq.WithConnectionRetryAttempts(10),
	)

	conn, err := factory.NewConnection()
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	log.Println("✓ Connected to RabbitMQ")

	// Monitor recovery
	go monitorRecovery(conn)

	// Create multiple channels with different purposes
	channels := make([]*ChannelManager, 5)

	for i := 0; i < 5; i++ {
		cm, err := NewChannelManager(conn, i)
		if err != nil {
			log.Fatalf("Failed to create channel manager %d: %v", i, err)
		}
		channels[i] = cm

		log.Printf("✓ Created channel %d (ID: %d) with purpose: %s, QoS: prefetch=%d",
			i, cm.Channel.GetChannelID(), cm.Purpose, cm.PrefetchCount)
	}

	// Start all channel operations
	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	for _, cm := range channels {
		wg.Add(1)
		go func(cm *ChannelManager) {
			defer wg.Done()
			cm.Run(stopChan)
		}(cm)
	}

	log.Println("")
	log.Println("🚀 All channels operational!")
	log.Println("💡 Try: docker restart rabbitmq")
	log.Println("")

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("\n🛑 Shutting down...")
	close(stopChan)
	wg.Wait()

	// Close all channels
	for _, cm := range channels {
		cm.Channel.Close()
	}

	log.Println("👋 Goodbye!")
}

// ChannelManager manages a single channel with specific purpose
type ChannelManager struct {
	Channel       *rabbitmq.Channel
	ChannelID     uint16
	Purpose       string
	PrefetchCount int
	OperationCount atomic.Int64
}

func NewChannelManager(conn *rabbitmq.Connection, index int) (*ChannelManager, error) {
	ch, err := conn.NewChannel()
	if err != nil {
		return nil, err
	}

	cm := &ChannelManager{
		Channel:   ch,
		ChannelID: ch.GetChannelID(),
	}

	// Configure based on index
	switch index {
	case 0:
		cm.Purpose = "High-priority publisher"
		cm.PrefetchCount = 1
		ch.Qos(1, 0, false)

	case 1:
		cm.Purpose = "Bulk publisher"
		cm.PrefetchCount = 100
		ch.Qos(100, 0, false)

	case 2:
		cm.Purpose = "Consumer (fast)"
		cm.PrefetchCount = 10
		ch.Qos(10, 0, false)

	case 3:
		cm.Purpose = "Consumer (slow)"
		cm.PrefetchCount = 1
		ch.Qos(1, 0, false)

	case 4:
		cm.Purpose = "Confirms channel"
		cm.PrefetchCount = 50
		ch.Qos(50, 0, false)
		ch.ConfirmSelect(false)
	}

	return cm, nil
}

func (cm *ChannelManager) Run(stop chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			count := cm.OperationCount.Add(1)

			// Verify channel ID is preserved after recovery
			currentID := cm.Channel.GetChannelID()
			if currentID != cm.ChannelID {
				log.Printf("⚠️  Channel ID changed! Was: %d, Now: %d (this shouldn't happen)",
					cm.ChannelID, currentID)
			}

			fmt.Printf("[Ch %d - %s] Operation #%d (QoS: prefetch=%d)\n",
				cm.ChannelID, cm.Purpose, count, cm.PrefetchCount)

		case <-stop:
			return
		}
	}
}

func monitorRecovery(conn *rabbitmq.Connection) {
	started := make(chan struct{}, 10)
	completed := make(chan struct{}, 10)
	failed := make(chan error, 100)

	conn.NotifyRecoveryStarted(started)
	conn.NotifyRecoveryCompleted(completed)
	conn.NotifyRecoveryFailed(failed)

	var startTime time.Time

	for {
		select {
		case <-started:
			startTime = time.Now()
			log.Println("")
			log.Println(strings.Repeat("=", 60))
			log.Println("🔄 RECOVERY STARTED - All channels will be recovered with:")
			log.Println("   - Same channel IDs")
			log.Println("   - Same QoS settings")
			log.Println("   - Same confirm mode")
			log.Println(strings.Repeat("=", 60))
			log.Println("")

		case <-completed:
			duration := time.Since(startTime)
			log.Println("")
			log.Println(strings.Repeat("=", 60))
			log.Println("✅ RECOVERY COMPLETED")
			log.Printf("   Duration: %v", duration)
			log.Println("   All channels restored with original settings!")
			log.Println(strings.Repeat("=", 60))
			log.Println("")

		case err := <-failed:
			log.Printf("⚠️  Recovery attempt failed: %v", err)
		}
	}
}
