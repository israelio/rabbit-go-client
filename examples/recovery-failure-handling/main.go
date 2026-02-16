package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

// This example demonstrates how to handle recovery failures gracefully
// Shows:
// 1. Detecting when recovery is in progress
// 2. Handling operations during recovery (they will fail)
// 3. Circuit breaker pattern
// 4. Graceful degradation
// 5. Recovery exhaustion handling

func main() {
	fmt.Println("=== Recovery Failure Handling Example ===")
	fmt.Println("Demonstrates graceful handling of recovery scenarios")
	fmt.Println("")

	// Create factory with limited retries to demonstrate exhaustion
	factory := rabbitmq.NewConnectionFactory(
		rabbitmq.WithHost("localhost"),
		rabbitmq.WithAutomaticRecovery(true),
		rabbitmq.WithTopologyRecovery(true),
		rabbitmq.WithRecoveryInterval(2*time.Second),
		rabbitmq.WithConnectionRetryAttempts(5), // Only 5 attempts for demo
	)

	conn, err := factory.NewConnection()
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	log.Println("✓ Connected to RabbitMQ")

	ch, err := conn.NewChannel()
	if err != nil {
		log.Fatalf("Failed to create channel: %v", err)
	}
	defer ch.Close()

	// Declare queue
	queue, err := ch.QueueDeclare("recovery.test", rabbitmq.QueueDeclareOptions{
		Durable: true,
	})
	if err != nil {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	// Create resilient publisher
	publisher := &ResilientPublisher{
		conn:          conn,
		ch:            ch,
		queue:         queue.Name,
		circuitBreaker: NewCircuitBreaker(5, 10*time.Second),
	}

	// Start publisher
	go publisher.Run()

	// Monitor recovery events
	go monitorRecoveryWithFailures(conn, publisher)

	log.Println("")
	log.Println("🚀 Publisher started with circuit breaker")
	log.Println("💡 Scenarios to test:")
	log.Println("   1. docker restart rabbitmq (should recover)")
	log.Println("   2. docker stop rabbitmq (will exhaust retries after 10s)")
	log.Println("")

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("\n🛑 Shutting down...")
	publisher.Stop()
}

// ResilientPublisher handles publishing with recovery awareness
type ResilientPublisher struct {
	conn           *rabbitmq.Connection
	ch             *rabbitmq.Channel
	queue          string
	circuitBreaker *CircuitBreaker
	stopChan       chan struct{}
	publishCount   atomic.Int64
	failureCount   atomic.Int64
}

func (rp *ResilientPublisher) Run() {
	rp.stopChan = make(chan struct{})
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rp.tryPublish()
		case <-rp.stopChan:
			return
		}
	}
}

func (rp *ResilientPublisher) tryPublish() {
	// Check circuit breaker first
	if !rp.circuitBreaker.AllowRequest() {
		log.Println("⛔ Circuit breaker OPEN - not attempting publish")
		return
	}

	// Check connection state
	state := rp.conn.GetState()
	if state == rabbitmq.StateRecovering {
		rp.circuitBreaker.RecordFailure()
		log.Println("⏳ Connection RECOVERING - skipping publish")
		return
	}

	if state != rabbitmq.StateOpen {
		rp.circuitBreaker.RecordFailure()
		log.Println("❌ Connection CLOSED - cannot publish")
		return
	}

	// Attempt to publish
	count := rp.publishCount.Add(1)
	msg := rabbitmq.Publishing{
		Properties: rabbitmq.Properties{
			ContentType:  "text/plain",
			DeliveryMode: 2,
		},
		Body: []byte(fmt.Sprintf("Message #%d", count)),
	}

	err := rp.ch.Publish("", rp.queue, false, false, msg)
	if err != nil {
		rp.failureCount.Add(1)
		rp.circuitBreaker.RecordFailure()

		// Handle specific errors
		if err == rabbitmq.ErrRecovering {
			log.Printf("⏳ Publish #%d failed: Connection is recovering", count)
		} else if err == rabbitmq.ErrClosed {
			log.Printf("❌ Publish #%d failed: Connection closed", count)
		} else {
			log.Printf("⚠️  Publish #%d failed: %v", count, err)
		}
	} else {
		rp.circuitBreaker.RecordSuccess()
		fmt.Printf("✓ Published message #%d (failures: %d)\n",
			count, rp.failureCount.Load())
	}
}

func (rp *ResilientPublisher) Stop() {
	close(rp.stopChan)
}

// CircuitBreaker implements circuit breaker pattern
type CircuitBreaker struct {
	failureThreshold int
	timeout          time.Duration

	failures       atomic.Int32
	lastFailure    atomic.Int64 // Unix timestamp
	state          atomic.Int32 // 0=closed, 1=open
}

func NewCircuitBreaker(threshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		failureThreshold: threshold,
		timeout:          timeout,
	}
}

func (cb *CircuitBreaker) AllowRequest() bool {
	if cb.state.Load() == 0 {
		return true // Circuit closed - allow
	}

	// Check if timeout has passed
	lastFail := time.Unix(cb.lastFailure.Load(), 0)
	if time.Since(lastFail) > cb.timeout {
		// Try half-open
		log.Println("🔄 Circuit breaker attempting half-open")
		cb.state.Store(0)
		cb.failures.Store(0)
		return true
	}

	return false // Circuit open - deny
}

func (cb *CircuitBreaker) RecordSuccess() {
	cb.failures.Store(0)
	cb.state.Store(0)
}

func (cb *CircuitBreaker) RecordFailure() {
	failures := cb.failures.Add(1)
	cb.lastFailure.Store(time.Now().Unix())

	if int(failures) >= cb.failureThreshold {
		if cb.state.CompareAndSwap(0, 1) {
			log.Printf("⛔ Circuit breaker OPENED (failures: %d)", failures)
		}
	}
}

func monitorRecoveryWithFailures(conn *rabbitmq.Connection, publisher *ResilientPublisher) {
	started := make(chan struct{}, 10)
	completed := make(chan struct{}, 10)
	failed := make(chan error, 100)

	conn.NotifyRecoveryStarted(started)
	conn.NotifyRecoveryCompleted(completed)
	conn.NotifyRecoveryFailed(failed)

	var startTime time.Time
	var attemptCount int

	for {
		select {
		case <-started:
			startTime = time.Now()
			attemptCount = 0
			log.Println("")
			log.Println(strings.Repeat("=", 60))
			log.Println("🔄 RECOVERY STARTED")
			log.Println("   Publisher will skip operations during recovery")
			log.Println("   Circuit breaker may open if failures accumulate")
			log.Println(strings.Repeat("=", 60))
			log.Println("")

		case <-completed:
			duration := time.Since(startTime)
			log.Println("")
			log.Println(strings.Repeat("=", 60))
			log.Println("✅ RECOVERY COMPLETED")
			log.Printf("   Duration: %v", duration)
			log.Printf("   Total attempts: %d", attemptCount)
			log.Println("   Publisher resuming normal operations")
			log.Println(strings.Repeat("=", 60))
			log.Println("")

			// Reset circuit breaker
			publisher.circuitBreaker.RecordSuccess()

		case err := <-failed:
			attemptCount++
			log.Printf("⚠️  Recovery attempt #%d failed: %v", attemptCount, err)
			log.Println("   Will retry with exponential backoff...")

			// After many failures, warn about exhaustion
			if attemptCount >= 4 {
				log.Println("")
				log.Println("⚠️  WARNING: Multiple recovery failures!")
				log.Println("   If RabbitMQ is still down, connection will be")
				log.Println("   permanently closed after max attempts.")
				log.Println("")
			}
		}
	}
}
