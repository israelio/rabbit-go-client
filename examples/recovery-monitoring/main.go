package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

// This example demonstrates comprehensive recovery monitoring
// Shows all notification mechanisms:
// 1. NotifyRecoveryStarted - when recovery begins
// 2. NotifyRecoveryCompleted - when recovery succeeds
// 3. NotifyRecoveryFailed - for each failed attempt
// 4. RecoveryHandler interface - custom hooks
// 5. ConnectionListener - lifecycle events
// 6. Connection state checking

func main() {
	fmt.Println("=== Recovery Monitoring Example ===")
	fmt.Println("Demonstrates all recovery notification mechanisms")
	fmt.Println("")

	// Create monitoring components
	handler := &DetailedRecoveryHandler{}
	listener := &DetailedConnectionListener{}
	logger := &DetailedLogger{}

	// Create factory with all monitoring hooks
	factory := rabbitmq.NewConnectionFactory(
		rabbitmq.WithHost("localhost"),
		rabbitmq.WithAutomaticRecovery(true),
		rabbitmq.WithTopologyRecovery(true),
		rabbitmq.WithRecoveryInterval(3*time.Second),
		rabbitmq.WithConnectionRetryAttempts(10),

		// Monitoring hooks
		rabbitmq.WithRecoveryHandler(handler),
		rabbitmq.WithLogger(logger),
	)

	conn, err := factory.NewConnection()
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Add connection listener
	conn.AddConnectionListener(listener)

	log.Println("✓ Connected with full monitoring enabled")

	// Setup notification channels
	recoveryStarted := make(chan struct{}, 10)
	recoveryCompleted := make(chan struct{}, 10)
	recoveryFailed := make(chan error, 100)
	connClose := make(chan *rabbitmq.Error, 1)

	conn.NotifyRecoveryStarted(recoveryStarted)
	conn.NotifyRecoveryCompleted(recoveryCompleted)
	conn.NotifyRecoveryFailed(recoveryFailed)
	conn.NotifyClose(connClose)

	// Start comprehensive monitoring
	monitor := &ComprehensiveMonitor{
		conn:              conn,
		recoveryStarted:   recoveryStarted,
		recoveryCompleted: recoveryCompleted,
		recoveryFailed:    recoveryFailed,
		connClose:         connClose,
	}

	go monitor.Run()

	// Create test channel
	ch, err := conn.NewChannel()
	if err != nil {
		log.Fatalf("Failed to create channel: %v", err)
	}
	defer ch.Close()

	// Declare test queue
	ch.QueueDeclare("monitor.test", rabbitmq.QueueDeclareOptions{Durable: true})

	log.Println("")
	log.Println("🎯 Monitoring active - try:")
	log.Println("   docker restart rabbitmq")
	log.Println("")
	log.Println("You will see notifications from:")
	log.Println("   ✓ RecoveryHandler (OnRecoveryStarted, OnRecoveryCompleted, etc.)")
	log.Println("   ✓ ConnectionListener (OnConnectionRecoveryStarted, etc.)")
	log.Println("   ✓ Notification channels (started, completed, failed)")
	log.Println("   ✓ State changes (Open → Recovering → Open)")
	log.Println("")

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("\n👋 Goodbye!")
}

// DetailedRecoveryHandler implements RecoveryHandler with detailed logging
type DetailedRecoveryHandler struct {
	startTime time.Time
}

func (h *DetailedRecoveryHandler) OnRecoveryStarted(conn *rabbitmq.Connection) {
	h.startTime = time.Now()
	log.Println("")
	log.Println("📢 [RecoveryHandler] OnRecoveryStarted called")
	log.Printf("   Connection state: %s", conn.GetState())
	log.Printf("   Channel count: %d", conn.GetChannelCount())
}

func (h *DetailedRecoveryHandler) OnRecoveryCompleted(conn *rabbitmq.Connection) {
	duration := time.Since(h.startTime)
	log.Println("")
	log.Println("📢 [RecoveryHandler] OnRecoveryCompleted called")
	log.Printf("   Recovery duration: %v", duration)
	log.Printf("   Connection state: %s", conn.GetState())
	log.Printf("   Channel count: %d", conn.GetChannelCount())
}

func (h *DetailedRecoveryHandler) OnRecoveryFailed(conn *rabbitmq.Connection, err error) {
	log.Println("")
	log.Printf("📢 [RecoveryHandler] OnRecoveryFailed called: %v", err)
	log.Printf("   Connection state: %s", conn.GetState())
}

func (h *DetailedRecoveryHandler) OnTopologyRecoveryStarted(conn *rabbitmq.Connection) {
	log.Println("📢 [RecoveryHandler] OnTopologyRecoveryStarted called")
	log.Println("   Restoring exchanges, queues, bindings...")
}

func (h *DetailedRecoveryHandler) OnTopologyRecoveryCompleted(conn *rabbitmq.Connection) {
	log.Println("📢 [RecoveryHandler] OnTopologyRecoveryCompleted called")
	log.Println("   Topology fully restored!")
}

// DetailedConnectionListener implements ConnectionListener
type DetailedConnectionListener struct{}

func (l *DetailedConnectionListener) OnConnectionCreated(conn *rabbitmq.Connection) {
	log.Println("📡 [ConnectionListener] OnConnectionCreated called")
}

func (l *DetailedConnectionListener) OnConnectionClosed(conn *rabbitmq.Connection, err error) {
	log.Printf("📡 [ConnectionListener] OnConnectionClosed called: %v", err)
}

func (l *DetailedConnectionListener) OnConnectionRecoveryStarted(conn *rabbitmq.Connection) {
	log.Println("📡 [ConnectionListener] OnConnectionRecoveryStarted called")
}

func (l *DetailedConnectionListener) OnConnectionRecoveryCompleted(conn *rabbitmq.Connection) {
	log.Println("📡 [ConnectionListener] OnConnectionRecoveryCompleted called")
}

func (l *DetailedConnectionListener) OnConnectionBlocked(conn *rabbitmq.Connection, reason string) {
	log.Printf("📡 [ConnectionListener] OnConnectionBlocked: %s", reason)
}

func (l *DetailedConnectionListener) OnConnectionUnblocked(conn *rabbitmq.Connection) {
	log.Println("📡 [ConnectionListener] OnConnectionUnblocked called")
}

// DetailedLogger implements Logger interface
type DetailedLogger struct{}

func (l *DetailedLogger) Printf(format string, v ...interface{}) {
	log.Printf("[CustomLogger] "+format, v...)
}

func (l *DetailedLogger) Println(v ...interface{}) {
	log.Println(append([]interface{}{"[CustomLogger]"}, v...)...)
}

// ComprehensiveMonitor monitors all notification channels
type ComprehensiveMonitor struct {
	conn              *rabbitmq.Connection
	recoveryStarted   chan struct{}
	recoveryCompleted chan struct{}
	recoveryFailed    chan error
	connClose         chan *rabbitmq.Error
	attemptCount      int
}

func (m *ComprehensiveMonitor) Run() {
	// Periodically check connection state
	stateTicker := time.NewTicker(2 * time.Second)
	defer stateTicker.Stop()

	for {
		select {
		case <-m.recoveryStarted:
			m.attemptCount = 0
			log.Println("")
			log.Println("🔔 [Notification Channel] Recovery STARTED")
			log.Printf("   State: %s", m.conn.GetState())

		case <-m.recoveryCompleted:
			log.Println("")
			log.Println("🔔 [Notification Channel] Recovery COMPLETED")
			log.Printf("   State: %s", m.conn.GetState())
			log.Printf("   Total attempts: %d", m.attemptCount)
			m.attemptCount = 0

		case err := <-m.recoveryFailed:
			m.attemptCount++
			log.Printf("🔔 [Notification Channel] Recovery attempt #%d FAILED: %v",
				m.attemptCount, err)

		case err := <-m.connClose:
			log.Println("")
			log.Printf("🔔 [Notification Channel] Connection CLOSED: %v", err)
			log.Printf("   Recoverable: %v", err.Recover)

		case <-stateTicker.C:
			state := m.conn.GetState()
			fmt.Printf("📊 [State Monitor] Current state: %s, Channels: %d\n",
				state, m.conn.GetChannelCount())
		}
	}
}
