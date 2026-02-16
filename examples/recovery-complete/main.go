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

// This example demonstrates ALL recovery scenarios:
// 1. Connection recovery with retry and backoff
// 2. Topology recovery (exchanges, queues, bindings)
// 3. Multiple channels recovery with preserved IDs
// 4. QoS settings preservation
// 5. Publisher confirms restoration
// 6. Consumer recovery with callbacks
// 7. Recovery notifications (started, completed, failed)
// 8. Custom recovery handlers
// 9. Graceful error handling during recovery
// 10. Application state preservation across reconnects

func main() {
	fmt.Println("=== Complete Recovery Example ===")
	fmt.Println("This demonstrates ALL automatic recovery scenarios")
	fmt.Println("")
	fmt.Println("To test recovery:")
	fmt.Println("  docker restart rabbitmq")
	fmt.Println("  OR")
	fmt.Println("  docker-compose restart rabbitmq")
	fmt.Println("")
	fmt.Println("Watch as the application automatically recovers!")
	fmt.Println(strings.Repeat("=", 50))
	fmt.Println("")

	// Create factory with comprehensive recovery settings
	factory := rabbitmq.NewConnectionFactory(
		rabbitmq.WithHost("localhost"),
		rabbitmq.WithPort(5672),
		rabbitmq.WithCredentials("guest", "guest"),

		// Recovery configuration
		rabbitmq.WithAutomaticRecovery(true),         // Enable auto-recovery
		rabbitmq.WithTopologyRecovery(true),          // Restore topology
		rabbitmq.WithRecoveryInterval(2*time.Second), // Initial retry interval
		rabbitmq.WithConnectionRetryAttempts(15),     // Max 15 attempts

		// Connection tuning
		rabbitmq.WithHeartbeat(10*time.Second),
		rabbitmq.WithConnectionTimeout(30*time.Second),

		// Custom recovery handler
		rabbitmq.WithRecoveryHandler(&CustomRecoveryHandler{}),
	)

	// Connect
	conn, err := factory.NewConnection()
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	log.Println("✓ Connected to RabbitMQ")

	// Setup recovery monitoring
	recoveryMonitor := NewRecoveryMonitor(conn)
	go recoveryMonitor.Start()

	// Setup application state
	app := &Application{
		conn:            conn,
		publisherChan:   nil,
		consumerChan:    nil,
		confirmChan:     nil,
		publishCount:    atomic.Int64{},
		consumeCount:    atomic.Int64{},
		recoveryMonitor: recoveryMonitor,
	}

	// Initialize all components
	if err := app.Initialize(); err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}

	// Start background operations
	app.Start()

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("\n🛑 Shutting down gracefully...")
	app.Shutdown()
	log.Println("👋 Goodbye!")
}

// Application holds all application state
type Application struct {
	conn            *rabbitmq.Connection
	publisherChan   *rabbitmq.Channel // Channel 1: For publishing
	consumerChan    *rabbitmq.Channel // Channel 2: For consuming
	confirmChan     *rabbitmq.Channel // Channel 3: For confirms

	publishCount    atomic.Int64
	consumeCount    atomic.Int64

	stopPublisher   chan struct{}
	stopConsumer    chan struct{}

	recoveryMonitor *RecoveryMonitor
}

// Initialize sets up topology and channels
func (app *Application) Initialize() error {
	log.Println("📋 Initializing application...")

	// Create Channel 1: Publisher channel with QoS
	publisherChan, err := app.conn.NewChannel()
	if err != nil {
		return fmt.Errorf("create publisher channel: %w", err)
	}
	app.publisherChan = publisherChan
	log.Printf("✓ Created publisher channel (ID: %d)", publisherChan.GetChannelID())

	// Set QoS on publisher channel (will be preserved after recovery)
	if err := publisherChan.Qos(10, 0, false); err != nil {
		return fmt.Errorf("set publisher QoS: %w", err)
	}
	log.Println("✓ Set publisher QoS: prefetch=10")

	// Create Channel 2: Consumer channel with QoS
	consumerChan, err := app.conn.NewChannel()
	if err != nil {
		return fmt.Errorf("create consumer channel: %w", err)
	}
	app.consumerChan = consumerChan
	log.Printf("✓ Created consumer channel (ID: %d)", consumerChan.GetChannelID())

	// Set QoS on consumer channel
	if err := consumerChan.Qos(5, 0, false); err != nil {
		return fmt.Errorf("set consumer QoS: %w", err)
	}
	log.Println("✓ Set consumer QoS: prefetch=5")

	// Create Channel 3: Publisher confirms channel
	confirmChan, err := app.conn.NewChannel()
	if err != nil {
		return fmt.Errorf("create confirm channel: %w", err)
	}
	app.confirmChan = confirmChan
	log.Printf("✓ Created confirm channel (ID: %d)", confirmChan.GetChannelID())

	// Enable publisher confirms (will be preserved after recovery)
	if err := confirmChan.ConfirmSelect(false); err != nil {
		return fmt.Errorf("enable confirms: %w", err)
	}
	log.Println("✓ Enabled publisher confirms")

	// Declare exchange (will be recovered automatically)
	if err := app.publisherChan.ExchangeDeclare(
		"recovery.exchange",
		"topic",
		rabbitmq.ExchangeDeclareOptions{Durable: true},
	); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}
	log.Println("✓ Declared exchange: recovery.exchange")

	// Declare queue (will be recovered automatically)
	queue, err := app.consumerChan.QueueDeclare(
		"recovery.queue",
		rabbitmq.QueueDeclareOptions{Durable: true},
	)
	if err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}
	log.Printf("✓ Declared queue: %s", queue.Name)

	// Bind queue to exchange (will be recovered automatically)
	if err := app.consumerChan.QueueBind(
		queue.Name,
		"recovery.exchange",
		"recovery.#",
		nil,
	); err != nil {
		return fmt.Errorf("bind queue: %w", err)
	}
	log.Println("✓ Bound queue to exchange with routing key: recovery.#")

	// Start consumer with callback (will be recovered automatically)
	consumer := &RecoveryConsumer{
		app:          app,
		consumerTag:  "recovery-consumer-1",
		deliveryCount: atomic.Int64{},
	}

	if err := app.consumerChan.ConsumeWithCallback(
		queue.Name,
		consumer.consumerTag,
		rabbitmq.ConsumeOptions{AutoAck: false},
		consumer,
	); err != nil {
		return fmt.Errorf("start consumer: %w", err)
	}
	log.Printf("✓ Started consumer: %s", consumer.consumerTag)

	log.Println("✅ Initialization complete!")
	return nil
}

// Start begins background operations
func (app *Application) Start() {
	app.stopPublisher = make(chan struct{})
	app.stopConsumer = make(chan struct{})

	// Start publisher
	go app.runPublisher()
	log.Println("🚀 Publisher started")

	// Start confirm monitor
	go app.monitorConfirms()
	log.Println("🚀 Confirm monitor started")
}

// runPublisher publishes messages continuously
func (app *Application) runPublisher() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			count := app.publishCount.Add(1)

			msg := rabbitmq.Publishing{
				Properties: rabbitmq.Properties{
					ContentType:  "text/plain",
					DeliveryMode: 2, // Persistent
					Timestamp:    time.Now(),
					MessageId:    fmt.Sprintf("msg-%d", count),
				},
				Body: []byte(fmt.Sprintf("Message #%d from publisher", count)),
			}

			// Try publishing
			err := app.publisherChan.Publish(
				"recovery.exchange",
				"recovery.test",
				false,
				false,
				msg,
			)

			if err != nil {
				// During recovery, publishes will fail
				log.Printf("⚠️  Publish failed (likely recovering): %v", err)
			} else {
				fmt.Printf("→ Published message #%d\n", count)
			}

		case <-app.stopPublisher:
			return
		}
	}
}

// monitorConfirms monitors publisher confirms
func (app *Application) monitorConfirms() {
	confirms := make(chan rabbitmq.Confirmation, 100)
	app.confirmChan.NotifyPublish(confirms)

	for {
		select {
		case confirm := <-confirms:
			if confirm.Ack {
				fmt.Printf("✓ Confirmed delivery tag: %d\n", confirm.DeliveryTag)
			} else {
				fmt.Printf("✗ Nacked delivery tag: %d\n", confirm.DeliveryTag)
			}
		case <-app.stopConsumer:
			return
		}
	}
}

// Shutdown gracefully stops the application
func (app *Application) Shutdown() {
	close(app.stopPublisher)
	close(app.stopConsumer)

	time.Sleep(500 * time.Millisecond) // Allow cleanup

	if app.confirmChan != nil {
		app.confirmChan.Close()
	}
	if app.consumerChan != nil {
		app.consumerChan.Close()
	}
	if app.publisherChan != nil {
		app.publisherChan.Close()
	}
}

// RecoveryConsumer handles message delivery
type RecoveryConsumer struct {
	rabbitmq.DefaultConsumer
	app           *Application
	consumerTag   string
	deliveryCount atomic.Int64
}

func (rc *RecoveryConsumer) HandleConsumeOk(consumerTag string) {
	log.Printf("✓ Consumer registered: %s", consumerTag)
}

func (rc *RecoveryConsumer) HandleDelivery(consumerTag string, delivery rabbitmq.Delivery) error {
	count := rc.deliveryCount.Add(1)
	rc.app.consumeCount.Add(1)

	fmt.Printf("← Received message #%d: %s (redelivered: %v)\n",
		count, delivery.Body, delivery.Redelivered)

	// Simulate processing
	time.Sleep(100 * time.Millisecond)

	return delivery.Ack(false)
}

func (rc *RecoveryConsumer) HandleCancel(consumerTag string) error {
	log.Printf("⚠️  Consumer cancelled: %s", consumerTag)
	return nil
}

func (rc *RecoveryConsumer) HandleShutdown(consumerTag string, cause *rabbitmq.Error) {
	log.Printf("⚠️  Consumer shutdown: %s, cause: %v", consumerTag, cause)
}

func (rc *RecoveryConsumer) HandleRecoverOk(consumerTag string) {
	deliveries := rc.deliveryCount.Load()
	log.Printf("✅ Consumer RECOVERED: %s (continuing from delivery #%d)",
		consumerTag, deliveries)
}

// RecoveryMonitor monitors recovery events
type RecoveryMonitor struct {
	conn              *rabbitmq.Connection
	recoveryStarted   chan struct{}
	recoveryCompleted chan struct{}
	recoveryFailed    chan error
	startTime         time.Time
}

func NewRecoveryMonitor(conn *rabbitmq.Connection) *RecoveryMonitor {
	rm := &RecoveryMonitor{
		conn:              conn,
		recoveryStarted:   make(chan struct{}, 10),
		recoveryCompleted: make(chan struct{}, 10),
		recoveryFailed:    make(chan error, 100),
	}

	conn.NotifyRecoveryStarted(rm.recoveryStarted)
	conn.NotifyRecoveryCompleted(rm.recoveryCompleted)
	conn.NotifyRecoveryFailed(rm.recoveryFailed)

	return rm
}

func (rm *RecoveryMonitor) Start() {
	log.Println("👀 Recovery monitor started")

	for {
		select {
		case <-rm.recoveryStarted:
			rm.startTime = time.Now()
			log.Println("")
			log.Println(strings.Repeat("=", 50))
			log.Println("🔄 RECOVERY STARTED")
			log.Println("   Connection lost, attempting to reconnect...")
			log.Println("   - Topology will be restored")
			log.Println("   - Channels will be recovered")
			log.Println("   - Consumers will be re-established")
			log.Println(strings.Repeat("=", 50))
			log.Println("")

		case <-rm.recoveryCompleted:
			duration := time.Since(rm.startTime)
			log.Println("")
			log.Println(strings.Repeat("=", 50))
			log.Println("✅ RECOVERY COMPLETED")
			log.Printf("   Recovery took: %v", duration)
			log.Println("   - Connection restored")
			log.Println("   - Topology recovered")
			log.Println("   - Channels recovered")
			log.Println("   - Consumers re-established")
			log.Println("   Application is fully operational!")
			log.Println(strings.Repeat("=", 50))
			log.Println("")

		case err := <-rm.recoveryFailed:
			log.Printf("⚠️  Recovery attempt failed: %v (will retry...)", err)
		}
	}
}

// CustomRecoveryHandler demonstrates custom recovery handling
type CustomRecoveryHandler struct{}

func (h *CustomRecoveryHandler) OnRecoveryStarted(conn *rabbitmq.Connection) {
	log.Println("📢 Custom handler: Recovery started")
	// Could: Send alert to monitoring system
	// Could: Update application metrics
	// Could: Pause non-critical operations
}

func (h *CustomRecoveryHandler) OnRecoveryCompleted(conn *rabbitmq.Connection) {
	log.Println("📢 Custom handler: Recovery completed")
	// Could: Send success notification
	// Could: Resume operations
	// Could: Update health check status
}

func (h *CustomRecoveryHandler) OnRecoveryFailed(conn *rabbitmq.Connection, err error) {
	log.Printf("📢 Custom handler: Recovery failed - %v", err)
	// Could: Log to external system
	// Could: Update error metrics
	// Could: Implement custom retry logic
}

func (h *CustomRecoveryHandler) OnTopologyRecoveryStarted(conn *rabbitmq.Connection) {
	log.Println("📢 Custom handler: Topology recovery started")
}

func (h *CustomRecoveryHandler) OnTopologyRecoveryCompleted(conn *rabbitmq.Connection) {
	log.Println("📢 Custom handler: Topology recovery completed")
}
