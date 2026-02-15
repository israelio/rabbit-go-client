package integration

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

// TestConfig holds test configuration
type TestConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	VHost    string
}

// GetTestConfig returns test configuration from environment or defaults
func GetTestConfig() TestConfig {
	config := TestConfig{
		Host:     getEnv("RABBITMQ_HOST", "localhost"),
		Port:     5672,
		Username: getEnv("RABBITMQ_USER", "guest"),
		Password: getEnv("RABBITMQ_PASS", "guest"),
		VHost:    getEnv("RABBITMQ_VHOST", "/"),
	}

	if portStr := os.Getenv("RABBITMQ_PORT"); portStr != "" {
		fmt.Sscanf(portStr, "%d", &config.Port)
	}

	return config
}

// getEnv gets environment variable or returns default
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// NewTestConnectionFactory creates a connection factory for testing
func NewTestConnectionFactory(t *testing.T) *rabbitmq.ConnectionFactory {
	config := GetTestConfig()

	return rabbitmq.NewConnectionFactory(
		rabbitmq.WithHost(config.Host),
		rabbitmq.WithPort(config.Port),
		rabbitmq.WithCredentials(config.Username, config.Password),
		rabbitmq.WithVHost(config.VHost),
		rabbitmq.WithConnectionTimeout(10*time.Second),
	)
}

// NewTestConnection creates a connection for testing
func NewTestConnection(t *testing.T) *rabbitmq.Connection {
	factory := NewTestConnectionFactory(t)

	conn, err := factory.NewConnection()
	if err != nil {
		t.Skipf("Skipping test: cannot connect to RabbitMQ at %s:%d: %v",
			GetTestConfig().Host, GetTestConfig().Port, err)
	}

	return conn
}

// NewTestChannel creates a channel for testing
func NewTestChannel(t *testing.T) (*rabbitmq.Connection, *rabbitmq.Channel) {
	conn := NewTestConnection(t)

	ch, err := conn.NewChannel()
	if err != nil {
		conn.Close()
		t.Fatalf("Failed to create channel: %v", err)
	}

	return conn, ch
}

// GenerateQueueName generates a unique queue name for testing
func GenerateQueueName(t *testing.T) string {
	return fmt.Sprintf("test.queue.%s.%d", t.Name(), time.Now().UnixNano())
}

// GenerateExchangeName generates a unique exchange name for testing
func GenerateExchangeName(t *testing.T) string {
	return fmt.Sprintf("test.exchange.%s.%d", t.Name(), time.Now().UnixNano())
}

// CleanupQueue deletes a queue, logging errors
func CleanupQueue(t *testing.T, ch *rabbitmq.Channel, queueName string) {
	if ch != nil && queueName != "" {
		ch.QueueDelete(queueName, rabbitmq.QueueDeleteOptions{
			IfUnused: false,
			IfEmpty:  false,
		})
	}
}

// CleanupExchange deletes an exchange, logging errors
func CleanupExchange(t *testing.T, ch *rabbitmq.Channel, exchangeName string) {
	if ch != nil && exchangeName != "" {
		ch.ExchangeDelete(exchangeName, rabbitmq.ExchangeDeleteOptions{
			IfUnused: false,
		})
	}
}

// WaitForCondition waits for a condition to be true with timeout
func WaitForCondition(timeout time.Duration, check func() bool) bool {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if check() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}

	return false
}

// RequireRabbitMQ skips the test if RabbitMQ is not available
func RequireRabbitMQ(t *testing.T) {
	config := GetTestConfig()
	factory := rabbitmq.NewConnectionFactory(
		rabbitmq.WithHost(config.Host),
		rabbitmq.WithPort(config.Port),
		rabbitmq.WithCredentials(config.Username, config.Password),
		rabbitmq.WithConnectionTimeout(2*time.Second),
	)

	conn, err := factory.NewConnection()
	if err != nil {
		t.Skipf("RabbitMQ not available at %s:%d - skipping test: %v",
			config.Host, config.Port, err)
	}
	conn.Close()
}
