package rabbitmq

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestMetricsCollectorBasic tests basic metrics collection
func TestMetricsCollectorBasic(t *testing.T) {
	metrics := NewStandardMetricsCollector()

	// Simulate operations
	metrics.ConnectionCreated()
	metrics.ChannelCreated()
	metrics.MessagePublished()
	metrics.MessagePublished()
	metrics.MessageConsumed()
	metrics.MessageAcked()

	if metrics.GetConnectionsCreated() != 1 {
		t.Errorf("Connections created: got %d, want 1", metrics.GetConnectionsCreated())
	}
	if metrics.GetChannelsCreated() != 1 {
		t.Errorf("Channels created: got %d, want 1", metrics.GetChannelsCreated())
	}
	if metrics.GetMessagesPublished() != 2 {
		t.Errorf("Messages published: got %d, want 2", metrics.GetMessagesPublished())
	}
	if metrics.GetMessagesConsumed() != 1 {
		t.Errorf("Messages consumed: got %d, want 1", metrics.GetMessagesConsumed())
	}
}

// TestMetricsConcurrency tests concurrent metric updates
func TestMetricsConcurrency(t *testing.T) {
	metrics := NewStandardMetricsCollector()

	numGoroutines := 100
	opsPerGoroutine := 100

	var wg sync.WaitGroup

	// Concurrent publishes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				metrics.MessagePublished()
			}
		}()
	}

	// Concurrent consumes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				metrics.MessageConsumed()
			}
		}()
	}

	wg.Wait()

	expectedCount := int64(numGoroutines * opsPerGoroutine)

	if metrics.GetMessagesPublished() != expectedCount {
		t.Errorf("Messages published: got %d, want %d", metrics.GetMessagesPublished(), expectedCount)
	}
	if metrics.GetMessagesConsumed() != expectedCount {
		t.Errorf("Messages consumed: got %d, want %d", metrics.GetMessagesConsumed(), expectedCount)
	}
}

// TestMetricsConfirms tests confirm metrics
func TestMetricsConfirms(t *testing.T) {
	metrics := NewStandardMetricsCollector()

	// Acks
	for i := 0; i < 10; i++ {
		metrics.ConfirmReceived(true)
	}

	// Nacks
	for i := 0; i < 3; i++ {
		metrics.ConfirmReceived(false)
	}

	if metrics.GetConfirmsAcked() != 10 {
		t.Errorf("Confirms acked: got %d, want 10", metrics.GetConfirmsAcked())
	}
	if metrics.GetConfirmsNacked() != 3 {
		t.Errorf("Confirms nacked: got %d, want 3", metrics.GetConfirmsNacked())
	}
}

// TestMetricsErrors tests error metrics
func TestMetricsErrors(t *testing.T) {
	metrics := NewStandardMetricsCollector()

	// Connection errors
	for i := 0; i < 5; i++ {
		metrics.ConnectionError(nil)
	}

	// Channel errors
	for i := 0; i < 7; i++ {
		metrics.ChannelError(nil)
	}

	if metrics.GetConnectionErrors() != 5 {
		t.Errorf("Connection errors: got %d, want 5", metrics.GetConnectionErrors())
	}
	if metrics.GetChannelErrors() != 7 {
		t.Errorf("Channel errors: got %d, want 7", metrics.GetChannelErrors())
	}
}

// TestMetricsLifecycle tests connection/channel lifecycle metrics
func TestMetricsLifecycle(t *testing.T) {
	metrics := NewStandardMetricsCollector()

	// Create connections and channels
	for i := 0; i < 3; i++ {
		metrics.ConnectionCreated()
		for j := 0; j < 5; j++ {
			metrics.ChannelCreated()
		}
	}

	// Close some
	for i := 0; i < 2; i++ {
		metrics.ConnectionClosed()
		for j := 0; j < 3; j++ {
			metrics.ChannelClosed()
		}
	}

	if metrics.GetConnectionsCreated() != 3 {
		t.Errorf("Connections created: got %d, want 3", metrics.GetConnectionsCreated())
	}
	if metrics.GetConnectionsClosed() != 2 {
		t.Errorf("Connections closed: got %d, want 2", metrics.GetConnectionsClosed())
	}
	if metrics.GetChannelsCreated() != 15 {
		t.Errorf("Channels created: got %d, want 15", metrics.GetChannelsCreated())
	}
	if metrics.GetChannelsClosed() != 6 {
		t.Errorf("Channels closed: got %d, want 6", metrics.GetChannelsClosed())
	}

	// Current open connections/channels
	openConnections := metrics.GetConnectionsCreated() - metrics.GetConnectionsClosed()
	openChannels := metrics.GetChannelsCreated() - metrics.GetChannelsClosed()

	if openConnections != 1 {
		t.Errorf("Open connections: got %d, want 1", openConnections)
	}
	if openChannels != 9 {
		t.Errorf("Open channels: got %d, want 9", openChannels)
	}
}

// TestMetricsAckNackReject tests acknowledgment metrics
func TestMetricsAckNackReject(t *testing.T) {
	metrics := NewStandardMetricsCollector()

	// Simulate message handling
	for i := 0; i < 100; i++ {
		metrics.MessageConsumed()

		switch i % 10 {
		case 0:
			metrics.MessageRejected()
		case 1, 2:
			metrics.MessageNacked()
		default:
			metrics.MessageAcked()
		}
	}

	if metrics.GetMessagesConsumed() != 100 {
		t.Errorf("Messages consumed: got %d, want 100", metrics.GetMessagesConsumed())
	}
	if metrics.GetMessagesAcked() != 70 {
		t.Errorf("Messages acked: got %d, want 70", metrics.GetMessagesAcked())
	}
	if metrics.GetMessagesNacked() != 20 {
		t.Errorf("Messages nacked: got %d, want 20", metrics.GetMessagesNacked())
	}
	if metrics.GetMessagesRejected() != 10 {
		t.Errorf("Messages rejected: got %d, want 10", metrics.GetMessagesRejected())
	}
}

// TestMetricsReturns tests return metrics
func TestMetricsReturns(t *testing.T) {
	metrics := NewStandardMetricsCollector()

	// Publish messages
	for i := 0; i < 50; i++ {
		metrics.MessagePublished()
	}

	// Some are returned (unroutable)
	for i := 0; i < 5; i++ {
		metrics.MessageReturned()
	}

	if metrics.GetMessagesPublished() != 50 {
		t.Errorf("Messages published: got %d, want 50", metrics.GetMessagesPublished())
	}
	if metrics.GetMessagesReturned() != 5 {
		t.Errorf("Messages returned: got %d, want 5", metrics.GetMessagesReturned())
	}
}

// TestMetricsReset tests resetting metrics
func TestMetricsReset(t *testing.T) {
	metrics := NewStandardMetricsCollector()

	// Generate some metrics
	metrics.ConnectionCreated()
	metrics.ChannelCreated()
	metrics.MessagePublished()

	// Reset
	metrics = NewStandardMetricsCollector()

	if metrics.GetConnectionsCreated() != 0 {
		t.Error("Metrics should be reset to 0")
	}
}

// TestMetricsSnapshot tests taking a snapshot
func TestMetricsSnapshot(t *testing.T) {
	metrics := NewStandardMetricsCollector()

	metrics.MessagePublished()
	metrics.MessagePublished()

	snapshot1 := metrics.GetMessagesPublished()

	metrics.MessagePublished()

	snapshot2 := metrics.GetMessagesPublished()

	if snapshot1 != 2 {
		t.Errorf("Snapshot 1: got %d, want 2", snapshot1)
	}
	if snapshot2 != 3 {
		t.Errorf("Snapshot 2: got %d, want 3", snapshot2)
	}
}

// BenchmarkMetricsCollection benchmarks metric collection performance
func BenchmarkMetricsCollection(b *testing.B) {
	metrics := NewStandardMetricsCollector()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		metrics.MessagePublished()
	}
}

// BenchmarkMetricsConcurrent benchmarks concurrent metric updates
func BenchmarkMetricsConcurrent(b *testing.B) {
	metrics := NewStandardMetricsCollector()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			metrics.MessagePublished()
		}
	})
}

// TestMetricsIntegration tests metrics with actual operations
func TestMetricsIntegration(t *testing.T) {
	factory := requireRabbitMQ(t)

	metrics := NewStandardMetricsCollector()

	conn, err := factory.NewConnection()
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	ch, err := conn.NewChannel()
	if err != nil {
		t.Fatalf("Failed to create channel: %v", err)
	}
	defer ch.Close()

	// Declare queue
	queueName := fmt.Sprintf("test-metrics-%d", time.Now().UnixNano())
	_, err = ch.QueueDeclare(queueName, QueueDeclareOptions{
		AutoDelete: true,
		Exclusive:  true,
	})
	if err != nil {
		t.Fatalf("Failed to declare queue: %v", err)
	}

	// Publish messages and track with metrics
	numMessages := 10
	for i := 0; i < numMessages; i++ {
		msg := Publishing{
			Body: []byte(fmt.Sprintf("msg-%d", i)),
		}
		err := ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Failed to publish: %v", err)
		}
		metrics.MessagePublished()
	}

	// Consume messages and track with metrics
	deliveries, err := ch.Consume(queueName, "", ConsumeOptions{
		AutoAck: false,
	})
	if err != nil {
		t.Fatalf("Failed to consume: %v", err)
	}

	consumed := 0
	timeout := time.After(5 * time.Second)
	for consumed < numMessages {
		select {
		case delivery := <-deliveries:
			metrics.MessageConsumed()
			delivery.Ack(false)
			metrics.MessageAcked()
			consumed++
		case <-timeout:
			t.Fatalf("Timeout consuming messages, got %d/%d", consumed, numMessages)
		}
	}

	// Get metrics values
	published := metrics.GetMessagesPublished()
	consumed64 := metrics.GetMessagesConsumed()
	acked := metrics.GetMessagesAcked()

	// Verify metrics were collected
	if published < int64(numMessages) {
		t.Errorf("Published metric: got %d, want >= %d", published, numMessages)
	}

	if consumed64 < int64(numMessages) {
		t.Errorf("Consumed metric: got %d, want >= %d", consumed64, numMessages)
	}

	if acked < int64(numMessages) {
		t.Errorf("Acked metric: got %d, want >= %d", acked, numMessages)
	}

	t.Logf("Metrics - Published: %d, Consumed: %d, Acked: %d", published, consumed64, acked)
}

// TestMetricsPerformanceImpact tests performance impact of metrics
func TestMetricsPerformanceImpact(t *testing.T) {
	metrics := NewStandardMetricsCollector()

	start := time.Now()
	iterations := 1000000

	for i := 0; i < iterations; i++ {
		metrics.MessagePublished()
	}

	elapsed := time.Since(start)
	perOp := elapsed / time.Duration(iterations)

	t.Logf("Metrics collection: %d ops in %v (%v per op)", iterations, elapsed, perOp)

	if perOp > 100*time.Nanosecond {
		t.Errorf("Metrics collection too slow: %v per op", perOp)
	}
}

// TestNoOpMetricsCollector tests the no-op collector
func TestNoOpMetricsCollector(t *testing.T) {
	metrics := NewNoOpMetricsCollector()

	// Should not panic
	metrics.ConnectionCreated()
	metrics.ConnectionClosed()
	metrics.ChannelCreated()
	metrics.ChannelClosed()
	metrics.MessagePublished()
	metrics.MessageConsumed()
	metrics.MessageAcked()
	metrics.MessageNacked()
	metrics.MessageRejected()
	metrics.MessageReturned()
	metrics.ConfirmReceived(true)
	metrics.ConnectionError(nil)
	metrics.ChannelError(nil)

	t.Log("NoOpMetricsCollector handled all operations without panic")
}
