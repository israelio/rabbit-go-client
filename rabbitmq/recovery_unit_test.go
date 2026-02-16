package rabbitmq

import (
	"testing"
	"time"
)

// TestRecoveryManager_RecordTopology tests topology recording
func TestRecoveryManager_RecordTopology(t *testing.T) {
	rm := newRecoveryManager(true, true, 5*time.Second, 3)

	// Record exchange
	rm.recordExchange("test-exchange", "direct", ExchangeDeclareOptions{
		Durable: true,
	})

	if len(rm.exchanges) != 1 {
		t.Errorf("Expected 1 exchange, got %d", len(rm.exchanges))
	}
	if rm.exchanges[0].name != "test-exchange" {
		t.Errorf("Expected exchange name test-exchange, got %s", rm.exchanges[0].name)
	}

	// Record queue
	rm.recordQueue("test-queue", QueueDeclareOptions{
		Durable: true,
	})

	if len(rm.queues) != 1 {
		t.Errorf("Expected 1 queue, got %d", len(rm.queues))
	}
	if rm.queues[0].name != "test-queue" {
		t.Errorf("Expected queue name test-queue, got %s", rm.queues[0].name)
	}

	// Record binding
	rm.recordBinding("test-queue", "test-exchange", "routing-key", nil)

	if len(rm.bindings) != 1 {
		t.Errorf("Expected 1 binding, got %d", len(rm.bindings))
	}
	if rm.bindings[0].queue != "test-queue" {
		t.Errorf("Expected binding queue test-queue, got %s", rm.bindings[0].queue)
	}

	// Record consumer
	consumer := &DefaultConsumer{}
	rm.recordConsumer("test-queue", "consumer-tag", consumer, ConsumeOptions{})

	if len(rm.consumers) != 1 {
		t.Errorf("Expected 1 consumer, got %d", len(rm.consumers))
	}
	if rm.consumers[0].tag != "consumer-tag" {
		t.Errorf("Expected consumer tag consumer-tag, got %s", rm.consumers[0].tag)
	}

	// Test update of existing exchange
	rm.recordExchange("test-exchange", "topic", ExchangeDeclareOptions{
		Durable: false,
	})

	if len(rm.exchanges) != 1 {
		t.Errorf("Expected 1 exchange after update, got %d", len(rm.exchanges))
	}
	if rm.exchanges[0].kind != "topic" {
		t.Errorf("Expected exchange kind topic after update, got %s", rm.exchanges[0].kind)
	}
}

// TestRecoveryManager_Disabled tests that recording is disabled when topology recovery is off
func TestRecoveryManager_Disabled(t *testing.T) {
	rm := newRecoveryManager(true, false, 5*time.Second, 3)

	rm.recordExchange("test-exchange", "direct", ExchangeDeclareOptions{})
	rm.recordQueue("test-queue", QueueDeclareOptions{})
	rm.recordBinding("test-queue", "test-exchange", "key", nil)
	rm.recordConsumer("test-queue", "tag", &DefaultConsumer{}, ConsumeOptions{})

	if len(rm.exchanges) != 0 {
		t.Errorf("Expected 0 exchanges when topology recovery disabled, got %d", len(rm.exchanges))
	}
	if len(rm.queues) != 0 {
		t.Errorf("Expected 0 queues when topology recovery disabled, got %d", len(rm.queues))
	}
	if len(rm.bindings) != 0 {
		t.Errorf("Expected 0 bindings when topology recovery disabled, got %d", len(rm.bindings))
	}
	if len(rm.consumers) != 0 {
		t.Errorf("Expected 0 consumers when topology recovery disabled, got %d", len(rm.consumers))
	}
}

// TestNotificationBroadcast tests recovery notification broadcasting
func TestNotificationBroadcast(t *testing.T) {
	factory := NewConnectionFactory()
	conn := &Connection{
		factory:                factory,
		recoveryStartedChans:   []chan struct{}{},
		recoveryCompletedChans: []chan struct{}{},
		recoveryFailedChans:    []chan error{},
	}

	// Register notification channels
	started := make(chan struct{}, 1)
	completed := make(chan struct{}, 1)
	failed := make(chan error, 1)

	conn.NotifyRecoveryStarted(started)
	conn.NotifyRecoveryCompleted(completed)
	conn.NotifyRecoveryFailed(failed)

	// Test started notification
	conn.notifyRecoveryStarted()
	select {
	case <-started:
		// Success
	case <-time.After(100 * time.Millisecond):
		t.Error("Recovery started notification not received")
	}

	// Test completed notification
	conn.notifyRecoveryCompleted()
	select {
	case <-completed:
		// Success
	case <-time.After(100 * time.Millisecond):
		t.Error("Recovery completed notification not received")
	}

	// Test failed notification
	testErr := NewError(500, "test error", false)
	conn.notifyRecoveryFailed(testErr)
	select {
	case err := <-failed:
		if err.Error() != testErr.Error() {
			t.Errorf("Expected error %v, got %v", testErr, err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Recovery failed notification not received")
	}
}

// TestCalculateBackoff tests exponential backoff calculation
func TestCalculateBackoff(t *testing.T) {
	factory := NewConnectionFactory(
		WithRecoveryInterval(2 * time.Second),
	)
	conn := &Connection{factory: factory}

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{0, 2 * time.Second},   // 2 * 1
		{1, 4 * time.Second},   // 2 * 2
		{2, 8 * time.Second},   // 2 * 4
		{3, 16 * time.Second},  // 2 * 8
		{4, 32 * time.Second},  // 2 * 16
		{5, 64 * time.Second},  // 2 * 32 (capped)
		{6, 64 * time.Second},  // 2 * 32 (capped)
		{10, 64 * time.Second}, // 2 * 32 (capped)
	}

	for _, tt := range tests {
		got := conn.calculateBackoff(tt.attempt)
		if got != tt.expected {
			t.Errorf("calculateBackoff(%d) = %v, want %v", tt.attempt, got, tt.expected)
		}
	}
}

// TestCaptureChannelState tests channel state capture
func TestCaptureChannelState(t *testing.T) {
	factory := NewConnectionFactory()
	conn := &Connection{
		factory:  factory,
		channels: make(map[uint16]*Channel),
	}

	// Create mock channels
	ch1 := &Channel{
		id:            1,
		prefetchCount: 10,
		prefetchSize:  0,
		globalQos:     false,
		consumers:     make(map[string]*consumerState),
	}
	ch1.state.Store(int32(ChannelStateOpen))

	// Add a callback consumer
	consumer := &consumerState{
		tag:      "consumer1",
		queue:    "queue1",
		callback: &DefaultConsumer{},
		autoAck:  false,
	}
	ch1.consumers["consumer1"] = consumer

	conn.channels[1] = ch1

	// Capture state
	snapshots := conn.captureChannelState()

	if len(snapshots) != 1 {
		t.Fatalf("Expected 1 snapshot, got %d", len(snapshots))
	}

	snap := snapshots[0]
	if snap.id != 1 {
		t.Errorf("Expected channel ID 1, got %d", snap.id)
	}
	if snap.prefetchCount != 10 {
		t.Errorf("Expected prefetch count 10, got %d", snap.prefetchCount)
	}
	if len(snap.consumers) != 1 {
		t.Fatalf("Expected 1 consumer, got %d", len(snap.consumers))
	}
	if snap.consumers[0].tag != "consumer1" {
		t.Errorf("Expected consumer tag consumer1, got %s", snap.consumers[0].tag)
	}
	if snap.consumers[0].queue != "queue1" {
		t.Errorf("Expected consumer queue queue1, got %s", snap.consumers[0].queue)
	}
}

// TestCaptureChannelState_SkipsChannelBasedConsumers tests that channel-based consumers are not captured
func TestCaptureChannelState_SkipsChannelBasedConsumers(t *testing.T) {
	factory := NewConnectionFactory()
	conn := &Connection{
		factory:  factory,
		channels: make(map[uint16]*Channel),
	}

	ch1 := &Channel{
		id:        1,
		consumers: make(map[string]*consumerState),
	}

	// Add a channel-based consumer (no callback)
	consumer := &consumerState{
		tag:          "consumer1",
		queue:        "queue1",
		deliveryChan: make(chan Delivery),
		callback:     nil, // No callback
	}
	ch1.consumers["consumer1"] = consumer

	conn.channels[1] = ch1

	// Capture state
	snapshots := conn.captureChannelState()

	if len(snapshots) != 1 {
		t.Fatalf("Expected 1 snapshot, got %d", len(snapshots))
	}

	// Should have 0 consumers because it was channel-based
	if len(snapshots[0].consumers) != 0 {
		t.Errorf("Expected 0 consumers (channel-based should be skipped), got %d", len(snapshots[0].consumers))
	}
}
