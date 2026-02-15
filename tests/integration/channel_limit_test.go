package integration

import (
	"testing"

	"github.com/israelio/rabbit-go-client/rabbitmq"
)

// TestChannelMax tests channel maximum negotiation
func TestChannelMax(t *testing.T) {
	RequireRabbitMQ(t)

	factory := NewTestConnectionFactory(t)
	factory.ChannelMax = 10 // Request max 10 channels

	conn, err := factory.NewConnection()
	if err != nil {
		t.Fatalf("NewConnection failed: %v", err)
	}
	defer conn.Close()

	// Negotiated channel max should be <= requested
	negotiated := conn.GetChannelMax()
	t.Logf("Requested channel max: %d, negotiated: %d", factory.ChannelMax, negotiated)

	if negotiated > factory.ChannelMax {
		t.Errorf("Negotiated (%d) exceeds requested (%d)", negotiated, factory.ChannelMax)
	}

	// Try creating channels up to the limit
	channels := make([]*rabbitmq.Channel, 0)
	defer func() {
		for _, ch := range channels {
			ch.Close()
		}
	}()

	// Create channels up to negotiated max (minus 1 for channel 0)
	maxChannels := int(negotiated) - 1
	if maxChannels > 10 {
		maxChannels = 10 // Reasonable limit for test
	}

	for i := 0; i < maxChannels; i++ {
		ch, err := conn.NewChannel()
		if err != nil {
			t.Fatalf("Failed to create channel %d: %v", i+1, err)
		}
		channels = append(channels, ch)
	}

	t.Logf("Successfully created %d channels", len(channels))

	// Try creating one more - should fail
	ch, err := conn.NewChannel()
	if err == nil {
		ch.Close()
		t.Error("Should have failed to create channel beyond limit")
	} else {
		t.Logf("Correctly rejected channel beyond limit: %v", err)
	}
}

// TestDefaultChannelMax tests default channel max
func TestDefaultChannelMax(t *testing.T) {
	RequireRabbitMQ(t)

	conn, _ := NewTestChannel(t)
	defer conn.Close()

	negotiated := conn.GetChannelMax()
	t.Logf("Default negotiated channel max: %d", negotiated)

	// Default should typically be 2047 or higher
	if negotiated < 100 {
		t.Errorf("Negotiated channel max seems too low: %d", negotiated)
	}
}

// TestChannelMaxZero tests channel max of 0 (unlimited)
func TestChannelMaxZero(t *testing.T) {
	RequireRabbitMQ(t)

	factory := NewTestConnectionFactory(t)
	factory.ChannelMax = 0 // Unlimited

	conn, err := factory.NewConnection()
	if err != nil {
		t.Fatalf("NewConnection failed: %v", err)
	}
	defer conn.Close()

	negotiated := conn.GetChannelMax()
	t.Logf("Channel max with 0 requested: %d", negotiated)

	// Should use server's maximum
	if negotiated == 0 {
		t.Error("Negotiated channel max should not be 0")
	}
}

// TestChannelReuseAfterClose tests channel ID reuse
func TestChannelReuseAfterClose(t *testing.T) {
	RequireRabbitMQ(t)

	conn, firstCh := NewTestChannel(t)
	defer conn.Close()

	// Get first channel's ID
	firstID := firstCh.GetChannelID()
	t.Logf("First channel ID: %d", firstID)

	// Close first channel
	firstCh.Close()

	// Create new channel - might reuse the ID
	secondCh, err := conn.NewChannel()
	if err != nil {
		t.Fatalf("NewChannel failed: %v", err)
	}
	defer secondCh.Close()

	secondID := secondCh.GetChannelID()
	t.Logf("Second channel ID: %d", secondID)

	if secondID == firstID {
		t.Log("Channel ID was reused (allowed)")
	} else {
		t.Log("Channel ID was not reused (also allowed)")
	}
}

// TestManyChannels tests creating many channels
func TestManyChannels(t *testing.T) {
	RequireRabbitMQ(t)

	conn, _ := NewTestChannel(t)
	defer conn.Close()

	numChannels := 50
	channels := make([]*rabbitmq.Channel, 0, numChannels)

	defer func() {
		for _, ch := range channels {
			ch.Close()
		}
	}()

	// Create many channels
	for i := 0; i < numChannels; i++ {
		ch, err := conn.NewChannel()
		if err != nil {
			t.Fatalf("Failed to create channel %d: %v", i, err)
		}
		channels = append(channels, ch)
	}

	t.Logf("Successfully created %d channels", len(channels))

	// Verify all channels are usable
	queueName := GenerateQueueName(t)
	defer func() {
		// Cleanup with first channel
		CleanupQueue(t, channels[0], queueName)
	}()

	_, err := channels[0].QueueDeclare(queueName, rabbitmq.QueueDeclareOptions{})
	if err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Publish from different channels
	for i, ch := range channels[:10] {
		msg := rabbitmq.Publishing{
			Body: []byte(string(rune('0' + i))),
		}
		err = ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Publish from channel %d failed: %v", i, err)
		}
	}

	t.Logf("Published from 10 different channels")
}

// TestChannelNegotiation tests various channel max negotiations
func TestChannelNegotiation(t *testing.T) {
	RequireRabbitMQ(t)

	tests := []struct {
		name       string
		requested  uint16
		shouldFail bool
	}{
		{"very low", 1, false},
		{"low", 5, false},
		{"medium", 100, false},
		{"high", 1000, false},
		{"max uint16", 65535, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := NewTestConnectionFactory(t)
			factory.ChannelMax = tt.requested

			conn, err := factory.NewConnection()
			if tt.shouldFail {
				if err == nil {
					conn.Close()
					t.Error("Should have failed")
				}
				return
			}

			if err != nil {
				t.Fatalf("NewConnection failed: %v", err)
			}
			defer conn.Close()

			negotiated := conn.GetChannelMax()
			t.Logf("Requested: %d, negotiated: %d", tt.requested, negotiated)

			if negotiated > tt.requested && tt.requested != 0 {
				t.Errorf("Negotiated (%d) > requested (%d)", negotiated, tt.requested)
			}
		})
	}
}

// TestChannelLeakDetection tests for channel leaks
func TestChannelLeakDetection(t *testing.T) {
	RequireRabbitMQ(t)

	conn, _ := NewTestChannel(t)
	defer conn.Close()

	initialCount := conn.GetChannelCount()

	// Create and close channels
	for i := 0; i < 10; i++ {
		ch, err := conn.NewChannel()
		if err != nil {
			t.Fatalf("NewChannel failed: %v", err)
		}
		ch.Close()
	}

	finalCount := conn.GetChannelCount()

	if finalCount != initialCount {
		t.Errorf("Channel leak detected: initial=%d, final=%d", initialCount, finalCount)
	} else {
		t.Logf("No channel leak: count stable at %d", finalCount)
	}
}

// TestConcurrentChannelCreation tests concurrent channel creation
func TestConcurrentChannelCreation(t *testing.T) {
	RequireRabbitMQ(t)

	conn, _ := NewTestChannel(t)
	defer conn.Close()

	numGoroutines := 20
	channels := make(chan *rabbitmq.Channel, numGoroutines)
	errors := make(chan error, numGoroutines)

	// Create channels concurrently
	for i := 0; i < numGoroutines; i++ {
		go func() {
			ch, err := conn.NewChannel()
			if err != nil {
				errors <- err
			} else {
				channels <- ch
			}
		}()
	}

	// Collect results
	created := make([]*rabbitmq.Channel, 0)
	for i := 0; i < numGoroutines; i++ {
		select {
		case ch := <-channels:
			created = append(created, ch)
		case err := <-errors:
			t.Errorf("Channel creation failed: %v", err)
		}
	}

	// Cleanup
	for _, ch := range created {
		ch.Close()
	}

	t.Logf("Created %d channels concurrently", len(created))

	if len(created) != numGoroutines {
		t.Errorf("Created: got %d, want %d", len(created), numGoroutines)
	}
}
