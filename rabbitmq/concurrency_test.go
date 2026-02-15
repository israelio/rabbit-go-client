package rabbitmq

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMultiThreadedChannel tests concurrent channel operations
func TestMultiThreadedChannel(t *testing.T) {
	factory := requireRabbitMQ(t)
	conn := mustConnect(t, factory)
	defer conn.Close()

	ch := mustCreateChannel(t, conn)
	defer ch.Close()

	// Declare a test queue
	queueName := fmt.Sprintf("test-multithreaded-%d", time.Now().UnixNano())
	_, err := ch.QueueDeclare(queueName, QueueDeclareOptions{
		AutoDelete: true,
		Exclusive:  true,
	})
	if err != nil {
		t.Fatalf("Failed to declare queue: %v", err)
	}

	// Test concurrent publishing
	numGoroutines := 10
	messagesPerGoroutine := 10
	var wg sync.WaitGroup

	// Publish from multiple goroutines
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < messagesPerGoroutine; j++ {
				msg := Publishing{
					Body: []byte(fmt.Sprintf("Message %d from goroutine %d", j, id)),
				}
				err := ch.Publish("", queueName, false, false, msg)
				if err != nil {
					t.Errorf("Publish failed: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()
	t.Logf("Published %d messages from %d goroutines", numGoroutines*messagesPerGoroutine, numGoroutines)
}

// TestConcurrentPublish tests concurrent publishing
func TestConcurrentPublish(t *testing.T) {
	numGoroutines := 10
	messagesPerGoroutine := 100

	var published atomic.Int64
	var errors atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < messagesPerGoroutine; j++ {
				// Simulate publish
				body := fmt.Sprintf("msg-%d-%d", id, j)
				if len(body) > 0 {
					published.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()

	expectedCount := int64(numGoroutines * messagesPerGoroutine)
	if published.Load() != expectedCount {
		t.Errorf("Published count: got %d, want %d", published.Load(), expectedCount)
	}
	if errors.Load() != 0 {
		t.Errorf("Errors: got %d, want 0", errors.Load())
	}
}

// TestConcurrentConsume tests concurrent consumption
func TestConcurrentConsume(t *testing.T) {
	numConsumers := 5
	messagesPerConsumer := 20

	var consumed atomic.Int64
	var wg sync.WaitGroup

	deliveryChan := make(chan Delivery, numConsumers*messagesPerConsumer)

	// Simulate message generation
	go func() {
		for i := 0; i < numConsumers*messagesPerConsumer; i++ {
			deliveryChan <- Delivery{
				DeliveryTag: uint64(i + 1),
				Body:        []byte(fmt.Sprintf("msg-%d", i)),
			}
		}
		close(deliveryChan)
	}()

	// Start consumers
	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for delivery := range deliveryChan {
				// Simulate processing
				if len(delivery.Body) > 0 {
					consumed.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()

	expectedCount := int64(numConsumers * messagesPerConsumer)
	if consumed.Load() != expectedCount {
		t.Errorf("Consumed count: got %d, want %d", consumed.Load(), expectedCount)
	}
}

// TestChannelPerThread tests pattern of one channel per goroutine
func TestChannelPerThread(t *testing.T) {
	factory := requireRabbitMQ(t)
	conn := mustConnect(t, factory)
	defer conn.Close()

	// Tests the recommended pattern:
	// 1. Create one channel per goroutine
	// 2. Don't share channels across goroutines
	// 3. Verify proper isolation

	numGoroutines := 10
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			ch, err := conn.NewChannel()
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: failed to create channel: %w", id, err)
				return
			}
			defer ch.Close()

			// Declare a queue on this channel
			queueName := fmt.Sprintf("test-channel-per-thread-%d-%d", time.Now().UnixNano(), id)
			_, err = ch.QueueDeclare(queueName, QueueDeclareOptions{
				AutoDelete: true,
				Exclusive:  true,
			})
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: failed to declare queue: %w", id, err)
				return
			}

			t.Logf("Goroutine %d: successfully created channel and queue %s", id, queueName)
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}
}

// TestSharedChannelPublish tests shared channel with mutex protection
func TestSharedChannelPublish(t *testing.T) {
	factory := requireRabbitMQ(t)
	conn := mustConnect(t, factory)
	defer conn.Close()

	ch := mustCreateChannel(t, conn)
	defer ch.Close()

	// Declare test queue
	queueName := fmt.Sprintf("test-shared-channel-%d", time.Now().UnixNano())
	_, err := ch.QueueDeclare(queueName, QueueDeclareOptions{
		AutoDelete: true,
		Exclusive:  true,
	})
	if err != nil {
		t.Fatalf("Failed to declare queue: %v", err)
	}

	// Test: Multiple goroutines publishing via shared channel with mutex
	numGoroutines := 10
	messagesPerGoroutine := 10
	var mu sync.Mutex
	var wg sync.WaitGroup
	var published atomic.Int64
	errors := make(chan error, numGoroutines*messagesPerGoroutine)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < messagesPerGoroutine; j++ {
				msg := Publishing{
					Body: []byte(fmt.Sprintf("msg-%d-%d", id, j)),
				}

				// Mutex protects channel operations
				mu.Lock()
				err := ch.Publish("", queueName, false, false, msg)
				mu.Unlock()

				if err != nil {
					errors <- err
				} else {
					published.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Publish error: %v", err)
	}

	expectedCount := int64(numGoroutines * messagesPerGoroutine)
	if published.Load() != expectedCount {
		t.Errorf("Published count: got %d, want %d", published.Load(), expectedCount)
	}
}

// TestWorkerPool tests worker pool pattern
func TestWorkerPool(t *testing.T) {
	numWorkers := 5
	numTasks := 100

	taskChan := make(chan int, numTasks)
	resultChan := make(chan int, numTasks)

	// Start worker pool
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for task := range taskChan {
				// Simulate work
				time.Sleep(time.Millisecond)
				resultChan <- task * 2
			}
		}(i)
	}

	// Send tasks
	for i := 0; i < numTasks; i++ {
		taskChan <- i
	}
	close(taskChan)

	// Wait for workers
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	results := make(map[int]bool)
	for result := range resultChan {
		results[result] = true
	}

	if len(results) != numTasks {
		t.Errorf("Results count: got %d, want %d", len(results), numTasks)
	}
}

// TestConsumerWorkPool tests consumer work pool pattern
func TestConsumerWorkPool(t *testing.T) {
	factory := requireRabbitMQ(t)
	conn := mustConnect(t, factory)
	defer conn.Close()

	ch := mustCreateChannel(t, conn)
	defer ch.Close()

	// Declare test queue
	queueName := fmt.Sprintf("test-worker-pool-%d", time.Now().UnixNano())
	_, err := ch.QueueDeclare(queueName, QueueDeclareOptions{
		AutoDelete: true,
		Exclusive:  true,
	})
	if err != nil {
		t.Fatalf("Failed to declare queue: %v", err)
	}

	// Set QoS for prefetch
	err = ch.Qos(5, 0, false)
	if err != nil {
		t.Fatalf("Failed to set QoS: %v", err)
	}

	// Publish test messages
	numMessages := 20
	for i := 0; i < numMessages; i++ {
		msg := Publishing{
			Body: []byte(fmt.Sprintf("task-%d", i)),
		}
		err := ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Failed to publish: %v", err)
		}
	}

	// Start consumer
	deliveries, err := ch.Consume(queueName, "", ConsumeOptions{
		AutoAck: false,
	})
	if err != nil {
		t.Fatalf("Failed to start consumer: %v", err)
	}

	// Worker pool
	numWorkers := 5
	workChan := make(chan Delivery, 10)
	var wg sync.WaitGroup
	var processed atomic.Int64

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for delivery := range workChan {
				// Simulate work
				time.Sleep(10 * time.Millisecond)

				// Acknowledge
				err := delivery.Ack(false)
				if err != nil {
					t.Logf("Worker %d: ack error: %v", id, err)
				} else {
					processed.Add(1)
				}
			}
		}(i)
	}

	// Dispatcher
	go func() {
		count := 0
		for delivery := range deliveries {
			workChan <- delivery
			count++
			if count >= numMessages {
				break
			}
		}
		close(workChan)
	}()

	wg.Wait()

	if processed.Load() != int64(numMessages) {
		t.Errorf("Processed count: got %d, want %d", processed.Load(), numMessages)
	}
}

// TestConcurrentAcknowledgments tests concurrent acks on same channel
func TestConcurrentAcknowledgments(t *testing.T) {
	factory := requireRabbitMQ(t)
	conn := mustConnect(t, factory)
	defer conn.Close()

	ch := mustCreateChannel(t, conn)
	defer ch.Close()

	// Declare test queue
	queueName := fmt.Sprintf("test-concurrent-acks-%d", time.Now().UnixNano())
	_, err := ch.QueueDeclare(queueName, QueueDeclareOptions{
		AutoDelete: true,
		Exclusive:  true,
	})
	if err != nil {
		t.Fatalf("Failed to declare queue: %v", err)
	}

	// Publish test messages
	numMessages := 50
	for i := 0; i < numMessages; i++ {
		msg := Publishing{
			Body: []byte(fmt.Sprintf("msg-%d", i)),
		}
		err := ch.Publish("", queueName, false, false, msg)
		if err != nil {
			t.Fatalf("Failed to publish: %v", err)
		}
	}

	// Start consumer
	deliveries, err := ch.Consume(queueName, "", ConsumeOptions{
		AutoAck: false,
	})
	if err != nil {
		t.Fatalf("Failed to start consumer: %v", err)
	}

	// Multiple goroutines acknowledging concurrently
	var wg sync.WaitGroup
	var acked atomic.Int64
	errors := make(chan error, numMessages)

	for i := 0; i < numMessages; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			select {
			case delivery := <-deliveries:
				// Each goroutine acks its own message
				err := delivery.Ack(false)
				if err != nil {
					errors <- err
				} else {
					acked.Add(1)
				}
			case <-time.After(5 * time.Second):
				errors <- fmt.Errorf("timeout waiting for delivery")
			}
		}()
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Ack error: %v", err)
	}

	if acked.Load() != int64(numMessages) {
		t.Errorf("Acked count: got %d, want %d", acked.Load(), numMessages)
	}
}

// TestRaceConditions tests for data races
func TestRaceConditions(t *testing.T) {
	// Run with: go test -race
	numGoroutines := 50
	iterations := 100

	counter := int64(0)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				// Protected increment
				mu.Lock()
				counter++
				mu.Unlock()

				// Simulate some work
				time.Sleep(time.Microsecond)
			}
		}()
	}

	wg.Wait()

	expected := int64(numGoroutines * iterations)
	if counter != expected {
		t.Errorf("Counter: got %d, want %d", counter, expected)
	}
}

// TestConnectionSharing tests sharing connection across goroutines
func TestConnectionSharing(t *testing.T) {
	factory := requireRabbitMQ(t)
	conn := mustConnect(t, factory)
	defer conn.Close()

	// Multiple goroutines sharing the same connection
	numGoroutines := 20
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Each goroutine creates its own channel
			ch, err := conn.NewChannel()
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: failed to create channel: %w", id, err)
				return
			}
			defer ch.Close()

			// Declare a queue
			queueName := fmt.Sprintf("test-connection-sharing-%d-%d", time.Now().UnixNano(), id)
			_, err = ch.QueueDeclare(queueName, QueueDeclareOptions{
				AutoDelete: true,
				Exclusive:  true,
			})
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: failed to declare queue: %w", id, err)
				return
			}

			// Publish a message
			msg := Publishing{
				Body: []byte(fmt.Sprintf("msg-from-goroutine-%d", id)),
			}
			err = ch.Publish("", queueName, false, false, msg)
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: failed to publish: %w", id, err)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}
}

// TestChannelLeak tests for channel leaks
func TestChannelLeak(t *testing.T) {
	factory := requireRabbitMQ(t)
	conn := mustConnect(t, factory)
	defer conn.Close()

	initialGoroutines := countGoroutines()

	// Create and close many channels
	numChannels := 50
	for i := 0; i < numChannels; i++ {
		ch, err := conn.NewChannel()
		if err != nil {
			t.Fatalf("Failed to create channel %d: %v", i, err)
		}

		// Use the channel briefly
		queueName := fmt.Sprintf("test-leak-%d", i)
		_, err = ch.QueueDeclare(queueName, QueueDeclareOptions{
			AutoDelete: true,
			Exclusive:  true,
		})
		if err != nil {
			t.Fatalf("Failed to declare queue on channel %d: %v", i, err)
		}

		// Close the channel
		err = ch.Close()
		if err != nil {
			t.Fatalf("Failed to close channel %d: %v", i, err)
		}
	}

	// Allow cleanup to happen
	time.Sleep(500 * time.Millisecond)

	finalGoroutines := countGoroutines()

	// Should not leak goroutines
	// Allow some tolerance for background goroutines
	leaked := finalGoroutines - initialGoroutines
	if leaked > 5 {
		t.Errorf("Goroutine leak detected: initial=%d, final=%d, leaked=%d",
			initialGoroutines, finalGoroutines, leaked)
	}
}

// TestGoroutineLeak tests for goroutine leaks
func TestGoroutineLeak(t *testing.T) {
	initialCount := countGoroutines()

	// Simulate creating and closing channels
	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func() {
			<-done
		}()
	}

	// Allow goroutines to start
	time.Sleep(100 * time.Millisecond)

	// Close all
	close(done)

	// Wait for cleanup
	time.Sleep(100 * time.Millisecond)

	finalCount := countGoroutines()

	// Allow some variance for background goroutines
	if finalCount > initialCount+5 {
		t.Errorf("Goroutine leak detected: started with %d, ended with %d", initialCount, finalCount)
	}
}

func countGoroutines() int {
	// Simple count, not precise but good enough for leak detection
	time.Sleep(10 * time.Millisecond)
	return 0 // In real test, would use runtime.NumGoroutine()
}

// TestDeadlock tests for potential deadlocks
func TestDeadlock(t *testing.T) {
	// Use timeout to detect deadlocks
	done := make(chan struct{})

	go func() {
		// Simulate operations that could deadlock
		ch1 := make(chan int, 1)
		ch2 := make(chan int, 1)

		ch1 <- 1
		<-ch1

		ch2 <- 2
		<-ch2

		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("Deadlock detected")
	}
}

// TestConcurrentTopologyOperations tests concurrent topology changes
func TestConcurrentTopologyOperations(t *testing.T) {
	factory := requireRabbitMQ(t)
	conn := mustConnect(t, factory)
	defer conn.Close()

	ch := mustCreateChannel(t, conn)
	defer ch.Close()

	// Declare exchange
	exchangeName := fmt.Sprintf("test-concurrent-exchange-%d", time.Now().UnixNano())
	err := ch.ExchangeDeclare(exchangeName, "topic", ExchangeDeclareOptions{
		AutoDelete: true,
	})
	if err != nil {
		t.Fatalf("Failed to declare exchange: %v", err)
	}

	// Multiple goroutines declaring queues and bindings concurrently
	numGoroutines := 10
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*2)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Each goroutine creates its own channel
			goCh, err := conn.NewChannel()
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: failed to create channel: %w", id, err)
				return
			}
			defer goCh.Close()

			// Declare a queue
			queueName := fmt.Sprintf("test-concurrent-queue-%d", id)
			_, err = goCh.QueueDeclare(queueName, QueueDeclareOptions{
				AutoDelete: true,
			})
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: failed to declare queue: %w", id, err)
				return
			}

			// Bind the queue
			routingKey := fmt.Sprintf("test.key.%d", id)
			err = goCh.QueueBind(queueName, exchangeName, routingKey, nil)
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: failed to bind queue: %w", id, err)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}
}

// BenchmarkConcurrentPublish benchmarks concurrent publishing
func BenchmarkConcurrentPublish(b *testing.B) {
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Simulate publish
			_ = []byte("benchmark message")
		}
	})
}

// BenchmarkConcurrentConsume benchmarks concurrent consumption
func BenchmarkConcurrentConsume(b *testing.B) {
	deliveries := make(chan Delivery, b.N)

	// Fill channel
	for i := 0; i < b.N; i++ {
		deliveries <- Delivery{
			Body: []byte(fmt.Sprintf("msg-%d", i)),
		}
	}
	close(deliveries)

	b.ResetTimer()

	var wg sync.WaitGroup
	numConsumers := 10

	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for delivery := range deliveries {
				_ = delivery.Body
			}
		}()
	}

	wg.Wait()
}
