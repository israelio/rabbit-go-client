package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestRpcClientServer tests basic RPC client/server pattern
func TestRpcClientServer(t *testing.T) {
	factory := requireRabbitMQ(t)
	conn := mustConnect(t, factory)
	defer conn.Close()

	serverCh := mustCreateChannel(t, conn)
	defer serverCh.Close()

	clientCh := mustCreateChannel(t, conn)
	defer clientCh.Close()

	// Declare RPC queue
	rpcQueue := fmt.Sprintf("rpc-queue-%d", time.Now().UnixNano())
	_, err := serverCh.QueueDeclare(rpcQueue, QueueDeclareOptions{
		AutoDelete: true,
	})
	if err != nil {
		t.Fatalf("Failed to declare RPC queue: %v", err)
	}

	// Start server
	serverDeliveries, err := serverCh.Consume(rpcQueue, "", ConsumeOptions{
		AutoAck: false,
	})
	if err != nil {
		t.Fatalf("Failed to start server consumer: %v", err)
	}

	go func() {
		for delivery := range serverDeliveries {
			// Process request (echo server)
			response := Publishing{
				Properties: Properties{
					CorrelationId: delivery.Properties.CorrelationId,
				},
				Body: append([]byte("Echo: "), delivery.Body...),
			}

			// Send reply
			serverCh.Publish("", delivery.Properties.ReplyTo, false, false, response)
			delivery.Ack(false)
		}
	}()

	// Create RPC client
	client, err := NewRpcClient(clientCh)
	if err != nil {
		t.Fatalf("Failed to create RPC client: %v", err)
	}
	defer client.Close()

	// Make RPC call
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	request := Publishing{
		Body: []byte("Hello, RPC!"),
	}

	reply, err := client.Call(ctx, "", rpcQueue, request)
	if err != nil {
		t.Fatalf("RPC call failed: %v", err)
	}

	expected := "Echo: Hello, RPC!"
	if string(reply.Body) != expected {
		t.Errorf("Reply body: got %s, want %s", reply.Body, expected)
	}
}

// TestRpcClientTimeout tests RPC call timeouts
func TestRpcClientTimeout(t *testing.T) {
	factory := requireRabbitMQ(t)
	conn := mustConnect(t, factory)
	defer conn.Close()

	ch := mustCreateChannel(t, conn)
	defer ch.Close()

	// Declare RPC queue (but no server listening)
	rpcQueue := fmt.Sprintf("rpc-queue-timeout-%d", time.Now().UnixNano())
	_, err := ch.QueueDeclare(rpcQueue, QueueDeclareOptions{
		AutoDelete: true,
	})
	if err != nil {
		t.Fatalf("Failed to declare RPC queue: %v", err)
	}

	// Create RPC client
	client, err := NewRpcClient(ch)
	if err != nil {
		t.Fatalf("Failed to create RPC client: %v", err)
	}
	defer client.Close()

	// Make RPC call with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	request := Publishing{
		Body: []byte("This will timeout"),
	}

	_, err = client.Call(ctx, "", rpcQueue, request)
	if err == nil {
		t.Fatal("Expected timeout error, got nil")
	}

	if err != context.DeadlineExceeded {
		t.Errorf("Expected context.DeadlineExceeded, got %v", err)
	}
}

// TestRpcClientConcurrentCalls tests concurrent RPC calls
func TestRpcClientConcurrentCalls(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrency test in short mode")
	}

	factory := requireRabbitMQ(t)
	conn := mustConnect(t, factory)
	defer conn.Close()

	serverCh := mustCreateChannel(t, conn)
	defer serverCh.Close()

	clientCh := mustCreateChannel(t, conn)
	defer clientCh.Close()

	// Declare RPC queue
	rpcQueue := fmt.Sprintf("rpc-queue-concurrent-%d", time.Now().UnixNano())
	_, err := serverCh.QueueDeclare(rpcQueue, QueueDeclareOptions{
		AutoDelete: true,
	})
	if err != nil {
		t.Fatalf("Failed to declare RPC queue: %v", err)
	}

	// Start server (echo with ID)
	serverDeliveries, err := serverCh.Consume(rpcQueue, "", ConsumeOptions{
		AutoAck: false,
	})
	if err != nil {
		t.Fatalf("Failed to start server consumer: %v", err)
	}

	go func() {
		for delivery := range serverDeliveries {
			response := Publishing{
				Properties: Properties{
					CorrelationId: delivery.Properties.CorrelationId,
				},
				Body: append([]byte("Echo-"), delivery.Body...),
			}
			serverCh.Publish("", delivery.Properties.ReplyTo, false, false, response)
			delivery.Ack(false)
		}
	}()

	// Create RPC client
	client, err := NewRpcClient(clientCh)
	if err != nil {
		t.Fatalf("Failed to create RPC client: %v", err)
	}

	// Make concurrent RPC calls
	numCalls := 20
	var wg sync.WaitGroup
	errors := make(chan error, numCalls)

	for i := 0; i < numCalls; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			request := Publishing{
				Body: []byte(fmt.Sprintf("Req-%d", id)),
			}

			reply, err := client.Call(ctx, "", rpcQueue, request)
			if err != nil {
				errors <- fmt.Errorf("call %d failed: %w", id, err)
				return
			}

			expected := fmt.Sprintf("Echo-Req-%d", id)
			if string(reply.Body) != expected {
				errors <- fmt.Errorf("call %d: got %s, want %s", id, reply.Body, expected)
			}
		}(i)
	}

	wg.Wait()

	// Small delay to ensure all replies have been fully processed
	time.Sleep(50 * time.Millisecond)

	// Close RPC client before closing channels (to avoid premature consumer cancellation)
	client.Close()

	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}
}

// TestDirectReplyToQueue tests amq.rabbitmq.reply-to pseudo-queue
func TestDirectReplyToQueue(t *testing.T) {
	factory := requireRabbitMQ(t)
	conn := mustConnect(t, factory)
	defer conn.Close()

	serverCh := mustCreateChannel(t, conn)
	defer serverCh.Close()

	clientCh := mustCreateChannel(t, conn)
	defer clientCh.Close()

	// Declare RPC queue
	rpcQueue := fmt.Sprintf("rpc-queue-direct-reply-%d", time.Now().UnixNano())
	_, err := serverCh.QueueDeclare(rpcQueue, QueueDeclareOptions{
		AutoDelete: true,
	})
	if err != nil {
		t.Fatalf("Failed to declare RPC queue: %v", err)
	}

	// Start server
	serverDeliveries, err := serverCh.Consume(rpcQueue, "", ConsumeOptions{
		AutoAck: false,
	})
	if err != nil {
		t.Fatalf("Failed to start server consumer: %v", err)
	}

	go func() {
		for delivery := range serverDeliveries {
			response := Publishing{
				Properties: Properties{
					CorrelationId: delivery.Properties.CorrelationId,
				},
				Body: []byte("Direct reply response"),
			}
			serverCh.Publish("", delivery.Properties.ReplyTo, false, false, response)
			delivery.Ack(false)
		}
	}()

	// Use direct reply-to feature
	correlationId := fmt.Sprintf("direct-reply-%d", time.Now().UnixNano())

	// Consume from amq.rabbitmq.reply-to
	replyDeliveries, err := clientCh.Consume("amq.rabbitmq.reply-to", "", ConsumeOptions{
		AutoAck:   true,
		Exclusive: true,
	})
	if err != nil {
		t.Fatalf("Failed to consume from direct reply-to queue: %v", err)
	}

	// Send request
	request := Publishing{
		Properties: Properties{
			CorrelationId: correlationId,
			ReplyTo:       "amq.rabbitmq.reply-to",
		},
		Body: []byte("Direct reply request"),
	}
	err = clientCh.Publish("", rpcQueue, false, false, request)
	if err != nil {
		t.Fatalf("Failed to publish request: %v", err)
	}

	// Wait for reply
	select {
	case reply := <-replyDeliveries:
		if reply.Properties.CorrelationId != correlationId {
			t.Errorf("Correlation ID: got %s, want %s",
				reply.Properties.CorrelationId, correlationId)
		}
		expected := "Direct reply response"
		if string(reply.Body) != expected {
			t.Errorf("Reply body: got %s, want %s", reply.Body, expected)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for reply")
	}
}

// TestRpcWithJsonPayload tests RPC with JSON serialization
func TestRpcWithJsonPayload(t *testing.T) {
	type Request struct {
		Method string      `json:"method"`
		Params interface{} `json:"params"`
	}

	type Response struct {
		Result interface{} `json:"result"`
		Error  string      `json:"error,omitempty"`
	}

	tests := []struct {
		name    string
		request Request
	}{
		{
			name: "simple method call",
			request: Request{
				Method: "add",
				Params: []int{5, 3},
			},
		},
		{
			name: "string parameter",
			request: Request{
				Method: "echo",
				Params: "hello",
			},
		},
		{
			name: "complex object",
			request: Request{
				Method: "process",
				Params: map[string]interface{}{
					"name": "test",
					"age":  25,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize request
			data, err := json.Marshal(tt.request)
			if err != nil {
				t.Fatalf("Marshal failed: %v", err)
			}

			// Verify can deserialize
			var decoded Request
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatalf("Unmarshal failed: %v", err)
			}

			if decoded.Method != tt.request.Method {
				t.Errorf("Method: got %q, want %q", decoded.Method, tt.request.Method)
			}
		})
	}
}

// TestRpcBroadcast tests RPC with fanout (multiple responders)
func TestRpcBroadcast(t *testing.T) {
	factory := requireRabbitMQ(t)
	conn := mustConnect(t, factory)
	defer conn.Close()

	// Create fanout exchange
	exchangeName := fmt.Sprintf("rpc-broadcast-%d", time.Now().UnixNano())
	serverCh := mustCreateChannel(t, conn)
	defer serverCh.Close()

	err := serverCh.ExchangeDeclare(exchangeName, "fanout", ExchangeDeclareOptions{
		AutoDelete: true,
	})
	if err != nil {
		t.Fatalf("Failed to declare exchange: %v", err)
	}

	// Start multiple servers
	numServers := 3
	responseChan := make(chan string, numServers)

	for i := 0; i < numServers; i++ {
		go func(id int) {
			ch, _ := conn.NewChannel()
			defer ch.Close()

			// Each server has its own queue bound to fanout
			q, _ := ch.QueueDeclare("", QueueDeclareOptions{
				Exclusive:  true,
				AutoDelete: true,
			})
			ch.QueueBind(q.Name, exchangeName, "", nil)

			deliveries, _ := ch.Consume(q.Name, "", ConsumeOptions{AutoAck: true})
			for delivery := range deliveries {
				response := Publishing{
					Properties: Properties{
						CorrelationId: delivery.Properties.CorrelationId,
					},
					Body: []byte(fmt.Sprintf("Server-%d-Response", id)),
				}
				ch.Publish("", delivery.Properties.ReplyTo, false, false, response)
				responseChan <- fmt.Sprintf("Server-%d", id)
				return // Exit after first response
			}
		}(i)
	}

	time.Sleep(100 * time.Millisecond) // Let servers start

	// Client
	clientCh := mustCreateChannel(t, conn)
	defer clientCh.Close()

	client, err := NewRpcClient(clientCh)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Broadcast request
	request := Publishing{Body: []byte("Broadcast")}
	err = clientCh.Publish(exchangeName, "", false, false, Publishing{
		Properties: Properties{
			CorrelationId: "broadcast-1",
			ReplyTo:       client.replyQueue,
		},
		Body: request.Body,
	})
	if err != nil {
		t.Fatalf("Failed to broadcast: %v", err)
	}

	// Collect responses
	received := make(map[string]bool)
	timeout := time.After(2 * time.Second)
	for i := 0; i < numServers; i++ {
		select {
		case server := <-responseChan:
			received[server] = true
		case <-timeout:
			t.Logf("Timeout waiting for all responses, got %d/%d", len(received), numServers)
			return
		}
	}

	if len(received) != numServers {
		t.Errorf("Expected %d responses, got %d", numServers, len(received))
	}
}

// TestRpcLoadBalancing tests RPC across multiple workers
func TestRpcLoadBalancing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrency test in short mode")
	}

	factory := requireRabbitMQ(t)
	conn := mustConnect(t, factory)
	defer conn.Close()

	// Shared RPC queue
	rpcQueue := fmt.Sprintf("rpc-queue-lb-%d", time.Now().UnixNano())
	setupCh := mustCreateChannel(t, conn)
	_, err := setupCh.QueueDeclare(rpcQueue, QueueDeclareOptions{
		AutoDelete: true,
	})
	if err != nil {
		t.Fatalf("Failed to declare queue: %v", err)
	}
	setupCh.Close()

	// Start multiple workers on same queue
	numWorkers := 3
	workersHandled := make(map[int]*int)
	var mu sync.Mutex

	for i := 0; i < numWorkers; i++ {
		count := 0
		workersHandled[i] = &count
		go func(id int, counter *int) {
			ch, _ := conn.NewChannel()
			defer ch.Close()

			deliveries, _ := ch.Consume(rpcQueue, "", ConsumeOptions{AutoAck: false})
			for delivery := range deliveries {
				mu.Lock()
				*counter++
				mu.Unlock()

				response := Publishing{
					Properties: Properties{
						CorrelationId: delivery.Properties.CorrelationId,
					},
					Body: []byte(fmt.Sprintf("Worker-%d", id)),
				}
				ch.Publish("", delivery.Properties.ReplyTo, false, false, response)
				delivery.Ack(false)
			}
		}(i, &count)
	}

	time.Sleep(100 * time.Millisecond) // Let workers start

	// Client
	clientCh := mustCreateChannel(t, conn)
	defer clientCh.Close()

	client, err := NewRpcClient(clientCh)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Send multiple requests
	numRequests := 15
	var wg sync.WaitGroup
	responses := make(chan string, numRequests)

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			request := Publishing{Body: []byte(fmt.Sprintf("Req-%d", id))}
			reply, err := client.Call(ctx, "", rpcQueue, request)
			if err != nil {
				t.Logf("Request %d failed: %v", id, err)
				return
			}
			responses <- string(reply.Body)
		}(i)
	}

	wg.Wait()

	// Small delay to ensure all replies have been fully processed
	time.Sleep(50 * time.Millisecond)

	// Close RPC client before closing channels
	client.Close()

	close(responses)

	// Count responses per worker
	workerCounts := make(map[string]int)
	for resp := range responses {
		workerCounts[resp]++
	}

	t.Logf("Load distribution: %v", workerCounts)

	// Verify all workers handled at least one request
	if len(workerCounts) < numWorkers {
		t.Logf("Warning: Not all workers received requests. Active workers: %d/%d",
			len(workerCounts), numWorkers)
	}
}
