# Automatic Recovery Example

This example demonstrates how to build a resilient RabbitMQ application that automatically reconnects when the connection is lost.

## What This Example Shows

- **Connection monitoring** using `NotifyClose()`
- **Automatic reconnection** with exponential backoff
- **Graceful error handling** when connection drops
- **Message persistence** across reconnections
- **Publisher and consumer recovery**

## How It Works

The example uses a **reconnection loop pattern**:

1. Connect to RabbitMQ
2. Set up queue, publisher, and consumer
3. Monitor for connection/channel errors
4. When connection drops, clean up and reconnect
5. Continue from where it left off

Key features:
- **Message counter persists** across reconnections
- **Durable queue** ensures messages aren't lost
- **Select statement** monitors delivery, connection, and channel errors
- **Graceful shutdown** of publisher before reconnecting

## Running the Example

### Terminal 1: Start the Example

```bash
cd examples/recovery
go run main.go
```

You'll see output like:
```
=== Automatic Recovery Example ===
This example demonstrates connection recovery
Try restarting RabbitMQ to see recovery in action:
  docker restart rabbitmq

2026/02/15 21:34:23 ✓ Connected to RabbitMQ
2026/02/15 21:34:23 ✓ Declared queue: recovery_test_queue
2026/02/15 21:34:23 ✓ Consumer started
→ Published: Message 0
← Received: Message 0
→ Published: Message 1
← Received: Message 1
```

### Terminal 2: Restart RabbitMQ

While the example is running, restart RabbitMQ:

```bash
docker restart rabbitmq
```

### Expected Behavior

You should see output like this:

```
→ Published: Message 5
← Received: Message 5
⚠ Publish error: AMQP error 504 (client): channel closed (connection lost)
⚠ Channel closed signal received in publisher
⚠ Delivery channel closed
2026/02/15 21:35:10 Connection lost: delivery channel closed
2026/02/15 21:35:10 Reconnecting in 5 seconds...
2026/02/15 21:35:15 ✓ Connected to RabbitMQ
2026/02/15 21:35:15 ✓ Declared queue: recovery_test_queue
2026/02/15 21:35:15 ✓ Consumer started
→ Published: Message 6
← Received: Message 6
→ Published: Message 7
← Received: Message 7
```

Notice:
- ✅ Message counter continues (5 → 6 → 7)
- ✅ Reconnects automatically after 5 seconds
- ✅ Queue and consumer are re-established
- ✅ Publishing and consuming resume

## Key Code Patterns

### 1. Monitoring Connection/Channel Errors

```go
// Monitor connection errors
connErrors := make(chan *rabbitmq.Error, 1)
conn.NotifyClose(connErrors)

// Monitor channel errors
channelErrors := make(chan *rabbitmq.Error, 1)
ch.NotifyClose(channelErrors)
```

### 2. Select Statement for Multiple Error Sources

```go
for {
    select {
    case delivery, ok := <-deliveries:
        if !ok {
            return fmt.Errorf("delivery channel closed")
        }
        // Process delivery...

    case err := <-connErrors:
        return fmt.Errorf("connection closed: %w", err)

    case err := <-channelErrors:
        return fmt.Errorf("channel closed: %w", err)
    }
}
```

### 3. Reconnection Loop with Backoff

```go
for {
    err := runWithRecovery(&messageCount, &mu)
    if err != nil {
        log.Printf("Connection lost: %v", err)
        log.Printf("Reconnecting in 5 seconds...")
        time.Sleep(5 * time.Second)
    }
}
```

### 4. State Preservation Across Reconnects

```go
// Message counter shared across reconnects
var messageCount int
var mu sync.Mutex

// Pass to recovery function
err := runWithRecovery(&messageCount, &mu)
```

## Testing Different Scenarios

### Scenario 1: Normal Operation
```bash
go run main.go
# Let it run for 10 seconds
# Press Ctrl+C to stop
```

### Scenario 2: RabbitMQ Restart
```bash
# Terminal 1
go run main.go

# Terminal 2
docker restart rabbitmq

# Watch Terminal 1 for automatic reconnection
```

### Scenario 3: RabbitMQ Crash
```bash
# Terminal 1
go run main.go

# Terminal 2
docker stop rabbitmq
# Wait 10 seconds to see reconnection attempts fail
docker start rabbitmq
# Watch Terminal 1 reconnect successfully
```

### Scenario 4: Network Partition
```bash
# Terminal 1
go run main.go

# Terminal 2 (simulate network issue)
docker network disconnect bridge rabbitmq
# Wait a few seconds
docker network connect bridge rabbitmq

# Watch Terminal 1 reconnect
```

## Production Considerations

This example demonstrates the basic pattern. For production use, consider:

### 1. Exponential Backoff
```go
retryDelay := time.Second
maxDelay := time.Minute

for {
    err := runWithRecovery()
    if err != nil {
        log.Printf("Reconnecting in %v...", retryDelay)
        time.Sleep(retryDelay)

        // Exponential backoff
        retryDelay *= 2
        if retryDelay > maxDelay {
            retryDelay = maxDelay
        }
    } else {
        // Reset on successful connection
        retryDelay = time.Second
    }
}
```

### 2. Circuit Breaker
```go
failureCount := 0
maxFailures := 10

for {
    err := runWithRecovery()
    if err != nil {
        failureCount++
        if failureCount >= maxFailures {
            log.Fatal("Too many failures, giving up")
        }
    } else {
        failureCount = 0
    }
}
```

### 3. Health Checks
```go
// Expose health endpoint
http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
    if conn != nil && !conn.IsClosed() {
        w.WriteHeader(200)
        w.Write([]byte("healthy"))
    } else {
        w.WriteHeader(503)
        w.Write([]byte("unhealthy"))
    }
})
```

### 4. Metrics
```go
// Track reconnection count
var reconnectCount atomic.Int64

func runWithRecovery() error {
    reconnectCount.Add(1)
    // ... rest of code
}
```

### 5. Graceful Shutdown
```go
func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Handle signals
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

    go func() {
        <-sigChan
        log.Println("Shutdown signal received")
        cancel()
    }()

    runWithRecovery(ctx)
}
```

## Comparison with Java Client

The Java RabbitMQ client has built-in automatic recovery that:
- Automatically reconnects on connection failure
- Re-declares topology (queues, exchanges, bindings)
- Re-registers consumers

This Go example demonstrates the **manual approach**, which gives you:
- ✅ More control over reconnection logic
- ✅ Custom backoff strategies
- ✅ Application-specific error handling
- ✅ State preservation logic
- ✅ Better observability

## Further Reading

- RabbitMQ Connection Best Practices: https://www.rabbitmq.com/connections.html
- RabbitMQ Reliability Guide: https://www.rabbitmq.com/reliability.html
- Go Patterns for Resilient Systems: https://pkg.go.dev/context

## Related Examples

- `basic_consume` - Simple consumer without recovery
- `work_queue` - Multiple workers (add recovery to make them resilient)
- `publisher_confirms` - Reliable publishing (combine with recovery for production)
