# RabbitMQ Go Client

A production-ready Go client library for RabbitMQ that provides API parity with the official [Java client](https://github.com/rabbitmq/rabbitmq-java-client). This library implements the AMQP 0-9-1 protocol with idiomatic Go patterns including goroutines, channels, and functional options.

## Features

- ✅ **Complete AMQP 0-9-1 Support**: Full implementation of the protocol
- ✅ **Connection Management**: TCP/TLS connections with heartbeat monitoring
- ✅ **Channel Operations**: Publish, consume, acknowledge, QoS
- ✅ **Topology Management**: Exchanges, queues, and bindings
- ✅ **Publisher Confirms**: Reliable message publishing with ack/nack
- ✅ **Multiple Consumer Patterns**: Channel-based (idiomatic Go) and callback-based (Java-like)
- ✅ **Automatic Recovery**: Connection and topology recovery on failure
- ✅ **Return Handling**: Notifications for unroutable messages
- ✅ **Transactions**: Full transaction support (tx.select, commit, rollback)
- ✅ **Idiomatic Go**: goroutines, channels, context support, functional options

## Installation

```bash
go get github.com/israelio/rabbit-go-client v1.0.0
```

## Quick Start

### Simple Publisher

```go
package main

import (
    "log"
    "github.com/israelio/rabbit-go-client/rabbitmq"
)

func main() {
    // Create connection factory
    factory := rabbitmq.NewConnectionFactory(
        rabbitmq.WithHost("localhost"),
        rabbitmq.WithPort(5672),
        rabbitmq.WithCredentials("guest", "guest"),
    )

    // Connect
    conn, err := factory.NewConnection()
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    // Create channel
    ch, err := conn.NewChannel()
    if err != nil {
        log.Fatal(err)
    }
    defer ch.Close()

    // Declare queue
    queue, err := ch.QueueDeclare("my-queue", rabbitmq.QueueDeclareOptions{
        Durable: true,
    })
    if err != nil {
        log.Fatal(err)
    }

    // Publish message
    msg := rabbitmq.Publishing{
        Properties: rabbitmq.PersistentTextPlain,
        Body:       []byte("Hello, RabbitMQ!"),
    }

    err = ch.Publish("", queue.Name, false, false, msg)
    if err != nil {
        log.Fatal(err)
    }

    log.Println("Message published!")
}
```

### Simple Consumer

```go
package main

import (
    "log"
    "github.com/israelio/rabbit-go-client/rabbitmq"
)

func main() {
    factory := rabbitmq.NewConnectionFactory(
        rabbitmq.WithHost("localhost"),
        rabbitmq.WithCredentials("guest", "guest"),
    )

    conn, err := factory.NewConnection()
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    ch, err := conn.NewChannel()
    if err != nil {
        log.Fatal(err)
    }
    defer ch.Close()

    // Set prefetch
    ch.Qos(10, 0, false)

    // Consume messages
    deliveries, err := ch.Consume("my-queue", "", rabbitmq.ConsumeOptions{
        AutoAck: false,
    })
    if err != nil {
        log.Fatal(err)
    }

    for delivery := range deliveries {
        log.Printf("Received: %s", delivery.Body)
        delivery.Ack(false)
    }
}
```

## Advanced Features

### Publisher Confirms

Ensure reliable message delivery with publisher confirms:

```go
// Enable publisher confirms
err := ch.ConfirmSelect(false)
if err != nil {
    log.Fatal(err)
}

// Publish with confirmation
err = ch.PublishWithConfirm("", "my-queue", false, false, msg, 5*time.Second)
if err != nil {
    log.Fatal(err)
}

// Or use async confirmations
confirms := make(chan rabbitmq.Confirmation, 1)
ch.NotifyPublish(confirms)

ch.Publish("", "my-queue", false, false, msg)

conf := <-confirms
if conf.Ack {
    log.Println("Message confirmed")
} else {
    log.Println("Message nacked")
}
```

### Callback-Based Consumers

For more control, use callback-based consumers similar to the Java client:

```go
type MyConsumer struct {
    rabbitmq.DefaultConsumer
}

func (c *MyConsumer) HandleDelivery(consumerTag string, delivery rabbitmq.Delivery) error {
    log.Printf("Received: %s", delivery.Body)
    return delivery.Ack(false)
}

func (c *MyConsumer) HandleShutdown(consumerTag string, cause *rabbitmq.Error) {
    log.Printf("Consumer shutdown: %v", cause)
}

// Register consumer
consumer := &MyConsumer{}
err := ch.ConsumeWithCallback("my-queue", "my-consumer", rabbitmq.ConsumeOptions{
    AutoAck: false,
}, consumer)
```

### Automatic Recovery

Enable automatic recovery to reconnect after network failures:

```go
factory := rabbitmq.NewConnectionFactory(
    rabbitmq.WithHost("localhost"),
    rabbitmq.WithAutomaticRecovery(true),
    rabbitmq.WithTopologyRecovery(true),
    rabbitmq.WithRecoveryInterval(5 * time.Second),
)

conn, err := factory.NewConnection()
// Connection will automatically reconnect and recover topology
```

### Return Handling

Handle unroutable messages (mandatory flag):

```go
returns := make(chan rabbitmq.Return, 1)
ch.NotifyReturn(returns)

go func() {
    for ret := range returns {
        log.Printf("Message returned: %s (code: %d)", ret.ReplyText, ret.ReplyCode)
    }
}()

// Publish with mandatory flag
ch.Publish("", "nonexistent-queue", true, false, msg)
```

### Transactions

Use transactions for atomic operations:

```go
// Start transaction
err := ch.TxSelect()
if err != nil {
    log.Fatal(err)
}

// Publish messages
ch.Publish("", "queue1", false, false, msg1)
ch.Publish("", "queue2", false, false, msg2)

// Commit or rollback
if someCondition {
    ch.TxCommit()
} else {
    ch.TxRollback()
}
```

## Configuration Options

The factory supports extensive configuration via functional options:

```go
factory := rabbitmq.NewConnectionFactory(
    rabbitmq.WithHost("localhost"),
    rabbitmq.WithPort(5672),
    rabbitmq.WithVHost("/"),
    rabbitmq.WithCredentials("user", "pass"),
    rabbitmq.WithTLS(tlsConfig),
    rabbitmq.WithHeartbeat(10 * time.Second),
    rabbitmq.WithConnectionTimeout(30 * time.Second),
    rabbitmq.WithChannelMax(100),
    rabbitmq.WithFrameMax(131072),
    rabbitmq.WithAutomaticRecovery(true),
    rabbitmq.WithClientProperty("application", "my-app"),
    rabbitmq.WithLogger(logger),
)
```

## Architecture

The library follows a layered architecture:

1. **Protocol Layer** (`internal/protocol`, `internal/frame`): AMQP 0-9-1 frame encoding/decoding
2. **Core Layer** (`rabbitmq/`): Connection, Channel, and lifecycle management
3. **Feature Layer**: Publisher confirms, recovery, transactions
4. **Consumer Layer**: Multiple consumption patterns (channels, callbacks)

### Design Principles

- **Functional Options**: Flexible configuration without breaking API
- **Goroutines**: Background operations (frame reading, heartbeats, consumers)
- **Channels**: Idiomatic Go communication patterns
- **Context Support**: Cancellation and timeouts
- **Error Handling**: Rich error types matching AMQP reply codes

## Comparison with Java Client

This Go client maintains conceptual parity with the Java client:

| Feature | Java Client | Go Client | Notes |
|---------|-------------|-----------|-------|
| ConnectionFactory | ✓ | ✓ | Functional options in Go |
| Connection | ✓ | ✓ | |
| Channel | ✓ | ✓ | |
| Publisher Confirms | ✓ | ✓ | |
| Consumer Callbacks | ✓ | ✓ | Plus channel-based |
| Automatic Recovery | ✓ | ✓ | |
| Return Handling | ✓ | ✓ | |
| Transactions | ✓ | ✓ | |
| Threading | ExecutorService | goroutines | Idiomatic Go |
| Error Handling | Exceptions | Errors | Idiomatic Go |

## Examples

See the [`examples/`](examples/) directory for complete examples:

- **basic_publish**: Simple message publishing
- **basic_consume**: Simple message consumption
- **publisher_confirms**: Reliable publishing with confirms
- **worker_pool**: Multiple consumer workers
- **rpc**: Request/reply pattern
- **recovery**: Automatic recovery demonstration
- **topics**: Topic exchange routing

## Requirements

- Go 1.19 or later
- RabbitMQ 3.8 or later

## Testing

The library includes comprehensive unit and integration tests with 200+ tests covering all features.

### Quick Start - All Tests

Use the provided script to run all tests:

```bash
# Automatically starts RabbitMQ if needed and runs all tests
./run-tests.sh
```

### Manual Testing

#### Unit Tests Only

```bash
# Run unit tests (no RabbitMQ required)
go test ./rabbitmq/... -v -short
```

#### Integration Tests

Integration tests require a running RabbitMQ instance. Use the provided script:

```bash
# Start RabbitMQ in Docker
./start-rabbitmq.sh

# Run all tests (unit + integration)
go test ./rabbitmq/... -v
go test ./tests/integration/... -v
```

Or start RabbitMQ manually:

```bash
# Start RabbitMQ with management UI
docker run -d --name rabbitmq-test \
  -p 5672:5672 \
  -p 15672:15672 \
  -e RABBITMQ_DEFAULT_USER=guest \
  -e RABBITMQ_DEFAULT_PASS=guest \
  rabbitmq:3-management

# Wait for RabbitMQ to start (about 10-15 seconds)
# Then run tests
go test ./rabbitmq/... -v
go test ./tests/integration/... -v

# Stop RabbitMQ when done
docker stop rabbitmq-test
docker rm rabbitmq-test
```

#### Benchmarks

```bash
# Run performance benchmarks
go test -bench=. -benchmem ./rabbitmq/
```

#### Test Coverage

```bash
# Generate coverage report
go test ./rabbitmq/... -coverprofile=coverage.out
go tool cover -html=coverage.out -o coverage.html
```

### Test Organization

- **Unit Tests** (`rabbitmq/*_test.go`): Fast tests that don't require RabbitMQ
  - Properties encoding/decoding
  - Metrics collection
  - Error handling
  - URI parsing
  - Configuration validation

- **Integration Tests** (marked with `t.Skip("Requires RabbitMQ")`): Tests requiring a live RabbitMQ broker
  - Connection lifecycle
  - Channel operations
  - Publishing/consuming
  - Publisher confirms
  - Transactions
  - Topology management
  - Automatic recovery
  - Consumer patterns
  - Dead letter exchanges
  - Priority queues
  - TTL features

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

MIT License

## Acknowledgments

This library is inspired by and maintains API parity with the official [RabbitMQ Java Client](https://github.com/rabbitmq/rabbitmq-java-client).
