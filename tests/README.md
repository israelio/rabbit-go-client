# RabbitMQ Go Client - Test Suite

This directory contains comprehensive tests for the RabbitMQ Go client, inspired by the official [RabbitMQ Java Client](https://github.com/rabbitmq/rabbitmq-java-client) test suite.

## Test Structure

```
tests/
├── integration/          # Integration tests (require live RabbitMQ)
│   ├── test_helper.go   # Test utilities and helpers
│   ├── connection_test.go
│   ├── publish_consume_test.go
│   ├── topology_test.go
│   └── confirms_test.go
└── README.md            # This file

internal/
├── protocol/
│   └── types_test.go    # Unit tests for AMQP type encoding
└── frame/
    └── frame_test.go    # Unit tests for frame handling

rabbitmq/
└── properties_test.go   # Unit tests for message properties
```

## Test Categories

### Unit Tests

Unit tests do not require a running RabbitMQ instance and test individual components in isolation:

- **Protocol Layer Tests** (`internal/protocol/types_test.go`)
  - AMQP table encoding/decoding
  - Short string and long string operations
  - Field value encoding for all AMQP types
  - Benchmarks for encoding/decoding performance

- **Frame Tests** (`internal/frame/frame_test.go`)
  - Frame creation (method, header, body, heartbeat)
  - Frame parsing and validation
  - Method argument building and parsing
  - Benchmarks for frame operations

- **Properties Tests** (`rabbitmq/properties_test.go`)
  - Message properties encoding/decoding
  - Predefined property constants
  - Complex header values
  - Round-trip encoding/decoding
  - Benchmarks for properties operations

### Integration Tests

Integration tests require a running RabbitMQ instance and test complete workflows:

- **Connection Tests** (`integration/connection_test.go`)
  - Connection lifecycle (open/close)
  - Parameter negotiation (channel max, frame max, heartbeat)
  - Multiple channel creation
  - Connection close notifications
  - Heartbeat mechanism
  - Blocked connection handling

- **Publish/Consume Tests** (`integration/publish_consume_test.go`)
  - Basic publish and consume
  - Synchronous message retrieval (BasicGet)
  - Manual acknowledgment (Ack, Nack, Reject)
  - Quality of Service (QoS/prefetch)
  - Multiple consumers on same queue
  - Message properties verification

- **Topology Tests** (`integration/topology_test.go`)
  - Queue operations (declare, delete, purge)
  - Auto-delete and exclusive queues
  - Exchange operations (declare, delete, bind)
  - All exchange types (direct, fanout, topic, headers)
  - Queue-to-exchange bindings
  - Topic pattern matching
  - Fanout broadcast
  - Exchange-to-exchange bindings

- **Publisher Confirms & Transactions** (`integration/confirms_test.go`)
  - Basic publisher confirms
  - Synchronous confirmation (WaitForConfirms)
  - Per-message confirmation (PublishWithConfirm)
  - Concurrent publishing with confirms
  - Return handling for unroutable messages
  - AMQP transactions (commit/rollback)

## Running Tests

### Prerequisites

For **unit tests only**:
- Go 1.19 or later

For **integration tests**:
- Go 1.19 or later
- Running RabbitMQ instance (version 3.8+)

### Quick Start

#### 1. Start RabbitMQ (for integration tests)

Using Docker:
```bash
docker run -d --name rabbitmq-test \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management
```

Or use your existing RabbitMQ installation.

#### 2. Run All Unit Tests

```bash
# Run all unit tests (no RabbitMQ required)
go test ./internal/... ./rabbitmq/... -v

# Run with coverage
go test ./internal/... ./rabbitmq/... -cover

# Run specific test
go test ./internal/protocol -run TestTableEncoding -v
```

#### 3. Run Integration Tests

```bash
# Run all integration tests (requires RabbitMQ)
go test ./tests/integration/... -v

# Run specific test file
go test ./tests/integration -run TestBasicPublishConsume -v

# Run with timeout (some tests may take time)
go test ./tests/integration/... -v -timeout 30s
```

### Environment Configuration

Integration tests use environment variables for RabbitMQ connection:

```bash
export RABBITMQ_HOST=localhost      # Default: localhost
export RABBITMQ_PORT=5672           # Default: 5672
export RABBITMQ_USER=guest          # Default: guest
export RABBITMQ_PASS=guest          # Default: guest
export RABBITMQ_VHOST=/             # Default: /
```

Example with custom settings:
```bash
RABBITMQ_HOST=rabbitmq.example.com \
RABBITMQ_USER=testuser \
RABBITMQ_PASS=testpass \
go test ./tests/integration/... -v
```

### Run Benchmarks

```bash
# Run all benchmarks
go test ./internal/... ./rabbitmq/... -bench=. -benchmem

# Run specific benchmark
go test ./internal/protocol -bench=BenchmarkTableEncoding -benchmem

# Run benchmarks with profiling
go test ./internal/protocol -bench=. -cpuprofile=cpu.prof -memprofile=mem.prof
```

### Test Coverage

```bash
# Generate coverage report
go test ./... -coverprofile=coverage.out

# View coverage in browser
go tool cover -html=coverage.out

# Get coverage percentage
go test ./... -cover | grep coverage
```

### Running Specific Test Suites

```bash
# Only unit tests (fast, no RabbitMQ needed)
go test ./internal/... ./rabbitmq/... -v -short

# Only connection tests
go test ./tests/integration -run TestConnection -v

# Only topology tests
go test ./tests/integration -run TestTopology -v

# Only publisher confirms tests
go test ./tests/integration -run TestPublisher -v
```

## Test Output

### Successful Test Run

```
=== RUN   TestBasicPublishConsume
--- PASS: TestBasicPublishConsume (0.05s)
=== RUN   TestManualAcknowledgment
--- PASS: TestManualAcknowledgment (0.03s)
PASS
ok      github.com/israelio/rabbit-go-client/tests/integration     0.234s
```

### Skipped Tests (No RabbitMQ)

```
=== RUN   TestBasicPublishConsume
    connection_test.go:25: Skipping test: cannot connect to RabbitMQ at localhost:5672: dial tcp [::1]:5672: connect: connection refused
--- SKIP: TestBasicPublishConsume (0.00s)
```

Tests will automatically skip if RabbitMQ is not available.

## Continuous Integration

### GitHub Actions Example

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest

    services:
      rabbitmq:
        image: rabbitmq:3-management
        ports:
          - 5672:5672
        options: >-
          --health-cmd "rabbitmq-diagnostics -q ping"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5

    steps:
      - uses: actions/checkout@v3

      - name: Set up Go
        uses: actions/setup-go@v4
        with:
          go-version: '1.19'

      - name: Run unit tests
        run: go test ./internal/... ./rabbitmq/... -v -cover

      - name: Run integration tests
        run: go test ./tests/integration/... -v
        env:
          RABBITMQ_HOST: localhost
          RABBITMQ_PORT: 5672
```

## Test Guidelines

### Writing New Tests

1. **Use descriptive test names**: `TestBasicPublishConsume`, not `TestPublish`
2. **Use subtests for variations**: `t.Run("empty table", func(t *testing.T) { ... })`
3. **Clean up resources**: Always defer cleanup (queue/exchange deletion)
4. **Generate unique names**: Use `GenerateQueueName(t)` to avoid conflicts
5. **Set timeouts**: Use `time.After()` for async operations
6. **Skip when appropriate**: Call `RequireRabbitMQ(t)` at the start of integration tests

### Example Integration Test

```go
func TestMyFeature(t *testing.T) {
    RequireRabbitMQ(t)  // Skip if RabbitMQ unavailable

    conn, ch := NewTestChannel(t)
    defer conn.Close()
    defer ch.Close()

    queueName := GenerateQueueName(t)
    defer CleanupQueue(t, ch, queueName)

    // Test implementation...

    // Use timeouts for async operations
    select {
    case result := <-resultChan:
        // Verify result
    case <-time.After(5 * time.Second):
        t.Fatal("Timeout waiting for result")
    }
}
```

### Example Unit Test

```go
func TestEncoding(t *testing.T) {
    tests := []struct {
        name  string
        input interface{}
        want  []byte
    }{
        {"empty", nil, []byte{}},
        {"string", "test", []byte{...}},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            got := encode(tt.input)
            if !bytes.Equal(got, tt.want) {
                t.Errorf("encode() = %v, want %v", got, tt.want)
            }
        })
    }
}
```

## Test Coverage Goals

- **Protocol Layer**: 90%+ coverage
- **Core Components**: 80%+ coverage
- **Integration Tests**: All major workflows covered

Current coverage can be checked with:
```bash
go test ./... -cover
```

## Troubleshooting

### Tests Hang

- Check if RabbitMQ is running: `docker ps` or `rabbitmqctl status`
- Verify network connectivity: `telnet localhost 5672`
- Increase test timeout: `go test -timeout 60s`

### Connection Refused

- Ensure RabbitMQ is running on correct port
- Check firewall rules
- Verify credentials with: `rabbitmqctl authenticate_user guest guest`

### Random Test Failures

- Run with `-count=10` to detect flaky tests: `go test ./tests/integration/... -count=10`
- Check for resource leaks (unclosed connections/channels)
- Ensure proper cleanup in defer statements

### Memory Issues

- Run with race detector: `go test -race ./...`
- Profile memory: `go test -memprofile=mem.prof`
- Check for goroutine leaks: `go test -trace=trace.out`

## Comparison with Java Client Tests

This test suite mirrors the structure and coverage of the RabbitMQ Java client tests:

| Java Test Suite | Go Equivalent | Coverage |
|----------------|---------------|----------|
| Unit Tests (53 files) | `internal/*_test.go`, `rabbitmq/*_test.go` | Core functionality |
| Functional Tests (66 files) | `tests/integration/*_test.go` | All major features |
| Server Tests (21 files) | Manual tests with notes | High-availability scenarios |
| SSL Tests (9 files) | Planned | TLS/SSL connections |

## Contributing

When adding new features to the library:

1. Add unit tests for isolated components
2. Add integration tests for end-to-end workflows
3. Update test coverage report
4. Document any special test setup requirements

## License

Tests are part of the RabbitMQ Go Client and follow the same license.
