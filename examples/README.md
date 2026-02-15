# RabbitMQ Go Client - Examples

This directory contains example applications demonstrating how to use the RabbitMQ Go client library.

## Prerequisites

### 1. Start RabbitMQ Server

You need a running RabbitMQ instance. The easiest way is using Docker:

```bash
# Start RabbitMQ with management plugin
docker run -d --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management

# Check that it's running
docker ps | grep rabbitmq

# Access management UI (optional)
# Open http://localhost:15672 in your browser
# Login: guest/guest
```

### 2. Verify Connection

```bash
# Test connection with telnet
telnet localhost 5672

# Or check with curl
curl -u guest:guest http://localhost:15672/api/overview
```

## Available Examples

### 1. Basic Publish/Consume

**Publisher** - Sends a simple message to a queue:
```bash
cd examples/basic_publish
go run main.go
```

**Consumer** - Receives and processes messages:
```bash
cd examples/basic_consume
go run main.go
```

**Demo**:
```bash
# Terminal 1: Start consumer
cd examples/basic_consume && go run main.go

# Terminal 2: Send messages
cd examples/basic_publish && go run main.go
```

---

### 2. Publisher Confirms

Demonstrates reliable publishing with publisher confirms:

```bash
cd examples/publisher_confirms
go run main.go
```

This example shows:
- Enabling publisher confirms
- Tracking message confirmations
- Handling acks and nacks
- Reliable publishing patterns

---

### 3. Work Queue Pattern

Distributes time-consuming tasks among multiple workers.

**Send tasks**:
```bash
cd examples/work_queue
go run new_task.go "First task"
go run new_task.go "Second task"
go run new_task.go "Third task"
```

**Start workers** (in separate terminals):
```bash
# Terminal 1: Worker 1
cd examples/work_queue && go run worker.go

# Terminal 2: Worker 2
cd examples/work_queue && go run worker.go

# Terminal 3: Worker 3
cd examples/work_queue && go run worker.go
```

This demonstrates:
- Round-robin message distribution
- Manual message acknowledgments
- Fair dispatch with QoS/prefetch
- Task durability

---

### 4. RPC (Remote Procedure Call)

Request/reply pattern for synchronous RPC calls.

**Start RPC server**:
```bash
cd examples/rpc_server
go run main.go
```

**Make RPC calls**:
```bash
cd examples/rpc_client
go run main.go 5
```

This demonstrates:
- Request/reply pattern
- Correlation IDs
- Exclusive reply queues
- Synchronous RPC with timeout

---

### 5. Publish/Subscribe with Topics

Topic-based routing for selective message consumption.

**Start consumers** (in separate terminals):
```bash
# Terminal 1: Receive all logs
cd examples/topics && go run receive_logs_topic.go "#"

# Terminal 2: Receive only error logs
cd examples/topics && go run receive_logs_topic.go "*.error"

# Terminal 3: Receive logs from auth service
cd examples/topics && go run receive_logs_topic.go "auth.*"
```

**Publish logs**:
```bash
cd examples/topics

go run emit_log_topic.go "auth.info" "User logged in"
go run emit_log_topic.go "auth.error" "Authentication failed"
go run emit_log_topic.go "payment.info" "Payment processed"
go run emit_log_topic.go "payment.error" "Payment declined"
```

This demonstrates:
- Topic exchanges
- Routing key patterns
- Wildcard matching (* and #)
- Selective message routing

---

### 6. Connection Recovery

Demonstrates building resilient applications with automatic reconnection:

```bash
cd examples/recovery
go run main.go
```

Then test recovery:
```bash
# While the example is running, restart RabbitMQ
docker restart rabbitmq

# Watch the example automatically reconnect after 5 seconds
```

This demonstrates:
- Connection monitoring with NotifyClose()
- Automatic reconnection loop
- Graceful error handling
- State preservation across reconnects
- Message counter that persists
- Publisher and consumer recovery

**Note**: This uses a manual reconnection pattern (not built-in automatic recovery) which gives you more control over the recovery process and is a production-ready approach.

---

## Common Issues & Solutions

### Connection Refused

**Error**: `Failed to connect: dial tcp [::1]:5672: connect: connection refused`

**Solution**: Make sure RabbitMQ is running:
```bash
docker ps | grep rabbitmq
# If not running:
docker start rabbitmq
```

### Permission Denied

**Error**: `Failed to connect: access refused`

**Solution**: Check credentials in the code (default is guest/guest)

### Port Already in Use

**Error**: `docker: Error response from daemon: port is already allocated`

**Solution**: Stop existing RabbitMQ instance:
```bash
docker stop rabbitmq
docker rm rabbitmq
# Then start again
```

### Module Not Found

**Error**: `cannot find module providing package github.com/israelio/rabbit-go-client`

**Solution**: Run from the project root or install the module:
```bash
# Option 1: Run from root directory
cd /Users/ohadisraeli/Workspace/rabbit-go-client
go run ./examples/basic_publish/main.go

# Option 2: Install module
go mod download
```

---

## Running All Examples (Demo Script)

Here's a script to quickly demo all examples:

```bash
#!/bin/bash

# Start RabbitMQ
echo "Starting RabbitMQ..."
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
sleep 10

# Basic Publish/Consume
echo -e "\n=== Basic Publish/Consume ==="
cd examples/basic_consume && go run main.go &
CONSUMER_PID=$!
sleep 2
cd ../basic_publish && go run main.go
sleep 2
kill $CONSUMER_PID

# Publisher Confirms
echo -e "\n=== Publisher Confirms ==="
cd ../publisher_confirms && go run main.go

# Work Queue
echo -e "\n=== Work Queue ==="
cd ../work_queue && go run worker.go &
WORKER_PID=$!
sleep 2
go run new_task.go "Task 1"
go run new_task.go "Task 2"
go run new_task.go "Task 3"
sleep 5
kill $WORKER_PID

# RPC
echo -e "\n=== RPC ==="
cd ../rpc_server && go run main.go &
RPC_SERVER_PID=$!
sleep 2
cd ../rpc_client && go run main.go 5
sleep 2
kill $RPC_SERVER_PID

# Topics
echo -e "\n=== Topics ==="
cd ../topics && go run receive_logs_topic.go "#" &
TOPIC_PID=$!
sleep 2
go run emit_log_topic.go "auth.info" "User logged in"
go run emit_log_topic.go "auth.error" "Auth failed"
sleep 2
kill $TOPIC_PID

# Cleanup
echo -e "\n=== Cleanup ==="
docker stop rabbitmq
docker rm rabbitmq

echo "Demo complete!"
```

---

## Next Steps

1. **Modify Examples**: Try changing queue names, routing keys, or message content
2. **Add Error Handling**: Enhance examples with retry logic and better error handling
3. **Monitor Messages**: Use the RabbitMQ Management UI at http://localhost:15672
4. **Scale Workers**: Run multiple instances of workers to see load distribution
5. **Test Recovery**: Kill RabbitMQ while examples are running to test recovery

---

## Example Output

### Basic Publish
```
2026/02/15 20:00:00 Connected to RabbitMQ
2026/02/15 20:00:00 Declared queue: hello
2026/02/15 20:00:00 Published message: Hello, RabbitMQ!
```

### Basic Consume
```
2026/02/15 20:00:00 Connected to RabbitMQ
2026/02/15 20:00:00 Declared queue: hello
2026/02/15 20:00:00 Waiting for messages...
2026/02/15 20:00:05 Received: Hello, RabbitMQ!
```

### RPC Client
```
2026/02/15 20:00:00 Connected to RabbitMQ
2026/02/15 20:00:00 Requesting fib(5)
2026/02/15 20:00:00 Got response: 5
```

---

## Additional Resources

- **RabbitMQ Tutorials**: https://www.rabbitmq.com/tutorials
- **Go Client Documentation**: See main README.md
- **AMQP 0-9-1 Spec**: https://www.rabbitmq.com/amqp-0-9-1-reference.html
- **RabbitMQ Best Practices**: https://www.rabbitmq.com/best-practices.html

---

## Contributing

Found an issue or want to add a new example? Please submit a pull request or open an issue!
