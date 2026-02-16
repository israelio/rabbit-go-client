# Recovery Examples

This directory contains comprehensive examples demonstrating all automatic recovery scenarios for the RabbitMQ Go client.

## Overview

The RabbitMQ Go client supports **automatic connection recovery** that transparently handles network failures, broker restarts, and connection issues. These examples show you how to use this feature effectively.

## Examples

### 1. Basic Recovery (`recovery/main.go`)

**Scenario**: Simple automatic recovery with callback-based consumer

**Demonstrates**:
- Enabling automatic recovery
- Recovery notifications (started, completed, failed)
- Callback-based consumer (auto-recovered)
- Handling publish failures during recovery

**Key Takeaways**:
- Use `WithAutomaticRecovery(true)` to enable
- Callback-based consumers are automatically re-established
- Recovery notifications let you monitor the process

**Run**:
```bash
go run examples/recovery/main.go
```

**Test Recovery**:
```bash
docker restart rabbitmq
```

---

### 2. Complete Recovery (`recovery-complete/main.go`)

**Scenario**: Production-ready application with ALL recovery features

**Demonstrates**:
- ✅ Multiple channels with different purposes
- ✅ QoS settings preservation across recovery
- ✅ Publisher confirms restoration
- ✅ Topology recovery (exchanges, queues, bindings)
- ✅ Consumer recovery with state tracking
- ✅ Custom recovery handlers
- ✅ Recovery monitoring
- ✅ Graceful error handling during recovery
- ✅ Application state preservation

**Architecture**:
```
Application
├── Publisher Channel (ID: 1, QoS: 10)
├── Consumer Channel (ID: 2, QoS: 5)
├── Confirms Channel (ID: 3, confirms enabled)
├── Recovery Monitor (tracks events)
└── Custom Recovery Handler (logging)
```

**Key Features**:
- All channel IDs are preserved after recovery
- QoS settings automatically restored
- Consumers continue from where they left off
- Publisher confirms mode re-enabled automatically

**Run**:
```bash
go run examples/recovery-complete/main.go
```

**What to Watch**:
1. Initial setup creates 3 channels with different configurations
2. Publisher and consumer operate normally
3. Restart RabbitMQ: `docker restart rabbitmq`
4. Recovery monitor shows detailed recovery process
5. All channels restored with same IDs and settings
6. Operations resume automatically

---

### 3. Multi-Channel Recovery (`recovery-multi-channel/main.go`)

**Scenario**: Application using many channels with different configurations

**Demonstrates**:
- 5 channels with different purposes and QoS settings
- Channel ID preservation during recovery
- Parallel recovery of all channels
- Verification that settings are maintained

**Channel Configuration**:
```
Channel 0: High-priority publisher (QoS: 1)
Channel 1: Bulk publisher (QoS: 100)
Channel 2: Fast consumer (QoS: 10)
Channel 3: Slow consumer (QoS: 1)
Channel 4: Confirms channel (QoS: 50, confirms enabled)
```

**Key Insight**:
Each channel maintains its specific configuration after recovery. This is critical for applications with specialized channels.

**Run**:
```bash
go run examples/recovery-multi-channel/main.go
```

**Verification**:
After recovery, check that:
- All 5 channels still have their original IDs
- QoS settings remain unchanged
- Operations continue without reconfiguration

---

### 4. Failure Handling (`recovery-failure-handling/main.go`)

**Scenario**: Gracefully handling recovery failures and connection loss

**Demonstrates**:
- ✅ Detecting when connection is recovering
- ✅ Handling `ErrRecovering` during operations
- ✅ Circuit breaker pattern for resilience
- ✅ Graceful degradation during connection issues
- ✅ Recovery exhaustion handling (when all retries fail)
- ✅ Checking connection state before operations

**Circuit Breaker Flow**:
```
Closed (normal) → Failures accumulate → Open (blocking)
       ↑                                      ↓
       └────────── Timeout expires ──────────┘
```

**Key Patterns**:

1. **Check connection state**:
```go
if conn.GetState() == rabbitmq.StateRecovering {
    log.Println("Connection recovering, skipping operation")
    return
}
```

2. **Handle ErrRecovering**:
```go
err := ch.Publish(...)
if err == rabbitmq.ErrRecovering {
    // Connection is recovering, retry later
}
```

3. **Circuit breaker**:
```go
if !circuitBreaker.AllowRequest() {
    // Too many failures, stop trying
    return
}
```

**Run**:
```bash
go run examples/recovery-failure-handling/main.go
```

**Test Scenarios**:
1. **Normal recovery**: `docker restart rabbitmq` (recovers in ~5-10s)
2. **Extended outage**: `docker stop rabbitmq` (exhausts retries after ~20s)

**Expected Behavior**:
- During recovery: Operations fail gracefully, circuit breaker may open
- After recovery: Circuit breaker resets, operations resume
- If exhausted: Connection remains closed, application handles gracefully

---

### 5. Comprehensive Monitoring (`recovery-monitoring/main.go`)

**Scenario**: Monitor recovery through all available notification mechanisms

**Demonstrates**:
- ✅ `NotifyRecoveryStarted` channel
- ✅ `NotifyRecoveryCompleted` channel
- ✅ `NotifyRecoveryFailed` channel
- ✅ `RecoveryHandler` interface
- ✅ `ConnectionListener` interface
- ✅ Connection state monitoring
- ✅ Custom logging integration

**All Notification Mechanisms**:

1. **Notification Channels** (Simple):
```go
started := make(chan struct{})
conn.NotifyRecoveryStarted(started)

<-started // Notified when recovery begins
```

2. **RecoveryHandler Interface** (Structured):
```go
type MyHandler struct{}

func (h *MyHandler) OnRecoveryStarted(conn *Connection)
func (h *MyHandler) OnRecoveryCompleted(conn *Connection)
func (h *MyHandler) OnRecoveryFailed(conn *Connection, err error)
func (h *MyHandler) OnTopologyRecoveryStarted(conn *Connection)
func (h *MyHandler) OnTopologyRecoveryCompleted(conn *Connection)
```

3. **ConnectionListener Interface** (Lifecycle):
```go
type MyListener struct{}

func (l *MyListener) OnConnectionRecoveryStarted(conn *Connection)
func (l *MyListener) OnConnectionRecoveryCompleted(conn *Connection)
// + other lifecycle methods
```

**Run**:
```bash
go run examples/recovery-monitoring/main.go
```

**What You'll See**:
When you restart RabbitMQ, all notification mechanisms fire in sequence:
```
1. NotifyRecoveryStarted → channel receives signal
2. RecoveryHandler.OnRecoveryStarted → called
3. ConnectionListener.OnConnectionRecoveryStarted → called
4. [multiple NotifyRecoveryFailed as retries happen]
5. RecoveryHandler.OnTopologyRecoveryStarted → called
6. RecoveryHandler.OnTopologyRecoveryCompleted → called
7. NotifyRecoveryCompleted → channel receives signal
8. RecoveryHandler.OnRecoveryCompleted → called
9. ConnectionListener.OnConnectionRecoveryCompleted → called
```

---

## Recovery Scenarios Covered

### ✅ Scenario 1: Network Failure
**Example**: `recovery/main.go`, `recovery-complete/main.go`

Simulates network cable unplugged or network partition.

**What happens**:
1. Frame read fails with I/O error
2. Recovery monitor detects error
3. Attempts reconnection with exponential backoff
4. Restores topology and channels
5. Application continues

### ✅ Scenario 2: Broker Restart
**Example**: All examples

Simulates `docker restart rabbitmq` or broker maintenance.

**What happens**:
1. Connection.Close received from server
2. Recovery initiates immediately
3. Reconnects once broker is available
4. Restores everything automatically
5. ~5-10 second downtime typically

### ✅ Scenario 3: Heartbeat Timeout
**Example**: `recovery-complete/main.go`

Simulates connection silently dying without notification.

**What happens**:
1. No frames received for 2x heartbeat interval
2. Heartbeat monitor triggers recovery
3. Recovery proceeds as normal

### ✅ Scenario 4: Multiple Channels
**Example**: `recovery-multi-channel/main.go`

Tests recovery with many channels having different settings.

**Verified**:
- All channel IDs preserved
- All QoS settings restored
- All confirms modes re-enabled
- All consumers re-established

### ✅ Scenario 5: Topology Restoration
**Example**: `recovery-complete/main.go`

Ensures exchanges, queues, and bindings are recreated.

**Verified**:
- Exchanges re-declared with same settings
- Queues re-declared with same settings
- Bindings re-created
- Messages continue routing correctly

### ✅ Scenario 6: Consumer Recovery
**Example**: `recovery/main.go`, `recovery-complete/main.go`

Callback-based consumers are automatically re-established.

**Verified**:
- Consumer re-registered with same tag
- `HandleRecoverOk()` called on consumer
- Message delivery resumes
- Consumer state (like message count) preserved

### ✅ Scenario 7: QoS Preservation
**Example**: `recovery-multi-channel/main.go`

Prefetch count and size are restored.

**Verified**:
- Prefetch count restored per channel
- Global vs. per-channel setting preserved

### ✅ Scenario 8: Publisher Confirms
**Example**: `recovery-complete/main.go`

Confirms mode is re-enabled automatically.

**Verified**:
- `ConfirmSelect` called automatically
- Confirm notifications resume
- No manual re-configuration needed

### ✅ Scenario 9: Recovery Failure
**Example**: `recovery-failure-handling/main.go`

Handles when broker stays down or recovery exhausts retries.

**Verified**:
- `NotifyRecoveryFailed` called for each attempt
- Circuit breaker pattern works
- Connection eventually closes if exhausted
- Application handles gracefully

### ✅ Scenario 10: Operations During Recovery
**Example**: `recovery-failure-handling/main.go`

Tests behavior when operations are attempted during recovery.

**Verified**:
- `NewChannel()` returns `ErrRecovering`
- `Publish()` returns errors
- State can be checked with `GetState()`
- Operations resume after recovery

---

## Configuration Reference

### Enabling Recovery

```go
factory := rabbitmq.NewConnectionFactory(
    // Enable automatic recovery
    rabbitmq.WithAutomaticRecovery(true),

    // Enable topology recovery
    rabbitmq.WithTopologyRecovery(true),

    // Set retry interval (default: 5s)
    rabbitmq.WithRecoveryInterval(5*time.Second),

    // Set max retry attempts (default: 3, 0=infinite)
    rabbitmq.WithConnectionRetryAttempts(10),
)
```

### Monitoring Recovery

```go
// Method 1: Notification channels
started := make(chan struct{})
completed := make(chan struct{})
failed := make(chan error, 10)

conn.NotifyRecoveryStarted(started)
conn.NotifyRecoveryCompleted(completed)
conn.NotifyRecoveryFailed(failed)

// Method 2: Custom handler
type MyHandler struct{}
// Implement RecoveryHandler interface

factory := rabbitmq.NewConnectionFactory(
    rabbitmq.WithRecoveryHandler(&MyHandler{}),
)

// Method 3: Connection listener
listener := &MyListener{} // Implements ConnectionListener
conn.AddConnectionListener(listener)
```

### Checking Connection State

```go
state := conn.GetState()

switch state {
case rabbitmq.StateOpen:
    // Normal operation
case rabbitmq.StateRecovering:
    // Recovery in progress
case rabbitmq.StateClosed:
    // Connection closed
}
```

---

## Best Practices

### ✅ DO:

1. **Use callback-based consumers** for automatic recovery:
```go
ch.ConsumeWithCallback(queue, tag, opts, consumer)
```

2. **Check connection state** before critical operations:
```go
if conn.GetState() != rabbitmq.StateOpen {
    return errors.New("connection not ready")
}
```

3. **Handle errors gracefully** during recovery:
```go
err := ch.Publish(...)
if err == rabbitmq.ErrRecovering {
    // Queue operation for retry
}
```

4. **Monitor recovery events** in production:
```go
conn.NotifyRecoveryStarted(started)
// Alert monitoring system
```

5. **Use circuit breakers** for resilience (see `recovery-failure-handling`)

### ❌ DON'T:

1. **Don't use channel-based consumers** if you need auto-recovery:
```go
// This WON'T auto-recover:
deliveries, _ := ch.Consume(...)
for d := range deliveries { ... } // Channel closes on failure
```

2. **Don't assume operations succeed during recovery**:
```go
// BAD:
ch.Publish(...) // Might fail during recovery

// GOOD:
if err := ch.Publish(...); err != nil {
    if err == rabbitmq.ErrRecovering {
        // Handle recovery case
    }
}
```

3. **Don't create channels during recovery**:
```go
// This returns ErrRecovering:
ch, err := conn.NewChannel()
```

---

## Testing Recovery

### Manual Testing

1. Start example:
```bash
go run examples/recovery-complete/main.go
```

2. Restart RabbitMQ:
```bash
docker restart rabbitmq
```

3. Observe:
- Recovery notifications
- Operations pause briefly
- Automatic resumption
- No manual intervention needed

### Automated Testing

See `tests/integration/recovery_test.go` for automated tests.

To automate further, use RabbitMQ Management API:
```bash
# Get connections
curl -u guest:guest http://localhost:15672/api/connections

# Force close connection (triggers recovery)
curl -u guest:guest -X DELETE \
  http://localhost:15672/api/connections/{connection-name}
```

---

## Troubleshooting

### Recovery Not Working?

**Check**:
1. Is `AutomaticRecovery` enabled?
```go
factory.AutomaticRecovery = true
```

2. Is error recoverable?
```go
// Only errors with Recover=true trigger recovery
err.Recover == true
```

3. Are retries exhausted?
```go
// Increase retry attempts
rabbitmq.WithConnectionRetryAttempts(20)
```

### Consumers Not Recovering?

**Check**:
1. Using callback-based consumer?
```go
// ✅ This recovers:
ch.ConsumeWithCallback(queue, tag, opts, consumer)

// ❌ This doesn't:
ch.Consume(queue, tag, opts)
```

2. Implement `HandleRecoverOk`:
```go
func (c *MyConsumer) HandleRecoverOk(tag string) {
    log.Println("Consumer recovered!")
}
```

### Topology Not Recovering?

**Check**:
1. Is `TopologyRecovery` enabled?
```go
factory.TopologyRecovery = true
```

2. Topology declared through library?
```go
// ✅ Recorded and recovered:
ch.ExchangeDeclare(...)
ch.QueueDeclare(...)
ch.QueueBind(...)

// ❌ NOT recovered:
// Topology created externally or before recovery was enabled
```

---

## Summary

All recovery scenarios are fully implemented and demonstrated:

| Scenario | Example | Status |
|----------|---------|--------|
| Basic recovery | `recovery/` | ✅ |
| Complete recovery | `recovery-complete/` | ✅ |
| Multi-channel | `recovery-multi-channel/` | ✅ |
| Failure handling | `recovery-failure-handling/` | ✅ |
| Monitoring | `recovery-monitoring/` | ✅ |
| Topology recovery | All examples | ✅ |
| Consumer recovery | `recovery/`, `recovery-complete/` | ✅ |
| QoS preservation | `recovery-multi-channel/` | ✅ |
| Confirms restoration | `recovery-complete/` | ✅ |
| State checking | `recovery-failure-handling/` | ✅ |

**The automatic recovery system is production-ready!** 🚀
