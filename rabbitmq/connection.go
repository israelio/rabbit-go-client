package rabbitmq

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/israelio/rabbit-go-client/internal/frame"
	"github.com/israelio/rabbit-go-client/internal/protocol"
)

// ConnectionState represents the current state of a connection
type ConnectionState int32

const (
	StateConnecting ConnectionState = iota
	StateOpen
	StateClosing
	StateClosed
	StateRecovering
)

// String returns a string representation of the connection state
func (cs ConnectionState) String() string {
	switch cs {
	case StateConnecting:
		return "connecting"
	case StateOpen:
		return "open"
	case StateClosing:
		return "closing"
	case StateClosed:
		return "closed"
	case StateRecovering:
		return "recovering"
	default:
		return "unknown"
	}
}

// Connection represents an AMQP connection
type Connection struct {
	factory *ConnectionFactory
	conn    net.Conn

	// Frame I/O
	frameReader *frame.Reader
	frameWriter *frame.Writer

	// Channels
	channelMux    sync.RWMutex
	channels      map[uint16]*Channel
	nextChannelID uint16

	// Connection parameters (negotiated)
	channelMax uint16
	frameMax   uint32
	heartbeat  time.Duration

	// State
	state     atomic.Int32
	closeOnce sync.Once
	closeChan chan *Error
	closed    chan struct{}

	// Blocked notifications
	blockedChan chan BlockedNotification
	blocked     atomic.Bool

	// Heartbeat
	lastActivity    atomic.Int64 // Unix timestamp
	heartbeatStop   chan struct{}
	heartbeatDone   chan struct{}

	// Frame dispatch
	dispatchStop chan struct{}
	dispatchDone chan struct{}

	// Recovery
	recovery *recoveryManager

	// Recovery notification channels
	recoveryStartedChans   []chan struct{}
	recoveryCompletedChans []chan struct{}
	recoveryFailedChans    []chan error
	notificationMux        sync.RWMutex

	// Recovery control
	recoveryStop chan struct{}
	recoveryDone chan struct{}

	// Listeners
	listenerMux sync.RWMutex
	listeners   []ConnectionListener
}

// BlockedNotification represents a connection blocked/unblocked event
type BlockedNotification struct {
	Blocked bool
	Reason  string
}

// ConnectionListener receives connection lifecycle events
type ConnectionListener interface {
	OnConnectionCreated(conn *Connection)
	OnConnectionClosed(conn *Connection, err error)
	OnConnectionRecoveryStarted(conn *Connection)
	OnConnectionRecoveryCompleted(conn *Connection)
	OnConnectionBlocked(conn *Connection, reason string)
	OnConnectionUnblocked(conn *Connection)
}

// handshake performs the AMQP connection handshake
func (c *Connection) handshake(ctx context.Context) error {
	c.frameReader = frame.NewReader(c.conn, protocol.FrameMinSize)
	c.frameWriter = frame.NewWriter(c.conn, protocol.FrameMinSize)

	// Send protocol header
	if err := c.frameWriter.WriteProtocolHeader(); err != nil {
		return fmt.Errorf("write protocol header: %w", err)
	}

	// Wait for Connection.Start
	startFrame, err := c.frameReader.ReadFrame()
	if err != nil {
		return fmt.Errorf("read start frame: %w", err)
	}

	if err := c.handleConnectionStart(startFrame); err != nil {
		return fmt.Errorf("handle start: %w", err)
	}

	// Send Connection.StartOk
	if err := c.sendConnectionStartOk(); err != nil {
		return fmt.Errorf("send start-ok: %w", err)
	}

	// Wait for Connection.Tune
	tuneFrame, err := c.frameReader.ReadFrame()
	if err != nil {
		return fmt.Errorf("read tune frame: %w", err)
	}

	if err := c.handleConnectionTune(tuneFrame); err != nil {
		return fmt.Errorf("handle tune: %w", err)
	}

	// Send Connection.TuneOk
	if err := c.sendConnectionTuneOk(); err != nil {
		return fmt.Errorf("send tune-ok: %w", err)
	}

	// Send Connection.Open
	if err := c.sendConnectionOpen(); err != nil {
		return fmt.Errorf("send open: %w", err)
	}

	// Wait for Connection.OpenOk
	openOkFrame, err := c.frameReader.ReadFrame()
	if err != nil {
		return fmt.Errorf("read open-ok frame: %w", err)
	}

	if err := c.handleConnectionOpenOk(openOkFrame); err != nil {
		return fmt.Errorf("handle open-ok: %w", err)
	}

	return nil
}

// handleConnectionStart processes Connection.Start method
func (c *Connection) handleConnectionStart(f *frame.Frame) error {
	method, err := f.ParseMethod()
	if err != nil {
		return err
	}

	if method.ClassID != protocol.ClassConnection || method.MethodID != protocol.MethodConnectionStart {
		return fmt.Errorf("expected Connection.Start, got %d.%d", method.ClassID, method.MethodID)
	}

	// Parse arguments
	args := frame.NewMethodArgs(method.Args)
	versionMajor, _ := args.ReadUint8()
	versionMinor, _ := args.ReadUint8()
	_, _ = args.ReadTable() // server-properties
	_, _ = args.ReadLongString() // mechanisms
	_, _ = args.ReadLongString() // locales

	// Validate version
	if versionMajor != 0 || versionMinor != 9 {
		return fmt.Errorf("unsupported AMQP version: %d.%d", versionMajor, versionMinor)
	}

	return nil
}

// sendConnectionStartOk sends Connection.StartOk method
func (c *Connection) sendConnectionStartOk() error {
	builder := frame.NewMethodArgsBuilder()

	// Client properties
	if err := builder.WriteTable(c.factory.ClientProperties); err != nil {
		return err
	}

	// Mechanism (PLAIN)
	if err := builder.WriteShortString("PLAIN"); err != nil {
		return err
	}

	// Response (username + password)
	response := fmt.Sprintf("\x00%s\x00%s", c.factory.Username, c.factory.Password)
	if err := builder.WriteLongString([]byte(response)); err != nil {
		return err
	}

	// Locale
	if err := builder.WriteShortString("en_US"); err != nil {
		return err
	}

	// Create and send frame
	f := frame.NewMethodFrame(0, protocol.ClassConnection, protocol.MethodConnectionStartOk, builder.Bytes())
	return c.frameWriter.WriteFrame(f)
}

// handleConnectionTune processes Connection.Tune method
func (c *Connection) handleConnectionTune(f *frame.Frame) error {
	method, err := f.ParseMethod()
	if err != nil {
		return err
	}

	if method.ClassID != protocol.ClassConnection || method.MethodID != protocol.MethodConnectionTune {
		return fmt.Errorf("expected Connection.Tune, got %d.%d", method.ClassID, method.MethodID)
	}

	// Parse tune parameters
	args := frame.NewMethodArgs(method.Args)
	serverChannelMax, _ := args.ReadUint16()
	serverFrameMax, _ := args.ReadUint32()
	serverHeartbeat, _ := args.ReadUint16()

	// Negotiate parameters
	c.channelMax = serverChannelMax
	if c.factory.ChannelMax > 0 && c.factory.ChannelMax < serverChannelMax {
		c.channelMax = c.factory.ChannelMax
	}
	if c.channelMax == 0 {
		c.channelMax = 65535
	}

	c.frameMax = serverFrameMax
	if c.factory.FrameMax > 0 && c.factory.FrameMax < serverFrameMax {
		c.frameMax = c.factory.FrameMax
	}
	if c.frameMax == 0 {
		c.frameMax = 131072
	}

	// Negotiate heartbeat
	requestedHeartbeat := uint16(c.factory.Heartbeat.Seconds())
	if requestedHeartbeat < serverHeartbeat {
		c.heartbeat = time.Duration(requestedHeartbeat) * time.Second
	} else {
		c.heartbeat = time.Duration(serverHeartbeat) * time.Second
	}

	// Update frame reader/writer with negotiated frame size
	c.frameReader.SetMaxFrameSize(c.frameMax)
	c.frameWriter.SetMaxFrameSize(c.frameMax)

	return nil
}

// sendConnectionTuneOk sends Connection.TuneOk method
func (c *Connection) sendConnectionTuneOk() error {
	builder := frame.NewMethodArgsBuilder()
	builder.WriteUint16(c.channelMax)
	builder.WriteUint32(c.frameMax)
	builder.WriteUint16(uint16(c.heartbeat.Seconds()))

	f := frame.NewMethodFrame(0, protocol.ClassConnection, protocol.MethodConnectionTuneOk, builder.Bytes())
	return c.frameWriter.WriteFrame(f)
}

// sendConnectionOpen sends Connection.Open method
func (c *Connection) sendConnectionOpen() error {
	builder := frame.NewMethodArgsBuilder()
	builder.WriteShortString(c.factory.VHost)
	builder.WriteShortString("") // capabilities (deprecated, empty)
	builder.WriteFlags(false)    // insist flag (deprecated, always false)

	f := frame.NewMethodFrame(0, protocol.ClassConnection, protocol.MethodConnectionOpen, builder.Bytes())
	return c.frameWriter.WriteFrame(f)
}

// handleConnectionOpenOk processes Connection.OpenOk method
func (c *Connection) handleConnectionOpenOk(f *frame.Frame) error {
	method, err := f.ParseMethod()
	if err != nil {
		return err
	}

	if method.ClassID != protocol.ClassConnection || method.MethodID != protocol.MethodConnectionOpenOk {
		return fmt.Errorf("expected Connection.OpenOk, got %d.%d", method.ClassID, method.MethodID)
	}

	// Connection is now open
	c.state.Store(int32(StateOpen))
	return nil
}

// start starts background goroutines
func (c *Connection) start() {
	// Only create control channels if they don't exist (initial connection)
	// During recovery, reuse existing channels so monitors don't get orphaned
	if c.closed == nil {
		c.closed = make(chan struct{})
	}
	c.dispatchStop = make(chan struct{})
	c.dispatchDone = make(chan struct{})
	c.heartbeatStop = make(chan struct{})
	c.heartbeatDone = make(chan struct{})

	// Update last activity
	c.updateActivity()

	// Start frame dispatcher
	go c.frameDispatcher()

	// Start heartbeat if enabled
	if c.heartbeat > 0 {
		go c.heartbeatSender()
		go c.heartbeatMonitor()
	}

	// Start recovery monitor if automatic recovery is enabled
	// But skip if already running (during recovery, we don't start a new monitor)
	if c.factory.AutomaticRecovery && c.recoveryStop == nil {
		c.recoveryStop = make(chan struct{})
		c.recoveryDone = make(chan struct{})
		go c.recoveryMonitor()
	}

	// Notify listeners
	c.notifyListeners(func(l ConnectionListener) {
		l.OnConnectionCreated(c)
	})
}

// frameDispatcher reads frames and dispatches them to channels
func (c *Connection) frameDispatcher() {
	defer close(c.dispatchDone)

	for {
		select {
		case <-c.dispatchStop:
			return
		default:
		}

		// Read frame with timeout
		c.conn.SetReadDeadline(time.Now().Add(c.heartbeat * 2))
		f, err := c.frameReader.ReadFrame()
		if err != nil {
			if c.GetState() != StateClosed {
				c.closeWithError(NewError(protocol.ReplyConnectionForced, fmt.Sprintf("read frame: %v", err), false))
			}
			return
		}

		// Update activity timestamp
		c.updateActivity()

		// Handle frame
		if err := c.dispatchFrame(f); err != nil {
			if c.GetState() != StateClosed {
				c.closeWithError(NewError(protocol.ReplyFrameError, fmt.Sprintf("dispatch frame: %v", err), false))
			}
			return
		}
	}
}

// dispatchFrame dispatches a frame to the appropriate handler
func (c *Connection) dispatchFrame(f *frame.Frame) error {
	switch f.Type {
	case protocol.FrameMethod:
		return c.handleMethodFrame(f)
	case protocol.FrameHeartbeat:
		// Heartbeat received, activity already updated
		return nil
	case protocol.FrameHeader, protocol.FrameBody:
		// Dispatch to channel
		return c.dispatchToChannel(f)
	default:
		return fmt.Errorf("unknown frame type: %d", f.Type)
	}
}

// handleMethodFrame handles method frames on channel 0 (connection)
func (c *Connection) handleMethodFrame(f *frame.Frame) error {
	if f.ChannelID == 0 {
		// Connection-level method
		method, err := f.ParseMethod()
		if err != nil {
			return err
		}

		switch method.ClassID {
		case protocol.ClassConnection:
			return c.handleConnectionMethod(method)
		default:
			return fmt.Errorf("unexpected method on channel 0: %d.%d", method.ClassID, method.MethodID)
		}
	}

	// Dispatch to channel
	return c.dispatchToChannel(f)
}

// handleConnectionMethod handles connection class methods
func (c *Connection) handleConnectionMethod(method *frame.Method) error {
	switch method.MethodID {
	case protocol.MethodConnectionClose:
		return c.handleConnectionClose(method)
	case protocol.MethodConnectionBlocked:
		return c.handleConnectionBlocked(method)
	case protocol.MethodConnectionUnblocked:
		return c.handleConnectionUnblocked(method)
	default:
		return fmt.Errorf("unexpected connection method: %d", method.MethodID)
	}
}

// handleConnectionClose processes Connection.Close method
func (c *Connection) handleConnectionClose(method *frame.Method) error {
	args := frame.NewMethodArgs(method.Args)
	replyCode, _ := args.ReadUint16()
	replyText, _ := args.ReadShortString()

	// Send Connection.CloseOk
	builder := frame.NewMethodArgsBuilder()
	f := frame.NewMethodFrame(0, protocol.ClassConnection, protocol.MethodConnectionCloseOk, builder.Bytes())
	c.frameWriter.WriteFrame(f)

	// Close connection
	err := NewError(int(replyCode), replyText, true)
	c.closeWithError(err)

	return nil
}

// handleConnectionBlocked processes Connection.Blocked method
func (c *Connection) handleConnectionBlocked(method *frame.Method) error {
	args := frame.NewMethodArgs(method.Args)
	reason, _ := args.ReadShortString()

	c.blocked.Store(true)

	// Notify on channel
	select {
	case c.blockedChan <- BlockedNotification{Blocked: true, Reason: reason}:
	default:
	}

	// Notify listeners
	c.notifyListeners(func(l ConnectionListener) {
		l.OnConnectionBlocked(c, reason)
	})

	// Notify factory handler
	if c.factory.BlockedHandler != nil {
		c.factory.BlockedHandler.OnBlocked(c, reason)
	}

	return nil
}

// handleConnectionUnblocked processes Connection.Unblocked method
func (c *Connection) handleConnectionUnblocked(method *frame.Method) error {
	c.blocked.Store(false)

	// Notify on channel
	select {
	case c.blockedChan <- BlockedNotification{Blocked: false}:
	default:
	}

	// Notify listeners
	c.notifyListeners(func(l ConnectionListener) {
		l.OnConnectionUnblocked(c)
	})

	// Notify factory handler
	if c.factory.BlockedHandler != nil {
		c.factory.BlockedHandler.OnUnblocked(c)
	}

	return nil
}

// dispatchToChannel dispatches a frame to a channel
func (c *Connection) dispatchToChannel(f *frame.Frame) error {
	c.channelMux.RLock()
	ch, exists := c.channels[f.ChannelID]
	c.channelMux.RUnlock()

	if !exists {
		return fmt.Errorf("frame for unknown channel: %d", f.ChannelID)
	}

	// Send frame to channel (non-blocking)
	select {
	case ch.incomingFrames <- f:
		return nil
	default:
		return fmt.Errorf("channel %d frame buffer full", f.ChannelID)
	}
}

// heartbeatSender sends periodic heartbeat frames
func (c *Connection) heartbeatSender() {
	defer close(c.heartbeatDone)

	ticker := time.NewTicker(c.heartbeat / 2)
	defer ticker.Stop()

	for {
		select {
		case <-c.heartbeatStop:
			return
		case <-ticker.C:
			if err := c.frameWriter.WriteFrame(frame.NewHeartbeatFrame()); err != nil {
				c.closeWithError(NewError(protocol.ReplyConnectionForced, fmt.Sprintf("send heartbeat: %v", err), false))
				return
			}
			c.updateActivity()
		}
	}
}

// heartbeatMonitor monitors for missing heartbeats
func (c *Connection) heartbeatMonitor() {
	ticker := time.NewTicker(c.heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-c.heartbeatStop:
			return
		case <-ticker.C:
			lastActivity := time.Unix(c.lastActivity.Load(), 0)
			if time.Since(lastActivity) > c.heartbeat*2 {
				c.closeWithError(NewError(protocol.ReplyConnectionForced, "heartbeat timeout", false))
				return
			}
		}
	}
}

// updateActivity updates the last activity timestamp
func (c *Connection) updateActivity() {
	c.lastActivity.Store(time.Now().Unix())
}

// recoveryMonitor monitors for connection failures and initiates recovery
func (c *Connection) recoveryMonitor() {
	defer close(c.recoveryDone)

	for {
		select {
		case err := <-c.closeChan:
			// Check if should recover
			if !c.factory.AutomaticRecovery || err == nil || !err.Recover {
				return
			}

			// Initiate recovery
			c.initiateRecovery(err)
			// Continue monitoring for subsequent disconnects (don't exit)

		case <-c.recoveryStop:
			return
		case <-c.closed:
			return
		}
	}
}

// initiateRecovery attempts to recover the connection after a failure
func (c *Connection) initiateRecovery(originalErr *Error) {
	// 1. Set state to StateRecovering
	c.state.Store(int32(StateRecovering))

	// 2. Notify recovery started
	c.notifyRecoveryStarted()
	c.notifyListeners(func(l ConnectionListener) {
		l.OnConnectionRecoveryStarted(c)
	})
	if c.factory.RecoveryHandler != nil {
		c.factory.RecoveryHandler.OnRecoveryStarted(c)
	}

	// 3. Capture channel state before cleanup
	channelSnapshots := c.captureChannelState()

	// 4. Retry loop
	maxAttempts := c.factory.ConnectionRetryAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3 // Default
	}

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			backoff := c.calculateBackoff(attempt)
			time.Sleep(backoff)
		}

		// 5. Reconnect
		netConn, err := c.factory.dial(context.Background())
		if err != nil {
			c.notifyRecoveryFailed(err)
			if c.factory.RecoveryHandler != nil {
				c.factory.RecoveryHandler.OnRecoveryFailed(c, err)
			}
			continue
		}

		// 6. Replace network connection
		c.conn = netConn

		// 7. Perform handshake
		if err := c.handshake(context.Background()); err != nil {
			netConn.Close()
			c.notifyRecoveryFailed(err)
			if c.factory.RecoveryHandler != nil {
				c.factory.RecoveryHandler.OnRecoveryFailed(c, err)
			}
			continue
		}

		// 8. Clear old channels before starting new connection
		c.channelMux.Lock()
		c.channels = make(map[uint16]*Channel)
		c.nextChannelID = 1
		c.channelMux.Unlock()

		// 8a. Reset closeOnce so future disconnects can be detected
		// Note: We keep the existing closeChan - the recovery monitor is listening on it
		c.closeOnce = sync.Once{}

		// 8b. Restart background goroutines (recovery monitor keeps running, so won't start duplicate)
		c.start()

		// 8c. Set state to StateOpen so we can create channels for recovery
		// The connection is now fully functional after handshake
		c.state.Store(int32(StateOpen))

		// 9. Recover topology
		if c.factory.TopologyRecovery {
			if c.factory.RecoveryHandler != nil {
				c.factory.RecoveryHandler.OnTopologyRecoveryStarted(c)
			}
			if err := c.recovery.recoverTopology(c); err != nil {
				c.Close()
				c.notifyRecoveryFailed(err)
				if c.factory.RecoveryHandler != nil {
					c.factory.RecoveryHandler.OnRecoveryFailed(c, err)
				}
				continue
			}
			if c.factory.RecoveryHandler != nil {
				c.factory.RecoveryHandler.OnTopologyRecoveryCompleted(c)
			}
		}

		// 10. Recover channels
		if err := c.recoverChannels(channelSnapshots); err != nil {
			c.Close()
			c.notifyRecoveryFailed(err)
			if c.factory.RecoveryHandler != nil {
				c.factory.RecoveryHandler.OnRecoveryFailed(c, err)
			}
			continue
		}

		// 11. Success! State is already StateOpen from step 8b
		c.notifyRecoveryCompleted()
		c.notifyListeners(func(l ConnectionListener) {
			l.OnConnectionRecoveryCompleted(c)
		})
		if c.factory.RecoveryHandler != nil {
			c.factory.RecoveryHandler.OnRecoveryCompleted(c)
		}
		return
	}

	// All attempts exhausted
	c.state.Store(int32(StateClosed))
	finalErr := fmt.Errorf("recovery exhausted after %d attempts", maxAttempts)
	c.notifyRecoveryFailed(finalErr)
	if c.factory.RecoveryHandler != nil {
		c.factory.RecoveryHandler.OnRecoveryFailed(c, finalErr)
	}
}

// calculateBackoff calculates the backoff duration for a recovery attempt
func (c *Connection) calculateBackoff(attempt int) time.Duration {
	baseInterval := c.factory.RecoveryInterval
	if baseInterval <= 0 {
		baseInterval = 5 * time.Second
	}

	// Exponential backoff: base * 2^attempt, capped at 32x base
	multiplier := 1 << uint(attempt)
	if multiplier > 32 {
		multiplier = 32
	}

	return baseInterval * time.Duration(multiplier)
}

// captureChannelState captures the current state of all channels for recovery
func (c *Connection) captureChannelState() []channelSnapshot {
	c.channelMux.RLock()
	defer c.channelMux.RUnlock()

	snapshots := make([]channelSnapshot, 0, len(c.channels))
	for id, ch := range c.channels {
		snap := channelSnapshot{
			ch:            ch, // Store reference to the actual channel object
			id:            id,
			prefetchCount: ch.prefetchCount,
			prefetchSize:  ch.prefetchSize,
			globalQos:     ch.globalQos,
			confirmMode:   ch.confirms != nil,
		}

		// Capture consumers
		ch.consumerMux.RLock()
		for _, consumer := range ch.consumers {
			if consumer.callback != nil { // Only callback-based consumers
				snap.consumers = append(snap.consumers, consumerSnapshot{
					queue:    consumer.queue,
					tag:      consumer.tag,
					callback: consumer.callback,
					opts: ConsumeOptions{
						AutoAck:   consumer.autoAck,
						Exclusive: consumer.exclusive,
						NoLocal:   consumer.noLocal,
						Args:      consumer.args,
					},
				})
			}
		}
		ch.consumerMux.RUnlock()

		snapshots = append(snapshots, snap)
	}
	return snapshots
}

// recoverChannels restores channels from snapshots after recovery
func (c *Connection) recoverChannels(snapshots []channelSnapshot) error {
	maxChannelID := uint16(0)

	for _, snap := range snapshots {
		// Reuse the existing channel object (so application references remain valid)
		ch := snap.ch

		// Reset channel internal state for new connection
		ch.conn = c
		ch.incomingFrames = make(chan *frame.Frame, 100)
		ch.closeChan = make(chan *Error, 1)
		ch.closed = make(chan struct{})
		ch.consumers = make(map[string]*consumerState)
		ch.rpcWaiters = make(map[uint32]chan *frame.Method)
		ch.closeOnce = sync.Once{} // Reset close guard
		ch.state.Store(int32(StateConnecting))

		// Re-register channel in the connection's channel map
		c.channelMux.Lock()
		c.channels[snap.id] = ch
		if snap.id > maxChannelID {
			maxChannelID = snap.id
		}
		c.channelMux.Unlock()

		// Open channel
		if err := ch.open(context.Background()); err != nil {
			return fmt.Errorf("recover channel %d: %w", snap.id, err)
		}

		// Restore QoS
		if snap.prefetchCount > 0 || snap.prefetchSize > 0 {
			if err := ch.Qos(snap.prefetchCount, snap.prefetchSize, snap.globalQos); err != nil {
				return fmt.Errorf("recover channel %d QoS: %w", snap.id, err)
			}
		}

		// Enable confirms if needed
		if snap.confirmMode {
			if err := ch.ConfirmSelect(false); err != nil {
				return fmt.Errorf("recover channel %d confirms: %w", snap.id, err)
			}
		}

		// Recover consumers
		for _, consumer := range snap.consumers {
			if err := ch.ConsumeWithCallback(consumer.queue, consumer.tag, consumer.opts, consumer.callback); err != nil {
				return fmt.Errorf("recover consumer %s: %w", consumer.tag, err)
			}
			// Notify consumer of recovery
			consumer.callback.HandleRecoverOk(consumer.tag)
		}
	}

	// Update nextChannelID to be after all recovered channels
	c.channelMux.Lock()
	c.nextChannelID = maxChannelID + 1
	c.channelMux.Unlock()

	return nil
}

// NewChannel creates a new channel on this connection
func (c *Connection) NewChannel() (*Channel, error) {
	return c.NewChannelWithContext(context.Background())
}

// NewChannelWithContext creates a new channel with context support
func (c *Connection) NewChannelWithContext(ctx context.Context) (*Channel, error) {
	state := c.GetState()
	if state == StateRecovering {
		return nil, ErrRecovering
	}
	if state != StateOpen {
		return nil, ErrClosed
	}

	c.channelMux.Lock()

	// Find available channel ID
	if c.nextChannelID >= c.channelMax {
		c.channelMux.Unlock()
		return nil, fmt.Errorf("channel limit reached: %d", c.channelMax)
	}

	channelID := c.nextChannelID
	c.nextChannelID++

	// Create channel
	ch := &Channel{
		conn:           c,
		id:             channelID,
		incomingFrames: make(chan *frame.Frame, 100),
		closeChan:      make(chan *Error, 1),
		closed:         make(chan struct{}),
		consumers:      make(map[string]*consumerState),
		rpcWaiters:     make(map[uint32]chan *frame.Method),
	}
	ch.state.Store(int32(StateConnecting))

	// Register channel BEFORE opening so it can receive response frames
	c.channels[channelID] = ch

	// Must unlock before calling open() to avoid deadlock
	c.channelMux.Unlock()

	// Open channel
	if err := ch.open(ctx); err != nil {
		// Unregister on error
		c.channelMux.Lock()
		delete(c.channels, channelID)
		c.channelMux.Unlock()
		return nil, err
	}

	return ch, nil
}

// Close gracefully closes the connection
func (c *Connection) Close() error {
	return c.CloseWithCode(protocol.ReplySuccess, "connection closed")
}

// GetChannelCount returns the current number of open channels
func (c *Connection) GetChannelCount() int {
	c.channelMux.RLock()
	defer c.channelMux.RUnlock()
	return len(c.channels)
}

// NotifyRecoveryStarted registers for recovery started notifications
func (c *Connection) NotifyRecoveryStarted(ch chan struct{}) chan struct{} {
	c.notificationMux.Lock()
	defer c.notificationMux.Unlock()
	c.recoveryStartedChans = append(c.recoveryStartedChans, ch)
	return ch
}

// NotifyRecoveryCompleted registers for recovery completed notifications
func (c *Connection) NotifyRecoveryCompleted(ch chan struct{}) chan struct{} {
	c.notificationMux.Lock()
	defer c.notificationMux.Unlock()
	c.recoveryCompletedChans = append(c.recoveryCompletedChans, ch)
	return ch
}

// NotifyRecoveryFailed registers for recovery failed notifications
func (c *Connection) NotifyRecoveryFailed(ch chan error) chan error {
	c.notificationMux.Lock()
	defer c.notificationMux.Unlock()
	c.recoveryFailedChans = append(c.recoveryFailedChans, ch)
	return ch
}

// notifyRecoveryStarted sends recovery started notifications to all registered channels
func (c *Connection) notifyRecoveryStarted() {
	c.notificationMux.RLock()
	defer c.notificationMux.RUnlock()

	for _, ch := range c.recoveryStartedChans {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// notifyRecoveryCompleted sends recovery completed notifications to all registered channels
func (c *Connection) notifyRecoveryCompleted() {
	c.notificationMux.RLock()
	defer c.notificationMux.RUnlock()

	for _, ch := range c.recoveryCompletedChans {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// notifyRecoveryFailed sends recovery failed notifications to all registered channels
func (c *Connection) notifyRecoveryFailed(err error) {
	c.notificationMux.RLock()
	defer c.notificationMux.RUnlock()

	for _, ch := range c.recoveryFailedChans {
		select {
		case ch <- err:
		default:
		}
	}
}

// CloseWithCode closes the connection with a specific reply code and text
func (c *Connection) CloseWithCode(code int, text string) error {
	if c.GetState() == StateClosed {
		return nil
	}

	c.state.Store(int32(StateClosing))

	// Send Connection.Close
	builder := frame.NewMethodArgsBuilder()
	builder.WriteUint16(uint16(code))
	builder.WriteShortString(text)
	builder.WriteUint16(0) // class-id
	builder.WriteUint16(0) // method-id

	f := frame.NewMethodFrame(0, protocol.ClassConnection, protocol.MethodConnectionClose, builder.Bytes())
	c.frameWriter.WriteFrame(f)

	// Wait for Connection.CloseOk with timeout
	timeout := time.After(5 * time.Second)
	select {
	case <-c.closed:
	case <-timeout:
	}

	c.cleanup()
	return nil
}

// closeWithError closes the connection with an error
func (c *Connection) closeWithError(err *Error) {
	c.closeOnce.Do(func() {
		c.state.Store(int32(StateClosed))

		// Send error to close channel
		select {
		case c.closeChan <- err:
		default:
		}

		// Notify listeners
		c.notifyListeners(func(l ConnectionListener) {
			l.OnConnectionClosed(c, err)
		})

		// Call error handler
		if c.factory.ErrorHandler != nil {
			c.factory.ErrorHandler.HandleConnectionError(c, err)
		}

		// Only close c.closed and cleanup if this is NOT a recoverable error
		// For recoverable errors, we want to keep the recovery monitor running
		if !err.Recover || !c.factory.AutomaticRecovery {
			close(c.closed)
			c.cleanup()
		}
	})
}

// cleanup releases resources
func (c *Connection) cleanup() {
	// Stop background goroutines (with panic recovery in case already closed)
	func() {
		defer func() { recover() }()
		close(c.dispatchStop)
	}()

	if c.heartbeat > 0 {
		func() {
			defer func() { recover() }()
			close(c.heartbeatStop)
		}()

		// Wait for heartbeat goroutine with timeout
		select {
		case <-c.heartbeatDone:
		case <-time.After(2 * time.Second):
			// Timeout waiting for heartbeat to stop
		}
	}

	// Stop recovery monitor if running
	if c.factory.AutomaticRecovery {
		func() {
			defer func() { recover() }()
			close(c.recoveryStop)
		}()

		select {
		case <-c.recoveryDone:
		case <-time.After(2 * time.Second):
		}
	}

	// Close network connection to unblock any pending reads
	// Must be done before waiting for dispatcher to finish
	if c.conn != nil {
		c.conn.Close()
	}

	// Wait for dispatcher with timeout
	select {
	case <-c.dispatchDone:
	case <-time.After(2 * time.Second):
		// Timeout waiting for dispatcher to stop
	}

	// Close all channels
	c.channelMux.Lock()
	channels := c.channels
	c.channels = make(map[uint16]*Channel)
	c.channelMux.Unlock()

	// Clean up channels without holding the lock to avoid deadlock
	for _, ch := range channels {
		ch.closeOnce.Do(func() {
			ch.state.Store(int32(ChannelStateClosed))

			select {
			case ch.closeChan <- ErrChannelClosed:
			default:
			}

			if c.factory.ErrorHandler != nil {
				c.factory.ErrorHandler.HandleChannelError(ch, ErrChannelClosed)
			}

			close(ch.closed)
			// Only clean up consumers, don't try to remove from connection
			// (already done above when we cleared c.channels)
			ch.cleanupConsumers()
		})
	}
}

// IsClosed returns whether the connection is closed
func (c *Connection) IsClosed() bool {
	return c.GetState() == StateClosed
}

// GetState returns the current connection state
func (c *Connection) GetState() ConnectionState {
	return ConnectionState(c.state.Load())
}

// IsBlocked returns whether the connection is currently blocked
func (c *Connection) IsBlocked() bool {
	return c.blocked.Load()
}

// NotifyClose registers a listener for connection closure
func (c *Connection) NotifyClose(ch chan *Error) chan *Error {
	go func() {
		err := <-c.closeChan
		ch <- err
	}()
	return ch
}

// NotifyBlocked registers a listener for connection blocked/unblocked events
func (c *Connection) NotifyBlocked(ch chan BlockedNotification) chan BlockedNotification {
	go func() {
		for notification := range c.blockedChan {
			ch <- notification
		}
	}()
	return ch
}

// AddConnectionListener adds a connection lifecycle listener
func (c *Connection) AddConnectionListener(listener ConnectionListener) {
	c.listenerMux.Lock()
	defer c.listenerMux.Unlock()
	c.listeners = append(c.listeners, listener)
}

// RemoveConnectionListener removes a connection listener
func (c *Connection) RemoveConnectionListener(listener ConnectionListener) {
	c.listenerMux.Lock()
	defer c.listenerMux.Unlock()

	for i, l := range c.listeners {
		if l == listener {
			c.listeners = append(c.listeners[:i], c.listeners[i+1:]...)
			return
		}
	}
}

// notifyListeners calls a function for each listener
func (c *Connection) notifyListeners(fn func(ConnectionListener)) {
	c.listenerMux.RLock()
	defer c.listenerMux.RUnlock()

	for _, listener := range c.listeners {
		fn(listener)
	}
}

// GetChannelMax returns the negotiated maximum number of channels
func (c *Connection) GetChannelMax() uint16 {
	return c.channelMax
}

// GetFrameMax returns the negotiated maximum frame size
func (c *Connection) GetFrameMax() uint32 {
	return c.frameMax
}

// GetHeartbeat returns the negotiated heartbeat interval
func (c *Connection) GetHeartbeat() time.Duration {
	return c.heartbeat
}
