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
	c.closed = make(chan struct{})
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

// NewChannel creates a new channel on this connection
func (c *Connection) NewChannel() (*Channel, error) {
	return c.NewChannelWithContext(context.Background())
}

// NewChannelWithContext creates a new channel with context support
func (c *Connection) NewChannelWithContext(ctx context.Context) (*Channel, error) {
	if c.GetState() != StateOpen {
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
// Note: This is a placeholder. Automatic recovery is not yet fully implemented.
func (c *Connection) NotifyRecoveryStarted(ch chan struct{}) {
	// TODO: Implement when automatic recovery is added
}

// NotifyRecoveryCompleted registers for recovery completed notifications
// Note: This is a placeholder. Automatic recovery is not yet fully implemented.
func (c *Connection) NotifyRecoveryCompleted(ch chan struct{}) {
	// TODO: Implement when automatic recovery is added
}

// NotifyRecoveryFailed registers for recovery failed notifications
// Note: This is a placeholder. Automatic recovery is not yet fully implemented.
func (c *Connection) NotifyRecoveryFailed(ch chan error) {
	// TODO: Implement when automatic recovery is added
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

		close(c.closed)
		c.cleanup()
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
