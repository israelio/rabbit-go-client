package frame

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/israelio/rabbit-go-client/internal/protocol"
)

// Writer writes AMQP frames to a connection
type Writer struct {
	w         *bufio.Writer
	mu        sync.Mutex
	maxFrame  uint32
	headerBuf [protocol.FrameHeaderSize + protocol.FrameEndSize]byte
}

// NewWriter creates a new frame writer
func NewWriter(w io.Writer, maxFrameSize uint32) *Writer {
	if maxFrameSize == 0 {
		maxFrameSize = protocol.FrameMinSize
	}

	return &Writer{
		w:        bufio.NewWriterSize(w, int(maxFrameSize)*2),
		maxFrame: maxFrameSize,
	}
}

// WriteFrame writes a single frame to the connection
func (fw *Writer) WriteFrame(frame *Frame) error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	// Validate payload size
	if uint32(len(frame.Payload)) > fw.maxFrame {
		return fmt.Errorf("frame payload too large: %d > %d", len(frame.Payload), fw.maxFrame)
	}

	// Write frame header
	fw.headerBuf[0] = frame.Type
	binary.BigEndian.PutUint16(fw.headerBuf[1:3], frame.ChannelID)
	binary.BigEndian.PutUint32(fw.headerBuf[3:7], uint32(len(frame.Payload)))

	if _, err := fw.w.Write(fw.headerBuf[:protocol.FrameHeaderSize]); err != nil {
		return fmt.Errorf("write frame header: %w", err)
	}

	// Write payload
	if len(frame.Payload) > 0 {
		if _, err := fw.w.Write(frame.Payload); err != nil {
			return fmt.Errorf("write frame payload: %w", err)
		}
	}

	// Write frame end marker
	if err := fw.w.WriteByte(protocol.FrameEnd); err != nil {
		return fmt.Errorf("write frame end: %w", err)
	}

	// Flush buffer
	if err := fw.w.Flush(); err != nil {
		return fmt.Errorf("flush frame: %w", err)
	}

	return nil
}

// WriteProtocolHeader writes the AMQP protocol header
func (fw *Writer) WriteProtocolHeader() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if _, err := fw.w.WriteString(protocol.ProtocolHeader); err != nil {
		return fmt.Errorf("write protocol header: %w", err)
	}

	if err := fw.w.Flush(); err != nil {
		return fmt.Errorf("flush protocol header: %w", err)
	}

	return nil
}

// SetMaxFrameSize updates the maximum frame size
func (fw *Writer) SetMaxFrameSize(size uint32) {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if size > 0 {
		fw.maxFrame = size
	}
}

// Flush flushes any buffered data
func (fw *Writer) Flush() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	return fw.w.Flush()
}
