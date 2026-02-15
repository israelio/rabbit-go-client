package frame

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/israelio/rabbit-go-client/internal/protocol"
)

// Reader reads AMQP frames from a connection
type Reader struct {
	r         *bufio.Reader
	maxFrame  uint32
	headerBuf [protocol.FrameHeaderSize]byte
}

// NewReader creates a new frame reader
func NewReader(r io.Reader, maxFrameSize uint32) *Reader {
	if maxFrameSize == 0 {
		maxFrameSize = protocol.FrameMinSize
	}

	return &Reader{
		r:        bufio.NewReaderSize(r, int(maxFrameSize)*2),
		maxFrame: maxFrameSize,
	}
}

// ReadFrame reads a single frame from the connection
func (fr *Reader) ReadFrame() (*Frame, error) {
	// Read frame header (7 bytes: type + channel + size)
	if _, err := io.ReadFull(fr.r, fr.headerBuf[:]); err != nil {
		return nil, fmt.Errorf("read frame header: %w", err)
	}

	frameType := fr.headerBuf[0]
	channelID := binary.BigEndian.Uint16(fr.headerBuf[1:3])
	payloadSize := binary.BigEndian.Uint32(fr.headerBuf[3:7])

	// Validate frame type
	if !isValidFrameType(frameType) {
		return nil, fmt.Errorf("invalid frame type: %d", frameType)
	}

	// Validate payload size
	if payloadSize > fr.maxFrame {
		return nil, fmt.Errorf("frame payload too large: %d > %d", payloadSize, fr.maxFrame)
	}

	// Read payload
	payload := make([]byte, payloadSize)
	if payloadSize > 0 {
		if _, err := io.ReadFull(fr.r, payload); err != nil {
			return nil, fmt.Errorf("read frame payload: %w", err)
		}
	}

	// Read frame end marker
	frameEnd, err := fr.r.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("read frame end: %w", err)
	}

	if frameEnd != protocol.FrameEnd {
		return nil, fmt.Errorf("invalid frame end marker: 0x%02X (expected 0x%02X)", frameEnd, protocol.FrameEnd)
	}

	return &Frame{
		Type:      frameType,
		ChannelID: channelID,
		Payload:   payload,
	}, nil
}

// ReadProtocolHeader reads the AMQP protocol header
func (fr *Reader) ReadProtocolHeader() (string, error) {
	header := make([]byte, 8)
	if _, err := io.ReadFull(fr.r, header); err != nil {
		return "", fmt.Errorf("read protocol header: %w", err)
	}

	return string(header), nil
}

// SetMaxFrameSize updates the maximum frame size
func (fr *Reader) SetMaxFrameSize(size uint32) {
	if size > 0 {
		fr.maxFrame = size
	}
}

// isValidFrameType checks if the frame type is valid
func isValidFrameType(frameType uint8) bool {
	switch frameType {
	case protocol.FrameMethod,
		protocol.FrameHeader,
		protocol.FrameBody,
		protocol.FrameHeartbeat:
		return true
	default:
		return false
	}
}
