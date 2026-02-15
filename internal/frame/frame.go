package frame

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/israelio/rabbit-go-client/internal/protocol"
)

// Frame represents an AMQP frame
type Frame struct {
	Type      uint8
	ChannelID uint16
	Payload   []byte
}

// Method represents a method frame payload
type Method struct {
	ClassID  uint16
	MethodID uint16
	Args     []byte
}

// Header represents a content header frame payload
type Header struct {
	ClassID    uint16
	Weight     uint16
	BodySize   uint64
	Properties []byte
}

// Body represents a content body frame payload
type Body struct {
	Data []byte
}

// NewMethodFrame creates a new method frame
func NewMethodFrame(channelID uint16, classID, methodID uint16, args []byte) *Frame {
	// Create method payload
	payload := make([]byte, 4+len(args))
	binary.BigEndian.PutUint16(payload[0:2], classID)
	binary.BigEndian.PutUint16(payload[2:4], methodID)
	copy(payload[4:], args)

	return &Frame{
		Type:      protocol.FrameMethod,
		ChannelID: channelID,
		Payload:   payload,
	}
}

// NewHeaderFrame creates a new content header frame
func NewHeaderFrame(channelID uint16, classID uint16, bodySize uint64, properties []byte) *Frame {
	// Create header payload
	payload := make([]byte, 12+len(properties))
	binary.BigEndian.PutUint16(payload[0:2], classID)
	binary.BigEndian.PutUint16(payload[2:4], 0) // weight (unused)
	binary.BigEndian.PutUint64(payload[4:12], bodySize)
	copy(payload[12:], properties)

	return &Frame{
		Type:      protocol.FrameHeader,
		ChannelID: channelID,
		Payload:   payload,
	}
}

// NewBodyFrame creates a new content body frame
func NewBodyFrame(channelID uint16, data []byte) *Frame {
	return &Frame{
		Type:      protocol.FrameBody,
		ChannelID: channelID,
		Payload:   data,
	}
}

// NewHeartbeatFrame creates a new heartbeat frame
func NewHeartbeatFrame() *Frame {
	return &Frame{
		Type:      protocol.FrameHeartbeat,
		ChannelID: 0,
		Payload:   []byte{},
	}
}

// ParseMethod parses a method frame payload
func (f *Frame) ParseMethod() (*Method, error) {
	if f.Type != protocol.FrameMethod {
		return nil, fmt.Errorf("not a method frame: type=%d", f.Type)
	}

	if len(f.Payload) < 4 {
		return nil, fmt.Errorf("method frame payload too short: %d", len(f.Payload))
	}

	return &Method{
		ClassID:  binary.BigEndian.Uint16(f.Payload[0:2]),
		MethodID: binary.BigEndian.Uint16(f.Payload[2:4]),
		Args:     f.Payload[4:],
	}, nil
}

// ParseHeader parses a content header frame payload
func (f *Frame) ParseHeader() (*Header, error) {
	if f.Type != protocol.FrameHeader {
		return nil, fmt.Errorf("not a header frame: type=%d", f.Type)
	}

	if len(f.Payload) < 12 {
		return nil, fmt.Errorf("header frame payload too short: %d", len(f.Payload))
	}

	return &Header{
		ClassID:    binary.BigEndian.Uint16(f.Payload[0:2]),
		Weight:     binary.BigEndian.Uint16(f.Payload[2:4]),
		BodySize:   binary.BigEndian.Uint64(f.Payload[4:12]),
		Properties: f.Payload[12:],
	}, nil
}

// ParseBody parses a content body frame payload
func (f *Frame) ParseBody() (*Body, error) {
	if f.Type != protocol.FrameBody {
		return nil, fmt.Errorf("not a body frame: type=%d", f.Type)
	}

	return &Body{
		Data: f.Payload,
	}, nil
}

// String returns a string representation of the frame
func (f *Frame) String() string {
	var frameType string
	switch f.Type {
	case protocol.FrameMethod:
		frameType = "METHOD"
	case protocol.FrameHeader:
		frameType = "HEADER"
	case protocol.FrameBody:
		frameType = "BODY"
	case protocol.FrameHeartbeat:
		frameType = "HEARTBEAT"
	default:
		frameType = fmt.Sprintf("UNKNOWN(%d)", f.Type)
	}

	return fmt.Sprintf("Frame{type=%s, channel=%d, size=%d}", frameType, f.ChannelID, len(f.Payload))
}

// MethodArgs provides helper methods for reading method arguments
type MethodArgs struct {
	buf *bytes.Reader
}

// NewMethodArgs creates a new MethodArgs from a byte slice
func NewMethodArgs(data []byte) *MethodArgs {
	return &MethodArgs{buf: bytes.NewReader(data)}
}

// ReadBool reads a boolean value
func (ma *MethodArgs) ReadBool() (bool, error) {
	var b byte
	err := binary.Read(ma.buf, binary.BigEndian, &b)
	return b != 0, err
}

// ReadUint8 reads a uint8 value
func (ma *MethodArgs) ReadUint8() (uint8, error) {
	var v uint8
	err := binary.Read(ma.buf, binary.BigEndian, &v)
	return v, err
}

// ReadUint16 reads a uint16 value
func (ma *MethodArgs) ReadUint16() (uint16, error) {
	var v uint16
	err := binary.Read(ma.buf, binary.BigEndian, &v)
	return v, err
}

// ReadUint32 reads a uint32 value
func (ma *MethodArgs) ReadUint32() (uint32, error) {
	var v uint32
	err := binary.Read(ma.buf, binary.BigEndian, &v)
	return v, err
}

// ReadUint64 reads a uint64 value
func (ma *MethodArgs) ReadUint64() (uint64, error) {
	var v uint64
	err := binary.Read(ma.buf, binary.BigEndian, &v)
	return v, err
}

// ReadShortString reads a short string
func (ma *MethodArgs) ReadShortString() (string, error) {
	return protocol.ReadShortString(ma.buf)
}

// ReadLongString reads a long string
func (ma *MethodArgs) ReadLongString() ([]byte, error) {
	return protocol.ReadLongString(ma.buf)
}

// ReadTable reads a field table
func (ma *MethodArgs) ReadTable() (protocol.Table, error) {
	return protocol.ReadTable(ma.buf)
}

// MethodArgsBuilder provides helper methods for writing method arguments
type MethodArgsBuilder struct {
	buf *bytes.Buffer
}

// NewMethodArgsBuilder creates a new MethodArgsBuilder
func NewMethodArgsBuilder() *MethodArgsBuilder {
	return &MethodArgsBuilder{buf: new(bytes.Buffer)}
}

// WriteBool writes a boolean value
func (mab *MethodArgsBuilder) WriteBool(v bool) error {
	var b byte
	if v {
		b = 1
	}
	return binary.Write(mab.buf, binary.BigEndian, b)
}

// WriteFlags packs multiple boolean flags into bytes (AMQP bit packing)
// Bits are packed from LSB to MSB, 8 bits per byte
// Example: flags [true, false, true] → 0b00000101 = 0x05
func (mab *MethodArgsBuilder) WriteFlags(flags ...bool) error {
	var packed byte
	bitPos := 0

	for i, flag := range flags {
		if flag {
			packed |= (1 << uint(bitPos))
		}
		bitPos++

		// Write byte when we have 8 bits or reached the end
		if bitPos == 8 || i == len(flags)-1 {
			if err := mab.buf.WriteByte(packed); err != nil {
				return err
			}
			packed = 0
			bitPos = 0
		}
	}

	return nil
}

// WriteUint8 writes a uint8 value
func (mab *MethodArgsBuilder) WriteUint8(v uint8) error {
	return binary.Write(mab.buf, binary.BigEndian, v)
}

// WriteUint16 writes a uint16 value
func (mab *MethodArgsBuilder) WriteUint16(v uint16) error {
	return binary.Write(mab.buf, binary.BigEndian, v)
}

// WriteUint32 writes a uint32 value
func (mab *MethodArgsBuilder) WriteUint32(v uint32) error {
	return binary.Write(mab.buf, binary.BigEndian, v)
}

// WriteUint64 writes a uint64 value
func (mab *MethodArgsBuilder) WriteUint64(v uint64) error {
	return binary.Write(mab.buf, binary.BigEndian, v)
}

// WriteShortString writes a short string
func (mab *MethodArgsBuilder) WriteShortString(s string) error {
	return protocol.WriteShortString(mab.buf, s)
}

// WriteLongString writes a long string
func (mab *MethodArgsBuilder) WriteLongString(data []byte) error {
	return protocol.WriteLongString(mab.buf, data)
}

// WriteTable writes a field table
func (mab *MethodArgsBuilder) WriteTable(table protocol.Table) error {
	return protocol.WriteTable(mab.buf, table)
}

// Bytes returns the built argument bytes
func (mab *MethodArgsBuilder) Bytes() []byte {
	return mab.buf.Bytes()
}
