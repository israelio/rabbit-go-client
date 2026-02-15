package rabbitmq

import (
	"encoding/binary"
	"io"
	"time"

	"github.com/israelio/rabbit-go-client/internal/protocol"
)

// Table is an alias for AMQP field table
type Table = protocol.Table

// Properties represents AMQP message properties (BasicProperties in Java)
type Properties struct {
	ContentType     string
	ContentEncoding string
	Headers         Table
	DeliveryMode    uint8
	Priority        uint8
	CorrelationId   string
	ReplyTo         string
	Expiration      string
	MessageId       string
	Timestamp       time.Time
	Type            string
	UserId          string
	AppId           string
}

// Publishing represents a message to publish
type Publishing struct {
	Properties
	Body []byte
}

// Property flags for encoding/decoding
const (
	flagContentType     = 0x8000
	flagContentEncoding = 0x4000
	flagHeaders         = 0x2000
	flagDeliveryMode    = 0x1000
	flagPriority        = 0x0800
	flagCorrelationId   = 0x0400
	flagReplyTo         = 0x0200
	flagExpiration      = 0x0100
	flagMessageId       = 0x0080
	flagTimestamp       = 0x0040
	flagType            = 0x0020
	flagUserId          = 0x0010
	flagAppId           = 0x0008
)

// EncodeProperties encodes properties to wire format
func EncodeProperties(props Properties) ([]byte, error) {
	// Calculate flags
	flags := uint16(0)
	if props.ContentType != "" {
		flags |= flagContentType
	}
	if props.ContentEncoding != "" {
		flags |= flagContentEncoding
	}
	if len(props.Headers) > 0 {
		flags |= flagHeaders
	}
	if props.DeliveryMode != 0 {
		flags |= flagDeliveryMode
	}
	if props.Priority != 0 {
		flags |= flagPriority
	}
	if props.CorrelationId != "" {
		flags |= flagCorrelationId
	}
	if props.ReplyTo != "" {
		flags |= flagReplyTo
	}
	if props.Expiration != "" {
		flags |= flagExpiration
	}
	if props.MessageId != "" {
		flags |= flagMessageId
	}
	if !props.Timestamp.IsZero() {
		flags |= flagTimestamp
	}
	if props.Type != "" {
		flags |= flagType
	}
	if props.UserId != "" {
		flags |= flagUserId
	}
	if props.AppId != "" {
		flags |= flagAppId
	}

	// Write to buffer
	buf := &propertyWriter{data: make([]byte, 0, 256)}

	// Write flags
	if err := binary.Write(buf, binary.BigEndian, flags); err != nil {
		return nil, err
	}

	// Write properties in order
	if flags&flagContentType != 0 {
		if err := protocol.WriteShortString(buf, props.ContentType); err != nil {
			return nil, err
		}
	}
	if flags&flagContentEncoding != 0 {
		if err := protocol.WriteShortString(buf, props.ContentEncoding); err != nil {
			return nil, err
		}
	}
	if flags&flagHeaders != 0 {
		if err := protocol.WriteTable(buf, props.Headers); err != nil {
			return nil, err
		}
	}
	if flags&flagDeliveryMode != 0 {
		if err := binary.Write(buf, binary.BigEndian, props.DeliveryMode); err != nil {
			return nil, err
		}
	}
	if flags&flagPriority != 0 {
		if err := binary.Write(buf, binary.BigEndian, props.Priority); err != nil {
			return nil, err
		}
	}
	if flags&flagCorrelationId != 0 {
		if err := protocol.WriteShortString(buf, props.CorrelationId); err != nil {
			return nil, err
		}
	}
	if flags&flagReplyTo != 0 {
		if err := protocol.WriteShortString(buf, props.ReplyTo); err != nil {
			return nil, err
		}
	}
	if flags&flagExpiration != 0 {
		if err := protocol.WriteShortString(buf, props.Expiration); err != nil {
			return nil, err
		}
	}
	if flags&flagMessageId != 0 {
		if err := protocol.WriteShortString(buf, props.MessageId); err != nil {
			return nil, err
		}
	}
	if flags&flagTimestamp != 0 {
		if err := binary.Write(buf, binary.BigEndian, uint64(props.Timestamp.Unix())); err != nil {
			return nil, err
		}
	}
	if flags&flagType != 0 {
		if err := protocol.WriteShortString(buf, props.Type); err != nil {
			return nil, err
		}
	}
	if flags&flagUserId != 0 {
		if err := protocol.WriteShortString(buf, props.UserId); err != nil {
			return nil, err
		}
	}
	if flags&flagAppId != 0 {
		if err := protocol.WriteShortString(buf, props.AppId); err != nil {
			return nil, err
		}
	}

	return buf.data, nil
}

// DecodeProperties decodes properties from wire format
func DecodeProperties(data []byte) (Properties, error) {
	props := Properties{}
	buf := &propertyReader{data: data, pos: 0}

	// Read flags
	var flags uint16
	if err := binary.Read(buf, binary.BigEndian, &flags); err != nil {
		return props, err
	}

	// Read properties in order
	if flags&flagContentType != 0 {
		contentType, err := protocol.ReadShortString(buf)
		if err != nil {
			return props, err
		}
		props.ContentType = contentType
	}
	if flags&flagContentEncoding != 0 {
		encoding, err := protocol.ReadShortString(buf)
		if err != nil {
			return props, err
		}
		props.ContentEncoding = encoding
	}
	if flags&flagHeaders != 0 {
		headers, err := protocol.ReadTable(buf)
		if err != nil {
			return props, err
		}
		props.Headers = headers
	}
	if flags&flagDeliveryMode != 0 {
		if err := binary.Read(buf, binary.BigEndian, &props.DeliveryMode); err != nil {
			return props, err
		}
	}
	if flags&flagPriority != 0 {
		if err := binary.Read(buf, binary.BigEndian, &props.Priority); err != nil {
			return props, err
		}
	}
	if flags&flagCorrelationId != 0 {
		corrId, err := protocol.ReadShortString(buf)
		if err != nil {
			return props, err
		}
		props.CorrelationId = corrId
	}
	if flags&flagReplyTo != 0 {
		replyTo, err := protocol.ReadShortString(buf)
		if err != nil {
			return props, err
		}
		props.ReplyTo = replyTo
	}
	if flags&flagExpiration != 0 {
		expiration, err := protocol.ReadShortString(buf)
		if err != nil {
			return props, err
		}
		props.Expiration = expiration
	}
	if flags&flagMessageId != 0 {
		msgId, err := protocol.ReadShortString(buf)
		if err != nil {
			return props, err
		}
		props.MessageId = msgId
	}
	if flags&flagTimestamp != 0 {
		var timestamp uint64
		if err := binary.Read(buf, binary.BigEndian, &timestamp); err != nil {
			return props, err
		}
		props.Timestamp = time.Unix(int64(timestamp), 0)
	}
	if flags&flagType != 0 {
		msgType, err := protocol.ReadShortString(buf)
		if err != nil {
			return props, err
		}
		props.Type = msgType
	}
	if flags&flagUserId != 0 {
		userId, err := protocol.ReadShortString(buf)
		if err != nil {
			return props, err
		}
		props.UserId = userId
	}
	if flags&flagAppId != 0 {
		appId, err := protocol.ReadShortString(buf)
		if err != nil {
			return props, err
		}
		props.AppId = appId
	}

	return props, nil
}

// propertyWriter wraps a byte slice for writing
type propertyWriter struct {
	data []byte
}

func (pw *propertyWriter) Write(p []byte) (int, error) {
	pw.data = append(pw.data, p...)
	return len(p), nil
}

// propertyReader wraps a byte slice for reading
type propertyReader struct {
	data []byte
	pos  int
}

func (pr *propertyReader) Read(p []byte) (int, error) {
	if pr.pos >= len(pr.data) {
		return 0, io.EOF
	}
	n := copy(p, pr.data[pr.pos:])
	pr.pos += n
	return n, nil
}

// Predefined message properties (matching Java client)
var (
	// MinimalBasic is an empty set of properties
	MinimalBasic = Properties{}

	// MinimalPersistentBasic has only persistent delivery mode
	MinimalPersistentBasic = Properties{
		DeliveryMode: protocol.DeliveryModePersistent,
	}

	// Basic is basic properties with default content type
	Basic = Properties{
		ContentType:  "application/octet-stream",
		DeliveryMode: protocol.DeliveryModeNonPersistent,
	}

	// PersistentBasic is basic properties with persistent delivery
	PersistentBasic = Properties{
		ContentType:  "application/octet-stream",
		DeliveryMode: protocol.DeliveryModePersistent,
	}

	// TextPlain is properties for text messages
	TextPlain = Properties{
		ContentType:  "text/plain",
		DeliveryMode: protocol.DeliveryModeNonPersistent,
	}

	// PersistentTextPlain is properties for persistent text messages
	PersistentTextPlain = Properties{
		ContentType:  "text/plain",
		DeliveryMode: protocol.DeliveryModePersistent,
	}
)
