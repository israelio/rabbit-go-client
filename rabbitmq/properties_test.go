package rabbitmq

import (
	"testing"
	"time"

	"github.com/israelio/rabbit-go-client/internal/protocol"
)

// TestPropertiesEncoding tests message properties encoding
func TestPropertiesEncoding(t *testing.T) {
	tests := []struct {
		name  string
		props Properties
	}{
		{
			name:  "empty properties",
			props: Properties{},
		},
		{
			name: "content type only",
			props: Properties{
				ContentType: "application/json",
			},
		},
		{
			name: "persistent delivery",
			props: Properties{
				DeliveryMode: protocol.DeliveryModePersistent,
			},
		},
		{
			name: "full properties",
			props: Properties{
				ContentType:     "text/plain",
				ContentEncoding: "utf-8",
				Headers: Table{
					"x-custom": "value",
				},
				DeliveryMode:  protocol.DeliveryModePersistent,
				Priority:      5,
				CorrelationId: "correlation-123",
				ReplyTo:       "reply-queue",
				Expiration:    "60000",
				MessageId:     "msg-456",
				Timestamp:     time.Unix(1234567890, 0),
				Type:          "user.created",
				UserId:        "guest",
				AppId:         "my-app",
			},
		},
		{
			name: "with headers",
			props: Properties{
				Headers: Table{
					"x-retry-count": int32(3),
					"x-source":      "service-a",
					"x-timestamp":   time.Now(),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded, err := EncodeProperties(tt.props)
			if err != nil {
				t.Fatalf("EncodeProperties failed: %v", err)
			}

			// Decode
			decoded, err := DecodeProperties(encoded)
			if err != nil {
				t.Fatalf("DecodeProperties failed: %v", err)
			}

			// Verify fields
			if decoded.ContentType != tt.props.ContentType {
				t.Errorf("ContentType: got %q, want %q", decoded.ContentType, tt.props.ContentType)
			}
			if decoded.ContentEncoding != tt.props.ContentEncoding {
				t.Errorf("ContentEncoding: got %q, want %q", decoded.ContentEncoding, tt.props.ContentEncoding)
			}
			if decoded.DeliveryMode != tt.props.DeliveryMode {
				t.Errorf("DeliveryMode: got %d, want %d", decoded.DeliveryMode, tt.props.DeliveryMode)
			}
			if decoded.Priority != tt.props.Priority {
				t.Errorf("Priority: got %d, want %d", decoded.Priority, tt.props.Priority)
			}
			if decoded.CorrelationId != tt.props.CorrelationId {
				t.Errorf("CorrelationId: got %q, want %q", decoded.CorrelationId, tt.props.CorrelationId)
			}
			if decoded.ReplyTo != tt.props.ReplyTo {
				t.Errorf("ReplyTo: got %q, want %q", decoded.ReplyTo, tt.props.ReplyTo)
			}
			if decoded.MessageId != tt.props.MessageId {
				t.Errorf("MessageId: got %q, want %q", decoded.MessageId, tt.props.MessageId)
			}
		})
	}
}

// TestPredefinedProperties tests predefined property constants
func TestPredefinedProperties(t *testing.T) {
	tests := []struct {
		name  string
		props Properties
		check func(Properties) bool
	}{
		{
			name:  "MinimalBasic",
			props: MinimalBasic,
			check: func(p Properties) bool {
				return p.ContentType == "" && p.DeliveryMode == 0
			},
		},
		{
			name:  "MinimalPersistentBasic",
			props: MinimalPersistentBasic,
			check: func(p Properties) bool {
				return p.DeliveryMode == protocol.DeliveryModePersistent
			},
		},
		{
			name:  "Basic",
			props: Basic,
			check: func(p Properties) bool {
				return p.ContentType == "application/octet-stream" &&
					p.DeliveryMode == protocol.DeliveryModeNonPersistent
			},
		},
		{
			name:  "PersistentBasic",
			props: PersistentBasic,
			check: func(p Properties) bool {
				return p.ContentType == "application/octet-stream" &&
					p.DeliveryMode == protocol.DeliveryModePersistent
			},
		},
		{
			name:  "TextPlain",
			props: TextPlain,
			check: func(p Properties) bool {
				return p.ContentType == "text/plain" &&
					p.DeliveryMode == protocol.DeliveryModeNonPersistent
			},
		},
		{
			name:  "PersistentTextPlain",
			props: PersistentTextPlain,
			check: func(p Properties) bool {
				return p.ContentType == "text/plain" &&
					p.DeliveryMode == protocol.DeliveryModePersistent
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.check(tt.props) {
				t.Error("Predefined properties check failed")
			}

			// Verify can be encoded/decoded
			encoded, err := EncodeProperties(tt.props)
			if err != nil {
				t.Fatalf("EncodeProperties failed: %v", err)
			}

			_, err = DecodeProperties(encoded)
			if err != nil {
				t.Fatalf("DecodeProperties failed: %v", err)
			}
		})
	}
}

// TestPropertiesWithComplexHeaders tests properties with complex header values
func TestPropertiesWithComplexHeaders(t *testing.T) {
	props := Properties{
		Headers: Table{
			"string":    "value",
			"int":       int32(42),
			"bool":      true,
			"float":     float64(3.14),
			"timestamp": time.Unix(1609459200, 0),
			"nested": Table{
				"inner": "value",
			},
			"array": []interface{}{
				int32(1),
				"two",
				true,
			},
		},
	}

	encoded, err := EncodeProperties(props)
	if err != nil {
		t.Fatalf("EncodeProperties failed: %v", err)
	}

	decoded, err := DecodeProperties(encoded)
	if err != nil {
		t.Fatalf("DecodeProperties failed: %v", err)
	}

	if len(decoded.Headers) != len(props.Headers) {
		t.Errorf("Headers length: got %d, want %d", len(decoded.Headers), len(props.Headers))
	}

	// Verify some header values exist
	if _, exists := decoded.Headers["string"]; !exists {
		t.Error("Header 'string' missing")
	}
	if _, exists := decoded.Headers["nested"]; !exists {
		t.Error("Header 'nested' missing")
	}
}

// TestEmptyPropertiesEncoding tests that empty properties encode correctly
func TestEmptyPropertiesEncoding(t *testing.T) {
	props := Properties{}

	encoded, err := EncodeProperties(props)
	if err != nil {
		t.Fatalf("EncodeProperties failed: %v", err)
	}

	// Should be just the flags (2 bytes of zeros)
	if len(encoded) != 2 {
		t.Errorf("Encoded length: got %d, want 2", len(encoded))
	}

	// Should be all zeros
	if encoded[0] != 0 || encoded[1] != 0 {
		t.Error("Empty properties should have zero flags")
	}
}

// TestPropertiesTimestamp tests timestamp encoding/decoding
func TestPropertiesTimestamp(t *testing.T) {
	timestamps := []time.Time{
		time.Unix(0, 0),
		time.Unix(1234567890, 0),
		time.Now(),
	}

	for _, ts := range timestamps {
		props := Properties{
			Timestamp: ts,
		}

		encoded, err := EncodeProperties(props)
		if err != nil {
			t.Fatalf("EncodeProperties failed: %v", err)
		}

		decoded, err := DecodeProperties(encoded)
		if err != nil {
			t.Fatalf("DecodeProperties failed: %v", err)
		}

		// Compare Unix timestamps (loses nanosecond precision)
		if decoded.Timestamp.Unix() != ts.Unix() {
			t.Errorf("Timestamp: got %v, want %v", decoded.Timestamp.Unix(), ts.Unix())
		}
	}
}

// BenchmarkPropertiesEncoding benchmarks properties encoding
func BenchmarkPropertiesEncoding(b *testing.B) {
	props := Properties{
		ContentType:     "text/plain",
		ContentEncoding: "utf-8",
		Headers: Table{
			"x-custom": "value",
			"x-count":  int32(100),
		},
		DeliveryMode:  protocol.DeliveryModePersistent,
		Priority:      5,
		CorrelationId: "correlation-123",
		MessageId:     "msg-456",
		Timestamp:     time.Now(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeProperties(props)
	}
}

// BenchmarkPropertiesDecoding benchmarks properties decoding
func BenchmarkPropertiesDecoding(b *testing.B) {
	props := Properties{
		ContentType:     "text/plain",
		ContentEncoding: "utf-8",
		Headers: Table{
			"x-custom": "value",
			"x-count":  int32(100),
		},
		DeliveryMode:  protocol.DeliveryModePersistent,
		Priority:      5,
		CorrelationId: "correlation-123",
		MessageId:     "msg-456",
		Timestamp:     time.Now(),
	}

	encoded, _ := EncodeProperties(props)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeProperties(encoded)
	}
}

// TestPropertiesRoundTrip tests encoding/decoding round trip
func TestPropertiesRoundTrip(t *testing.T) {
	original := Properties{
		ContentType:     "application/json",
		ContentEncoding: "gzip",
		Headers: Table{
			"x-retry":    int32(3),
			"x-delay":    int32(5000),
			"x-source":   "service-a",
			"x-priority": "high",
		},
		DeliveryMode:  2,
		Priority:      9,
		CorrelationId: "req-123",
		ReplyTo:       "amq.rabbitmq.reply-to",
		Expiration:    "60000",
		MessageId:     "msg-abc-123",
		Timestamp:     time.Unix(1609459200, 0),
		Type:          "order.created",
		UserId:        "service-account",
		AppId:         "order-service",
	}

	// Encode
	encoded, err := EncodeProperties(original)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Decode
	decoded, err := DecodeProperties(encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Verify all string fields
	if decoded.ContentType != original.ContentType {
		t.Errorf("ContentType mismatch")
	}
	if decoded.ContentEncoding != original.ContentEncoding {
		t.Errorf("ContentEncoding mismatch")
	}
	if decoded.CorrelationId != original.CorrelationId {
		t.Errorf("CorrelationId mismatch")
	}
	if decoded.ReplyTo != original.ReplyTo {
		t.Errorf("ReplyTo mismatch")
	}
	if decoded.Expiration != original.Expiration {
		t.Errorf("Expiration mismatch")
	}
	if decoded.MessageId != original.MessageId {
		t.Errorf("MessageId mismatch")
	}
	if decoded.Type != original.Type {
		t.Errorf("Type mismatch")
	}
	if decoded.UserId != original.UserId {
		t.Errorf("UserId mismatch")
	}
	if decoded.AppId != original.AppId {
		t.Errorf("AppId mismatch")
	}

	// Verify numeric fields
	if decoded.DeliveryMode != original.DeliveryMode {
		t.Errorf("DeliveryMode mismatch")
	}
	if decoded.Priority != original.Priority {
		t.Errorf("Priority mismatch")
	}

	// Verify timestamp (Unix precision)
	if decoded.Timestamp.Unix() != original.Timestamp.Unix() {
		t.Errorf("Timestamp mismatch")
	}

	// Verify headers exist
	if len(decoded.Headers) != len(original.Headers) {
		t.Errorf("Headers count mismatch: got %d, want %d", len(decoded.Headers), len(original.Headers))
	}
}
