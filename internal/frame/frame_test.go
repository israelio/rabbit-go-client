package frame

import (
	"bytes"
	"testing"

	"github.com/israelio/rabbit-go-client/internal/protocol"
)

// TestFrameCreation tests frame creation functions
func TestFrameCreation(t *testing.T) {
	t.Run("method frame", func(t *testing.T) {
		args := []byte{0x01, 0x02, 0x03}
		f := NewMethodFrame(1, protocol.ClassConnection, protocol.MethodConnectionStart, args)

		if f.Type != protocol.FrameMethod {
			t.Errorf("Frame type: got %d, want %d", f.Type, protocol.FrameMethod)
		}
		if f.ChannelID != 1 {
			t.Errorf("Channel ID: got %d, want 1", f.ChannelID)
		}
		if len(f.Payload) != 4+len(args) {
			t.Errorf("Payload length: got %d, want %d", len(f.Payload), 4+len(args))
		}
	})

	t.Run("header frame", func(t *testing.T) {
		props := []byte{0x80, 0x00}
		f := NewHeaderFrame(1, protocol.ClassBasic, 1024, props)

		if f.Type != protocol.FrameHeader {
			t.Errorf("Frame type: got %d, want %d", f.Type, protocol.FrameHeader)
		}
		if f.ChannelID != 1 {
			t.Errorf("Channel ID: got %d, want 1", f.ChannelID)
		}
	})

	t.Run("body frame", func(t *testing.T) {
		data := []byte("Hello, RabbitMQ!")
		f := NewBodyFrame(1, data)

		if f.Type != protocol.FrameBody {
			t.Errorf("Frame type: got %d, want %d", f.Type, protocol.FrameBody)
		}
		if !bytes.Equal(f.Payload, data) {
			t.Error("Body payload mismatch")
		}
	})

	t.Run("heartbeat frame", func(t *testing.T) {
		f := NewHeartbeatFrame()

		if f.Type != protocol.FrameHeartbeat {
			t.Errorf("Frame type: got %d, want %d", f.Type, protocol.FrameHeartbeat)
		}
		if f.ChannelID != 0 {
			t.Errorf("Channel ID: got %d, want 0", f.ChannelID)
		}
		if len(f.Payload) != 0 {
			t.Errorf("Payload length: got %d, want 0", len(f.Payload))
		}
	})
}

// TestFrameParsing tests frame payload parsing
func TestFrameParsing(t *testing.T) {
	t.Run("parse method frame", func(t *testing.T) {
		args := []byte{0x01, 0x02, 0x03}
		f := NewMethodFrame(1, protocol.ClassConnection, protocol.MethodConnectionStart, args)

		method, err := f.ParseMethod()
		if err != nil {
			t.Fatalf("ParseMethod failed: %v", err)
		}

		if method.ClassID != protocol.ClassConnection {
			t.Errorf("Class ID: got %d, want %d", method.ClassID, protocol.ClassConnection)
		}
		if method.MethodID != protocol.MethodConnectionStart {
			t.Errorf("Method ID: got %d, want %d", method.MethodID, protocol.MethodConnectionStart)
		}
		if !bytes.Equal(method.Args, args) {
			t.Error("Method args mismatch")
		}
	})

	t.Run("parse header frame", func(t *testing.T) {
		props := []byte{0x80, 0x00, 0x01, 0x02}
		f := NewHeaderFrame(1, protocol.ClassBasic, 1024, props)

		header, err := f.ParseHeader()
		if err != nil {
			t.Fatalf("ParseHeader failed: %v", err)
		}

		if header.ClassID != protocol.ClassBasic {
			t.Errorf("Class ID: got %d, want %d", header.ClassID, protocol.ClassBasic)
		}
		if header.BodySize != 1024 {
			t.Errorf("Body size: got %d, want 1024", header.BodySize)
		}
		if !bytes.Equal(header.Properties, props) {
			t.Error("Properties mismatch")
		}
	})

	t.Run("parse body frame", func(t *testing.T) {
		data := []byte("test body")
		f := NewBodyFrame(1, data)

		body, err := f.ParseBody()
		if err != nil {
			t.Fatalf("ParseBody failed: %v", err)
		}

		if !bytes.Equal(body.Data, data) {
			t.Error("Body data mismatch")
		}
	})

	t.Run("invalid frame type for parsing", func(t *testing.T) {
		f := NewHeartbeatFrame()

		_, err := f.ParseMethod()
		if err == nil {
			t.Error("Expected error parsing heartbeat as method")
		}

		_, err = f.ParseHeader()
		if err == nil {
			t.Error("Expected error parsing heartbeat as header")
		}

		_, err = f.ParseBody()
		if err == nil {
			t.Error("Expected error parsing heartbeat as body")
		}
	})
}

// TestMethodArgsBuilder tests method argument building
func TestMethodArgsBuilder(t *testing.T) {
	builder := NewMethodArgsBuilder()

	// Write various types
	builder.WriteBool(true)
	builder.WriteUint8(255)
	builder.WriteUint16(65535)
	builder.WriteUint32(4294967295)
	builder.WriteUint64(9223372036854775807)
	builder.WriteShortString("test")
	builder.WriteLongString([]byte("long string data"))
	builder.WriteTable(protocol.Table{"key": "value"})

	data := builder.Bytes()

	// Verify non-empty
	if len(data) == 0 {
		t.Error("Builder produced empty data")
	}

	// Parse back
	args := NewMethodArgs(data)

	b, _ := args.ReadBool()
	if !b {
		t.Error("Bool mismatch")
	}

	u8, _ := args.ReadUint8()
	if u8 != 255 {
		t.Errorf("Uint8: got %d, want 255", u8)
	}

	u16, _ := args.ReadUint16()
	if u16 != 65535 {
		t.Errorf("Uint16: got %d, want 65535", u16)
	}

	u32, _ := args.ReadUint32()
	if u32 != 4294967295 {
		t.Errorf("Uint32: got %d, want 4294967295", u32)
	}

	u64, _ := args.ReadUint64()
	if u64 != 9223372036854775807 {
		t.Errorf("Uint64: got %d, want 9223372036854775807", u64)
	}

	str, _ := args.ReadShortString()
	if str != "test" {
		t.Errorf("ShortString: got %q, want %q", str, "test")
	}

	longStr, _ := args.ReadLongString()
	if string(longStr) != "long string data" {
		t.Error("LongString mismatch")
	}

	table, _ := args.ReadTable()
	if len(table) != 1 {
		t.Errorf("Table length: got %d, want 1", len(table))
	}
}

// TestFrameString tests frame string representation
func TestFrameString(t *testing.T) {
	tests := []struct {
		name  string
		frame *Frame
		want  string
	}{
		{
			name:  "method frame",
			frame: NewMethodFrame(1, 0, 0, nil),
			want:  "METHOD",
		},
		{
			name:  "header frame",
			frame: NewHeaderFrame(1, 0, 0, nil),
			want:  "HEADER",
		},
		{
			name:  "body frame",
			frame: NewBodyFrame(1, nil),
			want:  "BODY",
		},
		{
			name:  "heartbeat frame",
			frame: NewHeartbeatFrame(),
			want:  "HEARTBEAT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			str := tt.frame.String()
			if !bytes.Contains([]byte(str), []byte(tt.want)) {
				t.Errorf("String() = %q, should contain %q", str, tt.want)
			}
		})
	}
}

// BenchmarkMethodFrameCreation benchmarks method frame creation
func BenchmarkMethodFrameCreation(b *testing.B) {
	args := []byte{0x01, 0x02, 0x03, 0x04, 0x05}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewMethodFrame(1, protocol.ClassConnection, protocol.MethodConnectionStart, args)
	}
}

// BenchmarkMethodArgsParsing benchmarks method args parsing
func BenchmarkMethodArgsParsing(b *testing.B) {
	builder := NewMethodArgsBuilder()
	builder.WriteUint16(100)
	builder.WriteShortString("test")
	builder.WriteBool(true)
	data := builder.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		args := NewMethodArgs(data)
		args.ReadUint16()
		args.ReadShortString()
		args.ReadBool()
	}
}
