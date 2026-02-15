package protocol

import (
	"bytes"
	"testing"
	"time"
)

// TestTableEncodingDecoding tests AMQP table encoding and decoding
func TestTableEncodingDecoding(t *testing.T) {
	tests := []struct {
		name  string
		table Table
	}{
		{
			name:  "empty table",
			table: Table{},
		},
		{
			name: "simple types",
			table: Table{
				"bool":   true,
				"int32":  int32(42),
				"int64":  int64(9223372036854775807),
				"string": "hello",
				"float":  float64(3.14159),
			},
		},
		{
			name: "nested table",
			table: Table{
				"outer": Table{
					"inner": "value",
					"num":   int32(123),
				},
			},
		},
		{
			name: "array values",
			table: Table{
				"array": []interface{}{
					int32(1),
					"two",
					true,
				},
			},
		},
		{
			name: "timestamp",
			table: Table{
				"timestamp": time.Unix(1234567890, 0),
			},
		},
		{
			name: "nil value",
			table: Table{
				"null": nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			buf := &bytes.Buffer{}
			err := WriteTable(buf, tt.table)
			if err != nil {
				t.Fatalf("WriteTable failed: %v", err)
			}

			// Decode
			decoded, err := ReadTable(bytes.NewReader(buf.Bytes()))
			if err != nil {
				t.Fatalf("ReadTable failed: %v", err)
			}

			// Compare length
			if len(decoded) != len(tt.table) {
				t.Errorf("Table length mismatch: got %d, want %d", len(decoded), len(tt.table))
			}

			// Verify all keys present
			for key := range tt.table {
				if _, exists := decoded[key]; !exists {
					t.Errorf("Key %q missing from decoded table", key)
				}
			}
		})
	}
}

// TestShortStringEncodingDecoding tests short string operations
func TestShortStringEncodingDecoding(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "empty string",
			input:   "",
			wantErr: false,
		},
		{
			name:    "short string",
			input:   "hello",
			wantErr: false,
		},
		{
			name:    "max length",
			input:   string(make([]byte, 255)),
			wantErr: false,
		},
		{
			name:    "too long",
			input:   string(make([]byte, 256)),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteShortString(buf, tt.input)

			if tt.wantErr {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("WriteShortString failed: %v", err)
			}

			decoded, err := ReadShortString(bytes.NewReader(buf.Bytes()))
			if err != nil {
				t.Fatalf("ReadShortString failed: %v", err)
			}

			if decoded != tt.input {
				t.Errorf("String mismatch: got %q, want %q", decoded, tt.input)
			}
		})
	}
}

// TestLongStringEncodingDecoding tests long string operations
func TestLongStringEncodingDecoding(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
	}{
		{
			name:  "empty",
			input: []byte{},
		},
		{
			name:  "small data",
			input: []byte("hello world"),
		},
		{
			name:  "large data",
			input: make([]byte, 10000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := WriteLongString(buf, tt.input)
			if err != nil {
				t.Fatalf("WriteLongString failed: %v", err)
			}

			decoded, err := ReadLongString(bytes.NewReader(buf.Bytes()))
			if err != nil {
				t.Fatalf("ReadLongString failed: %v", err)
			}

			if !bytes.Equal(decoded, tt.input) {
				t.Errorf("Data mismatch: lengths %d vs %d", len(decoded), len(tt.input))
			}
		})
	}
}

// TestFieldValueEncoding tests all field value type encoding
func TestFieldValueEncoding(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
	}{
		{"bool true", true},
		{"bool false", false},
		{"int8", int8(-128)},
		{"uint8", uint8(255)},
		{"int16", int16(-32768)},
		{"uint16", uint16(65535)},
		{"int32", int32(-2147483648)},
		{"uint32", uint32(4294967295)},
		{"int64", int64(-9223372036854775808)},
		{"float32", float32(3.14)},
		{"float64", float64(2.718281828)},
		{"string", "test string"},
		{"bytes", []byte{0x01, 0x02, 0x03}},
		{"timestamp", time.Unix(1609459200, 0)},
		{"table", Table{"key": "value"}},
		{"array", []interface{}{int32(1), "two", true}},
		{"nil", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err := writeFieldValue(buf, tt.value)
			if err != nil {
				t.Fatalf("writeFieldValue failed: %v", err)
			}

			decoded, err := readFieldValue(bytes.NewReader(buf.Bytes()))
			if err != nil {
				t.Fatalf("readFieldValue failed: %v", err)
			}

			// Basic type check (exact comparison depends on type)
			if decoded == nil && tt.value != nil {
				t.Error("Decoded value is nil but expected non-nil")
			}
		})
	}
}

// BenchmarkTableEncoding benchmarks table encoding performance
func BenchmarkTableEncoding(b *testing.B) {
	table := Table{
		"string":  "value",
		"int":     int32(42),
		"bool":    true,
		"float":   float64(3.14),
		"nested":  Table{"inner": "value"},
		"array":   []interface{}{int32(1), "two"},
		"timestamp": time.Now(),
	}

	buf := &bytes.Buffer{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		WriteTable(buf, table)
	}
}

// BenchmarkTableDecoding benchmarks table decoding performance
func BenchmarkTableDecoding(b *testing.B) {
	table := Table{
		"string":  "value",
		"int":     int32(42),
		"bool":    true,
		"float":   float64(3.14),
		"nested":  Table{"inner": "value"},
		"array":   []interface{}{int32(1), "two"},
	}

	buf := &bytes.Buffer{}
	WriteTable(buf, table)
	data := buf.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ReadTable(bytes.NewReader(data))
	}
}
