package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// Table represents an AMQP field table
type Table map[string]interface{}

// ReadShortString reads a short string (max 255 bytes)
func ReadShortString(r io.Reader) (string, error) {
	var length uint8
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return "", err
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}

	return string(buf), nil
}

// WriteShortString writes a short string
func WriteShortString(w io.Writer, s string) error {
	if len(s) > 255 {
		return fmt.Errorf("short string too long: %d", len(s))
	}

	if err := binary.Write(w, binary.BigEndian, uint8(len(s))); err != nil {
		return err
	}

	_, err := w.Write([]byte(s))
	return err
}

// ReadLongString reads a long string
func ReadLongString(r io.Reader) ([]byte, error) {
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, err
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	return buf, nil
}

// WriteLongString writes a long string
func WriteLongString(w io.Writer, data []byte) error {
	if err := binary.Write(w, binary.BigEndian, uint32(len(data))); err != nil {
		return err
	}

	_, err := w.Write(data)
	return err
}

// ReadTable reads an AMQP field table
func ReadTable(r io.Reader) (Table, error) {
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, err
	}

	if length == 0 {
		return Table{}, nil
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	table := make(Table)
	buf := &byteReader{data: data, pos: 0}

	for buf.pos < len(buf.data) {
		// Read field name
		name, err := ReadShortString(buf)
		if err != nil {
			return nil, err
		}

		// Read field value
		value, err := readFieldValue(buf)
		if err != nil {
			return nil, err
		}

		table[name] = value
	}

	return table, nil
}

// WriteTable writes an AMQP field table
func WriteTable(w io.Writer, table Table) error {
	if table == nil || len(table) == 0 {
		return binary.Write(w, binary.BigEndian, uint32(0))
	}

	// Write table contents to buffer to get size
	buf := &byteWriter{data: make([]byte, 0, 1024)}

	for name, value := range table {
		if err := WriteShortString(buf, name); err != nil {
			return err
		}

		if err := writeFieldValue(buf, value); err != nil {
			return err
		}
	}

	// Write length then data
	if err := binary.Write(w, binary.BigEndian, uint32(len(buf.data))); err != nil {
		return err
	}

	_, err := w.Write(buf.data)
	return err
}

// readFieldValue reads a field value based on its type indicator
func readFieldValue(r io.Reader) (interface{}, error) {
	var typeIndicator byte
	if err := binary.Read(r, binary.BigEndian, &typeIndicator); err != nil {
		return nil, err
	}

	switch typeIndicator {
	case 't': // Boolean
		var b uint8
		if err := binary.Read(r, binary.BigEndian, &b); err != nil {
			return nil, err
		}
		return b != 0, nil

	case 'b': // Signed 8-bit
		var i int8
		if err := binary.Read(r, binary.BigEndian, &i); err != nil {
			return nil, err
		}
		return i, nil

	case 'B': // Unsigned 8-bit
		var i uint8
		if err := binary.Read(r, binary.BigEndian, &i); err != nil {
			return nil, err
		}
		return i, nil

	case 's': // Signed 16-bit
		var i int16
		if err := binary.Read(r, binary.BigEndian, &i); err != nil {
			return nil, err
		}
		return i, nil

	case 'u': // Unsigned 16-bit
		var i uint16
		if err := binary.Read(r, binary.BigEndian, &i); err != nil {
			return nil, err
		}
		return i, nil

	case 'I': // Signed 32-bit
		var i int32
		if err := binary.Read(r, binary.BigEndian, &i); err != nil {
			return nil, err
		}
		return i, nil

	case 'i': // Unsigned 32-bit
		var i uint32
		if err := binary.Read(r, binary.BigEndian, &i); err != nil {
			return nil, err
		}
		return i, nil

	case 'l': // Signed 64-bit
		var i int64
		if err := binary.Read(r, binary.BigEndian, &i); err != nil {
			return nil, err
		}
		return i, nil

	case 'f': // 32-bit float
		var f float32
		if err := binary.Read(r, binary.BigEndian, &f); err != nil {
			return nil, err
		}
		return f, nil

	case 'd': // 64-bit float
		var f float64
		if err := binary.Read(r, binary.BigEndian, &f); err != nil {
			return nil, err
		}
		return f, nil

	case 'S': // Long string
		return ReadLongString(r)

	case 'A': // Array
		return readArray(r)

	case 'T': // Timestamp
		var timestamp int64
		if err := binary.Read(r, binary.BigEndian, &timestamp); err != nil {
			return nil, err
		}
		return time.Unix(timestamp, 0), nil

	case 'F': // Nested table
		return ReadTable(r)

	case 'V': // Void/null
		return nil, nil

	default:
		return nil, fmt.Errorf("unknown field type: %c", typeIndicator)
	}
}

// writeFieldValue writes a field value with its type indicator
func writeFieldValue(w io.Writer, value interface{}) error {
	switch v := value.(type) {
	case bool:
		if err := binary.Write(w, binary.BigEndian, byte('t')); err != nil {
			return err
		}
		var b uint8
		if v {
			b = 1
		}
		return binary.Write(w, binary.BigEndian, b)

	case int8:
		if err := binary.Write(w, binary.BigEndian, byte('b')); err != nil {
			return err
		}
		return binary.Write(w, binary.BigEndian, v)

	case uint8:
		if err := binary.Write(w, binary.BigEndian, byte('B')); err != nil {
			return err
		}
		return binary.Write(w, binary.BigEndian, v)

	case int16:
		if err := binary.Write(w, binary.BigEndian, byte('s')); err != nil {
			return err
		}
		return binary.Write(w, binary.BigEndian, v)

	case uint16:
		if err := binary.Write(w, binary.BigEndian, byte('u')); err != nil {
			return err
		}
		return binary.Write(w, binary.BigEndian, v)

	case int32:
		if err := binary.Write(w, binary.BigEndian, byte('I')); err != nil {
			return err
		}
		return binary.Write(w, binary.BigEndian, v)

	case uint32:
		if err := binary.Write(w, binary.BigEndian, byte('i')); err != nil {
			return err
		}
		return binary.Write(w, binary.BigEndian, v)

	case int64:
		if err := binary.Write(w, binary.BigEndian, byte('l')); err != nil {
			return err
		}
		return binary.Write(w, binary.BigEndian, v)

	case int: // Convert int to int32 or int64 based on platform
		if err := binary.Write(w, binary.BigEndian, byte('I')); err != nil {
			return err
		}
		return binary.Write(w, binary.BigEndian, int32(v))

	case float32:
		if err := binary.Write(w, binary.BigEndian, byte('f')); err != nil {
			return err
		}
		return binary.Write(w, binary.BigEndian, v)

	case float64:
		if err := binary.Write(w, binary.BigEndian, byte('d')); err != nil {
			return err
		}
		return binary.Write(w, binary.BigEndian, v)

	case string:
		if err := binary.Write(w, binary.BigEndian, byte('S')); err != nil {
			return err
		}
		return WriteLongString(w, []byte(v))

	case []byte:
		if err := binary.Write(w, binary.BigEndian, byte('S')); err != nil {
			return err
		}
		return WriteLongString(w, v)

	case time.Time:
		if err := binary.Write(w, binary.BigEndian, byte('T')); err != nil {
			return err
		}
		return binary.Write(w, binary.BigEndian, v.Unix())

	case Table:
		if err := binary.Write(w, binary.BigEndian, byte('F')); err != nil {
			return err
		}
		return WriteTable(w, v)

	case []interface{}:
		if err := binary.Write(w, binary.BigEndian, byte('A')); err != nil {
			return err
		}
		return writeArray(w, v)

	case nil:
		return binary.Write(w, binary.BigEndian, byte('V'))

	default:
		return fmt.Errorf("unsupported field value type: %T", value)
	}
}

// readArray reads an array of field values
func readArray(r io.Reader) ([]interface{}, error) {
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, err
	}

	if length == 0 {
		return []interface{}{}, nil
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	var values []interface{}
	buf := &byteReader{data: data, pos: 0}

	for buf.pos < len(buf.data) {
		value, err := readFieldValue(buf)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}

	return values, nil
}

// writeArray writes an array of field values
func writeArray(w io.Writer, values []interface{}) error {
	buf := &byteWriter{data: make([]byte, 0, 256)}

	for _, value := range values {
		if err := writeFieldValue(buf, value); err != nil {
			return err
		}
	}

	if err := binary.Write(w, binary.BigEndian, uint32(len(buf.data))); err != nil {
		return err
	}

	_, err := w.Write(buf.data)
	return err
}

// byteReader wraps a byte slice to implement io.Reader
type byteReader struct {
	data []byte
	pos  int
}

func (br *byteReader) Read(p []byte) (int, error) {
	if br.pos >= len(br.data) {
		return 0, io.EOF
	}

	n := copy(p, br.data[br.pos:])
	br.pos += n
	return n, nil
}

// byteWriter wraps a byte slice to implement io.Writer
type byteWriter struct {
	data []byte
}

func (bw *byteWriter) Write(p []byte) (int, error) {
	bw.data = append(bw.data, p...)
	return len(p), nil
}
