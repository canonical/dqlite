package client

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"io"
	"strings"
	"time"

	"github.com/CanonicalLtd/dqlite/internal/bindings"
)

type NamedValues = []driver.NamedValue
type Servers []bindings.ServerInfo

type Message struct {
	words  uint32
	mtype  uint8
	flags  uint8
	extra  uint16
	header []byte // Statically allocated header buffer
	body1  buffer // Statically allocated body data, using bytes
	body2  buffer // Dynamically allocated body data
}

func (m *Message) Init(staticSize int) {
	if (staticSize % messageWordSize) != 0 {
		panic("static size is not aligned to word boundary")
	}
	m.header = make([]byte, messageHeaderSize)
	m.body1.Bytes = make([]byte, staticSize)
	m.Reset()
}

func (m *Message) Reset() {
	m.body1.Offset = 0
	m.body2.Bytes = nil
	m.body2.Offset = 0
}

func (m *Message) PutString(v string) {
	size := len(v) + 1
	pad := 0
	if (size % messageWordSize) != 0 {
		// Account for padding
		pad = messageWordSize - (size % messageWordSize)
		size += pad
	}

	b := m.bufferForPut(size)
	defer b.Advance(size)

	// Copy the string bytes into the buffer.
	offset := b.Offset
	copy(b.Bytes[offset:], v)
	offset += len(v)

	// Add a nul byte
	b.Bytes[offset] = 0
	offset++

	// Add padding
	for i := 0; i < pad; i++ {
		b.Bytes[offset] = 0
		offset++
	}
}

func (m *Message) PutUint8(v uint8) {
	b := m.bufferForPut(1)
	defer b.Advance(1)

	b.Bytes[b.Offset] = v
}

func (m *Message) PutUint16(v uint16) {
	b := m.bufferForPut(2)
	defer b.Advance(2)

	binary.LittleEndian.PutUint16(b.Bytes[b.Offset:], v)
}

func (m *Message) PutUint32(v uint32) {
	b := m.bufferForPut(4)
	defer b.Advance(4)

	binary.LittleEndian.PutUint32(b.Bytes[b.Offset:], v)
}

func (m *Message) PutUint64(v uint64) {
	b := m.bufferForPut(8)
	defer b.Advance(8)

	binary.LittleEndian.PutUint64(b.Bytes[b.Offset:], v)
}

func (m *Message) PutInt64(v int64) {
	b := m.bufferForPut(8)
	defer b.Advance(8)

	binary.LittleEndian.PutUint64(b.Bytes[b.Offset:], uint64(v))
}

func (m *Message) PutNamedValues(values NamedValues) {
	n := uint8(len(values)) // N of params
	if n == 0 {
		return
	}

	m.PutUint8(n)

	for i := range values {
		if values[i].Ordinal != i+1 {
			panic("unexpected ordinal")
		}

		switch values[i].Value.(type) {
		case int64:
			m.PutUint8(bindings.Integer)
		case float64:
			m.PutUint8(bindings.Float)
		case bool:
			m.PutUint8(bindings.Integer)
		case []byte:
			m.PutUint8(bindings.Blob)
		case string:
			m.PutUint8(bindings.Text)
		case nil:
			m.PutUint8(bindings.Null)
		case time.Time:
			m.PutUint8(bindings.ISO8601)
		default:
			panic("unsupported value type")
		}
	}

	b := m.bufferForPut(1)

	if trailing := b.Offset % messageWordSize; trailing != 0 {
		// Skip padding bytes
		b.Advance(messageWordSize - trailing)
	}

	for i := range values {
		switch v := values[i].Value.(type) {
		case int64:
			m.PutInt64(v)
		case float64:
			panic("todo")
		case bool:
			panic("todo")
		case []byte:
			panic("todo")
		case string:
			m.PutString(v)
		case nil:
			m.PutInt64(0)
		case time.Time:
			timestamp := v.Format(iso8601)
			m.PutString(timestamp)
		default:
			panic("unsupported value type")
		}
	}

}

func (m *Message) PutHeader(mtype uint8) {
	if m.body1.Offset <= 0 {
		panic("static offset is not positive")
	}

	if (m.body1.Offset % messageWordSize) != 0 {
		panic("static body is not aligned")
	}

	m.mtype = mtype
	m.flags = 0
	m.extra = 0

	m.words = uint32(m.body1.Offset) / messageWordSize

	if m.body2.Bytes == nil {
		m.flushHeader()
		return
	}

	if m.body2.Offset <= 0 {
		panic("dynamic offset is not positive")
	}

	if (m.body2.Offset % messageWordSize) != 0 {
		panic("dynamic body is not aligned")
	}

	m.words += uint32(m.body2.Offset) / messageWordSize

	m.flushHeader()
}

func (m *Message) flushHeader() {
	if m.words == 0 {
		panic("empty message body")
	}

	binary.LittleEndian.PutUint32(m.header[0:], m.words)
	m.header[4] = m.mtype
	m.header[5] = m.flags
	binary.LittleEndian.PutUint16(m.header[6:], m.extra)
}

func (m *Message) bufferForPut(size int) *buffer {
	if m.body2.Bytes != nil {
		if (m.body2.Offset + size) > len(m.body2.Bytes) {
			// Grow body2.
			//
			// TODO: find a good grow strategy.
			bytes := make([]byte, m.body2.Offset+size)
			copy(bytes, m.body2.Bytes)
			m.body2.Bytes = bytes
		}

		return &m.body2
	}

	if (m.body1.Offset + size) > len(m.body1.Bytes) {
		m.body2.Bytes = make([]byte, size)
		m.body2.Offset = 0

		return &m.body2
	}

	return &m.body1
}

func (m *Message) GetHeader() (uint8, uint8) {
	return m.mtype, m.flags
}

func (m *Message) GetString() string {
	b := m.bufferForGet()

	index := bytes.IndexByte(b.Bytes[b.Offset:], 0)
	if index == -1 {
		panic("no string found")
	}
	s := string(b.Bytes[b.Offset : b.Offset+index])

	index++

	if trailing := index % messageWordSize; trailing != 0 {
		// Account for padding, moving index to the next word boundary.
		index += messageWordSize - trailing
	}

	b.Advance(index)

	return s
}

func (m *Message) GetUint8() uint8 {
	b := m.bufferForGet()
	defer b.Advance(1)

	return b.Bytes[b.Offset]
}

func (m *Message) GetUint16() uint16 {
	b := m.bufferForGet()
	defer b.Advance(2)

	return binary.LittleEndian.Uint16(b.Bytes[b.Offset:])
}

func (m *Message) GetUint32() uint32 {
	b := m.bufferForGet()
	defer b.Advance(4)

	return binary.LittleEndian.Uint32(b.Bytes[b.Offset:])
}

func (m *Message) GetUint64() uint64 {
	b := m.bufferForGet()
	defer b.Advance(8)

	return binary.LittleEndian.Uint64(b.Bytes[b.Offset:])
}

func (m *Message) GetInt64() int64 {
	b := m.bufferForGet()
	defer b.Advance(8)

	return int64(binary.LittleEndian.Uint64(b.Bytes[b.Offset:]))
}

func (m *Message) GetServers() (servers Servers) {
	defer func() {
		err := recover()
		if err != errMessageEOF {
			panic(err)
		}

	}()

	for {
		server := bindings.ServerInfo{
			ID:      m.GetUint64(),
			Address: m.GetString(),
		}
		servers = append(servers, server)
		m.bufferForGet()
	}
}

func (m *Message) GetResult() Result {
	return Result{
		LastInsertID: m.GetUint64(),
		RowsAffected: m.GetUint64(),
	}
}

func (m *Message) GetRows() Rows {
	// Read the column count and column names.
	columns := make([]string, m.GetUint64())

	for i := range columns {
		columns[i] = m.GetString()
	}

	rows := Rows{
		Columns: columns,
		message: m,
	}
	return rows
}

func (m *Message) bufferForGet() *buffer {
	size := int(m.words * messageWordSize)
	if m.body1.Offset == size || m.body1.Offset == len(m.body1.Bytes) {
		// The static body has been exahusted, use the dynamic one.
		if m.body1.Offset+m.body2.Offset == size {
			panic(errMessageEOF)
		}
		return &m.body2
	}

	return &m.body1
}

type Result struct {
	LastInsertID uint64
	RowsAffected uint64
}

type Rows struct {
	Columns []string
	message *Message
}

func (r *Rows) Next(dest []driver.Value) error {
	types := make([]uint8, len(r.Columns))

	// Each column needs a 4 byte slot to store the column type. The row
	// header must be padded to reach word boundary.
	headerBits := len(types) * 4
	padBits := 0
	if trailingBits := (headerBits % messageWordBits); trailingBits != 0 {
		padBits = (messageWordBits - trailingBits)
	}

	headerSize := (headerBits + padBits) / messageWordBits * messageWordSize

	for i := 0; i < headerSize; i++ {
		slot := r.message.GetUint8()

		if slot == 0xff {
			// Rows EOF marker
			return io.EOF
		}

		index := i * 2

		if index >= len(types) {
			continue // This is padding.
		}

		types[index] = slot & 0x0f

		index++

		if index >= len(types) {
			continue // This is padding byte.
		}

		types[index] = slot >> 4
	}

	for i := range types {
		switch types[i] {
		case bindings.Integer:
			dest[i] = r.message.GetInt64()
		case bindings.Float:
			panic("todo")
		case bindings.Blob:
			panic("todo")
		case bindings.Text:
			dest[i] = r.message.GetString()
		case bindings.Null:
			r.message.GetUint64()
			dest[i] = nil
		case bindings.UnixTime:
			timestamp := time.Unix(r.message.GetInt64(), 0)
			dest[i] = timestamp
		case bindings.ISO8601:
			value := r.message.GetString()
			if !strings.Contains(value, "+") {
				value += "+00:00"
			}
			timestamp, err := time.Parse(iso8601, value)
			if err != nil {
				return err
			}
			dest[i] = timestamp
		default:
			//panic("unknown data type")
		}
	}

	return nil
}

func (r *Rows) Close() {
	r.message.Reset()
}

const (
	messageWordSize                 = 8
	messageWordBits                 = messageWordSize * 8
	messageHeaderSize               = messageWordSize
	messageMaxConsecutiveEmptyReads = 100
)

const iso8601 = "2006-01-02 15:04:05+00:00"
