package client

import (
	"bytes"
	"encoding/binary"
)

type buffer struct {
	Bytes  []byte
	Offset uint64
}

func (b *buffer) WriteString(v string) {
	offset := b.Offset

	// Copy the string bytes into the buffer.
	buf := []byte(v)
	copy(b.Bytes[offset:], buf)
	offset += uint64(len(buf))

	// Add a nul byte
	b.Bytes[offset] = 0
	offset++

	if (offset % messageWordSize) != 0 {
		// Account for padding, moving offset to the next word boundary.
		offset += messageWordSize - (offset % messageWordSize)
	}

	b.advance(offset - b.Offset)
}

func (b *buffer) WriteUint8(v uint8) {
	defer b.advance(1)

	b.Bytes[b.Offset] = v
}

func (b *buffer) WriteUint16(v uint16) {
	defer b.advance(2)

	binary.LittleEndian.PutUint16(b.Bytes[b.Offset:], v)
}

func (b *buffer) WriteUint32(v uint32) {
	defer b.advance(4)

	binary.LittleEndian.PutUint32(b.Bytes[b.Offset:], v)
}

func (b *buffer) WriteUint64(v uint64) {
	defer b.advance(8)

	binary.LittleEndian.PutUint64(b.Bytes[b.Offset:], v)
}

func (b *buffer) ReadString() string {
	index := bytes.IndexByte(b.Bytes[b.Offset:], 0)
	if index == -1 {
		panic("no string found")
	}
	s := string(b.Bytes[b.Offset:index])

	index++

	if (index % messageWordSize) != 0 {
		// Account for padding, moving index to the next word boundary.
		index += messageWordSize - (index % messageWordSize)
	}

	b.advance(uint64(index) - b.Offset)

	return s
}

func (b *buffer) ReadUint8() uint8 {
	defer b.advance(1)

	return b.Bytes[b.Offset]
}

func (b *buffer) ReadUint16() uint16 {
	defer b.advance(2)

	return binary.LittleEndian.Uint16(b.Bytes[b.Offset:])
}

func (b *buffer) ReadUint32() uint32 {
	defer b.advance(4)

	return binary.LittleEndian.Uint32(b.Bytes[b.Offset:])
}

func (b *buffer) ReadUint64() uint64 {
	defer b.advance(8)

	return binary.LittleEndian.Uint64(b.Bytes[b.Offset:])
}

func (b *buffer) advance(amount uint64) {
	b.Offset += amount
}
