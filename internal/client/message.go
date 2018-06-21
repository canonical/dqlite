package client

import (
	"io"

	"github.com/pkg/errors"
)

type message struct {
	Words uint32
	Type  uint8
	Flags uint8
	Extra uint16
	Body  buffer
}

func (m *message) Write(writer io.Writer) error {
	n := len(m.Body.Bytes)

	if (n % messageWordSize) != 0 {
		panic("body is not aligned at word boundary")
	}

	m.Words = uint32(n / messageWordSize)

	if err := m.writeHeader(writer); err != nil {
		return errors.Wrap(err, "failed to write header")
	}

	if err := m.writeBody(writer); err != nil {
		return errors.Wrap(err, "failed to write body")
	}

	return nil
}

func (m *message) writeHeader(writer io.Writer) error {
	buffer := buffer{Bytes: make([]byte, messageHeaderSize)}

	buffer.WriteUint32(m.Words)
	buffer.WriteUint8(m.Type)
	buffer.WriteUint8(m.Flags)
	buffer.WriteUint16(m.Extra)

	// TODO: we should keep on with short writes
	n, err := writer.Write(buffer.Bytes)
	if err != nil {
		return errors.Wrap(err, "failed to write header")
	}

	if n != messageHeaderSize {
		return errors.Wrap(io.ErrShortWrite, "failed to write header")
	}

	return nil
}

func (m *message) writeBody(writer io.Writer) error {
	// TODO: we should keep on with short writes
	n, err := writer.Write(m.Body.Bytes)
	if err != nil {
		return errors.Wrap(err, "failed to write body")
	}

	if n != len(m.Body.Bytes) {
		return errors.Wrap(io.ErrShortWrite, "failed to write body")
	}

	return nil
}

func (m *message) Read(reader io.Reader) error {
	if err := m.readHeader(reader); err != nil {
		return errors.Wrap(err, "failed to read header")
	}

	if err := m.readBody(reader); err != nil {
		return errors.Wrap(err, "failed to read body")
	}

	return nil
}

func (m *message) readHeader(reader io.Reader) error {
	buf := buffer{Bytes: make([]byte, messageHeaderSize)}

	if err := m.readPeek(reader, buf.Bytes); err != nil {
		return errors.Wrap(err, "failed to read header")
	}

	m.Words = buf.ReadUint32()
	m.Type = buf.ReadUint8()
	m.Flags = buf.ReadUint8()
	m.Extra = buf.ReadUint16()

	return nil
}

func (m *message) readBody(reader io.Reader) error {
	n := int(m.Words) * messageWordSize
	buf := buffer{Bytes: make([]byte, n)}

	if err := m.readPeek(reader, buf.Bytes); err != nil {
		return errors.Wrap(err, "failed to read body")
	}

	m.Body = buf

	return nil
}

// Read until buf is full.
func (m *message) readPeek(reader io.Reader, buf []byte) error {
	for offset := 0; offset < len(buf); {
		n, err := m.readFill(reader, buf[offset:])
		if err != nil {
			return err
		}
		offset += n
	}

	return nil
}

// Try to fill buf, but perform at most one read.
func (m *message) readFill(reader io.Reader, buf []byte) (int, error) {
	// Read new data: try a limited number of times.
	//
	// This technique is copied from bufio.Reader.
	for i := messageMaxConsecutiveEmptyReads; i > 0; i-- {
		n, err := reader.Read(buf)
		if n < 0 {
			panic(errNegativeRead)
		}
		if err != nil {
			return -1, err
		}
		if n > 0 {
			return n, nil
		}
	}
	return -1, io.ErrNoProgress
}

const (
	messageWordSize                 = 8
	messageHeaderSize               = messageWordSize
	messageMaxConsecutiveEmptyReads = 100
)
