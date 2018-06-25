package client_test

import (
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/bindings"
	"github.com/CanonicalLtd/dqlite/internal/client"
	"github.com/stretchr/testify/assert"
)

func TestMessage_PutString(t *testing.T) {
	cases := []struct {
		String string
		Offset int
	}{
		{"hello", 8},
		{"hello!!", 8},
		{"hello world", 16},
	}

	for _, c := range cases {
		t.Run(c.String, func(t *testing.T) {
			message := client.Message{}
			message.Init(16)
			message.PutString(c.String)

			bytes, offset := message.Body1()

			assert.Equal(t, string(bytes[:len(c.String)]), c.String)
			assert.Equal(t, offset, c.Offset)
		})
	}
}

func TestMessage_PutUint8(t *testing.T) {
	message := client.Message{}
	message.Init(8)

	v := uint8(12)

	message.PutUint8(v)

	bytes, offset := message.Body1()

	assert.Equal(t, bytes[0], byte(v))

	assert.Equal(t, offset, 1)
}

func TestMessage_PutUint16(t *testing.T) {
	message := client.Message{}
	message.Init(8)

	v := uint16(666)

	message.PutUint16(v)

	bytes, offset := message.Body1()

	assert.Equal(t, bytes[0], byte((v & 0x00ff)))
	assert.Equal(t, bytes[1], byte((v&0xff00)>>8))

	assert.Equal(t, offset, 2)
}

func TestMessage_PutUint32(t *testing.T) {
	message := client.Message{}
	message.Init(8)

	v := uint32(130000)

	message.PutUint32(v)

	bytes, offset := message.Body1()

	assert.Equal(t, bytes[0], byte((v & 0x000000ff)))
	assert.Equal(t, bytes[1], byte((v&0x0000ff00)>>8))
	assert.Equal(t, bytes[2], byte((v&0x00ff0000)>>16))
	assert.Equal(t, bytes[3], byte((v&0xff000000)>>24))

	assert.Equal(t, offset, 4)
}

func TestMessage_PutUint64(t *testing.T) {
	message := client.Message{}
	message.Init(8)

	v := uint64(5000000000)

	message.PutUint64(v)

	bytes, offset := message.Body1()

	assert.Equal(t, bytes[0], byte((v & 0x00000000000000ff)))
	assert.Equal(t, bytes[1], byte((v&0x000000000000ff00)>>8))
	assert.Equal(t, bytes[2], byte((v&0x0000000000ff0000)>>16))
	assert.Equal(t, bytes[3], byte((v&0x00000000ff000000)>>24))
	assert.Equal(t, bytes[4], byte((v&0x000000ff00000000)>>32))
	assert.Equal(t, bytes[5], byte((v&0x0000ff0000000000)>>40))
	assert.Equal(t, bytes[6], byte((v&0x00ff000000000000)>>48))
	assert.Equal(t, bytes[7], byte((v&0xff00000000000000)>>56))

	assert.Equal(t, offset, 8)
}

func TestMessage_PutNamedValues(t *testing.T) {
	message := client.Message{}
	message.Init(256)

	values := client.NamedValues{
		{Ordinal: 1, Value: int64(123)},
		{Ordinal: 2, Value: "hello"},
		{Ordinal: 3, Value: nil},
	}

	message.PutNamedValues(values)

	bytes, offset := message.Body1()

	assert.Equal(t, offset, 32)
	assert.Equal(t, bytes[0], byte(3))
	assert.Equal(t, bytes[1], byte(bindings.Integer))
	assert.Equal(t, bytes[2], byte(bindings.Text))
	assert.Equal(t, bytes[3], byte(bindings.Null))
}

func BenchmarkMessage_PutString(b *testing.B) {
	message := client.Message{}
	message.Init(4096)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		message.Reset()
		message.PutString("hello")
	}
}

func BenchmarkMessage_PutUint64(b *testing.B) {
	message := client.Message{}
	message.Init(4096)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		message.Reset()
		message.PutUint64(270)
	}
}

func TestMessage_GetString(t *testing.T) {
	cases := []struct {
		String string
		Offset int
	}{
		{"hello", 8},
		{"hello!!", 8},
		{"hello world", 16},
	}

	for _, c := range cases {
		t.Run(c.String, func(t *testing.T) {
			message := client.Message{}
			message.Init(16)

			message.PutString(c.String)
			message.PutHeader(0)
			message.Reset()

			s := message.GetString()

			_, offset := message.Body1()

			assert.Equal(t, s, c.String)
			assert.Equal(t, offset, c.Offset)
		})
	}
}
