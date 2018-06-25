package client

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestMessage_StaticBytesAlignment(t *testing.T) {
	message := Message{}
	message.Init(4096)
	pointer := uintptr(unsafe.Pointer(&message.body1.Bytes))
	assert.Equal(t, pointer%messageWordSize, uintptr(0))
}
