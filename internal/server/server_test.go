package server_test

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/CanonicalLtd/dqlite/internal/server"
	"github.com/stretchr/testify/require"
)

func TestServer(t *testing.T) {
	protocol := server.Protocol
	server, err := server.New()
	require.NoError(t, err)
	defer server.Stop()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		conn, err := listener.Accept()
		require.NoError(t, err)
		require.NoError(t, server.Handle(conn))
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)

	err = binary.Write(conn, binary.LittleEndian, protocol)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
}
