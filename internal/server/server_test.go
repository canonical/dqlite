package server_test

import (
	"net"
	"testing"
	"time"

	"github.com/CanonicalLtd/dqlite/internal/server"
	"github.com/stretchr/testify/require"
)

func TestServer(t *testing.T) {
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

	conn.Write([]byte{0x39, 0xea, 0x93, 0xbf})
	//n, err := conn.Write([]byte("hello"))
	//require.NoError(t, err)
	//require.Equal(t, 5, n)
	time.Sleep(100 * time.Millisecond)
}
