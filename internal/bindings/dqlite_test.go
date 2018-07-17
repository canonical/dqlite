package bindings_test

import (
	"encoding/binary"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/bindings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServer_Lifecycle(t *testing.T) {
	file, cleanup := newTestFile(t)
	defer cleanup()

	cluster := newTestCluster()

	server, err := bindings.NewServer(file, cluster)
	require.NoError(t, err)

	server.Close()
	server.Free()
}

func TestServer_Run(t *testing.T) {
	file, cleanup := newTestFile(t)
	defer cleanup()

	cluster := newTestCluster()

	server, err := bindings.NewServer(file, cluster)
	require.NoError(t, err)

	defer server.Free()
	defer server.Close()

	ch := make(chan error)
	go func() {
		err := server.Run()
		ch <- err
	}()

	require.True(t, server.Ready())
	require.NoError(t, server.Stop())

	assert.NoError(t, <-ch)
}

func TestServer_Handle(t *testing.T) {
	file, cleanup := newTestFile(t)
	defer cleanup()

	cluster := newTestCluster()

	server, err := bindings.NewServer(file, cluster)
	require.NoError(t, err)

	defer server.Free()
	defer server.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	ch := make(chan error)
	go func() {
		err := server.Run()
		ch <- err
	}()

	require.True(t, server.Ready())

	go func() {
		conn, err := listener.Accept()
		require.NoError(t, err)
		require.NoError(t, server.Handle(conn))
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)

	// Handshake
	err = binary.Write(conn, binary.LittleEndian, bindings.ProtocolVersion)
	require.NoError(t, err)

	// Make a Leader request
	err = binary.Write(conn, binary.LittleEndian, uint32(1)) // Words
	require.NoError(t, err)
	n, err := conn.Write([]byte{bindings.RequestLeader, 0, 0, 0}) // Type, flags, extra
	require.NoError(t, err)
	require.Equal(t, 4, n)
	n, err = conn.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0}) // Unused single-word request payload
	require.NoError(t, err)
	require.Equal(t, 8, n)

	// Read the response
	buf := make([]byte, 64)
	n, err = conn.Read(buf)
	require.NoError(t, err)

	require.NoError(t, conn.Close())

	require.NoError(t, server.Stop())

	err = <-ch
	assert.NoError(t, err)
}

func TestServer_ConcurrentHandleAndClose(t *testing.T) {
	file, cleanup := newTestFile(t)
	defer cleanup()

	cluster := newTestCluster()

	server, err := bindings.NewServer(file, cluster)
	require.NoError(t, err)

	defer server.Free()
	defer server.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	runCh := make(chan error)
	go func() {
		err := server.Run()
		runCh <- err
	}()

	require.True(t, server.Ready())

	acceptCh := make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		require.NoError(t, err)
		server.Handle(conn)
		acceptCh <- struct{}{}
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)

	require.NoError(t, conn.Close())

	require.NoError(t, server.Stop())

	assert.NoError(t, <-runCh)
	<-acceptCh
}

func TestVfs_Content(t *testing.T) {
	vfs, err := bindings.NewVfs("test")
	require.NoError(t, err)

	defer vfs.Close()

	conn, err := bindings.Open("test.db", "test")
	require.NoError(t, err)

	err = conn.Exec("CREATE TABLE foo (n INT)")
	require.NoError(t, err)

	// Dump the in-memory files to a temporary directory.
	dir, err := ioutil.TempDir("", "dqlite-bindings-")
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	bytes, err := vfs.Content("test.db")
	require.NoError(t, err)

	err = ioutil.WriteFile(filepath.Join(dir, "test.db"), bytes, 0600)
	require.NoError(t, err)

	bytes, err = vfs.Content("test.db-wal")
	require.NoError(t, err)

	err = ioutil.WriteFile(filepath.Join(dir, "test.db-wal"), bytes, 0600)
	require.NoError(t, err)

	require.NoError(t, conn.Close())

	// Open the files that we have dumped and check that the table we
	// created is there.
	conn, err = bindings.Open(filepath.Join(dir, "test.db"), "unix")
	require.NoError(t, err)

	rows, err := conn.Query("SELECT * FROM foo")
	require.NoError(t, err)

	assert.Equal(t, io.EOF, rows.Next(nil))

	require.NoError(t, conn.Close())
}

func newTestFile(t *testing.T) (*os.File, func()) {
	t.Helper()

	file, err := ioutil.TempFile("", "dqlite-bindings-")
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, file.Close())
		require.NoError(t, os.Remove(file.Name()))
	}

	return file, cleanup
}

type testCluster struct {
}

func newTestCluster() *testCluster {
	return &testCluster{}
}

func (c *testCluster) Replication() string {
	return "test"
}

func (c *testCluster) Leader() string {
	return "127.0.0.1:666"
}

func (c *testCluster) Servers() ([]string, error) {
	addresses := []string{
		"1.2.3.4:666",
		"5.6.7.8:666",
	}

	return addresses, nil
}

func (c *testCluster) Register(*bindings.Conn) {
}

func (c *testCluster) Unregister(*bindings.Conn) {
}

func (c *testCluster) Barrier() error {
	return nil
}

func (c *testCluster) Recover(token uint64) error {
	return nil
}
