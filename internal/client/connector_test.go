package client_test

import (
	"context"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/CanonicalLtd/dqlite/internal/bindings"
	"github.com/CanonicalLtd/dqlite/internal/client"
	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestConnector(t *testing.T) {
	connector, cleanup := newConnector(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	client, err := connector.Connect(ctx)

	require.NoError(t, err)
	client.Close()
}

func newConnector(t *testing.T) (*client.Connector, func()) {
	t.Helper()

	addr, cleanup := newServer(t)

	store := newStore(t, []string{addr.String()})

	config := client.Config{
		AttemptTimeout: 100 * time.Millisecond,
		RetryStrategies: []strategy.Strategy{
			strategy.Backoff(backoff.BinaryExponential(time.Millisecond)),
		},
	}

	logger := zaptest.NewLogger(t)

	connector := client.NewConnector(0, store, config, logger)

	return connector, cleanup
}

// Create a new in-memory server store populated with the given addresses.
func newStore(t *testing.T, addresses []string) client.ServerStore {
	t.Helper()

	store := client.NewInmemServerStore()
	require.NoError(t, store.Set(context.Background(), addresses))

	return store
}

func newServer(t *testing.T) (net.Addr, func()) {
	t.Helper()

	file, fileCleanup := newFile(t)
	cluster := newTestCluster()

	server, err := bindings.NewServer(file, cluster)
	require.NoError(t, err)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	cluster.leader = listener.Addr().String()

	runCh := make(chan error)
	go func() {
		err := server.Run()
		runCh <- err
	}()

	require.True(t, server.Ready())

	acceptCh := make(chan error)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			acceptCh <- nil
			return
		}
		err = server.Handle(conn)
		if err == bindings.ErrServerStopped {
			acceptCh <- nil
			return
		}

		acceptCh <- err
	}()

	cleanup := func() {
		require.NoError(t, server.Stop())

		require.NoError(t, listener.Close())

		// Wait for the accept goroutine to exit.
		select {
		case err := <-acceptCh:
			assert.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("accept goroutine did not stop within a second")
		}

		// Wait for the run goroutine to exit.
		select {
		case err := <-runCh:
			assert.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("server did not stop within a second")
		}

		server.Close()
		server.Free()

		fileCleanup()
	}

	return listener.Addr(), cleanup
}

func newFile(t *testing.T) (*os.File, func()) {
	t.Helper()

	file, err := ioutil.TempFile("", "dqlite-bindings-")
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, file.Close())

		bytes, err := ioutil.ReadFile(file.Name())
		require.NoError(t, err)

		t.Logf("server log:\n%s\n", string(bytes))

		require.NoError(t, os.Remove(file.Name()))
	}

	return file, cleanup
}

type testCluster struct {
	leader string
}

func newTestCluster() *testCluster {
	return &testCluster{}
}

func (c *testCluster) Leader() string {
	return c.leader
}

func (c *testCluster) Servers() ([]string, error) {
	addresses := []string{
		"1.2.3.4:666",
		"5.6.7.8:666",
	}

	return addresses, nil
}

func (c *testCluster) Recover(token uint64) error {
	return nil
}

func init() {
	err := bindings.Init()
	if err != nil {
		panic(errors.Wrap(err, "failed to initialize dqlite"))
	}
}
