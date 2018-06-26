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

// Successful connection.
func TestConnector_Connect_Success(t *testing.T) {
	listener := newListener(t)

	cluster := newTestCluster()
	cluster.leader = listener.Addr().String()

	cleanup := newServer(t, 0, listener, cluster)
	defer cleanup()

	store := newStore(t, []string{cluster.leader})

	connector := newConnector(t, store)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	client, err := connector.Connect(ctx)
	require.NoError(t, err)

	assert.NoError(t, client.Close())
}

// Connection failed because the server store is empty.
func TestConnector_Connect_Error_EmptyServerStore(t *testing.T) {
	store := newStore(t, []string{})

	connector := newConnector(t, store)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	_, err := connector.Connect(ctx)
	require.EqualError(t, err, "no available dqlite leader server found")
}

// Connection failed because the context was canceled.
func TestConnector_Connect_Error_AfterCancel(t *testing.T) {
	listener := newListener(t)

	cluster := newTestCluster()
	cluster.leader = listener.Addr().String()

	cleanup := newServer(t, 0, listener, cluster)
	defer cleanup()

	store := newStore(t, []string{cluster.leader})

	connector := newConnector(t, store)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := connector.Connect(ctx)
	assert.EqualError(t, err, "no available dqlite leader server found")
}

// If an election is in progress, the connector will retry until a leader gets
// elected.
func TestConnector_Connect_ElectionInProgress(t *testing.T) {
	listener1 := newListener(t)
	listener2 := newListener(t)
	listener3 := newListener(t)

	cluster1 := newTestCluster()
	cluster2 := newTestCluster()
	cluster3 := newTestCluster()

	defer newServer(t, 1, listener1, cluster2)()
	defer newServer(t, 2, listener2, cluster2)()
	defer newServer(t, 3, listener3, cluster3)()

	store := newStore(t, []string{
		listener1.Addr().String(),
		listener2.Addr().String(),
		listener3.Addr().String(),
	})

	connector := newConnector(t, store)

	go func() {
		// Simulate server 1 winning the election after 10ms
		time.Sleep(10 * time.Millisecond)
		cluster1.leader = listener1.Addr().String()
		cluster2.leader = listener1.Addr().String()
		cluster3.leader = listener1.Addr().String()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	client, err := connector.Connect(ctx)
	require.NoError(t, err)

	assert.NoError(t, client.Close())
}

// If a server reports that it knows about the leader, the hint will be taken
// and an attempt will be made to connect to it.
func TestConnector_Connect_ServerKnowsAboutLeader(t *testing.T) {
	listener1 := newListener(t)
	listener2 := newListener(t)
	listener3 := newListener(t)

	cluster1 := newTestCluster()
	cluster2 := newTestCluster()
	cluster3 := newTestCluster()

	defer newServer(t, 1, listener1, cluster2)()
	defer newServer(t, 2, listener2, cluster2)()
	defer newServer(t, 3, listener3, cluster3)()

	// Server 1 will be contacted first, which will report that server 2 is
	// the leader.
	store := newStore(t, []string{
		listener1.Addr().String(),
		listener2.Addr().String(),
		listener3.Addr().String(),
	})
	cluster1.leader = listener2.Addr().String()
	cluster2.leader = listener2.Addr().String()
	cluster3.leader = listener2.Addr().String()

	connector := newConnector(t, store)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	client, err := connector.Connect(ctx)
	require.NoError(t, err)

	assert.NoError(t, client.Close())
}

// If a server reports that it knows about the leader, the hint will be taken
// and an attempt will be made to connect to it. If that leader has died, the
// next target will be tried.
func TestConnector_Connect_ServerKnowsAboutDeadLeader(t *testing.T) {
	listener1 := newListener(t)
	listener2 := newListener(t)
	listener3 := newListener(t)

	cluster1 := newTestCluster()
	cluster2 := newTestCluster()
	cluster3 := newTestCluster()

	defer newServer(t, 1, listener1, cluster2)()
	defer newServer(t, 3, listener3, cluster3)()

	// Simulate server 2 crashing.
	require.NoError(t, listener2.Close())

	// Server 1 will be contacted first, which will report that server 2 is
	// the leader. However server 2 has crashed, and after a bit server 1
	// gets elected.
	store := newStore(t, []string{
		listener1.Addr().String(),
		listener2.Addr().String(),
		listener3.Addr().String(),
	})
	cluster1.leader = listener2.Addr().String()
	cluster2.leader = listener2.Addr().String()
	cluster3.leader = listener2.Addr().String()

	go func() {
		// Simulate server 1 becoming the new leader after server 2
		// crashed.
		time.Sleep(10 * time.Millisecond)
		cluster1.leader = listener1.Addr().String()
		cluster2.leader = listener1.Addr().String()
		cluster3.leader = listener1.Addr().String()
	}()

	connector := newConnector(t, store)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	client, err := connector.Connect(ctx)
	require.NoError(t, err)

	assert.NoError(t, client.Close())
}

// If a server reports that it knows about the leader, the hint will be taken
// and an attempt will be made to connect to it. If that leader is not actually
// the leader the next target will be tried.
func TestConnector_Connect_ServerKnowsAboutStaleLeader(t *testing.T) {
	listener1 := newListener(t)
	listener2 := newListener(t)
	listener3 := newListener(t)

	cluster1 := newTestCluster()
	cluster2 := newTestCluster()
	cluster3 := newTestCluster()

	defer newServer(t, 1, listener1, cluster2)()
	defer newServer(t, 3, listener3, cluster3)()

	// Simulate server 2 crashing.
	require.NoError(t, listener2.Close())

	// Server 1 will be contacted first, which will report that server 2 is
	// the leader. However server 2 thinks that 3 is the leader, and server
	// 3 is actually the leader.
	store := newStore(t, []string{
		listener1.Addr().String(),
		listener2.Addr().String(),
		listener3.Addr().String(),
	})
	cluster1.leader = listener2.Addr().String()
	cluster2.leader = listener3.Addr().String()
	cluster3.leader = listener3.Addr().String()

	connector := newConnector(t, store)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	client, err := connector.Connect(ctx)
	require.NoError(t, err)

	assert.NoError(t, client.Close())
}

func newConnector(t *testing.T, store client.ServerStore) *client.Connector {
	t.Helper()

	config := client.Config{
		AttemptTimeout: 100 * time.Millisecond,
		RetryStrategies: []strategy.Strategy{
			strategy.Backoff(backoff.BinaryExponential(time.Millisecond)),
		},
	}

	logger := zaptest.NewLogger(t)

	connector := client.NewConnector(0, store, config, logger)

	return connector
}

// Create a new in-memory server store populated with the given addresses.
func newStore(t *testing.T, addresses []string) client.ServerStore {
	t.Helper()

	store := client.NewInmemServerStore()
	require.NoError(t, store.Set(context.Background(), addresses))

	return store
}

func newServer(t *testing.T, index int, listener net.Listener, cluster bindings.Cluster) func() {
	t.Helper()

	file, fileCleanup := newFile(t)

	server, err := bindings.NewServer(file, cluster)
	require.NoError(t, err)

	runCh := make(chan error)
	go func() {
		err := server.Run()
		runCh <- err
	}()

	require.True(t, server.Ready())

	acceptCh := make(chan error, 1)
	go func() {
		for {
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
			if err != nil {
				acceptCh <- err
				return
			}
		}
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

	return cleanup
}

func newListener(t *testing.T) net.Listener {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	return listener
}

func newFile(t *testing.T) (*os.File, func()) {
	t.Helper()

	file, err := ioutil.TempFile("", "dqlite-client-")
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

func (c *testCluster) Replication() string {
	return "test"
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

func (c *testCluster) Register(*bindings.Conn) {
}

func (c *testCluster) Unregister(*bindings.Conn) {
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
