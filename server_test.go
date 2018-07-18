package dqlite_test

import (
	"context"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/CanonicalLtd/dqlite"
	"github.com/CanonicalLtd/dqlite/internal/bindings"
	"github.com/CanonicalLtd/dqlite/internal/client"
	"github.com/CanonicalLtd/raft-test"
	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestServer_Dump(t *testing.T) {
	// Start a server and connect to it.
	listener := newListener(t)
	server, cleanup := newServer(t, listener)
	defer cleanup()

	address := listener.Addr().String()
	store := newStore(t, address)
	config := client.Config{
		Dial:           client.TCPDial,
		AttemptTimeout: 100 * time.Millisecond,
		RetryStrategies: []strategy.Strategy{
			strategy.Backoff(backoff.BinaryExponential(time.Millisecond)),
		},
	}
	logger := zaptest.NewLogger(t)

	connector := client.NewConnector(0, store, config, logger)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	c, err := connector.Connect(ctx)
	require.NoError(t, err)

	// Open a database and create a test table.
	request := client.Message{}
	request.Init(4096)

	response := client.Message{}
	response.Init(4096)

	flags := uint64(bindings.OpenReadWrite | bindings.OpenCreate)
	client.EncodeOpen(&request, "test.db", flags, "volatile")

	err = c.Call(ctx, &request, &response)
	require.NoError(t, err)

	db, err := client.DecodeDb(&response)
	require.NoError(t, err)

	request.Reset()
	response.Reset()

	client.EncodeExecSQL(&request, uint64(db), "CREATE TABLE foo (n INT)", nil)

	err = c.Call(ctx, &request, &response)
	require.NoError(t, err)

	// Dump the database to disk.
	dir, err := ioutil.TempDir("", "dqlite-server-")
	require.NoError(t, err)

	defer os.RemoveAll(dir)

	err = server.Dump("test.db", dir)
	require.NoError(t, err)

	require.NoError(t, c.Close())
}

// Create a new in-memory server store populated with the given addresses.
func newStore(t *testing.T, address string) *dqlite.DatabaseServerStore {
	t.Helper()

	store, err := dqlite.DefaultServerStore(":memory:")
	require.NoError(t, err)

	require.NoError(t, store.Set(context.Background(), []string{address}))

	return store
}

func newServer(t *testing.T, listener net.Listener) (*dqlite.Server, func()) {
	t.Helper()

	registry := dqlite.NewRegistry("0")

	fsm := dqlite.NewFSM(registry)

	r, raftCleanup := rafttest.Server(t, fsm, rafttest.Transport(func(i int) raft.Transport {
		require.Equal(t, i, 0)
		address := raft.ServerAddress(listener.Addr().String())
		_, transport := raft.NewInmemTransport(address)
		return transport
	}))

	logger := zaptest.NewLogger(t)

	server, err := dqlite.NewServer(
		r, registry, listener,
		dqlite.WithServerLogger(logger))
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, server.Close())
		raftCleanup()
	}

	return server, cleanup
}

func newListener(t *testing.T) net.Listener {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	return listener
}
