package replication_test

import (
	"database/sql/driver"
	"path/filepath"
	"testing"
	"time"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/internal/log"
	"github.com/CanonicalLtd/dqlite/internal/replication"
	"github.com/CanonicalLtd/dqlite/internal/transaction"
	"github.com/CanonicalLtd/go-sqlite3x"
	"github.com/CanonicalLtd/raft-test"
	"github.com/hashicorp/raft"
	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Check that the WAL is actually replicated.
func TestIntegration_Replication(t *testing.T) {
	cluster := newCluster(t)
	defer cluster.Cleanup()

	// Get a leader connection on the leader node.
	i := cluster.Notify.NextAcquired(time.Second)
	conn := cluster.OpenConn(i, 1000)

	// Run a WAL-writing query.
	_, err := conn.Exec("CREATE TABLE test (n INT)", nil)
	require.NoError(t, err)

	// Wait for a follower to replicate the WAL.
	j := rafttest.Other(cluster.Rafts, i)
	cluster.Watcher.WaitIndex(j, cluster.Rafts[i].AppliedIndex(), time.Second)

	// Get a leader conneciton on the follower and check that the change is
	// there.
	conn = cluster.OpenConn(j, 1000)
	_, err = conn.Exec("SELECT * FROM test", nil)
	assert.NoError(t, err)
}

// If the raft Apply() method fails, the query returns an error and the pending
// transaction is removed from the registry.
func TestIntegration_RaftApplyErrorRemovePendingTxn(t *testing.T) {
	cluster := newCluster(t)
	defer cluster.Cleanup()

	// Get a leader connection on the leader node.
	i := cluster.Notify.NextAcquired(time.Second)
	conn := cluster.OpenConn(i, 1000)

	// Disconnect the leader
	cluster.Network.Disconnect(i)

	_, err := conn.Exec("CREATE TABLE test (n INT)", nil)
	assert.EqualError(t, err, sqlite3x.ErrNotLeader.Error())

	fsm := cluster.FSMs[i]
	assert.Nil(t, fsm.Transactions().GetByConn(conn))
}

// Exercise automatically rolling back transactions failing because the leader
// has been deposed.
func TestIntegration_RaftApplyErrorWithInflightTxnAndRecoverOnNewLeader(t *testing.T) {
	cluster := newCluster(t)
	defer cluster.Cleanup()

	// Get a leader connection on the leader node.
	i := cluster.Notify.NextAcquired(time.Second)
	conn := cluster.OpenConn(i, 1000)

	// Start a write transaction.
	_, err := conn.Exec("BEGIN; CREATE TABLE test (n INT)", nil)
	require.NoError(t, err)

	// Disconnect the leader
	cluster.Network.Disconnect(i)

	// Try finish the transaction.
	_, err = conn.Exec("INSERT INTO test VALUES(1); COMMIT", nil)
	require.EqualError(t, err, sqlite3x.ErrNotLeader.Error())

	// Wait for a follower be promoted and catch up with logs.
	j := cluster.Notify.NextAcquired(time.Second)
	require.NoError(t, cluster.Rafts[j].Barrier(time.Second).Error())

	// XXX here we need to re-run the CREATE TABLE statement
	// because there are two failure modes for the first CREATE
	// TABLE: one is that the raft leadership is lost and the log
	// entry is not committed to the cluster log (in that case the
	// test table is not there on the new leader), the second is
	// that leadership is lost but still the log entry gets
	// committed to the other nodes, and in that case the table is
	// there on the new leader.
	conn = cluster.OpenConn(j, 1000)
	_, err = conn.Exec(
		"BEGIN; CREATE TABLE IF NOT EXISTS test (n INT); INSERT INTO test VALUES(2); COMMIT", nil)
	assert.NoError(t, err)

}

// Exercise creating and restoring snapshots.
func TestIntegration_Snapshot(t *testing.T) {
	config := rafttest.Config(func(n int, config *raft.Config) {
		config.SnapshotInterval = time.Second
		config.SnapshotThreshold = 10
		config.TrailingLogs = 2
	})
	cluster := newCluster(t, config)
	defer cluster.Cleanup()

	// Get a leader connection on the leader node.
	i := cluster.Notify.NextAcquired(time.Second)
	conn := cluster.OpenConn(i, 1000)

	// Disconnect a follower, so it will be forced to use the snapshot at
	// reconnection.
	j := rafttest.Other(cluster.Rafts, i)
	cluster.Network.Disconnect(j)

	// Run a couple of WAL-writing queries.
	_, err := conn.Exec("CREATE TABLE test (n INT)", nil)
	require.NoError(t, err)
	_, err = conn.Exec("INSERT INTO test VALUES(1)", nil)
	require.NoError(t, err)
	_, err = conn.Exec("INSERT INTO test VALUES(2)", nil)
	require.NoError(t, err)

	// Get the index of the alive follower.
	k := rafttest.Other(cluster.Rafts, i, j)

	// At this point node i and k shouldn't have made any snapshot yet.
	require.Equal(t, uint64(0), cluster.Watcher.LastSnapshot(i))
	require.Equal(t, uint64(0), cluster.Watcher.LastSnapshot(k))

	// Make sure snapshot is taken by the leader and the follower.
	cluster.Watcher.WaitSnapshot(i, 1, 3*time.Second)
	cluster.Watcher.WaitSnapshot(k, 1, 3*time.Second)

	// Reconnect the disconnected node and wait for it to catch up.
	cluster.Network.Reconnect(j)
	cluster.Watcher.WaitRestore(j, 1, 3*time.Second)

	// FIXME: not sure why a sleep is needed here, but without it the query
	// below occasionally fails with "no such table: 'test'", maybe
	// because the sqlite files haven't been synced to disk yet.
	time.Sleep(250 * time.Millisecond)

	// Verify that the follower has the expected data.
	conn = cluster.OpenConn(j, 1000)
	rows, err := conn.Query("SELECT n FROM test", nil)
	require.NoError(t, err)
	defer rows.Close()
	values := make([]driver.Value, 1)
	require.NoError(t, rows.Next(values))
	assert.Equal(t, int64(1), values[0].(int64))
	require.NoError(t, rows.Next(values))
	assert.Equal(t, int64(2), values[0].(int64))
}

type cluster struct {
	FSMs    []*replication.FSM
	Watcher *rafttest.FSMWatcherAPI
	Notify  *rafttest.NotifyKnob  // Notifications of leadership changes.
	Network *rafttest.NetworkKnob // Network control
	Rafts   []*raft.Raft          // Raft nodes.
	conns   []*sqlite3.SQLiteConn // Leader connections.

	cleanups []func()
	methods  []*replication.Methods
	t        *testing.T
}

// Create a new test cluster with 3 nodes, each of each has its own FSM,
// Methods and SQLiteConn in leader mode.
func newCluster(t *testing.T, knobs ...rafttest.Knob) *cluster {
	// All cleanups.
	succeeded := false
	cleanups := []func(){}
	defer func() {
		// If something goes wrong, cleanup the state we created so far.
		if succeeded == false {
			runCleanups(cleanups)
		}
	}()

	// FSMs.
	fsms := make([]*replication.FSM, 3)
	for i := range fsms {
		logger := log.New(log.Testing(t, 1), log.Trace)
		dir, cleanup := newDir(t)
		connections := connection.NewRegistry()
		transactions := transaction.NewRegistry()
		fsm := replication.NewFSM(logger, dir, connections, transactions)
		cleanups = append(cleanups, cleanup)
		fsms[i] = fsm
	}

	// Raft nodes.
	notify := rafttest.Notify()
	network := rafttest.Network()
	knobs = append(knobs, notify, network)
	raftFSMs := []raft.FSM{fsms[0], fsms[1], fsms[2]}
	watcher := rafttest.FSMWatcher(t, raftFSMs)
	rafts, cleanup := rafttest.Cluster(t, raftFSMs, knobs...)
	cleanups = append(cleanups, cleanup)

	// Methods
	methods := make([]*replication.Methods, 3)
	for i := range methods {
		raft := rafts[i]
		logger := log.New(log.Testing(t, 1), log.Trace)
		fsm := fsms[i]
		connections := fsm.Connections()
		transactions := fsm.Transactions()
		methods[i] = replication.NewMethods(raft, logger, connections, transactions)
	}

	// Leader connections.
	conns := make([]*sqlite3.SQLiteConn, 3)

	succeeded = true
	return &cluster{
		FSMs:     fsms,
		Watcher:  watcher,
		Notify:   notify,
		Network:  network,
		Rafts:    rafts,
		conns:    conns,
		cleanups: cleanups,
		methods:  methods,
		t:        t,
	}
}

func (c *cluster) Cleanup() {
	for i := range c.conns {
		c.CloseConn(i)
	}
	runCleanups(c.cleanups)
}

// Open a leader connection on node i.
func (c *cluster) OpenConn(i int, autoCheckpoint int) *sqlite3.SQLiteConn {
	fsm := c.FSMs[i]
	methods := c.methods[i]
	path := filepath.Join(fsm.Dir(), "test.db")
	conn, err := connection.OpenLeader(path, methods, autoCheckpoint)
	require.NoError(c.t, err)
	fsm.Connections().AddLeader("test.db", conn)
	c.conns[i] = conn
	return conn
}

// Close the leader connection associated with the i-th node.
func (c *cluster) CloseConn(i int) {
	if c.conns[i] == nil {
		return
	}
	conn := c.conns[i]
	fsm := c.FSMs[i]
	fsm.Connections().DelLeader(conn)
	connection.CloseLeader(conn)
}

// Execute the given cleanup functions in reverse order.
func runCleanups(cleanups []func()) {
	for i := len(cleanups) - 1; i >= 0; i-- {
		cleanups[i]()
	}
}
