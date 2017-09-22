package replication_test

import (
	"fmt"
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
	conn := cluster.Conns[i]

	// Run a WAL-writing query.
	_, err := conn.Exec("CREATE TABLE test (n INT)", nil)
	require.NoError(t, err)

	// Wait for a follower to replicate the WAL.
	j := rafttest.Other(cluster.Rafts, i)
	fsm := cluster.FSMs[j]
	fsm.Wait(cluster.Rafts[i].AppliedIndex())

	// Get a leader conneciton on the follower and check that the change is
	// there.
	conn = cluster.Conns[j]
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
	conn := cluster.Conns[i]

	// Disconnect the leader
	cluster.Network.Disconnect(i)

	_, err := conn.Exec("CREATE TABLE test (n INT)", nil)
	assert.EqualError(t, err, sqlite3x.ErrNotLeader.Error())

	fsm := cluster.FSMs[i]
	assert.Nil(t, fsm.Transactions().GetByConn(conn))
}

func TestIntegration_RaftApplyErrorWithInflightTxnAndRecoverOnNewLeader(t *testing.T) {
	cluster := newCluster(t)
	defer cluster.Cleanup()

	// Get a leader connection on the leader node.
	i := cluster.Notify.NextAcquired(time.Second)
	conn := cluster.Conns[i]

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
	conn = cluster.Conns[j]
	_, err = conn.Exec(
		"BEGIN; CREATE TABLE IF NOT EXISTS test (n INT); INSERT INTO test VALUES(2); COMMIT", nil)
	assert.NoError(t, err)

}

type cluster struct {
	FSMs    []*replication.FSM
	Notify  *rafttest.NotifyKnob  // Notifications of leadership changes.
	Network *rafttest.NetworkKnob // Network control
	Rafts   []*raft.Raft          // Raft nodes.
	Conns   []*sqlite3.SQLiteConn // Leader connections.

	cleanups []func()
	methods  []*replication.Methods
}

// Create a new test cluster with 3 nodes, each of each has its own FSM,
// Methods and SQLiteConn in leader mode.
func newCluster(t *testing.T) *cluster {
	// All cleanups.
	cleanups := []func(){}

	// Base logger, will be augmented with node-specific prefixes.
	logger := log.New(log.Testing(t), log.Trace)

	// FSMs.
	fsms := make([]*replication.FSM, 3)
	for i := range fsms {
		logger := logger.Augment(fmt.Sprintf("node %d", i))
		dir, cleanup := newDir(t)
		connections := connection.NewRegistry()
		transactions := transaction.NewRegistry()
		fsm := replication.NewFSM(logger, dir, connections, transactions)
		fsm.WithNotify()
		cleanups = append(cleanups, cleanup)
		fsms[i] = fsm
	}

	// Raft nodes.
	notify := rafttest.Notify()
	network := rafttest.Network()
	rafts, cleanup := rafttest.Cluster(t, []raft.FSM{fsms[0], fsms[1], fsms[2]}, notify, network)
	cleanups = append(cleanups, cleanup)

	// Methods and leader connections.
	methods := make([]*replication.Methods, 3)
	conns := make([]*sqlite3.SQLiteConn, 3)
	for i := range methods {
		raft := rafts[i]
		logger = logger.Augment(fmt.Sprintf("node %d", i))
		fsm := fsms[i]
		connections := fsm.Connections()
		transactions := fsm.Transactions()
		methods[i] = replication.NewMethods(raft, logger, connections, transactions)

		conn, cleanup := newLeaderConn(t, fsm.Dir(), methods[i])
		cleanups = append(cleanups, cleanup)
		conns[i] = conn

		methods[i].Connections().AddLeader("test.db", conn)
	}

	return &cluster{
		FSMs:     fsms,
		Notify:   notify,
		Network:  network,
		Rafts:    rafts,
		Conns:    conns,
		cleanups: cleanups,
		methods:  methods,
	}
}

func (c *cluster) Cleanup() {
	// Execute the cleanups in reverse order.
	for i := len(c.cleanups) - 1; i >= 0; i-- {
		c.cleanups[i]()
	}
}
