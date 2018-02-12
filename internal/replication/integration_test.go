// Copyright 2017 Canonical Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package replication_test

import (
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/internal/replication"
	"github.com/CanonicalLtd/go-sqlite3"
	"github.com/CanonicalLtd/raft-test"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Exercise various error conditions caused by lost raft leadership.
func TestIntegration_MethodHookLeadershipLost(t *testing.T) {
	// Indexses for important log commands for a "CREATE TABLE test (n INT)" query
	// issued on a fresh cluster.
	//
	// - index 3 is Open
	// - index 4 is Begin
	// - index 5 is WalFrames or Undo
	// - index 6 is End
	const (
		openIndex      = uint64(3)
		beginIndex     = uint64(4)
		walFramesIndex = uint64(5)
		undoIndex      = walFramesIndex
		endIndex       = uint64(5)
	)

	// Custom assertions for leadership lost errors happening upon
	// different hooks.
	assertBeginError := func(tx driver.Tx, err error) {
		// The error happens right away in the Exec statement.
		assert.EqualError(t, err, sqlite3.ErrNotLeader.Error())
		require.NoError(t, tx.Rollback())
	}
	assertWalFramesError := func(tx driver.Tx, err error) {
		// The error happens upon commit
		require.NoError(t, err)
		assert.EqualError(t, tx.Commit(), sqlite3.ErrNotLeader.Error())
	}
	assertUndoError := func(tx driver.Tx, err error) {
		require.NoError(t, err)
		// Rolling back always succeeds, since even if we fail to commit
		// the undo log command the transaction is aborted.
		require.NoError(t, tx.Rollback())
	}
	assertEndError := assertWalFramesError

	cases := []struct {
		hook               string                        // Method hook name that should fail
		disconnectUpon     uint64                        // Disconnect the leader upon applying this log index
		waitLeadershipLost bool                          // Whether to wait for leadership to be lost
		query              string                        // Query triggering the not-leader error.
		event              string                        // Expected entry in thetimeline.
		assert             func(tx driver.Tx, err error) // Assert the error returned by conn.Exec
	}{
		{
			"begin",
			0,
			true,
			"CREATE TABLE test (n INT)",
			"not leader",
			assertBeginError,
		},
		{
			"begin",
			0,
			false,
			"CREATE TABLE test (n INT)",
			"cmd=open apply error: leadership lost while committing log",
			assertBeginError,
		},
		{
			"begin",
			openIndex,
			true,
			"CREATE TABLE test (n INT)",
			"txn=3 cmd=begin apply error: node is not the leader",
			assertBeginError,
		},
		{
			"begin",
			openIndex,
			false,
			"CREATE TABLE test (n INT)",
			"txn=3 cmd=begin apply error: leadership lost while committing log",
			assertBeginError,
		},
		{
			"wal frames",
			beginIndex,
			true,
			"CREATE TABLE test (n INT)",
			"not leader",
			assertWalFramesError,
		},
		{
			"wal frames",
			beginIndex,
			false,
			"CREATE TABLE test (n INT)",
			"cmd=wal frames apply error: leadership lost while committing log",
			assertWalFramesError,
		},
		{
			"undo",
			beginIndex,
			true,
			"CREATE TABLE test (n INT)",
			"not leader",
			assertUndoError,
		},
		{
			"undo",
			beginIndex,
			false,
			"CREATE TABLE test (n INT)",
			"cmd=undo apply error: leadership lost while committing log",
			assertUndoError,
		},
		{
			"end",
			walFramesIndex,
			true,
			"CREATE TABLE test (n INT)",
			"not leader",
			assertEndError,
		},
		{
			"end",
			walFramesIndex,
			false,
			"CREATE TABLE test (n INT)",
			"cmd=end apply error: leadership lost while committing log",
			assertEndError,
		},
	}

	for _, c := range cases {
		title := fmt.Sprintf("%s-%d-%v", c.hook, c.disconnectUpon, c.waitLeadershipLost)
		t.Run(title, func(t *testing.T) {
			cluster := newCluster(t)
			defer cluster.Cleanup()

			// Get a leader connection on the leader node
			r1 := cluster.Control.LeadershipAcquired(time.Second)
			conn := cluster.OpenConn(r1)

			// Setup things to make the hook fail.
			disconnect := func() {
				cluster.Control.Disconnect(r1)
				if c.waitLeadershipLost {
					cluster.Control.LeadershipLost(r1, time.Second)
				}
			}
			if c.disconnectUpon == 0 {
				disconnect()
			} else {
				cluster.Control.ApplyHook(r1, c.disconnectUpon, func() {
					disconnect()
				})
			}

			// Run the query making the hook fail.
			tx, err := conn.Begin()
			require.NoError(t, err)
			_, err = conn.Exec(c.query, nil)
			c.assert(tx, err)

			// Check that the timeline has the expected event.
			event := fmt.Sprintf("hook=%s %s", c.hook, c.event)
			cluster.TimelineContains(r1, conn, event)

			// Check that the cluster is still functional without the node.
			if !c.waitLeadershipLost {
				cluster.Control.LeadershipLost(r1, time.Second)
			}
			r2 := cluster.Control.LeadershipAcquired(time.Second)
			require.NoError(t, r2.Barrier(time.Second).Error())

			query := "CREATE TABLE test (n INT)"

			// FIXME: when losing leadership while committing a wal
			// frames command, there is both the case when the log
			// gets actually committed and applied by the other
			// FSMs, and the case where it's not. Ideally we should
			// find a way to notify the clients about this, but for
			// now there's none.
			lastIndex := cluster.Control.AppliedIndex(r2)
			if c.hook == "wal frames" && c.waitLeadershipLost == false && lastIndex == walFramesIndex {
				// Since the wal frames log got through the table must exist.
				query = "INSERT INTO test (n) VALUES (1)"
			}

			// FIXME: even if the end hook fails, since the wal
			// frames command has commit=1 the table is
			// visible. This needs investigation and fixing.
			if c.hook == "end" {
				query = "INSERT INTO test (n) VALUES (1)"
			}

			conn = cluster.OpenConn(r2)
			_, err = conn.Exec(query, nil)
			require.NoError(t, err)

			// Check that the disconnected node can re-join and catch up.
			cluster.Control.Reconnect(r1)
			cluster.StillFunctional()
		})
	}
}

// If a leader connections tries to start a transaction while another leader
// connection has an in-progress transaction, an error is returned.
func TestIntegration_BeginConcurrent(t *testing.T) {
	cluster := newCluster(t)
	defer cluster.Cleanup()

	// Get a leader connection on the leader node.
	r1 := cluster.Control.LeadershipAcquired(time.Second)
	conn1 := cluster.OpenConn(r1)
	conn2 := cluster.OpenConn(r1)

	// Start a transaction on the first connection.
	tx1, err := conn1.Begin()
	require.NoError(t, err)
	_, err = conn1.Exec("CREATE TABLE test (n INT)", nil)
	require.NoError(t, err)

	// Try to start a transaction on the second connection. It will be
	// retried but will exhaust _busy_timeout, since the first transaction
	// is still in-flight.
	tx2, err := conn2.Begin()
	require.NoError(t, err)
	_, err = conn2.Exec("CREATE TABLE test2 (n INT)", nil)
	assert.EqualError(t, err, sqlite3.ErrBusy.Error())

	require.NoError(t, tx1.Commit())
	require.NoError(t, tx2.Rollback())

	cluster.TimelineContains(r1, conn2, "hook=begin txn=4 a transaction is already in progress")
	cluster.StillFunctional()
}

// If a leader connections tries to start a transaction while another leader
// connection has an in-progress transaction, a few attempts are made by the
// go-sqlite3 driver (according to the _busy_timeout option).
func TestIntegration_BeginConcurrentRetry(t *testing.T) {
	cluster := newCluster(t)
	defer cluster.Cleanup()

	// Get a leader connection on the leader node.
	r1 := cluster.Control.LeadershipAcquired(time.Second)
	conn1 := cluster.OpenConn(r1)
	conn2 := cluster.OpenConn(r1)

	// Start a transaction on the first connection.
	tx1, err := conn1.Begin()
	require.NoError(t, err)
	_, err = conn1.Exec("CREATE TABLE test (n INT)", nil)
	require.NoError(t, err)

	// Start a transaction on the second connection, in a different
	// goroutine. Tit will be retried and eventually succeed before
	// _busy_timeout expires, since we're going to commit the first
	// transaction in the meantime.
	tx2, err := conn2.Begin()
	require.NoError(t, err)
	ch := make(chan struct{})
	go func() {
		_, err = conn2.Exec("CREATE TABLE test2 (n INT)", nil)
		require.NoError(t, err)
		ch <- struct{}{}
	}()

	// Sleep a bit before committing, to force the second transaction to be
	// retried.
	time.Sleep(250 * time.Millisecond)
	require.NoError(t, tx1.Commit())

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("concurrent transaction did not returned within a second")
	}

	require.NoError(t, tx2.Commit())

	cluster.TimelineContains(r1, conn2, "hook=begin txn=4 a transaction is already in progress")
	cluster.StillFunctional()
}

// If applying an undo command for a leftover follower transaction fails, an
// error is returned.
func TestIntegration_BeginLeftoverFollowerUndoFailure(t *testing.T) {
	cluster := newCluster(t)
	defer cluster.Cleanup()

	// Get a leader connection on the leader node.
	r1 := cluster.Control.LeadershipAcquired(time.Second)
	conn := cluster.OpenConn(r1)

	// Start a write transaction.
	_, err := conn.Exec("BEGIN; CREATE TABLE test (n INT)", nil)
	require.NoError(t, err)

	// Disconnect the leader
	cluster.Control.Disconnect(r1)
	cluster.Control.LeadershipLost(r1, time.Second)

	// Try finish the transaction.
	_, err = conn.Exec("INSERT INTO test VALUES(1); COMMIT", nil)
	require.EqualError(t, err, sqlite3.ErrNotLeader.Error())

	// Wait for a follower be promoted and catch up with logs.
	r2 := cluster.Control.LeadershipAcquired(time.Second)
	require.NoError(t, r2.Barrier(time.Second).Error())

	// Disconnect also the other follower to trigger an apply failure.
	r3 := cluster.Control.Other(r1, r2)
	cluster.Control.Disconnect(r3)

	conn = cluster.OpenConn(r2)
	_, err = conn.Exec("CREATE TABLE test (n INT)", nil)
	assert.EqualError(t, err, sqlite3.ErrNotLeader.Error())

	cluster.TimelineContains(r2, conn, "hook=begin txn=6 undo stale transaction 3")
	cluster.TimelineContains(r2, conn, "hook=begin txn=6 cmd=undo apply error: leadership lost while committing log")

	// Reconnect node i and k.
	cluster.Control.Reconnect(r1)
	cluster.Control.Reconnect(r3)
	cluster.StillFunctional()
}

// If applying an end command for a leftover follower transaction fails, an
// error is returned.
func TestIntegration_BeginLeftoverFollowerEndFailure(t *testing.T) {
	cluster := newCluster(t)
	defer cluster.Cleanup()

	// Get a leader connection on the leader node.
	r1 := cluster.Control.LeadershipAcquired(time.Second)
	conn := cluster.OpenConn(r1)

	// Start a write transaction.
	_, err := conn.Exec("BEGIN; CREATE TABLE test (n INT)", nil)
	require.NoError(t, err)

	// Disconnect the leader
	cluster.Control.Disconnect(r1)
	cluster.Control.LeadershipLost(r1, time.Second)

	// Try finish the transaction.
	_, err = conn.Exec("INSERT INTO test VALUES(1); COMMIT", nil)
	require.EqualError(t, err, sqlite3.ErrNotLeader.Error())

	// Wait for a follower be promoted and catch up with logs.
	//cluster.Control.LeadershipLost(r1, time.Second)
	r2 := cluster.Control.LeadershipAcquired(time.Second)
	require.NoError(t, r2.Barrier(time.Second).Error())

	// Disconnect the new leader during the Undo command, so the End
	// command will fail.
	cluster.Control.ApplyHook(r2, 7, func() {
		cluster.Control.Disconnect(r2)
		cluster.Control.LeadershipLost(r2, time.Second)
	})

	conn = cluster.OpenConn(r2)
	_, err = conn.Exec("CREATE TABLE test (n INT)", nil)
	assert.EqualError(t, err, sqlite3.ErrNotLeader.Error())

	cluster.TimelineContains(r2, conn, "hook=begin txn=6 undo stale transaction 3")
	cluster.TimelineContains(r2, conn, "hook=begin txn=6 cmd=end apply error: node is not the leader")

	// Reconnect node i and j.
	cluster.Control.Reconnect(r1)
	cluster.Control.Reconnect(r2)
	cluster.StillFunctional()
}

// Check that the WAL is actually replicated.
func TestIntegration_Replication(t *testing.T) {
	cluster := newCluster(t)
	defer cluster.Cleanup()

	// Get a leader connection on the leader node.
	r1 := cluster.Control.LeadershipAcquired(time.Second)
	conn := cluster.OpenConn(r1)

	// Run a WAL-writing query.
	_, err := conn.Exec("CREATE TABLE test (n INT)", nil)
	require.NoError(t, err)

	// Wait for a follower to replicate the WAL.
	r2 := cluster.Control.Other(r1)
	cluster.Control.WaitIndex(r2, r2.AppliedIndex(), time.Second)

	// FIXME: wait a bit for the FSM to actually apply the log
	time.Sleep(50 * time.Millisecond)

	// Get a leader conneciton on the follower and check that the change is
	// there.
	conn = cluster.OpenConn(r2)
	_, err = conn.Exec("SELECT * FROM test", nil)
	assert.NoError(t, err)
}

// Exercise creating and restoring snapshots.
func TestIntegration_Snapshot(t *testing.T) {
	config := rafttest.Config(func(n int, config *raft.Config) {
		config.SnapshotInterval = 200 * time.Millisecond
		config.SnapshotThreshold = 10
		config.TrailingLogs = 2
	})
	cluster := newCluster(t, config)
	defer cluster.Cleanup()

	// Get a leader connection on the leader node.
	r1 := cluster.Control.LeadershipAcquired(time.Second)
	conn := cluster.OpenConn(r1)

	// Disconnect a follower, so it will be forced to use the snapshot at
	// reconnection.
	r2 := cluster.Control.Other(r1)
	cluster.Control.Disconnect(r2)

	// Run a couple of WAL-writing queries.
	_, err := conn.Exec("CREATE TABLE test (n INT)", nil)
	require.NoError(t, err)
	_, err = conn.Exec("INSERT INTO test VALUES(1)", nil)
	require.NoError(t, err)
	_, err = conn.Exec("INSERT INTO test VALUES(2)", nil)
	require.NoError(t, err)

	// Get the index of the alive follower.
	r3 := cluster.Control.Other(r1, r2)

	// Make sure snapshot is taken by the leader and the follower.
	cluster.Control.WaitSnapshot(r1, 1, time.Second)
	cluster.Control.WaitSnapshot(r3, 1, time.Second)

	// Reconnect the disconnected node and wait for it to catch up.
	cluster.Control.Reconnect(r2)
	cluster.Control.WaitRestore(r2, 1, time.Second)

	// FIXME: not sure why a sleep is needed here, but without it the query
	// below occasionally fails with "no such table: 'test'", maybe
	// because the sqlite files haven't been synced to disk yet.
	time.Sleep(50 * time.Millisecond)

	// Verify that the follower has the expected data.
	conn = cluster.OpenConn(r2)
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
	Control *rafttest.Control
	Rafts   []*raft.Raft // Raft nodes.
	Methods []*replication.Methods

	conns    []*sqlite3.SQLiteConn // Leader connections.
	cleanups []func()
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
	raftFSMs := make([]raft.FSM, 3)
	for i := range fsms {
		dir, cleanup := newDir(t)
		fsm := replication.NewFSM(dir)
		fsm.Tracers().Testing(t, i)
		cleanups = append(cleanups, cleanup)
		fsms[i] = fsm
		raftFSMs[i] = fsm
	}

	// Raft nodes.
	rafts, control := rafttest.Cluster(t, raftFSMs, knobs...)
	cleanups = append(cleanups, control.Close)

	// Methods
	methods := make([]*replication.Methods, 3)
	for i := range methods {
		raft := rafts[i]
		//logger := log.New(log.Testing(t, 1), log.Trace)
		fsm := fsms[i]
		//connections := fsm.Connections()
		//transactions := fsm.Transactions()
		methods[i] = replication.NewMethods(fsm, raft)
	}

	// Leader connections.
	conns := make([]*sqlite3.SQLiteConn, 3)

	succeeded = true
	return &cluster{
		FSMs:     fsms,
		Control:  control,
		Rafts:    rafts,
		Methods:  methods,
		conns:    conns,
		cleanups: cleanups,
		t:        t,
	}
}

func (c *cluster) Cleanup() {
	for i := range c.conns {
		c.CloseConn(i)
	}
	runCleanups(c.cleanups)
}

// Open a leader connection on the given node instance.
func (c *cluster) OpenConn(raft *raft.Raft) *sqlite3.SQLiteConn {
	i := c.Control.Index(raft)
	fsm := c.FSMs[i]
	methods := c.Methods[i]

	path := filepath.Join(fsm.Dir(), "test.db?_busy_timeout=500")
	conn, err := connection.OpenLeader(path, methods, 1000)
	require.NoError(c.t, err)

	methods.Connections().AddLeader("test.db", conn)
	methods.Tracers().Add(replication.TracerName(methods.Connections(), conn))

	c.conns[i] = conn

	return conn
}

// Close the leader connection associated with the i-th node.
func (c *cluster) CloseConn(i int) {
	if c.conns[i] == nil {
		return
	}
	conn := c.conns[i]
	methods := c.Methods[i]

	methods.Tracers().Remove(replication.TracerName(methods.Connections(), conn))
	methods.Connections().DelLeader(conn)

	c.conns[i] = nil

	connection.CloseLeader(conn)
}

// Check that the method timeline of the given node contains the given event
// for the given connection.
func (c *cluster) TimelineContains(raft *raft.Raft, conn *sqlite3.SQLiteConn, event string) {
	//c.t.Helper()

	i := c.Control.Index(raft)
	fsm := c.FSMs[i]
	serial := fsm.Connections().Serial(conn)
	message := fmt.Sprintf("methods %d: %s", serial, event)
	assert.Contains(c.t, fsm.Tracers().String(), message)
}

// Check the cluster is still functional after a failure that involved one or
// more nodes to be disconnected and after those nodes have been reconnected.
func (c *cluster) StillFunctional() {
	//c.t.Helper()

	// Start a new transaction and check that it works. Retry a few times
	// since the leader might not be stable at this point.
	var raft *raft.Raft
	var err error
	for n := 0; n < 3; n++ {
		// Figure out the current leader.
		raft = c.Control.FindLeader(time.Second)
		conn := c.OpenConn(raft)
		_, err = conn.Exec("CREATE TABLE _test (n INT)", nil)
		if err, ok := err.(sqlite3.Error); ok && err.Code == sqlite3.ErrNotLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		break

	}
	require.NoError(c.t, err)

	// Check that all FSMs eventually catch up with the leader.
	index := raft.AppliedIndex()
	for _, r := range c.Rafts {
		c.Control.WaitIndex(r, index, time.Second)
	}
}

// Execute the given cleanup functions in reverse order.
func runCleanups(cleanups []func()) {
	for i := len(cleanups) - 1; i >= 0; i-- {
		cleanups[i]()
	}
}
