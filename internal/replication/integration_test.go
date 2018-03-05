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
	"sync"
	"testing"
	"time"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/internal/registry"
	"github.com/CanonicalLtd/dqlite/internal/replication"
	"github.com/CanonicalLtd/go-sqlite3"
	"github.com/CanonicalLtd/raft-test"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Leadership is lost before the Begin hook fires.
func TestIntegration_Begin_HookCheck_NotLeader(t *testing.T) {
	// Don't do anything in stage1, since we want to fail right way at the
	// start of the Begin hook.
	stage1 := scenarioStageNoop()

	// Take a connection against the deposed leader node and try to
	// create a table.
	stage2 := scenarioStageCreateTable(t, sqlite3.ErrIoErrNotLeader)

	// Take a connection against the new leader node and create again the
	// test table.
	stage3 := scenarioStageCreateTable(t, 0)

	// Take a connection against the reconnected deposed leader nodes and check that
	// the change is there.
	stage4 := scenarioStageSelectAny(t)

	// Take a connection against the other follower and check that the
	// change is there.
	stage5 := scenarioStageSelectAny(t)

	runScenario(t, rafttest.NotLeaderScenario(0), stage1, stage2, stage3, stage4, stage5)
}

// Leadership is lost when trying to apply the Open command to open follower
// connections.
func TestIntegration_Begin_OpenFollower_NotLeader(t *testing.T) {
	// Skip the initial leader check since we want to fail when the Open
	// command is applied.
	stage1 := scenarioStageNoLeaderCheck(scenarioStageNoop())

	// Take a connection against the deposed leader node and try to
	// create a table.
	stage2 := scenarioStageCreateTable(t, sqlite3.ErrIoErrNotLeader)

	// Take a connection against the new leader node and create again the
	// test table.
	stage3 := scenarioStageCreateTable(t, 0)

	// Take a connection against the reconnected deposed leader nodes and check that
	// the change is there.
	stage4 := scenarioStageSelectAny(t)

	// Take a connection against the other follower and check that the
	// change is there.
	stage5 := scenarioStageSelectAny(t)

	runScenario(t, rafttest.NotLeaderScenario(0), stage1, stage2, stage3, stage4, stage5)
}

// Leadership is lost when committing the Open command to open follower
// connections. A quorum is not reached and the Open command does not get
// applied.
func TestIntegration_Begin_OpenFollower_LeadershipLost(t *testing.T) {
	// Don't do anything in stage1, since we want the a follower not to
	// exist and the Open FSM command to fail.
	stage1 := scenarioStageNoop()

	// Take a connection against a leader node that is going to be deposed
	// because followers are slow, but that will manage to commit the open
	// FSM command.
	stage2 := scenarioStageCreateTable(t, sqlite3.ErrIoErrLeadershipLost)

	// Take a connection against the re-elected leader node and create again
	// the test table.
	stage3 := scenarioStageCreateTable(t, 0)

	// Take a connection against a follower and check that the change is
	// there.
	stage4 := scenarioStageSelectAny(t)

	// Take a connection against the other follower and check that the
	// change is there.
	stage5 := scenarioStageSelectAny(t)

	runScenario(t, rafttest.LeadershipLostScenario(), stage1, stage2, stage3, stage4, stage5)
}

// Leadership is lost when committing the Open command to open follower
// connections. A quorum is reached and the Open command still gets applied,
// the same leader gets re-elected.
func TestIntegration_Begin_OpenFollower_LeadershipLostQuorumSameLeader(t *testing.T) {
	// Don't do anything in stage1, since we want the a follower not to
	// exist and the Open FSM command to fail.
	stage1 := scenarioStageNoop()

	// Take a connection against a leader node that is going to be deposed
	// because followers are slow, but that will manage to commit the open
	// FSM command.
	stage2 := scenarioStageCreateTable(t, sqlite3.ErrIoErrLeadershipLost)

	// Take a connection against the re-elected leader node and create again
	// the test table.
	stage3 := scenarioStageCreateTable(t, 0)

	// Take a connection against a follower and check that the change is
	// there.
	stage4 := scenarioStageSelectAny(t)

	// Take a connection against the other follower and check that the
	// change is there.
	stage5 := scenarioStageSelectAny(t)

	runScenario(t, rafttest.LeadershipLostQuorumSameLeaderScenario(3), stage1, stage2, stage3, stage4, stage5)
}

// Leadership is lost when committing the Open command to open follower
// connections. A quorum is reached and the Open command still gets applied, a
// different leader gets re-elected.
func TestIntegration_Begin_OpenFollower_LeadershipLostQuorumOtherLeader(t *testing.T) {
	// Don't do anything in stage1, since we want the a follower not to
	// exist and the Open FSM command to fail.
	stage1 := scenarioStageNoop()

	// Take a connection against a leader node that is going to be deposed
	// because followers are slow, but that will manage to commit the open
	// FSM command.
	stage2 := scenarioStageCreateTable(t, sqlite3.ErrIoErrLeadershipLost)

	// Take a connection against the re-elected leader node and create again
	// the test table.
	stage3 := scenarioStageCreateTable(t, 0)

	// Take a connection against a follower and check that the change is
	// there.
	stage4 := scenarioStageSelectAny(t)

	// Take a connection against the other follower and check that the
	// change is there.
	stage5 := scenarioStageSelectAny(t)

	runScenario(t, rafttest.LeadershipLostQuorumOtherLeaderScenario(3), stage1, stage2, stage3, stage4, stage5)
}

// A transaction on another leader connection is in progress, the Begin hook
// returns ErrBusy when trying to execute a new transaction. It eventually
// times out if the in-progress transaction does not end.
func TestIntegration_Begin_BusyTimeout(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	raft := control.LeadershipAcquired(time.Second)
	conn1 := conns[raft][0]
	conn2 := conns[raft][1]

	_, err := conn1.Exec("BEGIN; CREATE TABLE test (n INT)", nil)
	require.NoError(t, err)

	_, err = conn2.Exec("BEGIN; CREATE TABLE test (n INT)", nil)
	require.Error(t, err)
	if err, ok := err.(sqlite3.Error); ok {
		assert.Equal(t, sqlite3.ErrBusy, err.Code)
	} else {
		t.Fatal("expected a sqlite3.Error instance")
	}
}

// A transaction on another leader connection is in progress, the Begin hook
// returns ErrBusy when trying to execute a new transaction. It eventually
// succeeds if the in-progress transaction ends.
func TestIntegration_Begin_BusyRetry(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	r1 := control.LeadershipAcquired(time.Second)
	conn1 := conns[r1][0]
	conn2 := conns[r1][1]

	_, err := conn1.Exec("CREATE TABLE test (n INT)", nil)
	require.NoError(t, err)

	_, err = conn1.Exec("BEGIN; INSERT INTO test(n) VALUES(1)", nil)
	require.NoError(t, err)
	errors := make(chan error)
	go func() {
		time.Sleep(rafttest.Duration(25 * time.Millisecond))
		_, err = conn1.Exec("COMMIT", nil)
		errors <- err
	}()

	_, err = conn2.Exec("INSERT INTO test(n) VALUES(2)", nil)
	require.NoError(t, err)
	require.NoError(t, <-errors)

	// The change is visible from other nodes
	r2 := control.Other(r1)
	control.WaitIndex(r2, r1.AppliedIndex(), time.Second)
	conn := conns[r2][0]
	rows, err := conn.Query("SELECT n FROM test ORDER BY n", nil)
	require.NoError(t, err)
	values := make([]driver.Value, 1)
	require.NoError(t, rows.Next(values))
	assert.Equal(t, int64(1), values[0])
	require.NoError(t, rows.Next(values))
	assert.Equal(t, int64(2), values[0])
	require.NoError(t, rows.Close())
}

// A transaction on the same leader connection is in progress, the Begin hook
// succeeds, but SQLite fails to start the write transaction returing ErrBusy.
func TestIntegration_Begin_BusyRetrySameConn(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	r1 := control.LeadershipAcquired(time.Second)
	conn := conns[r1][0]

	_, err := conn.Exec("CREATE TABLE test (n INT)", nil)
	require.NoError(t, err)

	_, err = conn.Exec("BEGIN; INSERT INTO test(n) VALUES(1)", nil)
	require.NoError(t, err)
	errors := make(chan error)
	go func() {
		time.Sleep(rafttest.Duration(25 * time.Millisecond))
		_, err = conn.Exec("COMMIT", nil)
		errors <- err
	}()

	_, err = conn.Exec("BEGIN; INSERT INTO test(n) VALUES(2)", nil)
	require.EqualError(t, err, "cannot start a transaction within a transaction")
	require.NoError(t, <-errors)

	// The change is visible from other nodes
	r2 := control.Other(r1)
	control.WaitIndex(r2, r1.AppliedIndex(), time.Second)
	conn = conns[r2][0]
	rows, err := conn.Query("SELECT n FROM test ORDER BY n", nil)
	require.NoError(t, err)
	values := make([]driver.Value, 1)
	require.NoError(t, rows.Next(values))
	assert.Equal(t, int64(1), values[0])
	require.NoError(t, rows.Close())
}

// A transaction that performs no WAL write doesn't apply any FSM command.
func TestIntegration_Begin_NoWrite(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	r1 := control.LeadershipAcquired(time.Second)
	conn := conns[r1][0]

	_, err := conn.Exec("CREATE TABLE test (n INT)", nil)
	require.NoError(t, err)

	_, err = conn.Exec("BEGIN; UPDATE test SET n=1; COMMIT", nil)
	require.NoError(t, err)
	_, err = conn.Exec("BEGIN; INSERT INTO test(n) VALUES(1); COMMIT", nil)
	require.NoError(t, err)
}

// The node is not the leader anymore when the Frames hook fires.
func TestIntegration_Frames_HookCheck_NotLeader(t *testing.T) {
	// Take a connection against a the leader node and start a write transaction.
	stage1 := scenarioStageBeginCreateTable(t, 0)

	// Take a connection against the deposed leader node and try to
	// commit the transaction.
	stage2 := scenarioStageCommitCreateTable(t, sqlite3.ErrIoErrNotLeader)

	// Take a connection against the new leader node and create the same
	// test table of stage1/stage2.
	stage3 := scenarioStageCreateTable(t, 0)

	// Take a connection against the reconnected deposed leader nodes and check that
	// the change is there.
	stage4 := scenarioStageSelectAny(t)

	// Take a connection against the other follower and check that the
	// change is there.
	stage5 := scenarioStageSelectAny(t)

	runScenario(t, rafttest.NotLeaderScenario(3), stage1, stage2, stage3, stage4, stage5)
}

// The node is not the leader anymore when the Frames hook tries to apply the
// Frames command.
func TestIntegration_Frames_NotLeader(t *testing.T) {
	// Take a connection against a the leader node and start a write transaction.
	stage1 := scenarioStageBeginCreateTable(t, 0)

	// Take a connection against the deposed leader node and try to
	// commit the transaction.
	stage2 := scenarioStageNoLeaderCheck(scenarioStageCommitCreateTable(t, sqlite3.ErrIoErrNotLeader))

	// Take a connection against the new leader node and create the same
	// test table of stage1/stage2.
	stage3 := scenarioStageCreateTable(t, 0)

	// Take a connection against the reconnected deposed leader nodes and check that
	// the change is there.
	stage4 := scenarioStageSelectAny(t)

	// Take a connection against the other follower and check that the
	// change is there.
	stage5 := scenarioStageSelectAny(t)

	runScenario(t, rafttest.NotLeaderScenario(3), stage1, stage2, stage3, stage4, stage5)
}

// The node loses leadership when the Frames hook tries to apply the Frames
// command. The frames is a commit one, and no quorum is reached for the
// inflight Frames command.
func TestIntegration_Frames_LeadershipLost_Commit(t *testing.T) {
	// Take a connection against a the leader node and start a write transaction.
	stage1 := scenarioStageBeginCreateTable(t, 0)

	// Take a connection against the going-to-be-deposed leader node and try to
	// commit the transaction.
	stage2 := scenarioStageCommitCreateTable(t, sqlite3.ErrIoErrLeadershipLost)

	// Take a connection against the new leader node and create again the
	// test table.
	stage3 := scenarioStageCreateTable(t, 0)

	// Take a connection against the reconnected deposed leader nodes and check that
	// the change is there.
	stage4 := scenarioStageSelectAny(t)

	// Take a connection against the other follower and check that the
	// change is there.
	stage5 := scenarioStageSelectAny(t)

	runScenario(t, rafttest.LeadershipLostScenario(), stage1, stage2, stage3, stage4, stage5)
}

// Leadership is lost when applying the Frames command, but a quorum is reached
// and the command actually gets committed. The same node that lost leadership
// gets re-elected.
func TestIntegration_Frames_LeadershipLost_Quorum_SameLeader(t *testing.T) {
	// Take a connection against a the leader node and start a write transaction.
	stage1 := scenarioStageBeginCreateTable(t, 0)

	// Take a connection against the deposed leader node and try to
	// insert a value in the test table.
	stage2 := scenarioStageCommitCreateTable(t, sqlite3.ErrIoErrLeadershipLost)

	// Take a connection against the same leader node and insert the a value
	// into the test table. This works because the Frames log got committed
	// despite the lost leadership.
	stage3 := scenarioStageInsert(t, 0)

	// Take a connection against the first follower and check that the
	// change is there.
	stage4 := scenarioStageSelectOne(t)

	// Take a connection against the other follower and check that the
	// change is there.
	stage5 := scenarioStageSelectOne(t)

	runScenario(t, rafttest.LeadershipLostQuorumSameLeaderScenario(4), stage1, stage2, stage3, stage4, stage5)
}

// Leadership is lost when applying the Frames command, but a quorum is reached
// and the command actually gets committed. A different node than the one that
// lost leadership gets re-elected.
func TestIntegration_Frames_LeadershipLost_Quorum_OtherLeader(t *testing.T) {
	// Take a connection against a the leader node and start a write transaction.
	stage1 := scenarioStageBeginCreateTable(t, 0)

	// Take a connection against the going-to-be-deposed leader node and
	// try to commit the transaction.
	stage2 := scenarioStageCommitCreateTable(t, sqlite3.ErrIoErrLeadershipLost)

	// Take a connection against the new leader node and insert the a value
	// into the test table. This works because the Frames log got committed
	// despite the lost leadership.
	stage3 := scenarioStageInsert(t, 0)

	// Take a connection against the former leader and check that the
	// change is there.
	stage4 := scenarioStageSelectOne(t)

	// Take a connection against the other follower and check that the
	// change is there.
	stage5 := scenarioStageSelectOne(t)

	runScenario(t, rafttest.LeadershipLostQuorumOtherLeaderScenario(4), stage1, stage2, stage3, stage4, stage5)
}

/*

// The node is not the leader anymore when the Undo hook fires.
func TestIntegration_Undo_HookCheck_NotLeader(t *testing.T) {
	// Take a connection against a the leader node and start a write transaction.
	stage1 := scenarioStageBeginCreateTable(t, 0)

	// Take a connection against the deposed leader node and try to
	// rollback the transaction.
	stage2 := scenarioStageRollbackCreateTable(t)

	// Take a connection against the new leader node and create the same
	// test table of stage1/stage2.
	stage3 := scenarioStageCreateTable(t, 0)

	// Take a connection against the reconnected deposed leader nodes and check that
	// the change is there.
	stage4 := scenarioStageSelectAny(t)

	// Take a connection against the other follower and check that the
	// change is there.
	stage5 := scenarioStageSelectAny(t)

	runScenario(t, rafttest.NotLeaderScenario(4), stage1, stage2, stage3, stage4, stage5)
}

// The node is not the leader anymore when the Undo hook tries to apply the
// Undo command.
func TestIntegration_Undo_NotLeader(t *testing.T) {
	// Take a connection against a the leader node and start a write transaction.
	stage1 := scenarioStageBeginCreateTable(t, 0)

	// Take a connection against the deposed leader node and try to
	// rollback the transaction.
	stage2 := scenarioStageNoLeaderCheck(scenarioStageRollbackCreateTable(t))

	// Take a connection against the new leader node and create the same
	// test table of stage1/stage2.
	stage3 := scenarioStageCreateTable(t, 0)

	// Take a connection against the reconnected deposed leader nodes and check that
	// the change is there.
	stage4 := scenarioStageSelectAny(t)

	// Take a connection against the other follower and check that the
	// change is there.
	stage5 := scenarioStageSelectAny(t)

	runScenario(t, rafttest.NotLeaderScenario(4), stage1, stage2, stage3, stage4, stage5)
}

// The node loses leadership when the Frames hook tries to apply the Undo
// command. No quorum is reached for the inflight Undo command.
func TestIntegration_Undo_LeadershipLost(t *testing.T) {
	// Take a connection against a the leader node and start a write transaction.
	stage1 := scenarioStageBeginCreateTable(t, 0)

	// Take a connection against the going-to-be-deposed leader node and try to
	// commit the transaction.
	stage2 := scenarioStageRollbackCreateTable(t)

	// Take a connection against the new leader node and create again the
	// test table.
	stage3 := scenarioStageCreateTable(t, 0)

	// Take a connection against the reconnected deposed leader nodes and check that
	// the change is there.
	stage4 := scenarioStageSelectAny(t)

	// Take a connection against the other follower and check that the
	// change is there.
	stage5 := scenarioStageSelectAny(t)

	runScenario(t, rafttest.LeadershipLostScenario(), stage1, stage2, stage3, stage4, stage5)
}

// Leadership is lost when applying the Undo command, but a quorum is reached
// and the command actually gets committed. The same node that lost leadership
// gets re-elected.
func TestIntegration_Undo_LeadershipLost_Quorum_SameLeader(t *testing.T) {
	// Take a connection against a the leader node and start a write transaction.
	stage1 := scenarioStageBeginCreateTable(t, 0)

	// Take a connection against the going-to-be deposed leader node and
	// rollback the write transaction.
	stage2 := scenarioStageRollbackCreateTable(t)

	// Take a connection against the same leader node and create the same
	// test table agaiin.
	stage3 := scenarioStageCreateTable(t, 0)

	// Take a connection against the first follower and check that the
	// change is there.
	stage4 := scenarioStageSelectAny(t)

	// Take a connection against the other follower and check that the
	// change is there.
	stage5 := scenarioStageSelectAny(t)

	runScenario(t, rafttest.LeadershipLostQuorumSameLeaderScenario(5), stage1, stage2, stage3, stage4, stage5)
}

// Leadership is lost when applying the Undo command, but a quorum is reached
// and the command actually gets committed. A different node than the one that
// lost leadership gets re-elected.
func TestIntegration_Undo_LeadershipLost_Quorum_OtherLeader(t *testing.T) {
	// Take a connection against a the leader node and start a write transaction.
	stage1 := scenarioStageBeginCreateTable(t, 0)

	// Take a connection against the going-to-be deposed leader node and
	// rollback the write transaction.
	stage2 := scenarioStageRollbackCreateTable(t)

	// Take a connection against the new leader node and create the same
	// test table agaiin.
	stage3 := scenarioStageCreateTable(t, 0)

	// Take a connection against former leader and check that the
	// change is there.
	stage4 := scenarioStageSelectAny(t)

	// Take a connection against the other follower and check that the
	// change is there.
	stage5 := scenarioStageSelectAny(t)

	runScenario(t, rafttest.LeadershipLostQuorumOtherLeaderScenario(5), stage1, stage2, stage3, stage4, stage5)
}
*/

// Check that the WAL is actually replicated.
func TestIntegration_Replication(t *testing.T) {
	// Take a connection against the leader node and run a WAL-writing
	// query.
	stage1 := func(conn *sqlite3.SQLiteConn) {
		_, err := conn.Exec("CREATE TABLE test (n INT)", nil)
		require.NoError(t, err)
	}

	// Take a connection against one the follower nodes and check that the
	// change is there.
	stage2 := func(conn *sqlite3.SQLiteConn) {
		_, err := conn.Exec("SELECT * FROM test", nil)
		assert.NoError(t, err)
	}

	// Take a connection against the other follower nodes and check that
	// the change is there.
	stage3 := func(conn *sqlite3.SQLiteConn) {
		_, err := conn.Exec("SELECT * FROM test", nil)
		assert.NoError(t, err)
	}

	runScenario(t, rafttest.ReplicationScenario(), stage1, stage2, stage3)
}

// Exercise creating and restoring snapshots.
func TestIntegration_Snapshot(t *testing.T) {
	config := rafttest.Config(func(n int, config *raft.Config) {
		config.SnapshotInterval = 100 * time.Millisecond
		config.SnapshotThreshold = 5
		config.TrailingLogs = 1

		// Prevent the disconnected node from restarting election
		config.ElectionTimeout = 300 * time.Millisecond
		config.HeartbeatTimeout = 250 * time.Millisecond
		config.LeaderLeaseTimeout = 250 * time.Millisecond
	})
	conns, control, cleanup := newCluster(t, config)
	defer cleanup()

	// Get a leader connection on the leader node.
	r1 := control.LeadershipAcquired(time.Second)
	conn := conns[r1][0]

	// Figure out the database name
	rows, err := conn.Query("SELECT file FROM pragma_database_list WHERE name='main'", nil)
	require.NoError(t, err)
	values := make([]driver.Value, 1)
	require.NoError(t, rows.Next(values))
	require.NoError(t, rows.Close())
	path := string(values[0].([]byte))

	// Disconnect a follower, so it will be forced to use the snapshot at
	// reconnection.
	r2 := control.Other(r1)
	control.Disconnect(r2)

	// Run a couple of WAL-writing queries.
	_, err = conn.Exec("CREATE TABLE test (n INT)", nil)
	require.NoError(t, err)
	_, err = conn.Exec("INSERT INTO test VALUES(1)", nil)
	require.NoError(t, err)
	_, err = conn.Exec("INSERT INTO test VALUES(2)", nil)
	require.NoError(t, err)

	// Get the index of the alive follower.
	r3 := control.Other(r1, r2)

	// Make sure snapshot is taken by the leader and the follower.
	control.WaitSnapshot(r1, 1, time.Second)
	control.WaitSnapshot(r3, 1, time.Second)

	// Reconnect the disconnected node and wait for it to catch up.
	control.Reconnect(r2)
	control.WaitRestore(r2, 1, time.Second)

	// FIXME: not sure why a sleep is needed here, but without it the query
	// below occasionally fails with "no such table: 'test'", maybe
	// because the sqlite files haven't been synced to disk yet.
	time.Sleep(50 * time.Millisecond)

	methods := sqlite3.NoopReplicationMethods()
	conn, err = connection.OpenLeader(path, methods)
	rows, err = conn.Query("SELECT n FROM test", nil)
	require.NoError(t, err)
	defer rows.Close()
	values = make([]driver.Value, 1)
	require.NoError(t, rows.Next(values))
	assert.Equal(t, int64(1), values[0].(int64))
	require.NoError(t, rows.Next(values))
	assert.Equal(t, int64(2), values[0].(int64))
}

type scenarioStage func(*sqlite3.SQLiteConn)

// A stage function that does nothing.
func scenarioStageNoop() scenarioStage {
	return func(conn *sqlite3.SQLiteConn) {}
}

// A decorator that will call Methods.NoLeaderCheck before executing the actual scenario.
func scenarioStageNoLeaderCheck(stage scenarioStage) scenarioStage {
	return func(conn *sqlite3.SQLiteConn) {
		methods := methodByConnGet(conn)
		methods.NoLeaderCheck()
		stage(conn)
	}
}

// A stage function that creates a test table.
func scenarioStageCreateTable(t *testing.T, code sqlite3.ErrNoExtended) scenarioStage {
	return func(conn *sqlite3.SQLiteConn) {
		_, err := conn.Exec("CREATE TABLE test (n INT, UNIQUE (n))", nil)
		if code == 0 {
			require.NoError(t, err)
		} else {
			requireEqualErrNo(t, code, err)
		}
	}
}

// A stage function that begins a transaction to create a test table.
func scenarioStageBeginCreateTable(t *testing.T, code sqlite3.ErrNoExtended) scenarioStage {
	return func(conn *sqlite3.SQLiteConn) {
		_, err := conn.Exec("BEGIN; CREATE TABLE test (n INT)", nil)
		if code == 0 {
			require.NoError(t, err)
		} else {
			requireEqualErrNo(t, code, err)
		}
	}
}

// A stage function that commits a transaction to create a test table.
func scenarioStageCommitCreateTable(t *testing.T, code sqlite3.ErrNoExtended) scenarioStage {
	return func(conn *sqlite3.SQLiteConn) {
		_, err := conn.Exec("COMMIT", nil)
		if code == 0 {
			require.NoError(t, err)
		} else {
			requireEqualErrNo(t, code, err)
		}
	}
}

// A stage function that rollos back a transaction to create a test table.
func scenarioStageRollbackCreateTable(t *testing.T) scenarioStage {
	return func(conn *sqlite3.SQLiteConn) {
		_, err := conn.Exec("ROLLBACK", nil)
		require.NoError(t, err) // SQLite should never return an error here
	}
}

// A stage function that inserts a single row into the test table.
func scenarioStageInsert(t *testing.T, code sqlite3.ErrNoExtended) scenarioStage {
	return func(conn *sqlite3.SQLiteConn) {
		_, err := conn.Exec("INSERT INTO test(n) VALUES (1)", nil)
		if code == 0 {
			require.NoError(t, err)
		} else {
			requireEqualErrNo(t, code, err)
		}
	}
}

// A stage function that just selects from the test table to assert that it's
// there.
func scenarioStageSelectAny(t *testing.T) scenarioStage {
	return func(conn *sqlite3.SQLiteConn) {
		_, err := conn.Exec("SELECT n FROM test", nil)
		assert.NoError(t, err)
	}
}

// A stage function that just select a row from the test table and check that
// its value is 1.
func scenarioStageSelectOne(t *testing.T) scenarioStage {
	return func(conn *sqlite3.SQLiteConn) {
		rows, err := conn.Query("SELECT n FROM test", nil)
		require.NoError(t, err)
		values := make([]driver.Value, 1)
		rows.Next(values)
		require.Equal(t, int64(1), values[0])
		require.NoError(t, rows.Close())
	}
}

func runScenario(t *testing.T, scenario rafttest.Scenario, stages ...scenarioStage) {
	t.Helper()

	conns, control, cleanup := newCluster(t)
	defer cleanup()

	scenarioStages := make([]rafttest.ScenarioStage, len(stages))
	for i := range stages {
		stage := stages[i]
		scenarioStages[i] = func(raft *raft.Raft) {
			t.Helper()

			conn := conns[raft][0]
			stage(conn)
		}
	}

	scenario(control, scenarioStages...)
}

// Create a new test cluster with 3 nodes, each with its own FSM, Methods and
// two connections opened in leader mode.
func newCluster(t *testing.T, knobs ...rafttest.Knob) (map[*raft.Raft][2]*sqlite3.SQLiteConn, *rafttest.Control, func()) {
	t.Helper()

	// Registries and FSMs
	cleanups := []func(){}
	registries := make([]*registry.Registry, 3)
	fsms := make([]raft.FSM, 3)
	for i := range fsms {
		dir, cleanup := newDir(t)
		cleanups = append(cleanups, cleanup)

		registries[i] = registry.New(dir)
		registries[i].Testing(t, i)

		fsms[i] = replication.NewFSM(registries[i])
	}

	// Use an actual boltdb store, so the Log.Data bytes will be copied
	// when fetching a log from the store. This is necessary for the
	// Registry.HookSync mechanism to work properly.
	stores := make([]raft.LogStore, 3)
	for i := range stores {
		path := filepath.Join(registries[i].Dir(), "bolt.db")
		store, err := raftboltdb.NewBoltStore(path)
		require.NoError(t, err)

		stores[i] = store
	}

	// Raft instances.
	knobs = append(knobs, rafttest.LogStore(func(i int) raft.LogStore { return stores[i] }))
	rafts, control := rafttest.Cluster(t, fsms, knobs...)

	// Methods and connections.
	methods := make([]*replication.Methods, 3)
	conns := map[*raft.Raft][2]*sqlite3.SQLiteConn{}
	for i := range methods {
		//logger := log.New(log.Testing(t, 1), log.Trace)
		methods[i] = replication.NewMethods(registries[i], rafts[i])

		dir := methods[i].Registry().Dir()
		timeout := rafttest.Duration(100*time.Millisecond).Nanoseconds() / (1000 * 1000)
		path := filepath.Join(dir, fmt.Sprintf("test.db?_busy_timeout=%d", timeout))

		conn1, err := connection.OpenLeader(path, methods[i])
		require.NoError(t, err)
		methods[i].Registry().ConnLeaderAdd("test.db", conn1)

		conn2, err := connection.OpenLeader(path, methods[i])
		require.NoError(t, err)
		methods[i].Registry().ConnLeaderAdd("test.db", conn2)

		conns[rafts[i]] = [2]*sqlite3.SQLiteConn{conn1, conn2}

		methodByConnSet(conn1, methods[i])
		methodByConnSet(conn2, methods[i])
	}

	cleanup := func() {
		for i := range conns {
			require.NoError(t, conns[i][0].Close())
			require.NoError(t, conns[i][1].Close())
		}
		control.Close()
		for i := range cleanups {
			cleanups[i]()
		}
	}

	return conns, control, cleanup
}

func requireEqualErrNo(t *testing.T, code sqlite3.ErrNoExtended, err error) {
	t.Helper()

	sqliteErr, ok := err.(sqlite3.Error)
	if !ok {
		t.Fatal("expected sqlite3.Error, but got:", err)
	}
	require.Equal(t, code, sqliteErr.ExtendedCode)
}

// Global registry of Methods instances by connection. This is a poor man
// shortcut to avoid exposing Methods instances explicitely to the tests.
func methodByConnGet(conn *sqlite3.SQLiteConn) *replication.Methods {
	methodsByConnMu.RLock()
	defer methodsByConnMu.RUnlock()
	return methodsByConn[conn]
}
func methodByConnSet(conn *sqlite3.SQLiteConn, methods *replication.Methods) {
	methodsByConnMu.Lock()
	defer methodsByConnMu.Unlock()
	methodsByConn[conn] = methods
}

var methodsByConn = map[*sqlite3.SQLiteConn]*replication.Methods{}
var methodsByConnMu = sync.RWMutex{}
