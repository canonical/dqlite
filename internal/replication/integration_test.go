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
	"strconv"
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
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	// Since no leader was elected, we fail.
	createTable(t, conns["0"][0], sqlite3.ErrIoErrNotLeader)

	// Take a connection against a new leader and create again the
	// test table.
	control.Elect("1")
	createTable(t, conns["1"][0], 0)
	control.Barrier()

	// Check that deposed leader has caught up.
	selectAny(t, conns["0"][0])

	// Check that the third server has caught up as well.
	selectAny(t, conns["2"][0])
}

// Leadership is lost when trying to apply the Open command to open follower
// connections.
func TestIntegration_Begin_OpenFollower_NotLeader(t *testing.T) {
	conns, control, cleanup := newCluster(t, noLeaderCheck)
	defer cleanup()

	// Since no leader was elected, we fail.
	createTable(t, conns["0"][0], sqlite3.ErrIoErrNotLeader)

	// Take a connection against a new leader and create again the
	// test table.
	control.Elect("1")
	createTable(t, conns["1"][0], 0)
	control.Barrier()

	// Check that deposed leader has caught up.
	selectAny(t, conns["0"][0])

	// Check that the third server has caught up as well.
	selectAny(t, conns["2"][0])
}

// Leadership is lost when committing the Open command to open follower
// connections. A quorum is not reached and the Open command does not get
// applied.
func TestIntegration_Begin_OpenFollower_LeadershipLost(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	control.Elect("0").When().Command(1).Enqueued().Depose()
	createTable(t, conns["0"][0], sqlite3.ErrIoErrLeadershipLost)

	// Take a connection against a new leader and create again the test
	// table.
	control.Elect("1")
	createTable(t, conns["1"][0], 0)
	control.Barrier()

	// Take a connection against the deposed leader and check that the
	// change is there.
	selectAny(t, conns["0"][0])

	// Take a connection against the other follower and check that the
	// change is there.
	selectAny(t, conns["0"][0])
}

// Leadership is lost when committing the Open command to open follower
// connections. A quorum is reached and the Open command still gets applied,
// the same leader gets re-elected.
func TestIntegration_Begin_OpenFollower_LeadershipLostQuorumSameLeader(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	control.Elect("0").When().Command(1).Appended().Depose()
	createTable(t, conns["0"][0], sqlite3.ErrIoErrLeadershipLost)

	control.Elect("0")

	// Take a connection against the re-elected leader node and create again
	// the test table.
	control.Barrier()
	createTable(t, conns["0"][0], 0)
	control.Barrier()

	// The followers have it too.
	selectAny(t, conns["1"][0])
	selectAny(t, conns["2"][0])
}

// Leadership is lost when committing the Open command to open follower
// connections. A quorum is reached and the Open command still gets applied, a
// different leader gets re-elected.
func TestIntegration_Begin_OpenFollower_LeadershipLostQuorumOtherLeader(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	control.Elect("0").When().Command(1).Appended().Depose()
	createTable(t, conns["0"][0], sqlite3.ErrIoErrLeadershipLost)

	control.Elect("1")

	// Take a connection against the new leader and create again the test
	// table.
	createTable(t, conns["1"][0], 0)
	control.Barrier()

	// The followers have it too.
	selectAny(t, conns["0"][0])
	selectAny(t, conns["2"][0])
}

// A transaction on another leader connection is in progress, the Begin hook
// returns ErrBusy when trying to execute a new transaction. It eventually
// times out if the in-progress transaction does not end.
func TestIntegration_Begin_BusyTimeout(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	control.Elect("0")
	conn1 := conns["0"][0]
	conn2 := conns["0"][1]

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

	control.Elect("0")
	conn1 := conns["0"][0]
	conn2 := conns["0"][1]

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
	control.Barrier()
	conn := conns["1"][0]
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

	control.Elect("0")
	conn := conns["0"][0]

	createTable(t, conn, 0)

	_, err := conn.Exec("BEGIN; INSERT INTO test(n) VALUES(1)", nil)
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
	control.Barrier()
	conn = conns["1"][0]
	rows, err := conn.Query("SELECT n FROM test ORDER BY n", nil)
	require.NoError(t, err)
	values := make([]driver.Value, 1)
	require.NoError(t, rows.Next(values))
	assert.Equal(t, int64(1), values[0])
	require.NoError(t, rows.Close())
}

// The server is not the leader anymore when the Frames hook fires.
func TestIntegration_Frames_HookCheck_NotLeader(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	control.Elect("0").When().Command(1).Committed().Depose()

	// Take a connection against a the leader node and start a write transaction.
	conn := conns["0"][0]
	createTableBegin(t, conn, 0)

	// Take a connection against the deposed leader node and try to
	// commit the transaction.
	createTableCommit(t, conn, sqlite3.ErrIoErrNotLeader)

	// Take a connection against the new leader node and create the same
	// test table.
	control.Elect("1")
	createTable(t, conns["1"][0], 0)

	// The followers have it too.
	control.Barrier()
	selectAny(t, conns["0"][0])
	selectAny(t, conns["2"][0])
}

// The server is not the leader anymore when the Frames hook tries to apply the
// Frames command.
func TestIntegration_Frames_NotLeader(t *testing.T) {
	conns, control, cleanup := newCluster(t, noLeaderCheck)
	defer cleanup()

	control.Elect("0").When().Command(1).Committed().Depose()

	// Take a connection against a the leader node and start a write transaction.
	conn := conns["0"][0]
	createTableBegin(t, conn, 0)

	// Take a connection against the deposed leader node and try to
	// commit the transaction.
	createTableCommit(t, conn, sqlite3.ErrIoErrNotLeader)

	// Take a connection against the new leader node and create the same
	// test table.
	control.Elect("1")
	createTable(t, conns["1"][0], 0)

	// The followers have it too.
	control.Barrier()
	selectAny(t, conns["0"][0])
	selectAny(t, conns["2"][0])
}

// The node loses leadership when the Frames hook tries to apply the Frames
// command. The frames is a commit one, and no quorum is reached for the
// inflight Frames command.
func TestIntegration_Frames_LeadershipLost_Commit(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	control.Elect("0").When().Command(2).Enqueued().Depose()

	// Take a connection against a the leader node and start a write transaction.
	conn := conns["0"][0]
	createTableBegin(t, conn, 0)

	// Take a connection against the deposed leader node and try to
	// commit the transaction.
	createTableCommit(t, conn, sqlite3.ErrIoErrLeadershipLost)

	// Take a connection against the new leader node and create the same
	// test table.
	control.Elect("1")
	createTable(t, conns["1"][0], 0)

	// The followers have it too.
	control.Barrier()
	selectAny(t, conns["0"][0])
	selectAny(t, conns["2"][0])
}

// Leadership is lost when applying the Frames command, but a quorum is reached
// and the command actually gets committed. The same node that lost leadership
// gets re-elected.
func TestIntegration_Frames_LeadershipLost_Quorum_SameLeader(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	control.Elect("0").When().Command(2).Appended().Depose()

	// Take a connection against a the leader node and start a write transaction.
	conn := conns["0"][0]
	createTableBegin(t, conn, 0)

	// Take a connection against the deposed leader node and try to
	// commit the transaction.
	createTableCommit(t, conn, sqlite3.ErrIoErrLeadershipLost)

	// Take a connection against the same leader node and insert the a value
	// into the test table. This works because the Frames log got committed
	// despite the lost leadership.
	control.Elect("0")
	control.Barrier()
	insertOne(t, conn, 0)

	// The followers see the inserted row too.
	control.Barrier()
	selectOne(t, conns["1"][0])
	selectOne(t, conns["2"][0])
}

// Leadership is lost when applying the Frames command, but a quorum is reached
// and the command actually gets committed. A different node than the one that
// lost leadership gets re-elected.
func TestIntegration_Frames_LeadershipLost_Quorum_OtherLeader(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	control.Elect("0").When().Command(2).Appended().Depose()

	// Take a connection against a the leader node and start a write transaction.
	conn := conns["0"][0]
	createTableBegin(t, conn, 0)

	// Take a connection against the deposed leader node and try to
	// commit the transaction.
	createTableCommit(t, conn, sqlite3.ErrIoErrLeadershipLost)

	// Take a connection against the new leader and insert the a value
	// into the test table. This works because the Frames log got committed
	// despite the lost leadership.
	control.Elect("1")
	control.Barrier()
	insertOne(t, conns["1"][0], 0)

	// The followers see the inserted row too.
	control.Barrier()
	selectOne(t, conns["0"][0])
	selectOne(t, conns["2"][0])
}

/*
// The server is not the leader anymore when the Undo hook fires.
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
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	control.Elect("0")

	// Take a connection against the leader node and run a WAL-writing
	// query.
	createTable(t, conns["0"][0], 0)

	// The followers see the inserted table too.
	control.Barrier()
	selectAny(t, conns["1"][0])
	selectAny(t, conns["2"][0])
}

/*
FIXME: requires more fine grained control in raft-test snapshots

// Exercise creating and restoring snapshots.
func TestIntegration_Snapshot(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	term := control.Elect("0")

	// Disconnect the follower immediately, so it will be forced to use the
	// snapshot at reconnection.
	term.When().Command(1).Committed().Disconnect("1")

	// Take a snapshot on the leader after first batch of WAL-writing queries below is done.
	term.When().Command(4).Committed().Snapshot()

	// Reconnect the follower after a further query.
	term.When().Command(5).Committed().Reconnect("1")

	// Get a leader connection on the leader node.
	conn := conns["0"][0]

	// Run a few of WAL-writing queries.
	_, err := conn.Exec("CREATE TABLE test (n INT)", nil)
	require.NoError(t, err)
	_, err = conn.Exec("INSERT INTO test VALUES(1)", nil)
	require.NoError(t, err)
	t.Log("LAST QUERY")
	_, err = conn.Exec("INSERT INTO test VALUES(2)", nil)
	require.NoError(t, err)
	t.Log("LAST DONE")

	// Make sure snapshot is taken by the leader.
	time.Sleep(150 * time.Millisecond)
	control.Barrier()
	assert.Equal(t, uint64(1), control.Snapshots("0"))

	// This query will make the follower re-connect.
	_, err = conn.Exec("INSERT INTO test VALUES(3)", nil)
	require.NoError(t, err)

	// The follower will now restore the snapshot.
	control.Barrier()

	// Figure out the database name
	rows, err := conn.Query("SELECT file FROM pragma_database_list WHERE name='main'", nil)
	require.NoError(t, err)
	values := make([]driver.Value, 1)
	require.NoError(t, rows.Next(values))
	require.NoError(t, rows.Close())
	path := string(values[0].([]byte))

	// Open a new connection since the database file has been replaced.
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
	require.NoError(t, rows.Next(values))
	assert.Equal(t, int64(3), values[0].(int64))

}
*/

// Creates a test table using the given connection. If code is not zero, assert
// that the query fails.
func createTable(t *testing.T, conn *sqlite3.SQLiteConn, code sqlite3.ErrNoExtended) {
	t.Helper()
	_, err := conn.Exec("CREATE TABLE test (n INT, UNIQUE (n))", nil)
	if code == 0 {
		require.NoError(t, err)
	} else {
		requireEqualErrNo(t, code, err)
	}
}

// Start a transaction to create a test table, but don't commit it.
func createTableBegin(t *testing.T, conn *sqlite3.SQLiteConn, code sqlite3.ErrNoExtended) {
	t.Helper()
	_, err := conn.Exec("BEGIN; CREATE TABLE test (n INT)", nil)
	if code == 0 {
		require.NoError(t, err)
	} else {
		requireEqualErrNo(t, code, err)
	}
}

// Commit a transaction that was creating a table.
func createTableCommit(t *testing.T, conn *sqlite3.SQLiteConn, code sqlite3.ErrNoExtended) {
	t.Helper()
	_, err := conn.Exec("COMMIT", nil)
	if code == 0 {
		require.NoError(t, err)
	} else {
		requireEqualErrNo(t, code, err)
	}
}

// Rollback a transaction to create a test table.
func createTableRollback(t *testing.T, conn *sqlite3.SQLiteConn) {
	t.Helper()
	_, err := conn.Exec("ROLLBACK", nil)
	require.NoError(t, err) // SQLite should never return an error here
}

// Inserts a single row into the test table.
func insertOne(t *testing.T, conn *sqlite3.SQLiteConn, code sqlite3.ErrNoExtended) {
	t.Helper()
	_, err := conn.Exec("INSERT INTO test(n) VALUES (1)", nil)
	if code == 0 {
		require.NoError(t, err)
	} else {
		requireEqualErrNo(t, code, err)
	}
}

// Selects from the test table to assert that it's there.
func selectAny(t *testing.T, conn *sqlite3.SQLiteConn) {
	t.Helper()
	_, err := conn.Exec("SELECT n FROM test", nil)
	assert.NoError(t, err)
}

// Select a row from the test table and check that its value is 1.
func selectOne(t *testing.T, conn *sqlite3.SQLiteConn) {
	t.Helper()
	rows, err := conn.Query("SELECT n FROM test", nil)
	require.NoError(t, err)
	values := make([]driver.Value, 1)
	require.NoError(t, rows.Next(values))
	require.Equal(t, int64(1), values[0])
	require.NoError(t, rows.Close())
}

// Select N rows from the test table and check that their value is progressing.
func selectN(t *testing.T, conn *sqlite3.SQLiteConn, n int) {
	t.Helper()
	rows, err := conn.Query("SELECT n FROM test ORDER BY n", nil)
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		values := make([]driver.Value, 1)
		require.NoError(t, rows.Next(values))
		require.Equal(t, int64(i+1), values[0])
	}
	require.NoError(t, rows.Close())
}

type clusterOptions func([]*replication.Methods)

// Option that disable leadership checks in the methods hooks.
func noLeaderCheck(methods []*replication.Methods) {
	for _, methods := range methods {
		methods.NoLeaderCheck()
	}
}

// Create a new test cluster with 3 nodes, each with its own FSM, Methods and
// two connections opened in leader mode.
func newCluster(t *testing.T, opts ...clusterOptions) (map[raft.ServerID][2]*sqlite3.SQLiteConn, *rafttest.Control, func()) {
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
	store := rafttest.LogStore(func(i int) raft.LogStore { return stores[i] })
	rafts, control := rafttest.Cluster(t, fsms, store)

	// Methods and connections.
	methods := make([]*replication.Methods, 3)
	conns := map[raft.ServerID][2]*sqlite3.SQLiteConn{}
	for i := range methods {
		//logger := log.New(log.Testing(t, 1), log.Trace)
		id := raft.ServerID(strconv.Itoa(i))
		methods[i] = replication.NewMethods(registries[i], rafts[id])

		dir := methods[i].Registry().Dir()
		timeout := rafttest.Duration(100*time.Millisecond).Nanoseconds() / (1000 * 1000)
		path := filepath.Join(dir, fmt.Sprintf("test.db?_busy_timeout=%d", timeout))

		conn1, err := connection.OpenLeader(path, methods[i])
		require.NoError(t, err)
		methods[i].Registry().ConnLeaderAdd("test.db", conn1)

		conn2, err := connection.OpenLeader(path, methods[i])
		require.NoError(t, err)
		methods[i].Registry().ConnLeaderAdd("test.db", conn2)

		conns[id] = [2]*sqlite3.SQLiteConn{conn1, conn2}

		methodByConnSet(conn1, methods[i])
		methodByConnSet(conn2, methods[i])
	}

	for _, o := range opts {
		o(methods)
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
