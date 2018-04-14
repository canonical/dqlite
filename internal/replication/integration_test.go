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
	"io"
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

	control.Elect("0").When().Command(3).Committed().Depose()

	conn := conns["0"][0]
	begin(t, conn)
	insertOne(t, conn, 0)
	commit(t, conn, 0)

	begin(t, conn)
	insertTwo(t, conn, sqlite3.ErrIoErrNotLeader)

	// Take a connection against the new leader node and insert the same
	// value.
	control.Elect("1")
	control.Barrier()
	begin(t, conn)
	insertTwo(t, conns["1"][0], 0)
	commit(t, conn, 0)

	// The followers have it too.
	control.Barrier()
	selectN(t, conns["0"][0], 2)
	selectN(t, conns["2"][0], 2)
}

// Leadership is lost before the Begin hook fires, the same leader gets
// re-elected.
func TestIntegration_Begin_HookCheck_NotLeader_SameLeader(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	control.Elect("0").When().Command(3).Committed().Depose()

	conn := conns["0"][0]
	begin(t, conn)
	insertOne(t, conn, 0)
	commit(t, conn, 0)

	begin(t, conn)
	insertTwo(t, conn, sqlite3.ErrIoErrNotLeader)

	// Take a connection against the new leader node and insert the same
	// value.
	control.Elect("0")
	control.Barrier()
	begin(t, conn)
	insertTwo(t, conns["0"][0], 0)
	commit(t, conn, 0)

	// The followers have it too.
	control.Barrier()
	selectN(t, conns["1"][0], 2)
	selectN(t, conns["2"][0], 2)
}

// Leadership is lost before trying to apply the Open command to open follower
// connections.
func TestIntegration_Begin_OpenFollower_NotLeader(t *testing.T) {
	conns, control, cleanup := newCluster(t, noCreateTable, noLeaderCheck(1))
	defer cleanup()

	// Since no leader was elected, we fail.
	createTable(t, conns["0"][0], sqlite3.ErrIoErrNotLeader)

	// Take a connection against a new leader and create again the
	// test table.
	control.Elect("1")
	control.Barrier()
	createTable(t, conns["1"][0], 0)

	// Check that followers see the change.
	control.Barrier()
	selectAny(t, conns["0"][0])
	selectAny(t, conns["2"][0])
}

// Leadership is lost when committing the Open command to open follower
// connections. A quorum is not reached and the Open command does not get
// applied.
func TestIntegration_Begin_OpenFollower_LeadershipLost(t *testing.T) {
	conns, control, cleanup := newCluster(t, noCreateTable)
	defer cleanup()

	control.Elect("0").When().Command(1).Enqueued().Depose()
	createTable(t, conns["0"][0], sqlite3.ErrIoErrLeadershipLost)

	// Take a connection against a new leader and create again the test
	// table.
	control.Elect("1")
	control.Barrier()
	createTable(t, conns["1"][0], 0)

	// Check that followers see the change.
	control.Barrier()
	selectAny(t, conns["0"][0])
	selectAny(t, conns["0"][0])
}

// Leadership is lost when committing the Open command to open follower
// connections. A quorum is reached and the Open command still gets applied,
// the same leader gets re-elected.
func TestIntegration_Begin_OpenFollower_LeadershipLostQuorumSameLeader(t *testing.T) {
	conns, control, cleanup := newCluster(t, noCreateTable)
	defer cleanup()

	control.Elect("0").When().Command(1).Appended().Depose()
	createTable(t, conns["0"][0], sqlite3.ErrIoErrLeadershipLost)

	control.Elect("0")
	control.Barrier()

	// Take a connection against the re-elected leader node and create again
	// the test table.
	createTable(t, conns["0"][0], 0)

	// The followers have it too.
	control.Barrier()
	selectAny(t, conns["1"][0])
	selectAny(t, conns["2"][0])
}

// Leadership is lost when committing the Open command to open follower
// connections. A quorum is reached and the Open command still gets applied, a
// different leader gets re-elected.
func TestIntegration_Begin_OpenFollower_LeadershipLostQuorumOtherLeader(t *testing.T) {
	conns, control, cleanup := newCluster(t, noCreateTable)
	defer cleanup()

	control.Elect("0").When().Command(1).Appended().Depose()
	createTable(t, conns["0"][0], sqlite3.ErrIoErrLeadershipLost)

	// Take a connection against the a leader and create again the test
	// table.
	control.Elect("1")
	control.Barrier()
	createTable(t, conns["1"][0], 0)

	// The followers have it too.
	control.Barrier()
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

	begin(t, conn1)
	insertOne(t, conn1, 0)

	begin(t, conn2)
	insertTwo(t, conn2, sqlite3.ErrNoExtended(sqlite3.ErrBusy))

	commit(t, conn1, 0)
	selectOne(t, conn1)
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

	begin(t, conn1)
	insertOne(t, conn1, 0)
	go func() {
		time.Sleep(rafttest.Duration(25 * time.Millisecond))
		commit(t, conn1, 0)
	}()

	insertTwo(t, conn2, 0)

	// The change is visible from other nodes
	control.Barrier()

	selectN(t, conns["1"][0], 2)
	selectN(t, conns["2"][0], 2)
}

// Trying to start two write transaction on the same connection fails.
func TestIntegration_Begin_BusyRetrySameConn(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	control.Elect("0")
	conn := conns["0"][0]

	begin(t, conn)
	insertOne(t, conn, 0)
	done := make(chan struct{})
	go func() {
		time.Sleep(rafttest.Duration(25 * time.Millisecond))
		commit(t, conn, 0)
		close(done)
	}()

	_, err := conn.Exec("BEGIN", nil)
	require.EqualError(t, err, "cannot start a transaction within a transaction")
	<-done

	// The change is visible from other nodes
	control.Barrier()
	selectOne(t, conns["1"][0])
	selectOne(t, conns["2"][0])
}

// The server is not the leader anymore when the Frames hook fires.
func TestIntegration_Frames_HookCheck_NotLeader(t *testing.T) {
	conns, control, cleanup := newCluster(t, noLeaderCheck(3))
	defer cleanup()

	control.Elect("0").When().Command(3).Committed().Depose()

	conn := conns["0"][0]
	begin(t, conn)
	insertOne(t, conn, 0)
	commit(t, conn, 0)

	begin(t, conn)
	insertTwo(t, conn, sqlite3.ErrIoErrNotLeader)

	// Take a connection against the new leader node and insert the same
	// value.
	control.Elect("1")
	control.Barrier()
	begin(t, conn)
	insertTwo(t, conns["1"][0], 0)
	commit(t, conn, 0)

	// The followers have it too.
	control.Barrier()
	selectN(t, conns["0"][0], 2)
	selectN(t, conns["2"][0], 2)
}

// The server is not the leader anymore when the Frames hook tries to apply the
// Frames command.
func TestIntegration_Frames_NotLeader(t *testing.T) {
	conns, control, cleanup := newCluster(t, noLeaderCheck(5))
	defer cleanup()

	control.Elect("0").When().Command(3).Committed().Depose()

	conn := conns["0"][0]
	begin(t, conn)
	insertOne(t, conn, 0)
	commit(t, conn, 0)

	begin(t, conn)
	insertTwo(t, conn, 0)
	commit(t, conn, sqlite3.ErrIoErrNotLeader)

	// Take a connection against the new leader node and insert the same
	// value.
	control.Elect("1")
	control.Barrier()
	begin(t, conn)
	insertTwo(t, conns["1"][0], 0)
	commit(t, conn, 0)

	// The followers have it too.
	control.Barrier()
	selectN(t, conns["0"][0], 2)
	selectN(t, conns["2"][0], 2)
}

// The node loses leadership when the Frames hook tries to apply the Frames
// command. The frames is a commit one, and no quorum is reached for the
// inflight Frames command.
func TestIntegration_Frames_LeadershipLost_Commit(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	control.Elect("0").When().Command(1).Enqueued().Depose()

	conn := conns["0"][0]
	begin(t, conn)
	insertOne(t, conn, 0)
	commit(t, conn, sqlite3.ErrIoErrLeadershipLost)

	// Take a connection against the new leader node and insert the same
	// value.
	control.Elect("1")
	control.Barrier()
	begin(t, conn)
	insertOne(t, conns["1"][0], 0)
	commit(t, conn, 0)

	// The followers have it too.
	control.Barrier()
	selectOne(t, conns["0"][0])
	selectOne(t, conns["2"][0])
}

// Leadership is lost when applying the Frames command, but a quorum is reached
// and the command actually gets committed. The same node that lost leadership
// gets re-elected.
func TestIntegration_Frames_LeadershipLost_Quorum_SameLeader(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	control.Elect("0").When().Command(1).Appended().Depose()

	conn := conns["0"][0]
	begin(t, conn)
	insertOne(t, conn, 0)
	commit(t, conn, sqlite3.ErrIoErrLeadershipLost)

	// Take a connection against the new leader node and insert a new
	// value.
	control.Elect("0")
	control.Barrier()
	begin(t, conn)
	insertTwo(t, conns["0"][0], 0)
	commit(t, conn, 0)

	// The followers have it too.
	control.Barrier()
	selectN(t, conns["1"][0], 2)
	selectN(t, conns["2"][0], 2)
}

// Leadership is lost when applying the Frames command, but a quorum is reached
// and the command actually gets committed. A different node than the one that
// lost leadership gets re-elected.
func TestIntegration_Frames_LeadershipLost_Quorum_OtherLeader(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	control.Elect("0").When().Command(1).Appended().Depose()

	conn := conns["0"][0]
	begin(t, conn)
	insertOne(t, conn, 0)
	commit(t, conn, sqlite3.ErrIoErrLeadershipLost)

	// Take a connection against the new leader node and insert a new
	// value.
	control.Elect("1")
	control.Barrier()
	begin(t, conn)
	insertTwo(t, conns["1"][0], 0)
	commit(t, conn, 0)

	// The followers have it too.
	control.Barrier()
	selectN(t, conns["0"][0], 2)
	selectN(t, conns["2"][0], 2)
}

// The server is not the leader anymore when the Undo hook fires.
func TestIntegration_Undo_HookCheck_NotLeader(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	control.Elect("0").When().Command(3).Committed().Depose()

	conn := conns["0"][0]

	// Lower SQLite's page cache size to force it to write uncommitted
	// dirty pages to the WAL.
	lowerCacheSize(t, conn)

	// Start a write transaction and insert enough data to cause page cache
	// stress and flush to the WAL and trigger exactly one Frames
	// command. After the Frames command gets committed the leader gets
	// deposed.
	begin(t, conn)
	insertN(t, conn, 500)
	rollback(t, conn)

	control.Elect("1")
	control.Barrier()

	// No node sees the commit
	selectZero(t, conns["0"][0])
	selectZero(t, conns["1"][0])
	selectZero(t, conns["2"][0])
}

// The node is not the leader anymore when the Undo hook tries to apply the
// Undo command.
func TestIntegration_Undo_NotLeader(t *testing.T) {
	conns, control, cleanup := newCluster(t, noLeaderCheck(5))
	defer cleanup()

	control.Elect("0").When().Command(3).Committed().Depose()

	conn := conns["0"][0]

	// Lower SQLite's page cache size to force it to write uncommitted
	// dirty pages to the WAL.
	lowerCacheSize(t, conn)

	// Start a write transaction and insert enough data to cause page cache
	// stress and flush to the WAL and trigger exactly one Frames
	// command. After the Frames command gets committed the leader gets
	// deposed.
	begin(t, conn)
	insertN(t, conn, 500)
	rollback(t, conn)

	control.Elect("1")
	control.Barrier()

	// No node sees the commit
	selectZero(t, conns["0"][0])
	selectZero(t, conns["1"][0])
	selectZero(t, conns["2"][0])
}

// The node loses leadership when the Undo hook tries to apply the Undo
// command. No quorum is reached for the inflight Undo command.
func TestIntegration_Undo_LeadershipLost(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	control.Elect("0").When().Command(2).Enqueued().Depose()

	conn := conns["0"][0]

	// Lower SQLite's page cache size to force it to write uncommitted
	// dirty pages to the WAL.
	lowerCacheSize(t, conn)

	// Start a write transaction and insert enough data to cause page cache
	// stress and flush to the WAL and trigger exactly one Frames
	// command. After the Frames command gets committed the leader gets
	// deposed.
	begin(t, conn)
	insertN(t, conn, 500)
	rollback(t, conn)

	control.Elect("1")
	control.Barrier()

	// No node sees the commit
	selectZero(t, conns["0"][0])
	selectZero(t, conns["1"][0])
	selectZero(t, conns["2"][0])
}

// Leadership is lost when applying the Undo command, but a quorum is reached
// and the command actually gets committed. The same node that lost leadership
// gets re-elected.
func BROKEN_TestIntegration_Undo_LeadershipLost_Quorum_SameLeader(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	control.Elect("0").When().Command(2).Appended().Depose()

	conn := conns["0"][0]

	// Lower SQLite's page cache size to force it to write uncommitted
	// dirty pages to the WAL.
	lowerCacheSize(t, conn)

	// Start a write transaction and insert enough data to cause page cache
	// stress and flush to the WAL and trigger exactly one Frames
	// command. After the Frames command gets committed the leader gets
	// deposed.
	begin(t, conn)
	insertN(t, conn, 500)
	rollback(t, conn)

	control.Elect("0")
	control.Barrier()

	// No node sees the commit
	selectZero(t, conns["0"][0])
	selectZero(t, conns["1"][0])
	selectZero(t, conns["2"][0])
}

// Leadership is lost when applying the Undo command, but a quorum is reached
// and the command actually gets committed. A different node than the one that
// lost leadership gets re-elected.
func BROKEN_TestIntegration_Undo_LeadershipLost_Quorum_OtherLeader(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	control.Elect("0").When().Command(2).Appended().Depose()

	conn := conns["0"][0]

	// Lower SQLite's page cache size to force it to write uncommitted
	// dirty pages to the WAL.
	lowerCacheSize(t, conn)

	// Start a write transaction and insert enough data to cause page cache
	// stress and flush to the WAL and trigger exactly one Frames
	// command. After the Frames command gets committed the leader gets
	// deposed.
	begin(t, conn)
	insertN(t, conn, 500)
	rollback(t, conn)

	control.Elect("1")
	control.Barrier()

	// No node sees the commit
	selectZero(t, conns["0"][0])
	selectZero(t, conns["1"][0])
	selectZero(t, conns["2"][0])
}

// Check that the WAL is actually replicated.
func TestIntegration_Replication(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	control.Elect("0")

	conn := conns["0"][0]
	begin(t, conn)
	insertOne(t, conn, 0)
	commit(t, conn, 0)
	selectOne(t, conns["0"][0])

	// The followers see the inserted table too.
	control.Barrier()
	selectOne(t, conns["1"][0])
	selectOne(t, conns["2"][0])
}

// Exercise creating and restoring snapshots.
func TestIntegration_Snapshot(t *testing.T) {
	conns, control, cleanup := newCluster(t)
	defer cleanup()

	term := control.Elect("0")

	// Disconnect the follower immediately, so it will be forced to use the
	// snapshot at reconnection.
	term.Disconnect("1")

	// Get a leader connection on the leader node.
	conn := conns["0"][0]

	// Run a few of WAL-writing queries.
	insertOne(t, conn, 0)
	insertTwo(t, conn, 0)
	insertThree(t, conn, 0)

	// Take a snapshot on the leader after this first batch of queries.
	term.Snapshot("0")

	// Make sure snapshot is taken by the leader.
	control.Barrier()
	assert.Equal(t, uint64(1), control.Snapshots("0"))

	term.Reconnect("1")

	// Run an extra query to proof that the follower with the restored
	// snapshot is still functional.
	_, err := conn.Exec("INSERT INTO test VALUES(4)", nil)
	require.NoError(t, err)

	// The follower will now have to restore the snapshot.
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
	require.NoError(t, rows.Next(values))
	assert.Equal(t, int64(4), values[0].(int64))

}

// Set the page and cache size of SQLite in order to force it write pages to
// the WAL even before the transaction is committed. This allows to trigger the
// xUndo callback, which would be otherwise not called (because all dirty pages
// were still SQLite's page cache).
func lowerCacheSize(t *testing.T, conn *sqlite3.SQLiteConn) {
	t.Helper()
	_, err := conn.Exec("PRAGMA page_size = 1024", nil)
	require.NoError(t, err) // SQLite should never return an error here
	_, err = conn.Exec("PRAGMA cache_size = 1", nil)
	require.NoError(t, err) // SQLite should never return an error here
}

// Creates a test table in a transaction using the given connection. If code is
// not zero, assert that the query fails.
func createTable(t *testing.T, conn *sqlite3.SQLiteConn, code sqlite3.ErrNoExtended) {
	t.Helper()
	begin(t, conn)
	_, err := conn.Exec("CREATE TABLE test (n INT, UNIQUE(n))", nil)
	if code == 0 {
		require.NoError(t, err)
		commit(t, conn, 0)
	} else {
		requireEqualErrNo(t, code, err)
	}
}

// Begin a transaction
func begin(t *testing.T, conn *sqlite3.SQLiteConn) {
	t.Helper()
	_, err := conn.Exec("BEGIN", nil)
	require.NoError(t, err)
}

// Commit a transaction.
func commit(t *testing.T, conn *sqlite3.SQLiteConn, code sqlite3.ErrNoExtended) {
	t.Helper()
	_, err := conn.Exec("COMMIT", nil)
	if code == 0 {
		require.NoError(t, err)
	} else {
		requireEqualErrNo(t, code, err)
	}
}

// Rollback a transaction.
func rollback(t *testing.T, conn *sqlite3.SQLiteConn) {
	t.Helper()
	_, err := conn.Exec("ROLLBACK", nil)
	require.NoError(t, err) // SQLite should never return an error here
}

// Inserts a single row into the test table with value 1.
func insertOne(t *testing.T, conn *sqlite3.SQLiteConn, code sqlite3.ErrNoExtended) {
	t.Helper()
	_, err := conn.Exec("INSERT INTO test(n) VALUES (1)", nil)
	if code == 0 {
		require.NoError(t, err)
	} else {
		requireEqualErrNo(t, code, err)
	}
}

// Inserts a single row into the test table with value 2.
func insertTwo(t *testing.T, conn *sqlite3.SQLiteConn, code sqlite3.ErrNoExtended) {
	t.Helper()
	_, err := conn.Exec("INSERT INTO test(n) VALUES (2)", nil)
	if code == 0 {
		require.NoError(t, err)
	} else {
		requireEqualErrNo(t, code, err)
	}
}

// Inserts a single row into the test table with value 3.
func insertThree(t *testing.T, conn *sqlite3.SQLiteConn, code sqlite3.ErrNoExtended) {
	t.Helper()
	_, err := conn.Exec("INSERT INTO test(n) VALUES (3)", nil)
	if code == 0 {
		require.NoError(t, err)
	} else {
		requireEqualErrNo(t, code, err)
	}
}

// Inserts the given number of rows in the test table.
func insertN(t *testing.T, conn *sqlite3.SQLiteConn, n int) {
	t.Helper()

	values := ""
	for i := 0; i < n; i++ {
		values += fmt.Sprintf(" (%d),", i+1)
	}
	values = values[:len(values)-1]
	_, err := conn.Exec(fmt.Sprintf("INSERT INTO test(n) VALUES %s", values), nil)
	require.NoError(t, err)
}

// Selects from the test table to assert that it's there.
func selectAny(t *testing.T, conn *sqlite3.SQLiteConn) {
	t.Helper()
	_, err := conn.Exec("SELECT n FROM test", nil)
	assert.NoError(t, err)
}

// Select from the test table and assert that there are zero rows.
func selectZero(t *testing.T, conn *sqlite3.SQLiteConn) {
	t.Helper()
	rows, err := conn.Query("SELECT n FROM test", nil)
	require.NoError(t, err)
	values := make([]driver.Value, 1)
	assert.Equal(t, io.EOF, rows.Next(values))
	require.NoError(t, rows.Close())
}

// Select a row from the test table and check that its value is 1 and it's the
// only row.
func selectOne(t *testing.T, conn *sqlite3.SQLiteConn) {
	t.Helper()
	rows, err := conn.Query("SELECT n FROM test", nil)
	require.NoError(t, err)
	values := make([]driver.Value, 1)
	require.NoError(t, rows.Next(values))
	assert.Equal(t, int64(1), values[0])
	assert.Equal(t, io.EOF, rows.Next(values))
	require.NoError(t, rows.Close())
}

// Select N rows from the test table and check that their value is progressing
// and no other rows are there.
func selectN(t *testing.T, conn *sqlite3.SQLiteConn, n int) {
	t.Helper()
	rows, err := conn.Query("SELECT n FROM test ORDER BY n", nil)
	require.NoError(t, err)
	values := make([]driver.Value, 1)
	for i := 0; i < n; i++ {
		require.NoError(t, rows.Next(values))
		assert.Equal(t, int64(i+1), values[0])
	}
	assert.Equal(t, io.EOF, rows.Next(values))
	require.NoError(t, rows.Close())
}

// Expose various internal cluster parameters that tests can tweak with
// clusterOption functions.
type clusterOptions struct {
	CreateTable bool                   // Whether to create the initial test table, default is true
	Methods     []*replication.Methods // Methods instances
}

type clusterOption func(*clusterOptions)

// Option that disable leadership checks in the methods hooks.
func noLeaderCheck(n int) clusterOption {
	return func(options *clusterOptions) {
		for _, methods := range options.Methods {
			methods.NoLeaderCheck(n)
		}
	}
}

// Option that disable creating the initial test table. In particular this
// means that no initial Open command will be issued, so this option is
// relevant mostly for tests exercising the Begin hook.
func noCreateTable(options *clusterOptions) {
	options.CreateTable = false
}

// Create a new test cluster with 3 nodes, each with its own FSM, Methods and
// two connections opened in leader mode.
func newCluster(t *testing.T, opts ...clusterOption) (map[raft.ServerID][2]*sqlite3.SQLiteConn, *rafttest.Control, func()) {
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

	options := &clusterOptions{
		CreateTable: true,
		Methods:     methods,
	}
	for _, o := range opts {
		o(options)
	}

	if options.CreateTable {
		// Create the test table and the depose the leader so tests can
		// start fresh.
		control.Elect("0").When().Command(2).Committed().Depose()
		createTable(t, conns["0"][0], 0)
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
