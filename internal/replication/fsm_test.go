package replication_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/internal/protocol"
	"github.com/CanonicalLtd/dqlite/internal/replication"
	"github.com/CanonicalLtd/dqlite/internal/transaction"
	"github.com/CanonicalLtd/go-sqlite3"
	"github.com/hashicorp/raft"
	"github.com/ryanfaerman/fsm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Exercise error-leading situations.
func TestFSM_ApplyError(t *testing.T) {
	cases := []struct {
		title string
		f     func(*testing.T, *replication.FSM) error // Function leading to the error
		error string                                   // Expected error message.
	}{
		{
			`log data is garbage`,
			func(t *testing.T, fsm *replication.FSM) error {
				return fsm.Apply(&raft.Log{Data: []byte("garbage")}).(error)
			},
			"corrupted command data: protobuf failure: proto: illegal wireType 7",
		},
		{
			`open error`,
			func(t *testing.T, fsm *replication.FSM) error {
				fsm.SetDir("/foo/bar")
				return fsmApply(fsm, 0, protocol.NewOpen("test.db"))
			},
			"open test.db: open error for /foo/bar/test.db: unable to open database file",
		},
	}
	for _, c := range cases {
		t.Run(c.title, func(t *testing.T) {
			fsm, cleanup := newFSM(t)
			defer cleanup()

			fsm.PanicOnFailure(false)

			err, ok := c.f(t, fsm).(error)
			require.True(t, ok, "test case did not return an error")
			assert.EqualError(t, err, c.error)
		})
	}
}

// Exercise panic-leading situations.
func TestFSM_ApplyPanics(t *testing.T) {
	cases := []struct {
		title string
		f     func(*testing.T, *replication.FSM) // Function leading to the panic.
		panic string                             // Expected panic message.
	}{
		{
			`existing transaction has non-leader connection`,
			func(t *testing.T, fsm *replication.FSM) {
				require.NoError(t, fsmApply(fsm, 0, protocol.NewOpen("test.db")))
				fsm.Transactions().AddFollower(&sqlite3.SQLiteConn{}, 123)
				fsmApply(fsm, 1, protocol.NewBegin(123, "test.db"))
			},
			"unexpected follower connection for existing transaction",
		},
		{
			`new follower transaction started before previous is ended`,
			func(t *testing.T, fsm *replication.FSM) {
				require.NoError(t, fsmApply(fsm, 0, protocol.NewOpen("test.db")))
				require.NoError(t, fsmApply(fsm, 1, protocol.NewBegin(123, "test.db")))
				fsmApply(fsm, 2, protocol.NewBegin(456, "test.db"))
			},
			"a transaction for this connection is already registered with ID 123",
		},
		{
			`dangling leader connection`,
			func(t *testing.T, fsm *replication.FSM) {
				methods := sqlite3.PassthroughReplicationMethods()
				conn, cleanup := newLeaderConn(t, fsm.Dir(), methods)
				defer cleanup()

				fsm.Connections().AddLeader("test.db", conn)
				fsm.Transactions().AddLeader(conn, 1, nil)

				require.NoError(t, fsmApply(fsm, 0, protocol.NewOpen("test.db")))
				fsmApply(fsm, 1, protocol.NewBegin(123, "test.db"))
			},
			"unexpected transaction 1 pending as leader",
		},
		{
			`wal frames transaction not found`,
			func(t *testing.T, fsm *replication.FSM) {
				params := newWalFramesParams()
				fsmApply(fsm, 0, protocol.NewWalFrames(123, params))
			},
			"no transaction with ID 123",
		},
		{
			`undo transaction not found`,
			func(t *testing.T, fsm *replication.FSM) {
				fsmApply(fsm, 0, protocol.NewUndo(123))
			},
			"no transaction with ID 123",
		},
		{
			`end transaction not found`,
			func(t *testing.T, fsm *replication.FSM) {
				fsmApply(fsm, 0, protocol.NewEnd(123))
			},
			"no transaction with ID 123",
		},
	}
	for _, c := range cases {
		t.Run(c.title, func(t *testing.T) {
			fsm, cleanup := newFSM(t)
			defer cleanup()
			assert.PanicsWithValue(t, c.panic, func() { c.f(t, fsm) })
		})
	}
}

// Test the happy path of the various transaction-related protocol.
func TestFSM_Apply(t *testing.T) {
	cases := []struct {
		title string
		f     func(*testing.T, *replication.FSM) // Apply txn commands
		state fsm.State                          // Expected txn state.
	}{
		{
			`begin leader`,
			func(t *testing.T, fsm *replication.FSM) {
				methods := sqlite3.PassthroughReplicationMethods()
				conn, cleanup := newLeaderConn(t, fsm.Dir(), methods)
				defer cleanup()

				fsm.Connections().AddLeader("test.db", conn)

				txn := fsm.Transactions().AddLeader(conn, 1, nil)
				txn.DryRun(true)

				// Begin the WAL write transaction by hand, as it would be done
				// by the Methods.Begin hook.
				assert.NoError(t, txn.Do(txn.Begin))

				fsm.Apply(newRaftLog(0, protocol.NewBegin(1, "test.db")))
			},
			transaction.Started,
		},
		{
			`begin follower`,
			func(t *testing.T, fsm *replication.FSM) {
				fsm.Apply(newRaftLog(0, protocol.NewOpen("test.db")))
				fsm.Apply(newRaftLog(1, protocol.NewBegin(1, "test.db")))
			},
			transaction.Started,
		},
		{
			`wal frames`,
			func(t *testing.T, fsm *replication.FSM) {
				fsm.Apply(newRaftLog(0, protocol.NewOpen("test.db")))
				fsm.Apply(newRaftLog(1, protocol.NewBegin(1, "test.db")))

				txn := fsm.Transactions().GetByID(1)
				txn.DryRun(true)

				params := newWalFramesParams()
				fsm.Apply(newRaftLog(2, protocol.NewWalFrames(1, params)))

			},
			transaction.Writing,
		},
		{
			`undo`,
			func(t *testing.T, fsm *replication.FSM) {
				fsm.Apply(newRaftLog(0, protocol.NewOpen("test.db")))
				fsm.Apply(newRaftLog(1, protocol.NewBegin(1, "test.db")))

				txn := fsm.Transactions().GetByID(1)
				txn.DryRun(true)

				fsm.Apply(newRaftLog(2, protocol.NewUndo(1)))
			},
			transaction.Undoing,
		},
	}

	for _, c := range cases {
		t.Run(c.title, func(t *testing.T) {
			fsm, cleanup := newFSM(t)
			defer cleanup()

			c.f(t, fsm)

			txn := fsm.Transactions().GetByID(1)
			require.NotNil(t, txn)
			txn.Enter()
			assert.Equal(t, c.state, txn.State())
		})
	}
}

// The open command creates a new follower connection.
func TestFSM_ApplyOpen(t *testing.T) {
	fsm, cleanup := newFSM(t)
	defer cleanup()

	fsm.Apply(newRaftLog(0, protocol.NewOpen("test.db")))
	assert.Equal(t, true, fsm.Connections().HasFollower("test.db"))
}

// The end command commits the transaction and deletes it from the registry.
func TestFSM_ApplyEnd(t *testing.T) {
	fsm, cleanup := newFSM(t)
	defer cleanup()

	fsm.Apply(newRaftLog(0, protocol.NewOpen("test.db")))
	fsm.Apply(newRaftLog(1, protocol.NewBegin(1, "test.db")))

	txn := fsm.Transactions().GetByID(1)

	fsm.Apply(newRaftLog(2, protocol.NewEnd(1)))

	txn.Enter()
	assert.Equal(t, transaction.Ended, txn.State())
	assert.Nil(t, fsm.Transactions().GetByID(1))
}

func TestFSM_ApplyCheckpointPanicsIfFollowerTransactionIsInFlight(t *testing.T) {
	fsm, cleanup := newFSM(t)
	defer cleanup()

	fsm.Apply(newRaftLog(0, protocol.NewOpen("test.db")))
	fsm.Apply(newRaftLog(1, protocol.NewBegin(1, "test.db")))

	f := func() { fsm.Apply(newRaftLog(2, protocol.NewCheckpoint("test.db"))) }
	assert.PanicsWithValue(t, "can't run with transaction 1 started as follower", f)
}

func TestFSM_ApplyCheckpointWithLeaderConnection(t *testing.T) {
	fsm, cleanup := newFSM(t)
	defer cleanup()

	conn, cleanup := newLeaderConn(t, fsm.Dir(), sqlite3.PassthroughReplicationMethods())
	defer cleanup()

	fsm.Apply(newRaftLog(0, protocol.NewOpen("test.db")))

	_, err := conn.Exec("CREATE TABLE foo (n INT)", nil)
	require.NoError(t, err)
	require.Equal(t, true, sqlite3.WalSize(conn) > 0, "WAL has non-positive size")

	fsm.Apply(newRaftLog(1, protocol.NewCheckpoint("test.db")))

	require.Equal(t, int64(0), sqlite3.WalSize(conn), "WAL has non-zero size")
}

// In case the snapshot the source backup connection can't be opened, an error
// is returned.
func TestFSM_SnapshotSourceConnectionError(t *testing.T) {
	fsm, cleanup := newFSM(t)
	defer cleanup()

	// Register the database.
	fsmApply(fsm, 0, protocol.NewOpen("test.db"))

	// Remove the FSM dir to trigger a snapshot error.
	require.NoError(t, os.RemoveAll(fsm.Dir()))

	// Create a snapshot
	snapshot, err := fsm.Snapshot()
	assert.Nil(t, snapshot)

	expected := fmt.Sprintf(
		"test.db: source connection: open error for %s: unable to open database file",
		filepath.Join(fsm.Dir(), "test.db"))

	assert.EqualError(t, err, expected)
}

// In case the snapshot is made when an active transaction is in progress an
// error is returned.
func TestFSM_SnapshotActiveTransactionError(t *testing.T) {
	fsm, cleanup := newFSM(t)
	defer cleanup()

	// Register the database and start a writing transaction.
	fsmApply(fsm, 0, protocol.NewOpen("test.db"))
	fsmApply(fsm, 1, protocol.NewBegin(1, "test.db"))
	fsm.Transactions().GetByID(1).DryRun(true)
	fsmApply(fsm, 2, protocol.NewWalFrames(1, newWalFramesParams()))

	// Create a snapshot
	snapshot, err := fsm.Snapshot()
	assert.Nil(t, snapshot)

	expected := "test.db: transaction 1 writing as follower is in progress"
	assert.EqualError(t, err, expected)
}

// In case the snapshot is taken while there's an idle transaction, no error is
// returned, but the transaction is included in the snapshot.
func TestFSM_SnapshotIdleTransaction(t *testing.T) {
	fsm1, cleanup1 := newFSM(t)
	defer cleanup1()

	// Register the database and start an idle transaction.
	fsmApply(fsm1, 0, protocol.NewOpen("test.db"))
	fsmApply(fsm1, 1, protocol.NewBegin(1, "test.db"))

	store := newSnapshotStore()
	sink := newSnapshotSink(t, store)

	// Create and persist a snapshot.
	snapshot, err := fsm1.Snapshot()
	require.NoError(t, err)
	require.NoError(t, snapshot.Persist(sink))

	// Restore the snapshot on a new fsm.
	fsm2, cleanup2 := newFSM(t)
	defer cleanup2()
	_, reader, err := store.Open(sink.ID())
	require.NoError(t, err)

	require.NoError(t, fsm2.Restore(reader))

	// There's a registered transaction in Started state
	txn := fsm2.Transactions().GetByID(1)
	txn.Enter()
	assert.Equal(t, transaction.Started, txn.State())

}

func TestFSM_Snapshot(t *testing.T) {
	fsm, cleanup := newFSM(t)
	defer cleanup()

	// Create a database with some content.
	db, err := sql.Open("sqlite3", filepath.Join(fsm.Dir(), "test.db"))
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE foo (n INT); INSERT INTO foo VALUES(1)")
	require.NoError(t, err)
	db.Close()

	// Register the database.
	fsm.Apply(newRaftLog(0, protocol.NewOpen("test.db")))

	// Create a snapshot
	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	// Persist the snapshot in a store.
	store := newSnapshotStore()
	sink := newSnapshotSink(t, store)
	if err := snapshot.Persist(sink); err != nil {
		t.Fatal(err)
	}

	// Restore the snapshot
	_, reader, err := store.Open(sink.ID())
	if err != nil {
		t.Fatal(err)
	}
	if err := fsm.Restore(reader); err != nil {
		t.Fatal(err)
	}

	// The restored file has the expected data
	db, err = sql.Open("sqlite3", filepath.Join(fsm.Dir(), "test.db"))
	require.NoError(t, err)
	rows, err := db.Query("SELECT * FROM foo", nil)
	require.NoError(t, err)
	defer rows.Close()
	require.Equal(t, true, rows.Next())
	var n int
	require.NoError(t, rows.Scan(&n))
	assert.Equal(t, 1, n)
}

func TestFSM_RestoreLogIndexError(t *testing.T) {
	fsm, cleanup := newFSM(t)
	defer cleanup()

	reader := ioutil.NopCloser(bytes.NewBuffer([]byte("garbage")))

	err := fsm.Restore(reader)

	assert.EqualError(t, err, "failed to read FSM index: unexpected EOF")
}

func TestFSM_Restore(t *testing.T) {
	fsm, cleanup := newFSM(t)
	defer cleanup()

	// Create a database with some content.
	db, err := sql.Open("sqlite3", filepath.Join(fsm.Dir(), "test.db"))
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE foo (n INT); INSERT INTO foo VALUES(1)")
	require.NoError(t, err)
	db.Close()

	// Register the database.
	fsm.Apply(newRaftLog(0, protocol.NewOpen("test.db")))

	// Create a snapshot
	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	// Persist the snapshot in a store.
	store := newSnapshotStore()
	sink := newSnapshotSink(t, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatal(err)
	}

	// Restore the snapshot
	_, reader, err := store.Open(sink.ID())
	if err != nil {
		t.Fatal(err)
	}
	if err := fsm.Restore(reader); err != nil {
		t.Fatal(err)
	}

	// The restored file has the expected data
	db, err = sql.Open("sqlite3", filepath.Join(fsm.Dir(), "test.db"))
	require.NoError(t, err)
	rows, err := db.Query("SELECT * FROM foo", nil)
	require.NoError(t, err)
	defer rows.Close()
	require.Equal(t, true, rows.Next())
	var n int
	require.NoError(t, rows.Scan(&n))
	assert.Equal(t, 1, n)
}

// Return a fresh FSM for tests.
func newFSM(t *testing.T) (*replication.FSM, func()) {
	dir, cleanup := newDir(t)

	fsm := replication.NewFSM(dir)
	fsm.Tracers().Testing(t, 0)

	// We need to disable replication mode checks, because leader
	// connections created with newLeaderConn() haven't actually initiated
	// a WAL write transaction and acquired the db lock necessary for
	// sqlite3_replication_mode() to succeed.
	fsm.Transactions().SkipCheckReplicationMode(true)

	return fsm, cleanup
}

// Helper for applying a new log command.
func fsmApply(fsm *replication.FSM, index uint64, cmd *protocol.Command) error {
	log := newRaftLog(index, cmd)
	result := fsm.Apply(log)
	if result == nil {
		return nil
	}
	if err, ok := result.(error); ok {
		return err
	}
	return fmt.Errorf("fsm.Apply() did not return an error object: %v", result)
}

// Create a *raft.Log for the given dqlite protocol Command.
func newRaftLog(index uint64, cmd *protocol.Command) *raft.Log {
	data, err := protocol.MarshalCommand(cmd)
	if err != nil {
		panic(fmt.Sprintf("cannot marshal command: %v", err))
	}
	return &raft.Log{Data: data, Index: index}
}

// Return a new temporary directory.
func newDir(t *testing.T) (string, func()) {
	//t.Helper()

	dir, err := ioutil.TempDir("", "dqlite-replication-test-")
	assert.NoError(t, err)

	cleanup := func() {
		_, err := os.Stat(dir)
		if err != nil {
			assert.True(t, os.IsNotExist(err))
		} else {
			assert.NoError(t, os.RemoveAll(dir))
		}
	}

	return dir, cleanup
}

// Create a new SQLite connection in leader replication mode, opened against a
// database at a temporary file.
func newLeaderConn(t *testing.T, dir string, methods sqlite3.ReplicationMethods) (*sqlite3.SQLiteConn, func()) {
	conn, err := connection.OpenLeader(filepath.Join(dir, "test.db"), methods, 1000)
	if err != nil {
		t.Fatalf("failed to open leader connection: %v", err)
	}

	cleanup := func() {
		if err := connection.CloseLeader(conn); err != nil {
			t.Fatalf("failed to close leader connection: %v", err)
		}
	}

	return conn, cleanup
}

// Convenience to create test parameters for a wal frames command.
func newWalFramesParams() *sqlite3.ReplicationWalFramesParams {
	return &sqlite3.ReplicationWalFramesParams{
		Pages:     sqlite3.NewReplicationPages(2, 4096),
		PageSize:  4096,
		Truncate:  uint32(0),
		IsCommit:  1,
		SyncFlags: 10,
	}
}

// Return a raft snapshot store that uses the given directory to store
// snapshots.
func newSnapshotStore() raft.SnapshotStore {
	return raft.NewInmemSnapshotStore()
}

// Convenience to create a new test snapshot sink.
func newSnapshotSink(t *testing.T, store raft.SnapshotStore) raft.SnapshotSink {
	_, transport := raft.NewInmemTransport(raft.ServerAddress(""))
	sink, err := store.Create(
		raft.SnapshotVersionMax,
		uint64(1),
		uint64(1),
		raft.Configuration{},
		uint64(1),
		transport,
	)
	if err != nil {
		t.Fatalf("failed to create test snapshot sink: %v", err)
	}
	return sink
}
