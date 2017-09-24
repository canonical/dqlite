package replication_test

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/internal/log"
	"github.com/CanonicalLtd/dqlite/internal/protocol"
	"github.com/CanonicalLtd/dqlite/internal/replication"
	"github.com/CanonicalLtd/dqlite/internal/transaction"
	"github.com/CanonicalLtd/go-sqlite3x"
	"github.com/hashicorp/raft"
	"github.com/mattn/go-sqlite3"
	"github.com/mpvl/subtest"
	"github.com/ryanfaerman/fsm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Exercise panic-leading situations.
func TestFSM_ApplyPanics(t *testing.T) {
	for _, c := range fsmApplyPanicCases {
		subtest.Run(t, c.title, func(t *testing.T) {
			fsm, cleanup := newFSM(t)
			defer cleanup()
			assert.PanicsWithValue(t, c.panic, func() { c.f(t, fsm) })
		})
	}
}

var fsmApplyPanicCases = []struct {
	title string
	f     func(*testing.T, *replication.FSM) // Function leading to the panic.
	panic string                             // Expected panic message.
}{
	{
		`log data is garbage`,
		func(t *testing.T, fsm *replication.FSM) {
			fsm.Apply(&raft.Log{Data: []byte("garbage")})
		},
		"fsm: apply log 0: corrupted: protobuf failure: proto: illegal wireType 7",
	}, {
		`existing transaction has non-leader connection`,
		func(t *testing.T, fsm *replication.FSM) {
			fsm.Apply(newRaftLog(0, protocol.NewOpen("test.db")))
			fsm.Apply(newRaftLog(1, protocol.NewBegin("abcd", "test.db")))
			fsm.Apply(newRaftLog(2, protocol.NewBegin("abcd", "test.db")))
		},
		"fsm: apply log 2: begin: txn abcd: is started as follower instead of leader",
	}, {
		`new follower transaction started before previous is ended`,
		func(t *testing.T, fsm *replication.FSM) {
			fsm.Apply(newRaftLog(0, protocol.NewOpen("test.db")))
			fsm.Apply(newRaftLog(1, protocol.NewBegin("abcd", "test.db")))
			fsm.Apply(newRaftLog(2, protocol.NewBegin("efgh", "test.db")))
		},
		"a transaction for this connection is already registered with ID abcd",
	}, {
		`dangling leader connection`,
		func(t *testing.T, fsm *replication.FSM) {
			methods := sqlite3x.PassthroughReplicationMethods()
			conn, cleanup := newLeaderConn(t, fsm.Dir(), methods)
			defer cleanup()

			fsm.Connections().AddLeader("test.db", conn)
			fsm.Transactions().AddLeader(conn, "xxxx")

			fsm.Apply(newRaftLog(0, protocol.NewOpen("test.db")))
			fsm.Apply(newRaftLog(1, protocol.NewBegin("abcd", "test.db")))
		},
		"fsm: apply log 1: begin: txn abcd: dangling transaction pending as leader",
	}, {
		`wal frames transaction not found`,
		func(t *testing.T, fsm *replication.FSM) {
			params := newWalFramesParams()
			fsm.Apply(newRaftLog(0, protocol.NewWalFrames("abcd", params)))
		},
		"fsm: apply log 0: wal frames: txn abcd: not found",
	}, {
		`undo transaction not found`,
		func(t *testing.T, fsm *replication.FSM) {
			fsm.Apply(newRaftLog(0, protocol.NewUndo("abcd")))
		},
		"fsm: apply log 0: undo: txn abcd: not found",
	}, {
		`end transaction not found`,
		func(t *testing.T, fsm *replication.FSM) {
			fsm.Apply(newRaftLog(0, protocol.NewEnd("abcd")))
		},
		"fsm: apply log 0: end: txn abcd: not found",
	},
}

// Test the happy path of the various transaction-related protocol.
func TestFSM_ApplyTransactionCommands(t *testing.T) {
	for _, c := range fsmApplyCases {
		subtest.Run(t, c.title, func(t *testing.T) {
			fsm, cleanup := newFSM(t)
			defer cleanup()

			c.f(t, fsm)

			txn := fsm.Transactions().GetByID("0")
			require.NotNil(t, txn)
			txn.Enter()
			assert.Equal(t, c.state, txn.State())
		})
	}
}

var fsmApplyCases = []struct {
	title string
	f     func(*testing.T, *replication.FSM) // Apply txn commands
	state fsm.State                          // Expected txn state.
}{
	{
		`begin leader`,
		func(t *testing.T, fsm *replication.FSM) {
			methods := sqlite3x.PassthroughReplicationMethods()
			conn, cleanup := newLeaderConn(t, fsm.Dir(), methods)
			defer cleanup()

			txn := fsm.Transactions().AddLeader(conn, "0")
			txn.DryRun(true)

			fsm.Apply(newRaftLog(0, protocol.NewBegin("0", "test.db")))
		},
		transaction.Started,
	},
	{
		`begin follower`,
		func(t *testing.T, fsm *replication.FSM) {
			fsm.Apply(newRaftLog(0, protocol.NewOpen("test.db")))
			fsm.Apply(newRaftLog(1, protocol.NewBegin("0", "test.db")))
		},
		transaction.Started,
	},
	{
		`wal frames`,
		func(t *testing.T, fsm *replication.FSM) {
			fsm.Apply(newRaftLog(0, protocol.NewOpen("test.db")))
			fsm.Apply(newRaftLog(1, protocol.NewBegin("0", "test.db")))

			txn := fsm.Transactions().GetByID("0")
			txn.DryRun(true)

			params := newWalFramesParams()
			fsm.Apply(newRaftLog(2, protocol.NewWalFrames("0", params)))

		},
		transaction.Writing,
	},
	{
		`undo`,
		func(t *testing.T, fsm *replication.FSM) {
			fsm.Apply(newRaftLog(0, protocol.NewOpen("test.db")))
			fsm.Apply(newRaftLog(1, protocol.NewBegin("0", "test.db")))

			txn := fsm.Transactions().GetByID("0")
			txn.DryRun(true)

			fsm.Apply(newRaftLog(2, protocol.NewUndo("0")))
		},
		transaction.Undoing,
	},
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
	fsm.Apply(newRaftLog(1, protocol.NewBegin("abcd", "test.db")))

	txn := fsm.Transactions().GetByID("abcd")

	fsm.Apply(newRaftLog(2, protocol.NewEnd("abcd")))

	txn.Enter()
	assert.Equal(t, transaction.Ended, txn.State())
	assert.Nil(t, fsm.Transactions().GetByID("abcd"))
}

func TestFSM_ApplyCheckpointPanicsIfFollowerTransactionIsInFlight(t *testing.T) {
	fsm, cleanup := newFSM(t)
	defer cleanup()

	fsm.Apply(newRaftLog(0, protocol.NewOpen("test.db")))
	fsm.Apply(newRaftLog(1, protocol.NewBegin("abcd", "test.db")))

	f := func() { fsm.Apply(newRaftLog(2, protocol.NewCheckpoint("test.db"))) }
	msg := "fsm: apply log 2: checkpoint: can't run with transaction abcd started as follower"
	assert.PanicsWithValue(t, msg, f)
}

func TestFSM_ApplyCheckpointWithLeaderConnection(t *testing.T) {
	fsm, cleanup := newFSM(t)
	defer cleanup()

	conn, cleanup := newLeaderConn(t, fsm.Dir(), sqlite3x.PassthroughReplicationMethods())
	defer cleanup()

	fsm.Apply(newRaftLog(0, protocol.NewOpen("test.db")))

	_, err := conn.Exec("CREATE TABLE foo (n INT)", nil)
	require.NoError(t, err)
	require.Equal(t, true, sqlite3x.WalSize(conn) > 0, "WAL has non-positive size")

	fsm.Apply(newRaftLog(1, protocol.NewCheckpoint("test.db")))

	require.Equal(t, int64(0), sqlite3x.WalSize(conn), "WAL has non-zero size")
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
	store := newSnapshotStore(fsm.Dir())
	sink, err := store.Create(1, 1, []byte{})
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
	store := newSnapshotStore(fsm.Dir())
	sink, err := store.Create(1, 1, []byte{})
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

// Create a *raft.Log for the given dqlite protocol Command.
func newRaftLog(index uint64, cmd *protocol.Command) *raft.Log {
	data, err := protocol.MarshalCommand(cmd)
	if err != nil {
		panic(fmt.Sprintf("cannot marshal command: %v", err))
	}
	return &raft.Log{Data: data, Index: index}
}

// Return a fresh FSM for tests.
func newFSM(t *testing.T) (*replication.FSM, func()) {
	logger := log.New(log.Testing(t), log.Trace)
	dir, cleanup := newDir(t)
	connections := connection.NewRegistry()
	transactions := transaction.NewRegistry()

	fsm := replication.NewFSM(logger, dir, connections, transactions)

	// We need to disable replication mode checks, because leader
	// connections created with newLeaderConn() haven't actually initiated
	// a WAL write transaction and acquired the db lock necessary for
	// sqlite3_replication_mode() to succeed.
	transactions.SkipCheckReplicationMode(true)

	return fsm, cleanup
}

// Return a new temporary directory.
func newDir(t *testing.T) (string, func()) {
	dir, err := ioutil.TempDir("", "dqlite-replication-test-")
	assert.NoError(t, err)

	cleanup := func() {
		assert.NoError(t, os.RemoveAll(dir))
	}

	return dir, cleanup
}

// Create a new SQLite connection in leader replication mode, opened against a
// database at a temporary file.
func newLeaderConn(t *testing.T, dir string, methods sqlite3x.ReplicationMethods) (*sqlite3.SQLiteConn, func()) {
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
func newWalFramesParams() *sqlite3x.ReplicationWalFramesParams {
	return &sqlite3x.ReplicationWalFramesParams{
		Pages:     sqlite3x.NewReplicationPages(2, 4096),
		PageSize:  4096,
		Truncate:  uint32(0),
		IsCommit:  0,
		SyncFlags: 0,
	}
}

// Return a raft snapshot store that uses the given directory to store
// snapshots.
func newSnapshotStore(dir string) raft.SnapshotStore {
	store, err := raft.NewFileSnapshotStore(dir, 1, ioutil.Discard)
	if err != nil {
		panic(fmt.Sprintf("failed to create snapshot store: %v", err))
	}
	return store
}
