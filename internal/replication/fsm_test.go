package replication_test

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/commands"
	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/internal/logging"
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
	cases := []struct {
		title string
		f     func(fsm *replication.FSM) // Function leading to panic.
		panic string                     // Expected panic message.
	}{
		{
			`log data is garbage`,
			func(fsm *replication.FSM) {
				fsm.Apply(&raft.Log{Data: []byte("garbage")})
			},
			"fsm: log 0: corrupted command: protobuf failure: proto: illegal wireType 7",
		}, {
			`existing transaction has non-leader connection`,
			func(fsm *replication.FSM) {
				fsm.Apply(newRaftLog(commands.NewOpen("test.db")))
				fsm.Apply(newRaftLog(commands.NewBegin("abcd", "test.db")))
				fsm.Apply(newRaftLog(commands.NewBegin("abcd", "test.db")))
			},
			"fsm: log 2: begin: txn abcd: is started as follower instead of leader",
		}, {
			`new follower transaction started before previous is ended`,
			func(fsm *replication.FSM) {
				fsm.Apply(newRaftLog(commands.NewOpen("test.db")))
				fsm.Apply(newRaftLog(commands.NewBegin("abcd", "test.db")))
				fsm.Apply(newRaftLog(commands.NewBegin("efgh", "test.db")))
			},
			"a transaction for this connection is already registered with ID abcd",
		}, {
			`dangling leader connection`,
			func(fsm *replication.FSM) {
				methods := sqlite3x.PassthroughReplicationMethods()
				conn := newLeaderConn(fsm.Dir(), "test.db", methods)
				defer connection.CloseLeader(conn)

				fsm.Connections().AddLeader("test.db", conn)
				fsm.Transactions().AddLeader(conn)

				fsm.Apply(newRaftLog(commands.NewOpen("test.db")))
				fsm.Apply(newRaftLog(commands.NewBegin("abcd", "test.db")))
			},
			"fsm: log 1: begin: txn abcd: found dangling transaction pending as leader",
		}, {
			`wal frames transaction not found`,
			func(fsm *replication.FSM) {
				params := newWalFramesParams()
				fsm.Apply(newRaftLog(commands.NewWalFrames("abcd", params)))
			},
			"fsm: log 0: walframes: txn abcd: not found",
		}, {
			`undo transaction not found`,
			func(fsm *replication.FSM) {
				fsm.Apply(newRaftLog(commands.NewUndo("abcd")))
			},
			"fsm: log 0: undo: txn abcd: not found",
		}, {
			`end transaction not found`,
			func(fsm *replication.FSM) {
				fsm.Apply(newRaftLog(commands.NewEnd("abcd")))
			},
			"fsm: log 0: end: txn abcd: not found",
		},
	}
	for _, c := range cases {
		subtest.Run(t, c.title, func(t *testing.T) {
			fsm, cleanup := newFSM(t)
			defer cleanup()
			assert.PanicsWithValue(t, c.panic, func() { c.f(fsm) })
		})
	}
}

// Test the happy path of the various transaction-related commands.
func TestFSM_ApplyTransactionCommands(t *testing.T) {
	cases := []struct {
		title string
		f     func(*replication.FSM) string // Apply txn commands and return Txn ID.
		state fsm.State                     // Expected txn state.
	}{
		{
			`begin leader`,
			func(fsm *replication.FSM) string {
				methods := sqlite3x.PassthroughReplicationMethods()
				conn := newLeaderConn(fsm.Dir(), "test.db", methods)
				defer connection.CloseLeader(conn)

				txn := fsm.Transactions().AddLeader(conn)
				txn.DryRun(true)

				fsm.Apply(newRaftLog(commands.NewBegin(txn.ID(), "test.db")))

				return txn.ID()
			},
			transaction.Started,
		},
		{
			`begin follower`,
			func(fsm *replication.FSM) string {
				fsm.Apply(newRaftLog(commands.NewOpen("test.db")))
				fsm.Apply(newRaftLog(commands.NewBegin("abcd", "test.db")))
				return "abcd"
			},
			transaction.Started,
		},
		{
			`wal frames`,
			func(fsm *replication.FSM) string {
				fsm.Apply(newRaftLog(commands.NewOpen("test.db")))
				fsm.Apply(newRaftLog(commands.NewBegin("abcd", "test.db")))

				txn := fsm.Transactions().GetByID("abcd")
				txn.DryRun(true)

				params := newWalFramesParams()
				fsm.Apply(newRaftLog(commands.NewWalFrames("abcd", params)))
				return "abcd"
			},
			transaction.Writing,
		},
		{
			`undo`,
			func(fsm *replication.FSM) string {
				fsm.Apply(newRaftLog(commands.NewOpen("test.db")))
				fsm.Apply(newRaftLog(commands.NewBegin("abcd", "test.db")))

				txn := fsm.Transactions().GetByID("abcd")
				txn.DryRun(true)

				fsm.Apply(newRaftLog(commands.NewUndo("abcd")))
				return "abcd"
			},
			transaction.Undoing,
		},
	}
	for _, c := range cases {
		subtest.Run(t, c.title, func(t *testing.T) {
			fsm, cleanup := newFSM(t)
			defer cleanup()

			txid := c.f(fsm)

			txn := fsm.Transactions().GetByID(txid)
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

	fsm.Apply(newRaftLog(commands.NewOpen("test.db")))

	assert.Equal(t, true, fsm.Connections().HasFollower("test.db"))
}

// The end command commits the transaction and deletes it from the registry.
func TestFSM_ApplyEnd(t *testing.T) {
	fsm, cleanup := newFSM(t)
	defer cleanup()

	fsm.Apply(newRaftLog(commands.NewOpen("test.db")))
	fsm.Apply(newRaftLog(commands.NewBegin("abcd", "test.db")))

	txn := fsm.Transactions().GetByID("abcd")

	fsm.Apply(newRaftLog(commands.NewEnd("abcd")))
	txn.Enter()
	assert.Equal(t, transaction.Ended, txn.State())
	assert.Nil(t, fsm.Transactions().GetByID("abcd"))
}

func TestFSM_ApplyCheckpointPanicsIfFollowerTransactionIsInFlight(t *testing.T) {
	fsm, cleanup := newFSM(t)
	defer cleanup()

	fsm.Apply(newRaftLog(commands.NewOpen("test.db")))
	fsm.Apply(newRaftLog(commands.NewBegin("abcd", "test.db")))

	f := func() { fsm.Apply(newRaftLog(commands.NewCheckpoint("test.db"))) }
	msg := "fsm: log 2: checkpoint: can't run with transaction abcd started as follower"
	assert.PanicsWithValue(t, msg, f)
}

func TestFSM_ApplyCheckpointWithLeaderConnection(t *testing.T) {
	fsm, cleanup := newFSM(t)
	defer cleanup()

	conn, err := connection.OpenLeader(fsm.Path("test.db"), sqlite3x.PassthroughReplicationMethods(), 5)
	assert.NoError(t, err)
	defer connection.CloseLeader(conn)

	fsm.Apply(newRaftLog(commands.NewOpen("test.db")))

	_, err = conn.Exec("CREATE TABLE foo (n INT)", nil)
	require.NoError(t, err)
	require.Equal(t, true, sqlite3x.WalSize(conn) > 0, "WAL has non-positive size")

	fsm.Apply(newRaftLog(commands.NewCheckpoint("test.db")))

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
	fsm.Apply(newRaftLog(commands.NewOpen("test.db")))

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
	fsm.Apply(newRaftLog(commands.NewOpen("test.db")))

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

// Return a fresh FSM for tests.
func newFSM(t *testing.T) (*replication.FSM, func()) {
	dir, err := ioutil.TempDir("", "dqlite-replication-test-fsm-")
	assert.NoError(t, err)
	logger := logging.NewTesting(t)
	connections := connection.NewRegistry()
	transactions := transaction.NewRegistry()
	fsm := replication.NewFSM(dir, logger, connections, transactions)
	cleanup := func() {
		index = 0
		assert.NoError(t, os.RemoveAll(dir))
	}

	// We need to temporarily disable replication mode checks, because
	// leader connections created with newLeaderConn() haven't actually
	// initiated a WAL write transaction and acquired the db lock necessary
	// for sqlite3_replication_mode() to succeed.
	transactions.SkipCheckReplicationMode(true)

	return fsm, cleanup
}

// Create a *raft.Log for the given dqlite protocol Command.
func newRaftLog(cmd *commands.Command) *raft.Log {
	data, err := commands.Marshal(cmd)
	if err != nil {
		panic(fmt.Sprintf("cannot marshal command: %v", err))
	}
	defer func() { index++ }()
	return &raft.Log{Data: data, Index: index}
}

var index uint64

// Create a new SQLite in leader replication mode.
func newLeaderConn(dir, filename string, methods sqlite3x.ReplicationMethods) *sqlite3.SQLiteConn {
	uri := filepath.Join(dir, filename)

	conn, err := connection.OpenLeader(uri, methods, 1000)
	if err != nil {
		panic(fmt.Sprintf("failed to open leader connection: %v", err))
	}

	return conn
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
	logger := log.New(ioutil.Discard, "", 0)
	store, err := raft.NewFileSnapshotStoreWithLogger(dir, 1, logger)
	if err != nil {
		panic(fmt.Sprintf("failed to create snapshot store: %v", err))
	}
	return store
}

// Wrapper around NewFSM, returning its dependencies as well.
func newFSMLegacy() (*replication.FSM, *connection.Registry, *transaction.Registry) {
	return newFSMWithLogger(log.New(logOutput(), "", 0))
}

// Wrapper around NewFSM, returning its dependencies as well.
func newFSMWithLogger(logger *log.Logger) (*replication.FSM, *connection.Registry, *transaction.Registry) {
	connections := connection.NewTempRegistryWithDatabase()
	transactions := transaction.NewRegistry()
	transactions.SkipCheckReplicationMode(true)
	fsm := replication.NewFSMLegacy(logger, connections, transactions)

	return fsm, connections, transactions
}

// Wrapper around connections.Registry.OpenLeader(), panic'ing if any
// error occurs.
func openLeader(connections *connection.Registry) *sqlite3.SQLiteConn {
	return openLeaderWithMethods(connections, sqlite3x.PassthroughReplicationMethods())
}

// Wrapper around connections.Registry.OpenLeader(), panic'ing if any
// error occurs.
func openLeaderWithMethods(connections *connection.Registry, methods sqlite3x.ReplicationMethods) *sqlite3.SQLiteConn {
	dsn := connection.NewTestDSN()
	conn, err := connection.OpenLeader(filepath.Join(connections.Dir(), dsn.Filename), methods, 1000)
	if err != nil {
		panic(fmt.Sprintf("failed to open leader: %v", err))
	}
	connections.AddLeader(dsn.Filename, conn)

	return conn
}
