package replication_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/bindings"
	"github.com/CanonicalLtd/dqlite/internal/protocol"
	"github.com/CanonicalLtd/dqlite/internal/registry"
	"github.com/CanonicalLtd/dqlite/internal/replication"
	"github.com/CanonicalLtd/dqlite/internal/transaction"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The open command creates a new follower connection.
func TestFSM_Apply_Open(t *testing.T) {
	fsm, cleanup := newFSM(t)
	defer cleanup()

	fsmApply(fsm, 0, protocol.NewOpen("test.db"))
	assert.NotNil(t, fsm.Registry().ConnFollower("test.db"))
}

// Successful frames command with a leader connection.
func TestFSM_Apply_Frames_Leader(t *testing.T) {
	fsm, conn, txn, cleanup := newFSMWithLeader(t)
	defer cleanup()

	command := protocol.NewFrames(txn.ID(), "test.db", newFrameList())

	frames := command.Payload.(*protocol.Command_Frames)
	frames.Frames.IsCommit = 1
	frames.Frames.PageNumbers = []uint32{0}
	frames.Frames.PageData = []byte{0}

	fsmApply(fsm, 1, command)

	// The transaction is still in the registry and is in the Written
	// state.
	assert.Equal(t, txn, fsm.Registry().TxnByConn(conn))
	assert.Equal(t, transaction.Written, txn.State())

	// The transaction ID has been saved in the committed buffer.
	assert.True(t, fsm.Registry().TxnCommittedFind(1))
}

// Successful non-commit frames command with a leader connection.
func TestFSM_Apply_Frames_NonCommit_Leader(t *testing.T) {
	fsm, conn, txn, cleanup := newFSMWithLeader(t)
	defer cleanup()

	command := protocol.NewFrames(txn.ID(), "test.db", newFrameList())

	frames := command.Payload.(*protocol.Command_Frames)
	frames.Frames.IsCommit = 0
	frames.Frames.PageNumbers = []uint32{0}
	frames.Frames.PageData = []byte{0}

	fsmApply(fsm, 1, command)

	// The transaction is still in the registry and has transitioned to
	// Writing.
	assert.Equal(t, txn, fsm.Registry().TxnByConn(conn))
	assert.Equal(t, transaction.Writing, txn.State())
}

// Successful undo command with a leader connection.
func TestFSM_Apply_Frames_Undo(t *testing.T) {
	fsm, conn, txn, cleanup := newFSMWithLeader(t)
	defer cleanup()

	fsmApply(fsm, 2, protocol.NewUndo(txn.ID()))

	// The transaction is still in the registry and has transitioned to
	// Undone.
	assert.Equal(t, txn, fsm.Registry().TxnByConn(conn))
	require.Equal(t, transaction.Undone, txn.State())
}

// Successful frames command with a follower connection.
func TestFSM_Apply_Frames_Follower(t *testing.T) {
	fsm, cleanup := newFSMWithFollower(t)
	defer cleanup()

	fsm.Registry().TxnDryRun()

	command := protocol.NewFrames(123, "test.db", newFrameList())

	frames := command.Payload.(*protocol.Command_Frames)
	frames.Frames.IsCommit = 1
	frames.Frames.PageNumbers = []uint32{0}
	frames.Frames.PageData = []byte{0}

	fsmApply(fsm, 1, command)

	// The transaction has been removed from the registry
	assert.Nil(t, fsm.Registry().TxnByID(123))

	// The transaction ID has been saved in the committed buffer.
	assert.True(t, fsm.Registry().TxnCommittedFind(123))
}

// Successful undo command with a follower connection.
func TestFSM_Apply_Undo_Follower(t *testing.T) {
	fsm, cleanup := newFSMWithFollower(t)
	defer cleanup()

	fsm.Registry().TxnDryRun()

	command := protocol.NewFrames(123, "test.db", newFrameList())

	frames := command.Payload.(*protocol.Command_Frames)
	frames.Frames.IsCommit = 0
	frames.Frames.PageNumbers = []uint32{0}
	frames.Frames.PageData = []byte{0}

	fsmApply(fsm, 1, command)
	fsmApply(fsm, 2, protocol.NewUndo(123))

	// The transaction has been removed from the registry
	assert.Nil(t, fsm.Registry().TxnByID(123))
}

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
		// {
		// 	`open error`,
		// 	func(t *testing.T, fsm *replication.FSM) error {
		// 		registry := registry.New("/foo/bar")
		// 		fsm.RegistryReplace(registry)
		// 		return fsmApply(fsm, 0, protocol.NewOpen("test.db"))
		// 	},
		// 	"open test.db: open error for /foo/bar/test.db: unable to open database file",
		// },
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
func aTestFSM_ApplyPanics(t *testing.T) {
	cases := []struct {
		title string
		f     func(*testing.T, *replication.FSM) // Function leading to the panic.
		panic string                             // Expected panic message.
	}{
		{
			`existing transaction has non-leader connection`,
			func(t *testing.T, fsm *replication.FSM) {
				fsmApply(fsm, 0, protocol.NewOpen("test.db"))
				fsm.Registry().TxnFollowerAdd(&bindings.Conn{}, 123)
				fsmApply(fsm, 1, protocol.NewBegin(123, "test.db"))
			},
			"unexpected follower transaction 123 pending as follower",
		},
		{
			`new follower transaction started before previous is finished`,
			func(t *testing.T, fsm *replication.FSM) {
				fsmApply(fsm, 0, protocol.NewOpen("test.db"))
				fsmApply(fsm, 1, protocol.NewBegin(123, "test.db"))
				fsmApply(fsm, 2, protocol.NewBegin(456, "test.db"))
			},
			"unexpected transaction 123 started as follower",
		},
		{
			`dangling leader connection`,
			func(t *testing.T, fsm *replication.FSM) {
				fsmApply(fsm, 0, protocol.NewOpen("test.db"))

				conn := &bindings.Conn{}
				fsm.Registry().ConnLeaderAdd("test.db", conn)
				fsm.Registry().TxnLeaderAdd(conn, 1)

				fsmApply(fsm, 1, protocol.NewBegin(2, "test.db"))
			},
			"unexpected transaction 1 pending as leader",
		},
		// {
		// 	`wal frames transaction not found`,
		// 	func(t *testing.T, fsm *replication.FSM) {
		// 		params := newFramesParams()
		// 		fsmApply(fsm, 0, protocol.NewFrames(123, "test.db", params))
		// 	},
		// 	"no transaction with ID 123",
		// },
		{
			`undo transaction not found`,
			func(t *testing.T, fsm *replication.FSM) {
				fsmApply(fsm, 0, protocol.NewUndo(123))
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

/*

func TestFSM_ApplyCheckpoint(t *testing.T) {
	fsm, cleanup := newFSM(t)
	defer cleanup()

	methods := sqlite3.NoopReplicationMethods()
	conn, cleanup := newLeaderConn(t, fsm.Registry().Dir(), methods)
	defer cleanup()

	// Commit something to the WAL, otherwise the sqlite3_checkpoint_v2 API
	// would crash.
	_, err := conn.Exec("CREATE TABLE test (n INT)", nil)
	require.NoError(t, err)

	fsmApply(fsm, 0, protocol.NewOpen("test.db"))
	fsmApply(fsm, 1, protocol.NewCheckpoint("test.db"))
}

func TestFSM_ApplyCheckpointPanicsIfFollowerTransactionIsInFlight(t *testing.T) {
	fsm, cleanup := newFSM(t)
	defer cleanup()

	fsmApply(fsm, 0, protocol.NewOpen("test.db"))

	fsm.Registry().TxnDryRun()

	params := newFramesParams()
	params.IsCommit = 0
	fsmApply(fsm, 1, protocol.NewFrames(1, "test.db", params))

	f := func() { fsmApply(fsm, 2, protocol.NewCheckpoint("test.db")) }
	assert.PanicsWithValue(t, "can't run checkpoint concurrently with transaction 1 writing as follower", f)
}

// In case the snapshot the source backup connection can't be opened, an error
// is returned.
func TestFSM_SnapshotSourceConnectionError(t *testing.T) {
	fsm, cleanup := newFSM(t)
	defer cleanup()

	// Register the database.
	fsmApply(fsm, 0, protocol.NewOpen("test.db"))

	// Remove the FSM dir to trigger a snapshot error.
	require.NoError(t, os.RemoveAll(fsm.Registry().Dir()))

	// Create a snapshot
	snapshot, err := fsm.Snapshot()
	assert.Nil(t, snapshot)

	expected := fmt.Sprintf(
		"test.db: source connection: open error for %s: unable to open database file",
		filepath.Join(fsm.Registry().Dir(), "test.db"))

	assert.EqualError(t, err, expected)
}

// In case the snapshot is made when an active transaction is in progress an
// error is returned.
func TestFSM_SnapshotActiveTransactionError(t *testing.T) {
	fsm, cleanup := newFSM(t)
	defer cleanup()

	// Register the database and start a writing transaction.
	fsmApply(fsm, 0, protocol.NewOpen("test.db"))

	fsm.Registry().TxnDryRun()

	params := newFramesParams()
	params.IsCommit = 0
	fsmApply(fsm, 2, protocol.NewFrames(1, "test.db", params))

	// Create a snapshot
	snapshot, err := fsm.Snapshot()
	assert.Nil(t, snapshot)

	expected := "test.db: transaction 1 writing as follower is in progress"
	assert.EqualError(t, err, expected)
}

// In case the snapshot is taken while there's an idle transaction, no error is
// returned, but the transaction is included in the snapshot.
func TestFSM_SnapshotIdleTransaction(t *testing.T) {
	t.Skip("This feature is disabled, see fsm.go")
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
	//txn := fsm2.Registry().TxnByID(1)
	//assert.Equal(t, transaction.Started, txn.State())

}

func TestFSM_Snapshot(t *testing.T) {
	fsm, cleanup := newFSM(t)
	defer cleanup()

	// Create a database with some content.
	path := filepath.Join(fsm.Registry().Dir(), "test.db")
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE foo (n INT); INSERT INTO foo VALUES(1)")
	require.NoError(t, err)
	db.Close()

	// Register the database.
	fsmApply(fsm, 0, protocol.NewOpen("test.db"))

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
	db, err = sql.Open("sqlite3", path)
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
	path := filepath.Join(fsm.Registry().Dir(), "test.db")
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE foo (n INT); INSERT INTO foo VALUES(1)")
	require.NoError(t, err)
	db.Close()

	// Register the database.
	fsmApply(fsm, 0, protocol.NewOpen("test.db"))

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
	db, err = sql.Open("sqlite3", path)
	require.NoError(t, err)
	rows, err := db.Query("SELECT * FROM foo", nil)
	require.NoError(t, err)
	defer rows.Close()
	require.Equal(t, true, rows.Next())
	var n int
	require.NoError(t, rows.Scan(&n))
	assert.Equal(t, 1, n)
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
*/

// Return a fresh FSM for tests.
func newFSM(t *testing.T) (*replication.FSM, func()) {
	t.Helper()

	err := bindings.RegisterWalReplication("test", newNoopWalReplication())
	require.NoError(t, err)

	vfs, err := bindings.RegisterVfs("test")
	require.NoError(t, err)

	dir, dirCleanup := newDir(t)

	registry := registry.New(vfs, dir)
	registry.Testing(t, 0)

	fsm := replication.NewFSM(registry)

	cleanup := func() {
		dirCleanup()
		bindings.UnregisterVfs(vfs)
		bindings.UnregisterWalReplication("test")
	}

	// We need to disable replication mode checks, because leader
	// connections created with newLeaderConn() haven't actually initiated
	// a WAL write transaction and acquired the db lock necessary for
	// sqlite3_replication_mode() to succeed.
	//fsm.Registry().SkipCheckReplicationMode(true)

	return fsm, cleanup
}

// Return a fresh FSM for tests, along with a registered leader connection and
// transaction, as it would be created by a Methods.Begin hook.
func newFSMWithLeader(t *testing.T) (*replication.FSM, *bindings.Conn, *transaction.Txn, func()) {
	t.Helper()

	fsm, fsmCleanup := newFSM(t)

	name := fsm.Registry().Vfs().Name()
	conn, connCleanup := newLeaderConn(t, name, name)

	fsm.Registry().ConnLeaderAdd("test.db", conn)
	txn := fsm.Registry().TxnLeaderAdd(conn, 1)

	cleanup := func() {
		connCleanup()
		fsmCleanup()
	}

	return fsm, conn, txn, cleanup
}

// Return a fresh FSM for tests, with a registered open follower connection, as
// it would be created by an Open command.
func newFSMWithFollower(t *testing.T) (*replication.FSM, func()) {
	t.Helper()

	fsm, cleanup := newFSM(t)

	fsmApply(fsm, 0, protocol.NewOpen("test.db"))

	return fsm, cleanup
}

// Create a new SQLite connection in leader replication mode, opened against a
// database at a temporary file.
func newLeaderConn(t *testing.T, vfs string, replication string) (*bindings.Conn, func()) {
	t.Helper()

	conn, err := bindings.OpenLeader("test.db", vfs, replication)
	if err != nil {
		t.Fatalf("failed to open leader connection: %v", err)
	}

	cleanup := func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("failed to close leader connection: %v", err)
		}
	}

	return conn, cleanup
}

// Convenience to create test parameters for a wal frames command.
func newFrameList() bindings.WalReplicationFrameList {
	return bindings.WalReplicationFrameList{}
}

// Return a new temporary directory.
func newDir(t *testing.T) (string, func()) {
	t.Helper()

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

// NoopWalReplication returns a new instance of a WalReplication implementation
// whose hooks do nothing.
func newNoopWalReplication() bindings.WalReplication {
	return &noopWalReplication{}
}

type noopWalReplication struct{}

func (m *noopWalReplication) Begin(*bindings.Conn) int {
	return 0
}

func (m *noopWalReplication) Abort(*bindings.Conn) int {
	return 0
}

func (m *noopWalReplication) Frames(*bindings.Conn, bindings.WalReplicationFrameList) int {
	return 0
}

func (m *noopWalReplication) Undo(*bindings.Conn) int {
	return 0
}

func (m *noopWalReplication) End(*bindings.Conn) int {
	return 0
}
