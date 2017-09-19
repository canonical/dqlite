package replication_test

import (
	"database/sql/driver"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/commands"
	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/internal/replication"
	"github.com/CanonicalLtd/dqlite/internal/transaction"
	"github.com/CanonicalLtd/go-sqlite3x"
	"github.com/hashicorp/raft"
	"github.com/mattn/go-sqlite3"
)

func TestFSM_ApplyPanicsIfLogDataIsGarbage(t *testing.T) {
	fsm, connections, _ := newFSM()
	defer connections.Purge()

	const want = "fsm apply error: failed to unmarshal command"
	defer func() {
		got := recover()
		if !strings.Contains(got.(string), want) {
			t.Errorf("panic message '%s' does not contain '%s'", got, want)
		}
	}()
	fsm.Apply(&raft.Log{Data: []byte("garbage")})
}

func TestFSM_ApplyBeginPanicsIfExistingTxnHasNonLeaderConnection(t *testing.T) {
	fsm, connections, transactions := newFSM()
	defer connections.Purge()

	transactions.AddFollower(connections.Follower("test.db"), "abcd")

	data := marshalCommand(commands.NewBegin("abcd", "test.db"))

	const want = "fsm apply error for command Begin: existing transaction {id=abcd state=pending leader=false} has non-leader connection"
	defer func() {
		got := recover()
		if got != want {
			t.Errorf("expected\n%q\ngot\n%q", want, got)
		}
	}()
	fsm.Apply(&raft.Log{Data: data})
}

func TestFSM_ApplyBeginPanicsIfDanglingLeaderIsFound(t *testing.T) {
	fsm, connections, transactions := newFSM()
	defer connections.Purge()

	conn := openLeader(connections)
	txn := transactions.AddLeader(conn)

	data := marshalCommand(commands.NewBegin("abcd", "test.db"))

	want := fmt.Sprintf("fsm apply error for command Begin: found dangling leader connection %s", txn)
	defer func() {
		got := recover()
		if got != want {
			t.Errorf("expected\n%q\ngot\n%q", want, got)
		}
	}()
	fsm.Apply(&raft.Log{Data: data})
}

func TestFSM_ApplyBeginAddFollower(t *testing.T) {
	fsm, connections, transactions := newFSM()
	defer connections.Purge()

	data := marshalCommand(commands.NewBegin("abcd", "test.db"))
	fsm.Apply(&raft.Log{Data: data})

	txn := transactions.GetByID("abcd")

	if txn == nil {
		t.Error("no transaction created")
	}
	if txn.Conn() != connections.Follower("test.db") {
		t.Error("the created transaction does not use the follower connection")
	}
	txn.Enter()
	if txn.State() != transaction.Started {
		t.Errorf("the created transaction is not state Started: %s", txn.State())
	}
}

func TestFSM_ApplyBeginTwice(t *testing.T) {
	fsm, connections, transactions := newFSM()
	defer connections.Purge()

	transactions.DryRun()

	conn := openLeader(connections)
	txn := transactions.AddLeader(conn)

	data := marshalCommand(commands.NewBegin(txn.ID(), "test.db"))
	fsm.Apply(&raft.Log{Data: data})

	const want = "invalid started -> started transition"
	defer func() {
		got := recover()
		if got != want {
			t.Errorf("expected\n%q\ngot\n%q", want, got)
		}
	}()
	fsm.Apply(&raft.Log{Data: data})
}

func TestFSM_ApplyWalFrames(t *testing.T) {
	fsm, connections, transactions := newFSM()
	defer connections.Purge()

	transactions.DryRun()

	txn := transactions.AddFollower(connections.Follower("test.db"), "abcd")
	txn.Enter()
	if err := txn.Begin(); err != nil {
		t.Fatal(err)
	}
	txn.Exit()

	frames := &sqlite3x.ReplicationWalFramesParams{
		Pages:     sqlite3x.NewReplicationPages(2, 4096),
		PageSize:  4096,
		Truncate:  uint32(0),
		IsCommit:  0,
		SyncFlags: 0,
	}
	data := marshalCommand(commands.NewWalFrames("abcd", frames))

	fsm.Apply(&raft.Log{Data: data})

	txn.Enter()
	if txn.State() != transaction.Writing {
		t.Errorf("transaction didn't transition to Writing: %s", txn.State())
	}
}

func TestFSM_ApplyUndo(t *testing.T) {
	fsm, connections, transactions := newFSM()
	defer connections.Purge()

	txn := transactions.AddFollower(connections.Follower("test.db"), "abcd")
	txn.Enter()
	if err := txn.Begin(); err != nil {
		t.Fatal(err)
	}
	txn.Exit()

	data := marshalCommand(commands.NewUndo("abcd"))
	fsm.Apply(&raft.Log{Data: data})

	txn.Enter()
	if txn.State() != transaction.Undoing {
		t.Errorf("transaction didn't transition to Undoing: %s", txn.State())
	}
}

func TestFSM_ApplyEnd(t *testing.T) {
	fsm, connections, transactions := newFSM()
	defer connections.Purge()

	txn := transactions.AddFollower(connections.Follower("test.db"), "abcd")
	txn.Enter()
	if err := txn.Begin(); err != nil {
		t.Fatal(err)
	}
	txn.Exit()

	data := marshalCommand(commands.NewEnd("abcd"))
	fsm.Apply(&raft.Log{Data: data})

	txn.Enter()
	if txn.State() != transaction.Ended {
		t.Errorf("transaction didn't transition to Ended: %s", txn.State())
	}
}

func TestFSM_ApplyCheckpointPanicsIfFollowerTransactionIsInFlight(t *testing.T) {
	fsm, connections, transactions := newFSM()
	defer connections.Purge()

	txn := transactions.AddFollower(connections.Follower("test.db"), "abcd")
	txn.Enter()
	if err := txn.Begin(); err != nil {
		t.Fatal(err)
	}
	txn.Exit()

	data := marshalCommand(commands.NewCheckpoint("test.db"))

	const want = "fsm apply error for command Checkpoint: checkpoint for database 'test.db' can't run with transaction {id=abcd state=started leader=false}"
	defer func() {
		got := recover()
		if got != want {
			t.Errorf("expected\n%q\ngot\n%q", want, got)
		}
	}()
	fsm.Apply(&raft.Log{Data: data})
}

func TestFSM_ApplyCheckpointWithLeaderConnection(t *testing.T) {
	fsm, connections, _ := newFSM()
	defer connections.Purge()

	conn := openLeader(connections)
	if _, err := conn.Exec("CREATE TABLE test (n INT)", nil); err != nil {
		t.Fatal(err)
	}
	if size := sqlite3x.WalSize(conn); !(size > 0) {
		t.Fatalf("WAL has non-positive size: %d", size)
	}

	data := marshalCommand(commands.NewCheckpoint("test.db"))
	fsm.Apply(&raft.Log{Data: data})

	if size := sqlite3x.WalSize(conn); size != 0 {
		t.Fatalf("WAL has non-zero size: %d", size)
	}
}

func TestFSM_Snapshot(t *testing.T) {
	// Create an FSM and write some data to the underlying test database.
	fsm, connections, _ := newFSM()
	conn := openLeader(connections)
	if _, err := conn.Exec("BEGIN; CREATE TABLE foo (n INT); INSERT INTO foo VALUES(1); COMMIT", nil); err != nil {
		t.Fatal(err)
	}

	// Save the modification time of the current database file, to show
	// that it changed after the restore.
	timestamp := sqlite3x.DatabaseModTime(conn)

	// Create a snapshot
	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := connections.CloseLeader(conn); err != nil {
		t.Fatal(err)
	}

	// Persist the snapshot in a store.
	store := newSnapshotStore(connections.Dir())
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

	// The restored database file is a new one.
	conn = openLeader(connections)
	if sqlite3x.DatabaseModTime(conn) == timestamp {
		t.Fatal("database file was not modified by restoring it")
	}

	// The restored file has the expected data
	rows, err := conn.Query("SELECT * FROM foo", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	values := make([]driver.Value, 1)
	if err := rows.Next(values); err != nil {
		t.Fatal(err)
	}
	got := values[0].(int64)
	if got != 1 {
		t.Fatalf("got row value %d, expected 1", got)
	}
}

func TestFSM_SnapshotAfterCheckpoint(t *testing.T) {
	// Create an FSM and write some data to the underlying test database.
	fsm, connections, _ := newFSM()
	conn := openLeader(connections)
	if _, err := conn.Exec("BEGIN; CREATE TABLE foo (n INT); INSERT INTO foo VALUES(1); COMMIT", nil); err != nil {
		t.Fatal(err)
	}

	// Create a snapshot
	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	// Persist the snapshot in a store.
	store := newSnapshotStore(connections.Dir())
	sink, err := store.Create(1, 1, []byte{})
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatal(err)
	}
	if err := connections.CloseLeader(conn); err != nil {
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
	conn = openLeader(connections)
	rows, err := conn.Query("SELECT * FROM foo", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	values := make([]driver.Value, 1)
	if err := rows.Next(values); err != nil {
		t.Fatal(err)
	}
	got := values[0].(int64)
	if got != 1 {
		t.Fatalf("got row value %d, expected 1", got)
	}

}

// Wrapper around NewFSM, returning its dependencies as well.
func newFSM() (*replication.FSM, *connection.Registry, *transaction.Registry) {
	return newFSMWithLogger(log.New(logOutput(), "", 0))
}

// Wrapper around NewFSM, returning its dependencies as well.
func newFSMWithLogger(logger *log.Logger) (*replication.FSM, *connection.Registry, *transaction.Registry) {
	connections := connection.NewTempRegistryWithDatabase()
	transactions := transaction.NewRegistry()
	transactions.SkipCheckReplicationMode(true)
	fsm := replication.NewFSM(logger, connections, transactions)

	return fsm, connections, transactions
}

// Wrapper around commands.Marshal(), panic'ing if any error occurs.
func marshalCommand(cmd *commands.Command) []byte {
	data, err := commands.Marshal(cmd)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal command: %v", err))
	}
	return data
}

// Wrapper around connections.Registry.OpenLeader(), panic'ing if any
// error occurs.
func openLeader(connections *connection.Registry) *sqlite3.SQLiteConn {
	return openLeaderWithMethods(connections, sqlite3x.PassthroughReplicationMethods())
}

// Wrapper around connections.Registry.OpenLeader(), panic'ing if any
// error occurs.
func openLeaderWithMethods(connections *connection.Registry, methods sqlite3x.ReplicationMethods) *sqlite3.SQLiteConn {
	conn, err := connections.OpenLeader(connection.NewTestDSN(), methods)
	if err != nil {
		panic(fmt.Sprintf("failed to open leader: %v", err))
	}
	return conn
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
