package replication_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/internal/transaction"
	"github.com/CanonicalLtd/dqlite/replication"
	"github.com/CanonicalLtd/go-sqlite3x"
	"github.com/hashicorp/raft"
)

func TestMethods_Begin(t *testing.T) {
	methods, raft, connections, transactions := newMethodsWaitElection()
	defer cleanupRaftAndConnections(raft, connections)

	conn := openLeaderWithMethods(connections, methods)
	transactions.DryRun()

	if rc := methods.Begin(conn); rc != 0 {
		t.Fatalf("begin failed: %d", rc)
	}

	txn := transactions.GetByConn(conn)
	if txn == nil {
		t.Fatalf("no transaction was created for leader connection")
	}
	txn.Enter()
	if state := txn.State(); state != transaction.Started {
		t.Errorf("leader transaction not in Started state: %s", state)
	}

}

func TestMethods_BeginFailsIfNotLeader(t *testing.T) {
	methods, raft, connections, _ := newMethods()
	defer cleanupRaftAndConnections(raft, connections)

	conn := openLeader(connections)
	if rc := methods.Begin(conn); rc != sqlite3x.ErrNotLeader {
		t.Errorf("expected ErrNotLeader got %d", rc)
	}
}

func TestMethods_BeginRollbackStaleFollowerTransaction(t *testing.T) {
	methods, raft, connections, transactions := newMethodsWaitElection()
	defer cleanupRaftAndConnections(raft, connections)

	transactions.DryRun()

	txn := transactions.AddFollower(connections.Follower("test.db"), "abcd")
	txn.Enter()
	if err := txn.Begin(); err != nil {
		t.Fatal(err)
	}
	txn.Exit()

	conn := openLeader(connections)
	if rc := methods.Begin(conn); rc != 0 {
		t.Fatalf("begin failed: %d", rc)
	}

	txn.Enter()
	if txn.State() != transaction.Ended {
		t.Errorf("follower transaction was not rolled back")
	}
}

func TestMethods_WalFrames(t *testing.T) {
	methods, raft, connections, transactions := newMethodsWaitElection()
	defer cleanupRaftAndConnections(raft, connections)

	transactions.DryRun()

	conn := openLeader(connections)
	txn := transactions.AddLeader(conn)
	txn.Enter()
	if err := txn.Begin(); err != nil {
		t.Fatal(err)
	}
	txn.Exit()

	size := 4096
	pages := sqlite3x.NewReplicationPages(2, size)

	for i := range pages {
		pages[i].Fill(make([]byte, 4096), 1, 1)
	}

	frames := &sqlite3x.ReplicationWalFramesParams{
		Pages:    pages,
		PageSize: size,
		Truncate: 1,
	}
	if rc := methods.WalFrames(conn, frames); rc != 0 {
		t.Fatalf("wal frames failed: %d", rc)
	}

	txn.Enter()
	if state := txn.State(); state != transaction.Writing {
		t.Errorf("leader transaction not in Writing state: %s", state)
	}
}

func TestMethods_Undo(t *testing.T) {
	methods, raft, connections, transactions := newMethodsWaitElection()
	defer cleanupRaftAndConnections(raft, connections)

	transactions.DryRun()

	conn := openLeader(connections)
	txn := transactions.AddLeader(conn)
	txn.Enter()
	if err := txn.Begin(); err != nil {
		t.Fatal(err)
	}
	txn.Exit()

	if rc := methods.Undo(conn); rc != 0 {
		t.Fatalf("undo failed: %d", rc)
	}

	txn.Enter()
	if state := txn.State(); state != transaction.Undoing {
		t.Errorf("leader transaction not in Undoing state: %s", state)
	}
}

func TestMethods_End(t *testing.T) {
	methods, raft, connections, transactions := newMethodsWaitElection()
	defer cleanupRaftAndConnections(raft, connections)

	transactions.DryRun()

	conn := openLeader(connections)
	txn := transactions.AddLeader(conn)
	txn.Enter()
	if err := txn.Begin(); err != nil {
		t.Fatal(err)
	}
	txn.Exit()

	if rc := methods.End(conn); rc != 0 {
		t.Fatalf("end failed: %d", rc)
	}

	txn.Enter()
	if state := txn.State(); state != transaction.Ended {
		t.Errorf("leader transaction not in Ended state: %s", state)
	}
}

func TestMethods_Checkpoint(t *testing.T) {
	methods, raft, connections, _ := newMethodsWaitElection()
	defer cleanupRaftAndConnections(raft, connections)

	conn := openLeader(connections)

	var log, ckpt int
	if rc := methods.Checkpoint(conn, sqlite3x.WalCheckpointTruncate, &log, &ckpt); rc != 0 {
		t.Fatalf("checkpoint failed: %d", rc)
	}
}

// Wrapper around NewMethods, creating an instance along with its
// dependencies.
func newMethods() (*replication.Methods, *raft.Raft, *connection.Registry, *transaction.Registry) {
	logger := log.New(logOutput(), "", 0)

	config := newRaftConfig()
	config.EnableSingleNode = true
	config.Logger = logger

	fsm, connections, transactions := newFSM()

	_, transport := raft.NewInmemTransport("1.2.3.4")
	peers := &raft.StaticPeers{}
	raft := newRaft(config, fsm, transport, peers)

	methods := replication.NewMethods(logger, raft, connections, transactions)
	return methods, raft, connections, transactions
}

// Like newMethods but also waits for the raft node to self-elect itself.
func newMethodsWaitElection() (*replication.Methods, *raft.Raft, *connection.Registry, *transaction.Registry) {
	methods, raft, connections, transactions := newMethods()

	select {
	case leader := <-raft.LeaderCh():
		if !leader {
			panic("raft node did not self-elect itself")
		}
	case <-time.After(time.Second):
		panic("raft node did not self-elect itself within 1 second")
	}

	return methods, raft, connections, transactions
}

// Create a new in-memory raft instance configured to run in single mode.
func newRaft(config *raft.Config, fsm raft.FSM, transport raft.Transport, peers raft.PeerStore) *raft.Raft {

	raft, err := raft.NewRaft(
		config,
		fsm,
		raft.NewInmemStore(),
		raft.NewInmemStore(),
		raft.NewDiscardSnapshotStore(),
		peers,
		transport,
	)
	if err != nil {
		panic(fmt.Sprintf("failed to start raft: %v", err))
	}
	return raft
}

// Create a new raft.Config tweaked for in-memory testing.
func newRaftConfig() *raft.Config {
	config := raft.DefaultConfig()
	config.HeartbeatTimeout = 50 * time.Millisecond
	config.ElectionTimeout = 50 * time.Millisecond
	config.LeaderLeaseTimeout = 50 * time.Millisecond
	config.CommitTimeout = 25 * time.Millisecond

	return config
}

// Convenience to shutdown raft and purge the connections registry.
func cleanupRaftAndConnections(raft *raft.Raft, connections *connection.Registry) {
	if err := raft.Shutdown().Error(); err != nil {
		panic(fmt.Sprintf("failed to shutdown raft: %v", err))
	}
	if err := connections.Purge(); err != nil {
		panic(fmt.Sprintf("failed to purge connections registry: %v", err))
	}
}

func logOutput() io.Writer {
	writer := ioutil.Discard
	if testing.Verbose() {
		writer = os.Stdout
	}
	return writer
}
