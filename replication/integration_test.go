package replication_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/dqlite/dqlite/connection"
	"github.com/dqlite/dqlite/replication"
	"github.com/dqlite/dqlite/transaction"
	"github.com/dqlite/go-sqlite3x"
	"github.com/dqlite/raft-test"
	"github.com/mattn/go-sqlite3"
)

func TestIntegration_Replication(t *testing.T) {
	cluster, cleanup := newCluster()
	defer cleanup()

	node1 := cluster.LeadershipAcquired()
	conn1 := openConn(node1)
	if _, err := conn1.Exec("CREATE TABLE test (n INT)", nil); err != nil {
		t.Fatal(err)
	}

	node2 := cluster.Peers(node1)[0]

	fsm := node2.FSM.(*replication.FSM)
	fsm.Wait(node1.Raft().AppliedIndex())

	conn2 := openConn(node2)
	if _, err := conn2.Query("SELECT * FROM test", nil); err != nil {
		t.Fatal(err)
	}
}

func TestIntegration_RaftApplyErrorRemovePendingTxn(t *testing.T) {
	cluster, cleanup := newCluster()
	defer cleanup()

	node := cluster.LeadershipAcquired()

	conn := openConn(node)
	cluster.Disconnect(node)

	_, err := conn.Exec("CREATE TABLE test (n INT)", nil)

	if err == nil {
		t.Fatal("expected error when executing a statement on a disconnected leader")
	}
	want := sqlite3x.ErrorString(sqlite3x.ErrNotLeader)
	got := err.Error()
	if got != want {
		t.Errorf("expected\n%q\ngot\n%q", want, got)
	}
	if txn := findTxn(cluster, conn); txn != nil {
		t.Errorf("expected pending transaction to be removed, found %s", txn)
	}
}

func TestIntegration_RaftApplyErrorWithInflightTxnAndRecoverOnNewLeader(t *testing.T) {
	cluster, cleanup := newCluster()
	defer cleanup()

	node := cluster.LeadershipAcquired()

	conn := openConn(node)

	if _, err := conn.Exec("BEGIN; CREATE TABLE test (n INT)", nil); err != nil {
		t.Fatal(err)
	}

	cluster.Disconnect(node)

	_, err := conn.Exec("INSERT INTO test VALUES(1); COMMIT", nil)
	if err == nil {
		t.Fatal("expected error when executing a statement on a disconnected leader")
	}

	want := sqlite3x.ErrorString(sqlite3x.ErrNotLeader)
	got := err.Error()
	if got != want {
		t.Errorf("expected\n%q\ngot\n%q", want, got)
	}

	node = cluster.LeadershipAcquired()
	if err := node.Raft().Barrier(time.Second).Error(); err != nil {
		t.Fatal(err)
	}

	conn = openConn(node)

	// XXX here we need to re-run the CREATE TABLE statement
	// because there are two failure modes for the first CREATE
	// TABLE: one is that the raft leadership is lost and the log
	// entry is not committed to the cluster log (in that case the
	// test table is not there on the new leader), the second is
	// that leadership is lost but still the log entry gets
	// committed to the other nodes, and in that case the table is
	// there on the new leader.
	if _, err := conn.Exec("BEGIN; CREATE TABLE IF NOT EXISTS test (n INT); INSERT INTO test VALUES(2); COMMIT", nil); err != nil {
		t.Fatal(err)
	}

}

// Create a new test raft cluster and configure each node to perform
// SQLite replication.
func newCluster() (*rafttest.Cluster, func()) {
	cluster := rafttest.NewCluster(3)
	cluster.Timeout = 25 * time.Second

	for i := 0; i < 3; i++ {
		node := cluster.Node(i)
		fsm, connections, transactions := newFSMWithLogger(node.Config.Logger)
		node.FSM = fsm
		node.Data = &nodeData{
			connections:  connections,
			transactions: transactions,
		}
	}
	cluster.Start()

	for i := 0; i < 3; i++ {
		node := cluster.Node(i)
		logger := node.Config.Logger

		data := node.Data.(*nodeData)
		methods := replication.NewMethods(logger, node.Raft(), data.connections, data.transactions)
		data.methods = methods
	}

	cleanup := func() {
		cluster.Shutdown()
		for i := 0; i < 3; i++ {
			node := cluster.Node(i)
			data := node.Data.(*nodeData)
			if err := data.connections.Purge(); err != nil {
				panic(fmt.Sprintf("failed to purge connections registry: %v", err))
			}

		}
	}

	return cluster, cleanup
}

// Open a new leader connection on the given node.
func openConn(node *rafttest.Node) *sqlite3.SQLiteConn {
	data := node.Data.(*nodeData)
	conn, err := data.connections.OpenLeader("test", data.methods)
	if err != nil {
		panic(fmt.Sprintf(
			"failed to open leader on node %s: %v",
			node.Transport.LocalAddr(), err))
	}
	return conn
}

// Find the transaction associated with the given connection.
func findTxn(cluster *rafttest.Cluster, conn *sqlite3.SQLiteConn) *transaction.Txn {
	for i := 0; i < 3; i++ {
		node := cluster.Node(i)
		data := node.Data.(*nodeData)
		if txn := data.transactions.GetByConn(conn); txn != nil {
			return txn
		}
	}
	return nil
}

// Node-level data specific to dqlite. They'll be saved in node.Data.
type nodeData struct {
	connections  *connection.Registry
	transactions *transaction.Registry
	methods      *replication.Methods
}
