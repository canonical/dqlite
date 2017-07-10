package replication_test

import (
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/dqlite/dqlite/connection"
	"github.com/dqlite/dqlite/replication"
	"github.com/dqlite/dqlite/transaction"
	"github.com/dqlite/go-sqlite3x"
	"github.com/hashicorp/raft"
	"github.com/mattn/go-sqlite3"
)

func TestIntegration_Replication(t *testing.T) {
	cluster := newCluster()
	defer cluster.Cleanup()

	cluster.WaitElection()

	conn1 := cluster.Open(0)
	if _, err := conn1.Exec("CREATE TABLE test (n INT)", nil); err != nil {
		t.Fatal(err)
	}

	cluster.Barrier(1)

	conn2 := cluster.Open(1)
	if _, err := conn2.Query("SELECT * FROM test", nil); err != nil {
		t.Fatal(err)
	}
}

func TestIntegration_RaftApplyErrorRemovePendingTxn(t *testing.T) {
	cluster := newCluster()
	defer cluster.Cleanup()

	cluster.WaitElection()

	conn := cluster.Open(0)
	cluster.Disconnect(0)

	_, err := conn.Exec("CREATE TABLE test (n INT)", nil)

	if err == nil {
		t.Fatal("expected error when executing a statement on a disconnected leader")
	}
	want := sqlite3x.ErrorString(sqlite3x.ErrNotLeader)
	got := err.Error()
	if got != want {
		t.Errorf("expected\n%q\ngot\n%q", want, got)
	}
	if txn := cluster.Txn(conn); txn != nil {
		t.Errorf("expected pending transaction to be removed, found %s", txn)
	}
}

func TestIntegration_RaftApplyErrorWithInflightTxnAndRecoverOnNewLeader(t *testing.T) {
	cluster := newCluster()
	defer cluster.Cleanup()

	cluster.WaitElection()

	conn := cluster.Open(0)

	if _, err := conn.Exec("BEGIN; CREATE TABLE test (n INT)", nil); err != nil {
		t.Fatal(err)
	}

	cluster.Disconnect(0)

	_, err := conn.Exec("INSERT INTO test VALUES(1); COMMIT", nil)
	if err == nil {
		t.Fatal("expected error when executing a statement on a disconnected leader")
	}

	want := sqlite3x.ErrorString(sqlite3x.ErrNotLeader)
	got := err.Error()
	if got != want {
		t.Errorf("expected\n%q\ngot\n%q", want, got)
	}

	cluster.WaitElection()
	cluster.Barrier(0)

	conn = cluster.Open(0)

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

type cluster struct {
	connections  []*connection.Registry
	transactions []*transaction.Registry
	methods      []*replication.Methods
	fsms         []*replication.FSM
	transports   []*raft.InmemTransport
	rafts        []*raft.Raft
	leader       int
	follower1    int
	follower2    int
}

func (c *cluster) Cleanup() {
	for i := range c.rafts {
		cleanupRaftAndConnections(c.rafts[i], c.connections[i])
	}
}

func (c *cluster) WaitElection() {
	leader := -1

	// Since notifications from LeaderCh are not buffered it could
	// be that election already happened, so check it first.
	for i, r := range c.rafts {
		if r.State() == raft.Leader {
			leader = i
			break
		}
	}

	// Wait for leader changes.
	if leader == -1 {
		flags := make([]bool, len(c.rafts))
		select {
		case flags[0] = <-c.rafts[0].LeaderCh():
		case flags[1] = <-c.rafts[1].LeaderCh():
		case flags[2] = <-c.rafts[2].LeaderCh():
		}
		for i, isLeader := range flags {
			if isLeader {
				leader = i
			}
		}
	}

	if leader == -1 {
		panic("no leader elected")
	}
	c.leader = leader
	c.follower1 = (leader + 1) % len(c.rafts)
	c.follower2 = (leader + 2) % len(c.rafts)
}

func (c *cluster) Open(node int) *sqlite3.SQLiteConn {
	i := c.index(node)
	conn, err := c.connections[i].OpenLeader("test", c.methods[i])
	if err != nil {
		panic(fmt.Sprintf("failed to open leader on node %d: %v", node, err))
	}
	return conn
}

func (c *cluster) Barrier(node int) {
	if node == 0 {
		if err := c.rafts[c.leader].Barrier(time.Second).Error(); err != nil {
			panic(fmt.Sprintf("failed to apply leader barrier: %v", err))
		}
	} else {
		i := c.index(node)
		c.fsms[i].Wait(c.rafts[c.leader].AppliedIndex())
	}
}

func (c *cluster) Disconnect(node int) {
	i := c.index(node)
	c.transports[i].DisconnectAll()
}

func (c *cluster) Txn(conn *sqlite3.SQLiteConn) *transaction.Txn {
	for _, transactions := range c.transactions {
		if txn := transactions.GetByConn(conn); txn != nil {
			return txn
		}
	}
	return nil
}

func (c *cluster) index(node int) int {
	switch node {
	case 0:
		return c.leader
	case 1:
		return c.follower1
	case 2:
		return c.follower2
	default:
		panic(fmt.Sprintf("invalid node index %d", node))
	}
}

func newCluster() *cluster {
	n := 3

	cluster := &cluster{
		transports: newRaftTransports(n),
	}
	peerStores := newRaftPeerStores(cluster.transports)

	for i := range cluster.transports {
		logger := log.New(logOutput(), fmt.Sprintf("%d: ", i), log.Lmicroseconds)

		fsm, connections, transactions := newFSMWithLogger(logger)

		config := newRaftConfig()
		config.Logger = logger

		raft := newRaft(config, fsm, cluster.transports[i], peerStores[i])

		methods := replication.NewMethods(logger, raft, connections, transactions)

		cluster.connections = append(cluster.connections, connections)
		cluster.transactions = append(cluster.transactions, transactions)
		cluster.methods = append(cluster.methods, methods)
		cluster.fsms = append(cluster.fsms, fsm)
		cluster.rafts = append(cluster.rafts, raft)
	}

	return cluster
}

// Create the given number of in-memory raft transports and connect
// them with each others.
func newRaftTransports(n int) []*raft.InmemTransport {
	transports := make([]*raft.InmemTransport, n)

	for i := range transports {
		_, transport := raft.NewInmemTransport(strconv.Itoa(i))
		transports[i] = transport
	}

	for i, transport1 := range transports {
		for j, transport2 := range transports {
			if i != j {
				transport1.Connect(transport2.LocalAddr(), transport2)
				transport2.Connect(transport1.LocalAddr(), transport1)
			}
		}
	}

	return transports
}

// Creates in-memory static peers each one populated with the
// addresses of the other ones.
func newRaftPeerStores(transports []*raft.InmemTransport) []*raft.StaticPeers {
	stores := make([]*raft.StaticPeers, len(transports))

	for i := range stores {
		stores[i] = &raft.StaticPeers{}
	}

	for i := range transports {
		store := stores[i]
		for j, transport := range transports {
			if i != j {
				store.StaticPeers = append(store.StaticPeers, transport.LocalAddr())
			}
		}
	}

	return stores
}
