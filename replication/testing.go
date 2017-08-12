package replication

import (
/*	"fmt"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/CanonicalLtd/dqlite/connection"
	"github.com/CanonicalLtd/dqlite/transaction"
	"github.com/CanonicalLtd/go-sqlite3x"
	"github.com/CanonicalLtd/raft-http"
	"github.com/hashicorp/raft"
	"github.com/mattn/go-sqlite3"*/
)

/*
func NewTempTestFSM() {
	logger := log.New(ioutil.Discard, "", 0)
	connections := connection.NewTempRegistry()
	defer connections.Purge()
	transactions := transaction.NewRegistry()
	replication.NewFSM(logger, connections, transactions)
}
*/
/*
// NewInmemRaftConfig returns configurations optimized for in-memory
// testing.
func NewInmemRaftConfig(logger *log.Logger, single bool) *raft.Config {
	conf := raft.DefaultConfig()
	conf.Logger = logger
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond
	conf.CommitTimeout = 5 * time.Millisecond
	conf.EnableSingleNode = single
	return conf
}

// InmemMembershipChanger implements peer join/leave operations in-memory.
type InmemMembershipChanger struct {
	addr        string                               // local address of the peer
	memberships map[string]chan *rafthttp.Membership // Memberships channels by remote address
}

// Join the cluster by driving the raft instance at the given address.
func (c *InmemMembershipChanger) Join(addr string, timeout time.Duration) error {
	membership := rafthttp.NewMembershipJoin(c.addr)
	memberships, ok := c.memberships[addr]
	if !ok {
		return fmt.Errorf("unknown peer '%s'", addr)
	}
	memberships <- membership
	return membership.Error()
}

// Leave tries to leave the cluster by contacting the leader at the
// given address.
func (c *InmemMembershipChanger) Leave(addr string, timeout time.Duration) error {
	membership := rafthttp.NewMembershipLeave(c.addr)
	memberships, ok := c.memberships[addr]
	if !ok {
		return fmt.Errorf("unknown peer '%s'", addr)
	}
	memberships <- membership
	return membership.Error()
}

// NewInmemMembershipChanger returns a new InmemMembershipChanger.
func NewInmemMembershipChanger(addr string, memberships map[string]chan *rafthttp.Membership) *InmemMembershipChanger {
	return &InmemMembershipChanger{
		addr:        addr,
		memberships: memberships,
	}
}

// NewConnectedInmemTransports creates the given number of in-memory
// transports and connects them together.
func NewConnectedInmemTransports(n int) []*raft.InmemTransport {
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

// NewConnectedStaticPeers creates in-memory static peers each
// populated with the addresses of the other ones.
func NewConnectedStaticPeers(transports []*raft.InmemTransport) []*raft.StaticPeers {
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

// TestingHelper provides fixtures and helpers for sqlite-related tests.
func TestingHelper(t *testing.T) *Testing {
	h := &Testing{
		t:          t,
		sqlite:     sqlite3x.TestingHelper(t),
		connection: connection.TestingHelper(t),
	}
	h.addCleanup(h.sqlite.Cleanup)
	return h
}

type Testing struct {
	t          *testing.T
	sqlite     *sqlite3x.Testing
	connection *connection.Testing
	cleanups   []func()
}

func (h *Testing) Cleanup() {
	for i := len(h.cleanups) - 1; i >= 0; i-- {
		h.cleanups[i]()
	}
}

// FSM returns a new test FSM. The FSM connections registry is
// configured with a single database named 'test', whose follower
// transaction is open.
func (h *Testing) FSM() *FSM {

	logger := h.sqlite.BufferLogger()

	connections := h.connection.Registry()
	connections.Add("test", h.connection.DSN())
	if err := connections.OpenFollower("test"); err != nil {
		h.t.Fatal(err)
	}

	transactions := transaction.NewRegistry()

	return NewFSM(logger, connections, transactions)
}

// Leader opens a new leader connection in the given FSM, using the
// 'test' database. No replication will be performed, and frames will
// only be written locally.
func (h *Testing) Leader(fsm *FSM) *sqlite3.SQLiteConn {
	conn, err := fsm.connections.OpenLeader("test", h.connection.Methods())
	if err != nil {
		h.t.Fatal(err)
	}
	h.addCleanup(func() {
		fsm.connections.CloseLeader(conn)
	})
	return conn
}

// SnapshotStore returns a raft snapshot store that uses the database
// directory of the given FSM to store snapshots.
func (h *Testing) SnapshotStore(fsm *FSM) raft.SnapshotStore {
	path := fsm.connections.Dir()
	logger := h.sqlite.BufferLogger()
	store, err := raft.NewFileSnapshotStoreWithLogger(path, 1, logger)
	if err != nil {
		h.t.Fatal(err)
	}
	return store
}

// DatabaseModTime returns the modefication time of the database
// file for the given connection. Used in tests to check if a file has
// been changed.
func (h *Testing) DatabaseModTime(conn *sqlite3.SQLiteConn) time.Time {
	info, err := os.Stat(sqlite3x.DatabaseFilename(conn))
	if err != nil {
		h.t.Fatal(err)
	}
	return info.ModTime()
}

// DatabaseSize returns the size of the database file for the given
// connection.
func (h *Testing) DatabaseSize(conn *sqlite3.SQLiteConn) int64 {
	info, err := os.Stat(sqlite3x.DatabaseFilename(conn))
	if err != nil {
		h.t.Fatal(err)
	}
	return info.Size()
}

// TriggerCheckpoint writes enough data to the WAL to trigger a
// checkpoint.
func (h *Testing) TriggerCheckpoint(fsm *FSM) {
	fsm.connections.AutoCheckpoint(1)

	conn := h.Leader(fsm)
	size := h.DatabaseSize(conn)

	if _, err := conn.Exec("CREATE TABLE foo (n INT)", nil); err != nil {
		h.t.Fatal(err)
	}
	if _, err := conn.Exec("INSERT INTO foo VALUES(1)", nil); err != nil {
		h.t.Fatal(err)
	}
	if _, err := conn.Exec("BEGIN; COMMIT", nil); err != nil {
		h.t.Fatal(err)
	}

	if h.DatabaseSize(conn) == size {
		h.t.Fatal("checkpoint did not trigger: database size is the same")
	}
}

// CheckPanic asserts that a panic occurs matches the given string.
func (h *Testing) CheckPanic(want string) {
	got := recover()
	if got == "" {
		h.t.Errorf("no panic occurred")
		return
	}
	if got != want {
		h.t.Errorf("unexpected recover:\n want: %v\n  got: %v", want, got)
	}
}

func (h *Testing) addCleanup(cleanup func()) {
	h.cleanups = append(h.cleanups, cleanup)
}
*/
