package transaction_test

import (
	"fmt"
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/transaction"
	"github.com/CanonicalLtd/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func TestRegistry_AddLeader(t *testing.T) {
	registry := newRegistry()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.AddLeader(conn, 1, nil)

	if txn.ID() == 0 {
		t.Error("no ID assigned to transaction")
	}
	if txn.Conn() != conn {
		t.Error("transaction associated with wrong connection")
	}
	if !txn.IsLeader() {
		t.Error("transaction reported wrong replication mode")
	}
}

func TestRegistry_AddLeaderPanicsIfPassedSameLeaderConnectionTwice(t *testing.T) {
	registry := newRegistry()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.AddLeader(conn, 1, nil)

	want := fmt.Sprintf("a transaction for this connection is already registered with ID %d", txn.ID())
	defer func() {
		got := recover()
		if got != want {
			t.Errorf("expected\n%q\ngot\n%q", want, got)
		}
	}()
	registry.AddLeader(conn, 2, nil)
}

// If one of the other leader connections has already a transaction registered,
// nil is returned.
func TestRegistry_AddLeaderWithOnGoingTxn(t *testing.T) {
	registry := newRegistry()

	conn1 := &sqlite3.SQLiteConn{}
	txn1 := registry.AddLeader(conn1, 1, nil)
	assert.NotNil(t, txn1)

	conn2 := &sqlite3.SQLiteConn{}
	txn2 := registry.AddLeader(conn2, 2, []*sqlite3.SQLiteConn{conn1, conn2})
	assert.Nil(t, txn2)
}

func TestRegistry_AddFollower(t *testing.T) {
	registry := newRegistry()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.AddFollower(conn, 123)

	if txn.ID() != 123 {
		t.Errorf("expected transaction ID 123, got %d", txn.ID())
	}
	if txn.Conn() != conn {
		t.Error("transaction associated with wrong connection")
	}
	if txn.IsLeader() {
		t.Error("transaction reported wrong replication mode")
	}
}

func TestRegistry_GetByID(t *testing.T) {
	registry := newRegistry()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.AddLeader(conn, 0, nil)
	if registry.GetByID(txn.ID()) != txn {
		t.Error("transactions instances don't match")
	}
}

func TestRegistry_GetByIDNotFound(t *testing.T) {
	registry := newRegistry()
	if registry.GetByID(123) != nil {
		t.Error("expected no transaction instance for non-existing ID")
	}
}

func TestRegistry_GetByConn(t *testing.T) {
	registry := newRegistry()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.AddLeader(conn, 0, nil)
	if registry.GetByConn(conn) != txn {
		t.Error("transactions instances don't match")
	}
}

func TestRegistry_GetByConnFound(t *testing.T) {
	registry := newRegistry()

	conn := &sqlite3.SQLiteConn{}
	if registry.GetByConn(conn) != nil {
		t.Error("expected no transaction instance for non-registered conn")
	}
}

func TestRegistry_Remove(t *testing.T) {
	registry := newRegistry()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.AddLeader(conn, 0, nil)

	registry.Remove(txn.ID())
	if registry.GetByID(txn.ID()) != nil {
		t.Error("expected no transaction instance for unregistered ID")
	}
}

func TestRegistry_RemovePanicsIfPassedNonRegisteredID(t *testing.T) {
	registry := transaction.NewRegistry()
	const want = "attempt to remove unregistered transaction 123"
	defer func() {
		got := recover()
		if got != want {
			t.Errorf("expected\n%q\ngot\n%q", want, got)
		}
	}()
	registry.Remove(123)
}

// Dump returns a string with the contents of the registry.
func TestRegistry_Dump(t *testing.T) {
	registry := newRegistry()

	conn1 := &sqlite3.SQLiteConn{}
	registry.AddLeader(conn1, 0, nil)

	dump := registry.Dump()
	assert.Equal(t, "transactions:\n-> 0 pending as leader\n", dump)
}

// Create a new registry with the replication mode check disabled.
func newRegistry() *transaction.Registry {
	registry := transaction.NewRegistry()
	registry.SkipCheckReplicationMode(true)
	return registry
}
