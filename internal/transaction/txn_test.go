package transaction_test

import (
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/transaction"
	"github.com/CanonicalLtd/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func TestTxn_String(t *testing.T) {
	registry := newRegistry()

	conn1 := &sqlite3.SQLiteConn{}
	txn1 := registry.AddFollower(conn1, 0)
	assert.Equal(t, "0 pending as follower", txn1.String())

	conn2 := &sqlite3.SQLiteConn{}
	txn2 := registry.AddLeader(conn2, 1, nil)
	assert.Equal(t, "1 pending as leader", txn2.String())
}

func TestTxn_CheckEntered(t *testing.T) {
	registry := newRegistry()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.AddFollower(conn, 123)

	f := func() { txn.State() }
	assert.PanicsWithValue(t, "accessing or modifying txn state without mutex: 123", f)
}

func TestTxn_IsStale(t *testing.T) {
	registry := newRegistry()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.AddLeader(conn, 0, nil)

	assert.False(t, txn.IsStale())

	txn.DryRun(true)
	txn.Do(txn.Begin)
	txn.Do(txn.Stale)

	assert.True(t, txn.IsStale())
}

// Follower transactions can't transition to the stale state.
func TestTxn_IsStaleFollower(t *testing.T) {
	registry := newRegistry()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.AddFollower(conn, 123)

	assert.False(t, txn.IsStale())

	txn.DryRun(true)
	txn.Do(txn.Begin)

	f := func() { txn.Do(txn.Stale) }

	assert.PanicsWithValue(t, "invalid ended -> stale transition", f)
}

func TestTxn_Pending(t *testing.T) {
	registry := newRegistry()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.AddFollower(conn, 123)
	txn.Enter()

	state := txn.State()
	if state != transaction.Pending {
		t.Errorf("initial txn state is not Pending: %s", state)
	}
}

func TestTxn_Started(t *testing.T) {
	registry := newRegistry()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.AddFollower(conn, 123)
	txn.Enter()

	txn.DryRun(true)
	if err := txn.Begin(); err != nil {
		t.Fatal(err)
	}

	state := txn.State()
	if state != transaction.Started {
		t.Errorf("txn state after Begin is not Started: %s", state)
	}
}

func TestTxn_Writing(t *testing.T) {
	registry := newRegistry()
	registry.DryRun()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.AddFollower(conn, 123)
	txn.Enter()

	if err := txn.Begin(); err != nil {
		t.Fatal(err)
	}
	params := &sqlite3.ReplicationWalFramesParams{
		Pages: sqlite3.NewReplicationPages(2, 4096),
	}
	if err := txn.WalFrames(params); err != nil {
		t.Fatal(err)
	}

	state := txn.State()
	if state != transaction.Writing {
		t.Errorf("txn state after WalFrames is not Writing: %s", state)
	}
}

func TestTxn_Undoing(t *testing.T) {
	registry := newRegistry()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.AddFollower(conn, 123)
	txn.Enter()

	txn.DryRun(true)
	if err := txn.Begin(); err != nil {
		t.Fatal(err)
	}
	if err := txn.Undo(); err != nil {
		t.Fatal(err)
	}

	state := txn.State()
	if state != transaction.Undoing {
		t.Errorf("txn state after Undo is not Undoing: %s", state)
	}
}

func TestTxn_Ended(t *testing.T) {
	registry := newRegistry()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.AddFollower(conn, 123)
	txn.Enter()

	txn.DryRun(true)
	if err := txn.Begin(); err != nil {
		t.Fatal(err)
	}
	if err := txn.End(); err != nil {
		t.Fatal(err)
	}

	state := txn.State()
	if state != transaction.Ended {
		t.Errorf("txn state after End is not Ended: %s", state)
	}
}

func TestTxn_StaleFromPending(t *testing.T) {
	registry := newRegistry()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.AddLeader(conn, 0, nil)
	txn.Enter()

	txn.DryRun(true)
	if err := txn.Stale(); err != nil {
		t.Fatal(err)
	}

	state := txn.State()
	if state != transaction.Stale {
		t.Errorf("txn state after Stale from Pending is not Stale: %s", state)
	}
}

func TestTxn_StaleFromStarted(t *testing.T) {
	registry := newRegistry()
	registry.DryRun()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.AddLeader(conn, 0, nil)
	txn.Enter()

	txn.DryRun(true)
	if err := txn.Begin(); err != nil {
		t.Fatal(err)
	}

	if err := txn.Stale(); err != nil {
		t.Fatal(err)
	}

	state := txn.State()
	if state != transaction.Stale {
		t.Errorf("txn state after Stale from Begin is not Stale: %s", state)
	}
}

func TestTxn_StaleFromWriting(t *testing.T) {
	registry := newRegistry()
	registry.DryRun()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.AddLeader(conn, 0, nil)
	txn.Enter()

	txn.DryRun(true)
	if err := txn.Begin(); err != nil {
		t.Fatal(err)
	}
	params := &sqlite3.ReplicationWalFramesParams{
		Pages: sqlite3.NewReplicationPages(2, 4096),
	}
	if err := txn.WalFrames(params); err != nil {
		t.Fatal(err)
	}
	if err := txn.Stale(); err != nil {
		t.Fatal(err)
	}

	state := txn.State()
	if state != transaction.Stale {
		t.Errorf("txn state after Stale from Writing is not Stale: %s", state)
	}
}

func TestTxn_StaleFromUndoing(t *testing.T) {
	registry := newRegistry()
	registry.DryRun()

	conn := &sqlite3.SQLiteConn{}

	// Pretend that the follower transaction is the leader, since
	// invoking Begin() on an actual leader connection would fail
	// because the WAL has not started a read transaction.
	txn := registry.AddLeader(conn, 0, nil)
	txn.Enter()

	txn.DryRun(true)
	if err := txn.Begin(); err != nil {
		t.Fatal(err)
	}
	if err := txn.Undo(); err != nil {
		t.Fatal(err)
	}
	if err := txn.Stale(); err != nil {
		t.Fatal(err)
	}

	state := txn.State()
	if state != transaction.Stale {
		t.Errorf("txn state after Stale from Undoing is not Stale: %s", state)
	}
}

func TestTxn_StaleFromEnded(t *testing.T) {
	registry := newRegistry()
	registry.DryRun()

	conn := &sqlite3.SQLiteConn{}

	txn := registry.AddLeader(conn, 0, nil)
	txn.Enter()

	txn.DryRun(true)
	if err := txn.Begin(); err != nil {
		t.Fatal(err)
	}
	if err := txn.End(); err != nil {
		t.Fatal(err)
	}
	if err := txn.Stale(); err != nil {
		t.Fatal(err)
	}

	state := txn.State()
	if state != transaction.Stale {
		t.Errorf("txn state after Stale from End is not Stale: %s", state)
	}
}

func TestTxn_StalePanicsIfInvokedOnFollowerTransaction(t *testing.T) {
	registry := newRegistry()

	conn := &sqlite3.SQLiteConn{}
	txn := registry.AddFollower(conn, 123)
	txn.Enter()

	const want = "invalid pending -> stale transition"
	defer func() {
		got := recover()
		if got != want {
			t.Errorf("expected\n%q\ngot\n%q", want, got)
		}
	}()
	txn.Stale()
}
