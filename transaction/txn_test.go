package transaction_test

import (
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/transaction"
	"github.com/CanonicalLtd/go-sqlite3x"
)

func TestTxn_String(t *testing.T) {
	connections := connection.NewTempRegistryWithDatabase()
	defer connections.Purge()

	registry := newRegistry()
	txn := registry.AddFollower(connections.Follower("test.db"), "abcd")

	want := "{id=abcd state=pending leader=false}"
	got := txn.String()
	if got != want {
		t.Errorf("expected\n%q\ngot\n%q", want, got)
	}
}

func TestTxn_CheckEntered(t *testing.T) {
	connections := connection.NewTempRegistryWithDatabase()
	defer connections.Purge()

	registry := newRegistry()
	txn := registry.AddFollower(connections.Follower("test.db"), "abcd")

	const want = "accessing or modifying txn state without mutex: abcd"
	defer func() {
		got := recover()
		if got != want {
			t.Errorf("expected\n%q\ngot\n%q", want, got)
		}
	}()
	txn.State()
}

func TestTxn_Pending(t *testing.T) {
	connections := connection.NewTempRegistryWithDatabase()
	defer connections.Purge()

	registry := newRegistry()
	txn := registry.AddFollower(connections.Follower("test.db"), "abcd")
	txn.Enter()

	state := txn.State()
	if state != transaction.Pending {
		t.Errorf("initial txn state is not Pending: %s", state)
	}
}

func TestTxn_Started(t *testing.T) {
	connections := connection.NewTempRegistryWithDatabase()
	defer connections.Purge()

	registry := newRegistry()
	txn := registry.AddFollower(connections.Follower("test.db"), "abcd")
	txn.Enter()

	if err := txn.Begin(); err != nil {
		t.Fatal(err)
	}

	state := txn.State()
	if state != transaction.Started {
		t.Errorf("txn state after Begin is not Started: %s", state)
	}
}

func TestTxn_Writing(t *testing.T) {
	connections := connection.NewTempRegistryWithDatabase()
	defer connections.Purge()

	registry := newRegistry()
	registry.DryRun()

	txn := registry.AddFollower(connections.Follower("test.db"), "abcd")
	txn.Enter()

	if err := txn.Begin(); err != nil {
		t.Fatal(err)
	}
	params := &sqlite3x.ReplicationWalFramesParams{
		Pages: sqlite3x.NewReplicationPages(2, 4096),
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
	connections := connection.NewTempRegistryWithDatabase()
	defer connections.Purge()

	registry := newRegistry()
	txn := registry.AddFollower(connections.Follower("test.db"), "abcd")
	txn.Enter()

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
	connections := connection.NewTempRegistryWithDatabase()
	defer connections.Purge()

	registry := newRegistry()
	txn := registry.AddFollower(connections.Follower("test.db"), "abcd")
	txn.Enter()

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
	connections, conn := connection.NewTempRegistryWithLeader()
	defer connections.Purge()

	registry := newRegistry()
	txn := registry.AddLeader(conn)
	txn.Enter()

	if err := txn.Stale(); err != nil {
		t.Fatal(err)
	}

	state := txn.State()
	if state != transaction.Stale {
		t.Errorf("txn state after Stale from Pending is not Stale: %s", state)
	}
}

func TestTxn_StaleFromStarted(t *testing.T) {
	connections, conn := connection.NewTempRegistryWithLeader()
	defer connections.Purge()

	registry := newRegistry()
	registry.DryRun()

	txn := registry.AddLeader(conn)
	txn.Enter()

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
	connections, conn := connection.NewTempRegistryWithLeader()
	defer connections.Purge()

	registry := newRegistry()
	registry.DryRun()

	txn := registry.AddLeader(conn)
	txn.Enter()

	if err := txn.Begin(); err != nil {
		t.Fatal(err)
	}
	params := &sqlite3x.ReplicationWalFramesParams{
		Pages: sqlite3x.NewReplicationPages(2, 4096),
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
	connections, conn := connection.NewTempRegistryWithLeader()
	defer connections.Purge()

	registry := newRegistry()
	registry.DryRun()

	// Pretend that the follower transaction is the leader, since
	// invoking Begin() on an actual leader connection would fail
	// because the WAL has not started a read transaction.
	txn := registry.AddLeader(conn)
	txn.Enter()

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
	connections, conn := connection.NewTempRegistryWithLeader()
	defer connections.Purge()

	registry := newRegistry()
	registry.DryRun()

	txn := registry.AddLeader(conn)
	txn.Enter()

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
	connections := connection.NewTempRegistryWithDatabase()
	defer connections.Purge()

	registry := newRegistry()
	txn := registry.AddFollower(connections.Follower("test.db"), "abcd")
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
