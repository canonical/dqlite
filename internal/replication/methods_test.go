package replication_test

import (
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/bindings"
	"github.com/CanonicalLtd/dqlite/internal/registry"
	"github.com/CanonicalLtd/dqlite/internal/replication"
	"github.com/CanonicalLtd/dqlite/internal/transaction"
	"github.com/CanonicalLtd/raft-test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The Begin hook registers a new transaction and transitions it to Started by
// applying a begin FSM command.
func TestMethods_Begin(t *testing.T) {
	methods, conn, cleanup := newMethods(t)
	defer cleanup()

	assert.Equal(t, 0, methods.Begin(conn))
	txn := methods.Registry().TxnByConn(conn)
	require.NotNil(t, txn)
	assert.Equal(t, transaction.Pending, txn.State())
}

/*
// The Begin hook applies an undo FSM command to rollback stale follower
// transactions that might have been left dangling by a deposed leader.
func TestMethods_Begin_UndoStaleFollowerTransaction(t *testing.T) {
	methods, conn, cleanup := newMethods(t)
	defer cleanup()

	// Begin and undo a transaction to trigger a follower open
	require.Equal(t, errZero, methods.Begin(conn))
	require.Equal(t, errZero, methods.Undo(conn))

	// Register a follower transaction in Started state.
	followerConn := methods.Registry().ConnFollower("test.db")
	txn := methods.Registry().TxnFollowerAdd(followerConn, 2)
	require.NoError(t, txn.Begin())

	// Trigger the begin hook.
	require.Equal(t, errZero, methods.Begin(conn))

	// The leftover follower transaction has been ended and remved.
	require.Equal(t, transaction.Undone, txn.State())
	require.Nil(t, methods.Registry().TxnByID(txn.ID()))
}

// The Frames hook applies a frames FSM command.
func TestMethods_Frames(t *testing.T) {
	methods, conn, cleanup := newMethods(t)
	defer cleanup()

	require.Equal(t, errZero, methods.Begin(conn))
	txn := methods.Registry().TxnByConn(conn)

	assert.Equal(t, errZero, methods.Frames(conn, newFramesParams()))
	assert.Equal(t, transaction.Written, txn.State())

	require.Nil(t, methods.Registry().TxnByID(txn.ID()))
}

// The Undo hook applies an undo FSM command.
func TestMethods_Undo(t *testing.T) {
	methods, conn, cleanup := newMethods(t)
	defer cleanup()

	require.Equal(t, errZero, methods.Begin(conn))
	txn := methods.Registry().TxnByConn(conn)

	assert.Equal(t, errZero, methods.Undo(conn))
	assert.Equal(t, transaction.Undone, txn.State())

	require.Nil(t, methods.Registry().TxnByID(txn.ID()))
}

// Exercise panic-leading situations.
func TestMethods_HookPanic(t *testing.T) {
	cases := []struct {
		title string
		f     func(*testing.T, *replication.Methods, *sqlite3.SQLiteConn) sqlite3.ErrNo
		panic string // Expected panic message
	}{
		{
			`begin with existing txn for same conn`,
			func(t *testing.T, m *replication.Methods, conn *sqlite3.SQLiteConn) sqlite3.ErrNo {
				txn := m.Registry().TxnLeaderAdd(conn, 1)
				require.NotNil(t, txn)
				return m.Begin(conn)
			},
			"unexpected registered transaction 1 pending as leader",
		},
	}
	for _, c := range cases {
		subtest.Run(t, c.title, func(t *testing.T) {
			methods, conn, cleanup := newMethods(t)
			defer cleanup()

			assert.PanicsWithValue(t, c.panic, func() { c.f(t, methods, conn) })
		})
	}
}

var errZero = sqlite3.ErrNo(0) // Convenience for assertions
*/

func newMethods(t *testing.T) (*replication.Methods, *bindings.Conn, func()) {
	t.Helper()

	vfs, err := bindings.RegisterVfs("test")
	require.NoError(t, err)

	dir, dirCleanup := newDir(t)

	registry := registry.New(vfs, dir)
	registry.Testing(t, 0)

	fsm := replication.NewFSM(registry)

	raft, raftCleanup := rafttest.Server(t, fsm)

	methods := replication.NewMethods(registry, raft)

	err = bindings.RegisterWalReplication("test", methods)
	require.NoError(t, err)

	conn, connCleanup := newLeaderConn(t, "test", "test")

	methods.Registry().ConnLeaderAdd("test.db", conn)

	cleanup := func() {
		methods.Registry().ConnLeaderDel(conn)

		connCleanup()
		raftCleanup()
		dirCleanup()
		bindings.UnregisterVfs(vfs)
		bindings.UnregisterWalReplication("test")
	}

	// Don't actually run the SQLite replication APIs, since the tests
	// using this helper are meant to drive the methods instance directly
	// (as opposed to indirectly via hooks triggered by SQLite), so the
	// leader connection hasn't entered a WAL transaction and would trigger
	// assertions in SQLite code.
	methods.Registry().TxnDryRun()

	return methods, conn, cleanup
}
