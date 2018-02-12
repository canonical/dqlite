package replication_test

import (
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/replication"
	"github.com/CanonicalLtd/dqlite/internal/transaction"
	"github.com/CanonicalLtd/go-sqlite3"
	"github.com/CanonicalLtd/raft-test"
	"github.com/mpvl/subtest"
	"github.com/ryanfaerman/fsm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The various methods hooks perform the correct state transition on the
// relevant transaction.
func TestMethods_Hooks(t *testing.T) {
	errZero := sqlite3.ErrNo(0) // Convenience for assertions

	cases := []struct {
		title string
		f     func(*testing.T, *replication.Methods, *sqlite3.SQLiteConn)
		state fsm.State // Expected transaction state after f is executed
	}{
		{
			`begin`,
			func(t *testing.T, methods *replication.Methods, conn *sqlite3.SQLiteConn) {
				assert.Equal(t, errZero, methods.Begin(conn))
			},
			transaction.Started,
		},
		{
			`rollback stale follower transaction`,
			func(t *testing.T, methods *replication.Methods, conn *sqlite3.SQLiteConn) {
				require.Equal(t, errZero, methods.Begin(conn))
				require.Equal(t, errZero, methods.End(conn))

				followerConn := methods.Connections().Follower("test.db")
				txn := methods.Transactions().AddFollower(followerConn, 2)
				require.NoError(t, txn.Do(txn.Begin))
				require.Equal(t, errZero, methods.Begin(conn))

				// The leftover follower transaction has been ended and remved.
				txn.Enter()
				require.Equal(t, transaction.Ended, txn.State())
				require.Nil(t, methods.Transactions().GetByID(txn.ID()))
			},
			transaction.Started,
		},
		{
			`wal frames`,
			func(t *testing.T, methods *replication.Methods, conn *sqlite3.SQLiteConn) {
				require.Equal(t, errZero, methods.Begin(conn))
				params := newWalFramesParams()
				assert.Equal(t, errZero, methods.WalFrames(conn, params))
			},
			transaction.Writing,
		},
		{
			`undo`,
			func(t *testing.T, methods *replication.Methods, conn *sqlite3.SQLiteConn) {
				require.Equal(t, errZero, methods.Begin(conn))
				assert.Equal(t, errZero, methods.Undo(conn))
			},
			transaction.Undoing,
		},
		{
			`end`,
			func(t *testing.T, methods *replication.Methods, conn *sqlite3.SQLiteConn) {
				require.Equal(t, errZero, methods.Begin(conn))
				assert.Equal(t, errZero, methods.End(conn))
			},
			"",
		},
	}

	for _, c := range cases {
		subtest.Run(t, c.title, func(t *testing.T) {
			methods, conn, cleanup := newMethods(t)
			defer cleanup()

			c.f(t, methods, conn)

			if c.state == "" {
				assert.Nil(t, methods.Transactions().GetByConn(conn))
				return
			}
			txn := methods.Transactions().GetByConn(conn)
			require.NotNil(t, txn)
			txn.Enter()
			assert.Equal(t, c.state, txn.State())
		})
	}
}

func TestMethods_HookPanic(t *testing.T) {
	cases := []struct {
		title string
		f     func(*testing.T, *replication.Methods, *sqlite3.SQLiteConn) sqlite3.ErrNo
		panic string // Expected panic message
	}{
		{
			`begin fails if node is not raft leader`,
			func(t *testing.T, m *replication.Methods, conn *sqlite3.SQLiteConn) sqlite3.ErrNo {
				txn := m.Transactions().AddLeader(conn, 1, nil)
				require.NotNil(t, txn)
				return m.Begin(conn)
			},
			"connection has existing transaction 1 pending as leader",
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

func newMethods(t *testing.T) (*replication.Methods, *sqlite3.SQLiteConn, func()) {
	fsm, fsmCleanup := newFSM(t)
	raft, raftCleanup := rafttest.Node(t, fsm)

	methods := replication.NewMethods(fsm, raft)

	conn, connCleanup := newLeaderConn(t, fsm.Dir(), methods)

	methods.Connections().AddLeader("test.db", conn)
	methods.Tracers().Add(replication.TracerName(methods.Connections(), conn))

	cleanup := func() {
		methods.Tracers().Remove(replication.TracerName(methods.Connections(), conn))
		methods.Connections().DelLeader(conn)

		connCleanup()
		raftCleanup()
		fsmCleanup()
	}

	// Don't actually run the SQLite replication APIs, since the tests
	// using this helper are meant to drive the methods instance directly
	// (as opposed to indirectly via hooks triggered by SQLite), so the
	// leader connection hasn't entered a WAL transaction and would trigger
	// assertions in SQLite code.
	methods.Transactions().DryRun()

	return methods, conn, cleanup
}
