package replication_test

import (
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/log"
	"github.com/CanonicalLtd/dqlite/internal/replication"
	"github.com/CanonicalLtd/dqlite/internal/transaction"
	"github.com/CanonicalLtd/go-sqlite3x"
	"github.com/CanonicalLtd/raft-test"
	"github.com/mattn/go-sqlite3"
	"github.com/mpvl/subtest"
	"github.com/ryanfaerman/fsm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errZero = sqlite3.ErrNo(0) // Convenience for assertions

func TestMethods_Hooks(t *testing.T) {
	for _, c := range methodsHooksCases {
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

var methodsHooksCases = []struct {
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
			txn := methods.Transactions().AddFollower(followerConn, "2")
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

func TestMethods_HookErrors(t *testing.T) {
	for _, c := range methodsHookErrorCases {
		subtest.Run(t, c.title, func(t *testing.T) {
			methods, conn, cleanup := newMethods(t)
			defer cleanup()

			assert.Equal(t, c.error, c.f(t, methods, conn))
		})
	}
}

var methodsHookErrorCases = []struct {
	title string
	f     func(*testing.T, *replication.Methods, *sqlite3.SQLiteConn) sqlite3.ErrNo
	error sqlite3.ErrNo // Expected error
}{
	{
		`begin fails if node is not raft leader`,
		func(t *testing.T, m *replication.Methods, conn *sqlite3.SQLiteConn) sqlite3.ErrNo {
			require.NoError(t, m.Raft().Shutdown().Error())
			return m.Begin(conn)
		},
		sqlite3x.ErrNotLeader,
	},
}

func newMethods(t *testing.T) (*replication.Methods, *sqlite3.SQLiteConn, func()) {
	logger := log.New(log.Testing(t), log.Trace)
	fsm, fsmCleanup := newFSM(t)
	raft := rafttest.Node(t, fsm)

	methods := replication.NewMethods(raft, logger, fsm.Connections(), fsm.Transactions())

	conn, connCleanup := newLeaderConn(t, fsm.Dir(), methods)

	cleanup := func() {
		connCleanup()
		assert.NoError(t, raft.Shutdown().Error())
		fsmCleanup()
	}

	methods.Connections().AddLeader("test.db", conn)

	// We need to disable replication mode checks, because leader
	// connections created with newLeaderConn() haven't actually initiated
	// a WAL write transaction and acquired the db lock necessary for
	// sqlite3_replication_mode() to succeed.
	methods.Transactions().SkipCheckReplicationMode(true)

	// Don't actually run the SQLite replication APIs, since the tests
	// using this helper are meant to drive the methods instance directly
	// (as opposed to indirectly via hooks triggered by SQLite), so the
	// leader connection hasn't entered a WAL transaction and would trigger
	// assertions in SQLite code.
	methods.Transactions().DryRun()

	return methods, conn, cleanup
}
