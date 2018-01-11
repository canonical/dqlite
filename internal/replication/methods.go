package replication

import (
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/internal/log"
	"github.com/CanonicalLtd/dqlite/internal/protocol"
	"github.com/CanonicalLtd/dqlite/internal/transaction"
	"github.com/CanonicalLtd/go-sqlite3x"
	"github.com/hashicorp/raft"
	"github.com/mattn/go-sqlite3"
)

// NewMethods returns a new Methods instance that can be used as
// callbacks API for raft-based sqlite replication of a single
// connection.
func NewMethods(raft *raft.Raft, l *log.Logger, c *connection.Registry, t *transaction.Registry) *Methods {
	return &Methods{
		raft:         raft,
		logger:       l.Augment("methods"),
		connections:  c,
		transactions: t,
		applyTimeout: 10 * time.Second,
		stale:        map[*sqlite3.SQLiteConn]*transaction.Txn{},
	}
}

// Methods implements the sqlite replication C API using the sqlite3x
// bindings.
type Methods struct {
	raft         *raft.Raft  // Eaft engine to use
	logger       *log.Logger // Log messages go here
	connections  *connection.Registry
	transactions *transaction.Registry // Registry of leader and follower transactions
	applyTimeout time.Duration         // Timeout for applying raft commands

	mu    sync.RWMutex // Serialize access to internal state
	stale map[*sqlite3.SQLiteConn]*transaction.Txn
}

// ApplyTimeout sets the maximum amount of time to wait before giving
// up applying a raft command. The default is 10 seconds.
func (m *Methods) ApplyTimeout(timeout time.Duration) {
	m.applyTimeout = timeout
}

// Begin is the hook invoked by sqlite when a new write transaction is
// being started within a connection in leader replication mode on
// this node.
func (m *Methods) Begin(conn *sqlite3.SQLiteConn) sqlite3.ErrNo {
	var logger *log.Logger
	var txn *transaction.Txn
	var err error

	// Insert a new transaction in the registry, preventing more than one
	// transaction to be active at the same time.
	logger, txn, err = m.tryBegin(conn)
	if err != nil {
		return errno(err)
	}
	if txn == nil {
		logger.Tracef("a transaction is already running")
		return sqlite3.ErrBusy
	}

	filename := m.connections.FilenameOfLeader(conn)
	if err := m.maybeUndoFollowerTxn(logger, filename); err != nil {
		// We failed to rollback a leftover follower, let's
		// mark the transaction as stale. It should then be
		// purged by client code.
		txn.Enter()
		defer txn.Exit()
		if err := m.markStale(logger, txn); err != nil {
			return errno(err)
		}
		return errno(err)
	}

	command := protocol.NewBegin(txn.ID(), filename)
	if err := m.apply(logger, command); err != nil {
		txn.Enter()
		defer txn.Exit()

		// Let's stale the transaction, so follow-up rollback hooks
		// triggered by client clean-up will no-op.
		if err := m.markStale(logger, txn); err != nil {
			return errno(err)
		}
		if txn.State() == transaction.Started {
			// This means that the raft still managed to succeed, a very
			// unlikely but in principle possible event with particular
			// bad timing of thread and goroutine scheduling. Create a
			// surrogate leftover follower so any rollback raft command
			// initiated by the next leader will find it.
			if err := m.addFollowerTxn(logger, filename, txn.ID()); err != nil {
				return errno(err)
			}
		}
		return errno(err)
	}
	return 0
}

// This is the core logic of the Begin method, which fails in case there's
// already an ongoing transaction for another leader connection.
func (m *Methods) tryBegin(conn *sqlite3.SQLiteConn) (*log.Logger, *transaction.Txn, error) {
	logger := m.hookLogger(conn, "begin")
	filename := m.connections.FilenameOfLeader(conn)
	logger = logger.Augment(filepath.Base(filename))

	if err := checkIsLeader(logger, m.raft); err != nil {
		return nil, nil, err
	}

	// It should never happen that we have an ongoing write transaction for
	// this connection, since SQLite locking system itself prevents it by
	// serializing write transactions. Still double check it for sanity.
	if txn := m.transactions.GetByConn(conn); txn != nil {
		logger.Panicf("found leader transaction %s (%s)", txn.ID(), txn)
	}

	if err := m.maybeAddFollowerConn(logger, filename); err != nil {
		return nil, nil, err
	}

	leaders := m.connections.Leaders(filename)

	txid := strconv.Itoa(int(m.raft.LastIndex()))
	logger = logger.Augment(fmt.Sprintf("txn %s", txid))
	logger.Tracef("register transaction")

	txn := m.transactions.AddLeader(conn, txid, leaders)

	return logger, txn, nil
}

// WalFrames is the hook invoked by sqlite when new frames need to be
// flushed to the write-ahead log.
func (m *Methods) WalFrames(conn *sqlite3.SQLiteConn, frames *sqlite3x.ReplicationWalFramesParams) sqlite3.ErrNo {
	logger := m.hookLogger(conn, "wal frames")
	logger.Tracef("pages=%d commit=%d", len(frames.Pages), frames.IsCommit)

	txn := m.lookupTxn(logger, conn)
	logger = logger.Augment(fmt.Sprintf("txn %s", txn.ID()))

	if err := checkIsLeader(logger, m.raft); err != nil {
		// If we have lost leadership we're in a state where
		// the transaction began on this node and a quorum of
		// followers. When we return an error, SQLite will try
		// to automatically rollback the WAL: we need to mark
		// the transaction as stale (so the follow-up undo and
		// end command will succeed as no-op) and create a
		// follower (so the next leader will roll it back as
		// leftover). See also #2.
		txn.Enter()
		defer txn.Exit()
		if err := m.markStaleAndAddFollowerTxn(logger, txn); err != nil {
			return errno(err)
		}
		return errno(err)
	}

	m.assertNoFollowerTxn(logger, txn.Conn())

	command := protocol.NewWalFrames(txn.ID(), frames)
	if err := m.apply(logger, command); err != nil {
		txn.Enter()
		defer txn.Exit()
		if err := m.markStaleAndAddFollowerTxn(logger, txn); err != nil {
			return errno(err)
		}
		return errno(err)
	}
	return 0
}

// Undo is the hook invoked by sqlite when a write transaction needs
// to be rolled back.
func (m *Methods) Undo(conn *sqlite3.SQLiteConn) sqlite3.ErrNo {
	logger := m.hookLogger(conn, "undo")

	txn := m.lookupTxn(logger, conn)
	logger = logger.Augment(fmt.Sprintf("txn %s", txn.ID()))

	if txn.IsStale() {
		logger.Tracef("transaction is stale, skip undo")
		return 0
	}

	if err := checkIsLeader(logger, m.raft); err != nil {
		return errno(err)
	}

	m.assertNoFollowerTxn(logger, txn.Conn())

	command := protocol.NewUndo(txn.ID())
	if err := m.apply(logger, command); err != nil {
		txn.Enter()
		defer txn.Exit()
		if err := m.markStaleAndAddFollowerTxn(logger, txn); err != nil {
			return errno(err)
		}
		return errno(err)
	}

	return 0
}

// End is the hook invoked by sqlite when a write transaction needs
// to be ended.
func (m *Methods) End(conn *sqlite3.SQLiteConn) sqlite3.ErrNo {
	logger := m.hookLogger(conn, "end")

	txn := m.lookupTxn(logger, conn)
	logger = logger.Augment(fmt.Sprintf("txn %s", txn.ID()))

	if txn.IsStale() {
		logger.Tracef("transaction is stale, remove it and skip end")
		m.dropStale(txn)
		return 0
	}

	if err := checkIsLeader(logger, m.raft); err != nil {
		return errno(err)
	}

	m.assertNoFollowerTxn(logger, txn.Conn())

	command := protocol.NewEnd(txn.ID())
	if err := m.apply(logger, command); err != nil {
		txn.Enter()
		defer txn.Exit()
		if txn.State() == transaction.Ended {
			// The command still has managed to be applied
			// so let's just remove the transaction, it should not
			// pop up again.
			logger.Tracef("finished transaction that failed to apply end command")
		} else {
			// Let's end the transaction ourselves, since we won't be
			// called back again as end replication hook by client hook.
			if err := txn.End(); err != nil {
				logger.Tracef("failed to end transaction after end command")
				return sqlite3x.ErrReplication
			}
			// Create a surrogate follower, in case the raft command
			// eventually gets committed or in case we become leader
			// again and need to rollback.
			name := m.connections.FilenameOfLeader(txn.Conn())
			if err := m.addFollowerTxn(logger, name, txn.ID()); err != nil {
				return errno(err)
			}
		}
		return errno(err)
	}

	logger.Tracef("remove transaction")

	return 0
}

// Checkpoint is the hook invoked by sqlite when the WAL file needs
// to be checkpointed.
func (m *Methods) Checkpoint(conn *sqlite3.SQLiteConn, mode sqlite3x.WalCheckpointMode, log *int, ckpt *int) sqlite3.ErrNo {
	logger := m.logger.Augment(fmt.Sprintf("conn %d checkpoint", m.connections.Serial(conn)))
	logger.Tracef("mode %d", mode)

	if err := checkIsLeader(logger, m.raft); err != nil {
		return errno(err)
	}

	name := m.connections.FilenameOfLeader(conn)

	command := protocol.NewCheckpoint(name)
	if err := m.apply(logger, command); err != nil {
		return errno(err)
	}

	return 0
}

// Augment our logger with connection/hook specific prefix containing the
// connection serial number and hook name.
func (m *Methods) hookLogger(conn *sqlite3.SQLiteConn, hook string) *log.Logger {
	logger := m.logger.Augment(fmt.Sprintf("conn %d %s", m.connections.Serial(conn), hook))
	logger.Tracef("start")
	return logger
}

// Wrapper around Registry.GetByConn, panic'ing if no matching transaction is
// found.
func (m *Methods) lookupTxn(logger *log.Logger, conn *sqlite3.SQLiteConn) *transaction.Txn {
	txn := m.transactions.GetByConn(conn)
	if txn == nil {
		if _, ok := m.stale[conn]; ok {
			txn = m.stale[conn]
			logger.Tracef("found stale transaction %s (%s)", txn.ID(), txn)
		} else {
			logger.Panicf("no ongoing transactions")
		}
	}
	return txn
}

// Acquire the lock and check if a follower connection is already open
// for this database, if not open one with the Open raft command.
func (m *Methods) maybeAddFollowerConn(logger *log.Logger, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.connections.HasFollower(name) {
		return m.apply(logger.Augment("new follower"), protocol.NewOpen(name))
	}
	return nil
}

// This method ensures that there is no follower write transactions
// happening on this node for the given database filename. If one is
// found the method will try to apply a rollback command to clear it
// up.
//
// It's necessary to run this safeguard before trying to apply any
// leader command, because if we do find an ongoing follower write
// transaction for the same filename associated to a leader connection
// on this node, it means that the leader node that started the
// transaction got deposed (this node is now the new leader), and it's
// not eligeable to complete it. So we need to interrupt the
// transaction by applying a rollback command, to make sure all nodes
// (including this one) close their follower transactions.
//
// TODO: if the transaction was committed and the leader died
// just after, we should detect that and report to clients
// that it was all good.
func (m *Methods) maybeUndoFollowerTxn(logger *log.Logger, name string) error {
	txn := m.transactions.GetByConn(m.connections.Follower(name))
	if txn == nil {
		return nil
	}

	logger.Tracef("undo stale transaction %s (%s)", txn.ID(), txn)
	if err := m.apply(logger, protocol.NewUndo(txn.ID())); err != nil {
		return err
	}
	return m.apply(logger, protocol.NewEnd(txn.ID()))
}

// Sanity check that there is no ongoing follower write transaction on
// this node for the given database.
func (m *Methods) assertNoFollowerTxn(logger *log.Logger, conn *sqlite3.SQLiteConn) {
	name := m.connections.FilenameOfLeader(conn)
	if txn := m.transactions.GetByConn(m.connections.Follower(name)); txn != nil {
		logger.Panicf("detected follower write transaction %s", txn)
	}
}

// Apply the given command through raft.
func (m *Methods) apply(logger *log.Logger, cmd *protocol.Command) error {
	logger.Tracef("apply %s", cmd.Name())

	data, err := protocol.MarshalCommand(cmd)
	if err != nil {
		logger.Tracef("failed to marshal %s: %s", cmd.Name(), err)
		return err
	}

	future := m.raft.Apply(data, m.applyTimeout)
	if err := future.Error(); err != nil {
		m.logger.Tracef("failed to apply %s command: %s", cmd.Name(), err)

		// If the node has lost leadership, we return a
		// dedicated error, so clients will typically retry
		// against the new leader.
		if err == raft.ErrNotLeader || err == raft.ErrLeadershipLost {
			return sqlite3x.ErrNotLeader
		}

		// Generic replication error.
		return sqlite3x.ErrReplication
	}

	logger.Tracef("done")
	return nil
}

func (m *Methods) markStale(logger *log.Logger, txn *transaction.Txn) error {
	logger.Tracef("mark as stale (%s)", txn)
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := txn.Stale(); err != nil {
		logger.Tracef("failed stale transition: %v", err)
		return err
	}

	// Add the transaction to the stale index.
	m.stale[txn.Conn()] = txn

	// Stale transactions instances are not involved anymore in
	// raft-applied commands, so remove them from the registry.
	logger.Tracef("remove stale transaction from registry")
	m.transactions.Remove(txn.ID())

	return nil
}

func (m *Methods) dropStale(txn *transaction.Txn) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.stale, txn.Conn())
}

// Create a surrogate follower transaction with the given ID.
func (m *Methods) addFollowerTxn(logger *log.Logger, name string, id string) error {
	logger.Tracef("create surrogate follower transaction")
	txn := m.transactions.AddFollower(m.connections.Follower(name), id)

	txn.Enter()
	defer txn.Exit()

	if err := txn.Begin(); err != nil {
		logger.Tracef("failed to begin surrogate follower: %v", err)
		return err
	}

	return nil
}

// Mark the transaction as stale and create a surrogate follower transaction
// with its ID.
func (m *Methods) markStaleAndAddFollowerTxn(logger *log.Logger, txn *transaction.Txn) error {
	if err := m.markStale(logger, txn); err != nil {
		return err
	}
	name := m.connections.FilenameOfLeader(txn.Conn())
	return m.addFollowerTxn(logger, name, txn.ID())
}

// Wrapper around IsLeader, logging a message if we don't think we are the
// leader anymore. This is called by replication hooks, and calling logic will
// short-circuit if we return non-zero, i.e. if we were deposed between the
// time of the last successful raft command and now.
func checkIsLeader(logger *log.Logger, r *raft.Raft) error {
	if r.State() != raft.Leader {
		logger.Tracef("deposed leader")
		return sqlite3x.ErrNotLeader
	}
	return nil
}

// Convert a Go error into a SQLite error number.
func errno(err error) sqlite3.ErrNo {
	switch e := err.(type) {
	case sqlite3.ErrNo:
		return e
	default:
		return sqlite3x.ErrReplication // Generic replication error.
	}
}
