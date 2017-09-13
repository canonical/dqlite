package replication

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/CanonicalLtd/dqlite/internal/command"
	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/transaction"
	"github.com/CanonicalLtd/go-sqlite3x"
	"github.com/hashicorp/raft"
	"github.com/mattn/go-sqlite3"
)

// NewMethods returns a new Methods instance that can be used as
// callbacks API for raft-based sqlite replication of a single
// connection.
func NewMethods(logger *log.Logger, raft *raft.Raft, connections *connection.Registry, transactions *transaction.Registry) *Methods {
	return &Methods{
		logger:       logger,
		raft:         raft,
		connections:  connections,
		transactions: transactions,
		applyTimeout: 10 * time.Second,
		stale:        map[*sqlite3.SQLiteConn]*transaction.Txn{},
	}
}

// Methods implements the sqlite replication C API using the sqlite3x
// bindings.
type Methods struct {
	logger       *log.Logger // Log messages go here
	raft         *raft.Raft  // Eaft engine to use
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
	m.logger.Printf("[DEBUG] dqlite: methods: begin hook for %p", conn)

	m.checkNoTxnForConn(conn)

	if errno := m.checkIsLeader("begin"); errno != 0 {
		return errno
	}

	// Insert a new transaction in the registry.
	txn := m.transactions.AddLeader(conn)

	name := m.connections.NameByLeader(conn)

	if errno := m.ensureNoFollower(name); errno != 0 {
		// We failed to rollback a leftover follower, let's
		// mark the transaction as stale. It should then be
		// purged by client code.
		txn.Enter()
		defer txn.Exit()
		if errno := m.markStale(txn); errno != 0 {
			return errno
		}
		return errno
	}

	if errno := m.apply(command.NewBegin(txn.ID(), name)); errno != 0 {
		txn.Enter()
		defer txn.Exit()

		// Let's stale the transaction, so follow-up rollback hooks
		// triggered by client clean-up will no-op.
		if errno := m.markStale(txn); errno != 0 {
			return errno
		}
		if txn.State() == transaction.Started {
			// This means that the raft still managed to succeed, a very
			// unlikely but in principle possible event with particular
			// bad timing of thread and goroutine scheduling. Create a
			// surrogate leftover follower so any rollback raft command
			// initiated by the next leader will find it.
			if errno := m.createSurrogateFollower(name, txn.ID()); errno != 0 {
				return errno
			}
		}

		return errno
	}
	return 0
}

// WalFrames is the hook invoked by sqlite when new frames need to be
// flushed to the write-ahead log.
func (m *Methods) WalFrames(conn *sqlite3.SQLiteConn, frames *sqlite3x.ReplicationWalFramesParams) sqlite3.ErrNo {
	m.logger.Printf("[DEBUG] dqlite: methods: wal frames hook for %p", conn)

	txn := m.lookupExistingLeaderForConn(conn)

	if errno := m.checkIsLeader("wal frames"); errno != 0 {
		// If we have lost leadership we're in a state where
		// the transaction began on this node and a quorum of
		// follower. When we return an error, SQLite will try
		// to automatically rollback the WAL: we need to mark
		// the transaction as stale (so the follow-up undo and
		// end command will succeed as no-op) and create a
		// follower (so the next leader will roll it back as
		// leftover). See also #2.
		txn.Enter()
		defer txn.Exit()
		if errno := m.markStaleAndCreateSurrogateFollower(txn); errno != 0 {
			return errno
		}
		return errno
	}

	m.assertNoFollowerForDatabase(txn.Conn())

	if errno := m.apply(command.NewWalFrames(txn.ID(), frames)); errno != 0 {
		txn.Enter()
		defer txn.Exit()
		if errno := m.markStaleAndCreateSurrogateFollower(txn); errno != 0 {
			return errno
		}
		return errno
	}
	return 0
}

// Undo is the hook invoked by sqlite when a write transaction needs
// to be rolled back.
func (m *Methods) Undo(conn *sqlite3.SQLiteConn) sqlite3.ErrNo {
	m.logger.Printf("[DEBUG] dqlite: methods: undo hook for %p", conn)

	txn := m.lookupExistingLeaderForConn(conn)

	if m.checkIsStale(txn) {
		m.logger.Printf("[DEBUG] dqlite: methods: transaction %s is stale, skip undo", txn)
		return 0
	}

	if errno := m.checkIsLeader("undo"); errno != 0 {
		return errno
	}

	m.assertNoFollowerForDatabase(txn.Conn())

	if errno := m.apply(command.NewUndo(txn.ID())); errno != 0 {
		txn.Enter()
		defer txn.Exit()
		if errno := m.markStaleAndCreateSurrogateFollower(txn); errno != 0 {
			return errno
		}
		return errno
	}

	return 0
}

// End is the hook invoked by sqlite when a write transaction needs
// to be ended.
func (m *Methods) End(conn *sqlite3.SQLiteConn) sqlite3.ErrNo {
	m.logger.Printf("[DEBUG] dqlite: methods: end hook for %p", conn)

	txn := m.lookupExistingLeaderForConn(conn)

	if m.checkIsStale(txn) {
		m.logger.Printf("[DEBUG] dqlite: methods: transaction %s is stale, remove it and skip end", txn)
		m.dropStale(txn)
		return 0
	}

	if errno := m.checkIsLeader("end"); errno != 0 {
		return errno
	}

	m.assertNoFollowerForDatabase(txn.Conn())

	if errno := m.apply(command.NewEnd(txn.ID())); errno != 0 {
		txn.Enter()
		defer txn.Exit()
		if txn.State() == transaction.Ended {
			// The command still has managed to be applied
			// so let's just remove the transaction, it should not
			// pop up again.
			m.logger.Printf("[DEBUG] dqlite: methods: finished transaction that failed to apply end command %s", txn)
		} else {
			// Let's end the transaction ourselves, since we won't be
			// called back again as end replication hook by client hook.
			if err := txn.End(); err != nil {
				m.logger.Printf("[DEBUG] dqlite: methods: failed to end transaction after end command %s", txn)
				return sqlite3x.ErrReplication
			}
			// Create a surrogate follower, in case the raft command
			// eventually gets committed or in case we become leader
			// again and need to rollback.
			name := m.connections.NameByLeader(txn.Conn())
			if errno := m.createSurrogateFollower(name, txn.ID()); errno != 0 {
				return errno
			}
		}
		return errno
	}

	m.logger.Printf("[DEBUG] dqlite: methods: remove transaction %s", txn)

	return 0
}

// Checkpoint is the hook invoked by sqlite when the WAL file needs
// to be checkpointed.
func (m *Methods) Checkpoint(conn *sqlite3.SQLiteConn, mode sqlite3x.WalCheckpointMode, log *int, ckpt *int) sqlite3.ErrNo {
	m.logger.Printf("[DEBUG] dqlite: Start firing checkpoint hook, mode %d", mode)

	if errno := m.checkIsLeader("checkpoint"); errno != 0 {
		return errno
	}

	name := m.connections.NameByLeader(conn)

	if errno := m.apply(command.NewCheckpoint(name)); errno != 0 {
		return errno
	}

	return 0
}

// Wrapper around IsLeader, logging a message if we don't think we are
// the leader anymore. This is called by replication hooks, and
// calling logic will short-circuit if we return non-zero, i.e. if we
// were deposed between the time of the last successful raft command
// and now.
func (m *Methods) checkIsLeader(method string) sqlite3.ErrNo {
	if m.raft.State() != raft.Leader {
		m.logger.Printf("[INFO] dqlite: methods: attempted %s method on deposed leader\n", method)
		return sqlite3x.ErrNotLeader
	}
	return 0
}

// Wrapper around Txn.State()
func (m *Methods) checkIsStale(txn *transaction.Txn) bool {
	txn.Enter()
	defer txn.Exit()
	return txn.State() == transaction.Stale
}

// Wrapper around Registry.LeaderByConn, panic'ing if a matching
// leader transaction is found.
func (m *Methods) checkNoTxnForConn(conn *sqlite3.SQLiteConn) {
	// It should never happen that we have an ongoing write
	// transaction for this connection, since SQLite locking
	// system itself prevents it by serializing write
	// transactions. Still double checkit for sanity.
	if txn := m.transactions.GetByConn(conn); txn != nil {
		panic(fmt.Sprintf("[ERR] dqlite: methods: leader transaction %s is already open", txn))
	}
}

// Wrapper around Registry.LeaderByConn, logging an error if no
// matching leader transaction is found.
func (m *Methods) lookupExistingLeaderForConn(conn *sqlite3.SQLiteConn) *transaction.Txn {
	txn := m.transactions.GetByConn(conn)
	if txn == nil {
		if _, ok := m.stale[conn]; ok {
			txn = m.stale[conn]
		} else {
			panic(fmt.Sprintf("connection %p has no ongoing transactions", conn))
		}
	}
	return txn
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
func (m *Methods) ensureNoFollower(name string) sqlite3.ErrNo {
	txn := m.transactions.GetByConn(m.connections.Follower(name))

	if txn == nil {
		return 0
	}

	m.logger.Printf("[DEBUG] dqlite: methods: rolling back stale transaction %s", txn)

	if errno := m.apply(command.NewUndo(txn.ID())); errno != 0 {
		return errno
	}
	return m.apply(command.NewEnd(txn.ID()))
}

// Sanity check that there is no ongoing follower write transaction on
// this node for the given database.
func (m *Methods) assertNoFollowerForDatabase(conn *sqlite3.SQLiteConn) {
	name := m.connections.NameByLeader(conn)
	if txn := m.transactions.GetByConn(m.connections.Follower(name)); txn != nil {
		panic(fmt.Sprintf("detected follower write transaction %s", txn))
	}
}

// Apply the given command through raft.
func (m *Methods) apply(params command.Params) sqlite3.ErrNo {
	code := command.CodeOf(params)
	m.logger.Printf("[DEBUG] dqlite: methods: apply command %s", code)

	data, err := command.Marshal(params)
	if err != nil {
		m.logger.Printf("[ERR] dqlite: methods: failed to marshal %s: %s", code, err)
		return sqlite3x.ErrReplication
	}

	future := m.raft.Apply(data, m.applyTimeout)
	if err := future.Error(); err != nil {
		m.logger.Printf("[ERR] dqlite: methods: failed to apply %s command: %s", code, err)

		// If the node has lost leadership, we return a
		// dedicated error, so clients will typically retry
		// against the new leader.
		if err == raft.ErrNotLeader || err == raft.ErrLeadershipLost {
			return sqlite3x.ErrNotLeader
		}

		// Generic replication error.
		return sqlite3x.ErrReplication
	}

	m.logger.Printf("[DEBUG] dqlite: methods: applied command %s", code)
	return 0
}

func (m *Methods) markStale(txn *transaction.Txn) sqlite3.ErrNo {
	m.logger.Printf("[DEBUG] dqlite: methods: mark transaction %s as stale", txn)
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := txn.Stale(); err != nil {
		m.logger.Printf("[ERR] dqlite: methods: failed to mark transaction %s as stale: %s", txn, err)
		return sqlite3x.ErrReplication
	}

	// Add the transaction to the stale index.
	m.stale[txn.Conn()] = txn

	// Stale transactions instances are not involved anymore in
	// raft-applied commands, so remove them from the registry.
	m.transactions.Remove(txn.ID())

	return 0
}

func (m *Methods) dropStale(txn *transaction.Txn) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.stale, txn.Conn())
}

// Create a surrogate follower transaction with the given ID.
func (m *Methods) createSurrogateFollower(name string, id string) sqlite3.ErrNo {
	m.logger.Printf("[DEBUG] dqlite: methods: create surrogate follower transaction transaction on '%s' for %s", name, id)
	txn := m.transactions.AddFollower(m.connections.Follower(name), id)

	txn.Enter()
	defer txn.Exit()

	if err := txn.Begin(); err != nil {
		m.logger.Printf("[ERR] dqlite: methods: failed to begin surrogate follower %s: %s", txn, err)
		return sqlite3x.ErrReplication
	}

	return 0
}

// Mark the transaction as stale and create a surrogate follower transaction with its ID.
func (m *Methods) markStaleAndCreateSurrogateFollower(txn *transaction.Txn) sqlite3.ErrNo {
	if errno := m.markStale(txn); errno != 0 {
		return errno
	}
	name := m.connections.NameByLeader(txn.Conn())
	return m.createSurrogateFollower(name, txn.ID())
}
