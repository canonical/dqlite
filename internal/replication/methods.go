// Copyright 2017 Canonical Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package replication

import (
	"sync"
	"time"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/internal/protocol"
	"github.com/CanonicalLtd/dqlite/internal/trace"
	"github.com/CanonicalLtd/dqlite/internal/transaction"
	"github.com/CanonicalLtd/go-sqlite3"
	"github.com/hashicorp/raft"
)

// NewMethods returns a new Methods instance that can be used as callbacks API
// for raft-based SQLite replication of a single connection.
func NewMethods(fsm *FSM, raft *raft.Raft) *Methods {
	methods := &Methods{
		raft:         raft,
		connections:  fsm.Connections(),
		transactions: fsm.Transactions(),
		tracers:      fsm.Tracers(),
		applyTimeout: 10 * time.Second,
		stale:        map[*sqlite3.SQLiteConn]*transaction.Txn{},
	}
	methods.tracers.Add("methods")
	return methods
}

// Methods implements the SQLite replication C API using the sqlite3 bindings.
type Methods struct {
	raft         *raft.Raft            // Raft engine to use
	connections  *connection.Registry  // Registry of leader and follower connections
	transactions *transaction.Registry // Registry of leader and follower transactions
	tracers      *trace.Registry       // Registry of event tracers.
	applyTimeout time.Duration         // Timeout for applying raft commands
	mu           sync.RWMutex          // Serialize access to internal state

	// Index of stale transactions
	stale map[*sqlite3.SQLiteConn]*transaction.Txn
}

// ApplyTimeout sets the maximum amount of time to wait before giving
// up applying a raft command. The default is 10 seconds.
func (m *Methods) ApplyTimeout(timeout time.Duration) {
	m.applyTimeout = timeout
}

// Begin is the hook invoked by SQLite when a new write transaction is
// being started within a connection in leader replication mode on
// this node.
func (m *Methods) Begin(conn *sqlite3.SQLiteConn) sqlite3.ErrNo {
	tracer := m.tracer(conn, "begin")
	//tracer.Message("start")

	// Check if we're the leader.
	if m.raft.State() != raft.Leader {
		//tracer.Message("not leader")
		return sqlite3.ErrNotLeader
	}

	// It should never happen that we have an ongoing write transaction for
	// this connection, since SQLite locking system itself prevents it by
	// serializing write transactions. Still double check it for sanity.
	if txn := m.transactions.GetByConn(conn); txn != nil {
		tracer.Panic("connection has existing transaction %s", txn)
	}

	// Possibly open a follower for this database if it doesn't exist yet.
	if err := m.beginMaybeAddFollowerConn(tracer, conn); err != nil {
		return errno(err)
	}

	// Use the last raft index as transaction ID.
	txid := m.raft.LastIndex()

	//tracer = tracer.With(trace.Integer("txn", int64(txid)))
	//tracer.Message("register transaction")

	// Try to create a new transaction in the registry, this fails if there
	// is another leader connection with an ongoing transaction.
	filename := m.connections.FilenameOfLeader(conn)
	leaders := m.connections.Leaders(filename)
	txn := m.transactions.AddLeader(conn, txid, leaders)
	if txn == nil {
		//tracer.Message("a transaction is already in progress")
		return sqlite3.ErrBusy
	}

	// Rolloback any leftover follower transaction.
	if err := m.beginMaybeUndoFollowerTxn(tracer, conn); err != nil {
		// We failed to rollback a leftover follower, let's
		// mark the transaction as stale. It should then be
		// purged by client code.
		txn.Enter()
		defer txn.Exit()
		if err := m.markStale(tracer, conn, txn); err != nil {
			return errno(err)
		}
		return errno(err)
	}

	// Try to acquire the WAL write lock by beginning a write
	// transaction. This can fail with SQLITE_BUSY_SNAPSHOT if there's a
	// concurrent connection that has started a transaction but is not
	// quite done yet. We return ErrBusy and client code should retry. See
	// also sqlite3WalBeginWriteTransaction in the wal.c file of sqlite.
	if err := txn.Do(txn.Begin); err != nil {
		m.transactions.Remove(txn.ID())
		//tracer.Error("failed to begin WAL write transaction", err)
		if err, ok := err.(sqlite3.Error); ok {
			return err.Code
		}
		return sqlite3.ErrReplication
	}

	command := protocol.NewBegin(txn.ID(), filename)
	if err := m.apply(tracer, conn, command); err != nil {
		txn.Enter()
		defer txn.Exit()

		// Let's stale the transaction, so follow-up rollback hooks
		// triggered by client clean-up will no-op.
		if err := m.markStale(tracer, conn, txn); err != nil {
			return errno(err)
		}
		if txn.State() == transaction.Started {
			// This means that the raft still managed to succeed, a very
			// unlikely but in principle possible event with particular
			// bad timing of thread and goroutine scheduling. Create a
			// surrogate leftover follower so any rollback raft command
			// initiated by the next leader will find it.
			if err := m.addFollowerTxn(tracer, conn, filename, txn.ID()); err != nil {
				return errno(err)
			}
		}
		return errno(err)
	}

	//tracer.Message("done")

	return 0
}

// Acquire the lock and check if a follower connection is already open
// for this database, if not open one with the Open raft command.
func (m *Methods) beginMaybeAddFollowerConn(tracer *trace.Tracer, conn *sqlite3.SQLiteConn) error {
	filename := m.connections.FilenameOfLeader(conn)

	// We take a the lock for the entire duration of the method to avoid
	// the race of two leader connections trying to add a follower.
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.connections.HasFollower(filename) {
		//tracer.Message("open follower for %s", filename)
		return m.apply(tracer, conn, protocol.NewOpen(filename))
	}
	return nil
}

// This method ensures that there is no follower write transactions happening
// on this node for the given database filename. If one is found the method
// will try to apply undo and end commands to clear it up.
//
// It's necessary to run this safeguard before trying to apply a begin command,
// because. If we do find an ongoing follower write transaction for the same
// filename associated to a leader connection on this node, it means that the
// leader node that started the transaction got deposed (this node is now the
// new leader), and it's not eligeable to complete it. So we need to interrupt
// the transaction by applying undo and end commands, to make sure all nodes
// (including this one) rollback.
//
// TODO: if the transaction was committed and the leader died just after, we
// should detect that and report to clients that it was all good.
func (m *Methods) beginMaybeUndoFollowerTxn(tracer *trace.Tracer, conn *sqlite3.SQLiteConn) error {
	filename := m.connections.FilenameOfLeader(conn)

	txn := m.transactions.GetByConn(m.connections.Follower(filename))
	if txn == nil {
		return nil
	}

	//tracer.Message("undo stale transaction %d", txn.ID())

	if err := m.apply(tracer, conn, protocol.NewUndo(txn.ID())); err != nil {
		return err
	}
	if err := m.apply(tracer, conn, protocol.NewEnd(txn.ID())); err != nil {
		return err
	}

	return nil
}

// WalFrames is the hook invoked by sqlite when new frames need to be
// flushed to the write-ahead log.
func (m *Methods) WalFrames(conn *sqlite3.SQLiteConn, frames *sqlite3.ReplicationWalFramesParams) sqlite3.ErrNo {
	tracer := m.tracer(conn, "wal frames")
	//tracer.Message("start")

	txn := m.getTxnByConn(tracer, conn)

	// Check if we're the leader.
	if m.raft.State() != raft.Leader {
		// If we have lost leadership we're in a state where the
		// transaction began on this node and a quorum of
		// followers. When we return an error, SQLite will try to
		// automatically rollback the WAL: we need to mark the
		// transaction as stale (so the follow-up undo and end command
		// will succeed as no-op) and create a follower (so the next
		// leader will roll it back as leftover). See also #2.
		//tracer.Message("not leader")
		txn.Enter()
		defer txn.Exit()
		if err := m.markStaleAndAddFollowerTxn(tracer, conn, txn); err != nil {
			return errno(err)
		}
		return sqlite3.ErrNotLeader
	}

	m.checkNoFollowerTxnExists(tracer, txn.Conn())

	command := protocol.NewWalFrames(txn.ID(), frames)
	if err := m.apply(tracer, conn, command); err != nil {
		// If we have lost leadership we're in a state where the
		// transaction began on this node and a quorum of
		// followers. When we return an error, SQLite will try to
		// automatically rollback the WAL: we need to mark the
		// transaction as stale (so the follow-up undo and end command
		// will succeed as no-op) and create a follower (so the next
		// leader will roll it back as leftover). See also #2.
		txn.Enter()
		defer txn.Exit()
		if err := m.markStaleAndAddFollowerTxn(tracer, conn, txn); err != nil {
			return errno(err)
		}
		return errno(err)
	}

	//tracer.Message("done")

	return 0
}

// Undo is the hook invoked by sqlite when a write transaction needs
// to be rolled back.
func (m *Methods) Undo(conn *sqlite3.SQLiteConn) sqlite3.ErrNo {
	tracer := m.tracer(conn, "undo")
	//tracer.Message("start")

	txn := m.getTxnByConn(tracer, conn)

	// Check if the txn is stale.
	if txn.IsStale() {
		// This transaction is stale, so this Undo hook is being invoked
		// by SQLite as part of the rollback sequence after a failed
		// WalFrames. We just no-op.
		//tracer.Message("transaction is stale, no-op")
		return 0
	}

	// Check if we're the leader.
	if m.raft.State() != raft.Leader {
		//tracer.Message("not leader")
		return sqlite3.ErrNotLeader
	}

	m.checkNoFollowerTxnExists(tracer, txn.Conn())

	command := protocol.NewUndo(txn.ID())
	if err := m.apply(tracer, conn, command); err != nil {
		txn.Enter()
		defer txn.Exit()
		if err := m.markStaleAndAddFollowerTxn(tracer, conn, txn); err != nil {
			return errno(err)
		}
		return errno(err)
	}

	//tracer.Message("done")

	return 0
}

// End is the hook invoked by sqlite when a write transaction needs
// to be ended.
func (m *Methods) End(conn *sqlite3.SQLiteConn) sqlite3.ErrNo {
	tracer := m.tracer(conn, "end")
	//tracer.Message("start")

	txn := m.getTxnByConn(tracer, conn)

	// Check if the txn is stale.
	if txn.IsStale() {
		// This transaction is stale, so this End hook is being invoked
		// by SQLite as part of the rollback sequence after a failed
		// WalFrames. We just no-op and remove the transaction from the
		// stale index, since it's should be gone forever.
		//tracer.Message("transaction is stale, no-op and drop it")

		m.mu.Lock()
		defer m.mu.Unlock()
		delete(m.stale, txn.Conn())
		return 0
	}

	// Check if we're the leader.
	if m.raft.State() != raft.Leader {
		// Since we are not able to commit an End command, we need to
		// rollback the transaction here, to release the exclusive WAL
		// lock. The transaction will be marked as stale and will be
		// rolled back by the next leader with an Undo command.
		//tracer.Message("not leader")

		// Failing to release the WAL write lock is fatal,
		// since SQLite assumes this can never fail.
		if err := txn.Do(txn.Undo); err != nil {
			tracer.Panic("failed to end undo transaction upon lost leadership: %v", err)
		}
		if err := txn.Do(txn.Stale); err != nil {
			tracer.Panic("failed to end WAL transaction upon lost leadership: %v", err)
		}

		return sqlite3.ErrNotLeader
	}

	m.checkNoFollowerTxnExists(tracer, txn.Conn())

	command := protocol.NewEnd(txn.ID())
	if err := m.apply(tracer, conn, command); err != nil {
		txn.Enter()
		defer txn.Exit()
		if txn.State() == transaction.Ended {
			// The command still has managed to be applied
			// so let's just remove the transaction, it should not
			// pop up again.
			//tracer.Message("finished transaction that failed to apply end command")
		} else {
			// Let's end the transaction ourselves, since we won't be
			// called back again as end replication hook by client hook.
			if err := txn.End(); err != nil {
				//tracer.Error("failed to finish txn after apply failure", err)
				return sqlite3.ErrReplication
			}
			// Create a surrogate follower, in case the raft command
			// eventually gets committed or in case we become leader
			// again and need to rollback.
			name := m.connections.FilenameOfLeader(txn.Conn())
			if err := m.addFollowerTxn(tracer, conn, name, txn.ID()); err != nil {
				return errno(err)
			}
		}
		return errno(err)
	}

	//tracer.Message("done")

	return 0
}

// Checkpoint is the hook invoked by sqlite when the WAL file needs
// to be checkpointed.
func (m *Methods) Checkpoint(conn *sqlite3.SQLiteConn, mode sqlite3.WalCheckpointMode, log *int, ckpt *int) sqlite3.ErrNo {
	tracer := m.tracer(conn, "checkpoint")
	//tracer.Message("start")

	// Check if we're the leader.
	if m.raft.State() != raft.Leader {
		return sqlite3.ErrNotLeader
	}

	name := m.connections.FilenameOfLeader(conn)

	command := protocol.NewCheckpoint(name)
	if err := m.apply(tracer, conn, command); err != nil {
		return errno(err)
	}

	return 0
}

// Return the tracer for the given connection. It must have been registered with TracerAdd().
func (m *Methods) tracer(conn *sqlite3.SQLiteConn, hook string) *trace.Tracer {
	tracer := m.tracers.Get(TracerName(m.connections, conn))
	return tracer.With(trace.String("hook", hook))
}

// Wrapper around Registry.GetByConn, panic'ing if no matching transaction is
// found.
func (m *Methods) getTxnByConn(tracer *trace.Tracer, conn *sqlite3.SQLiteConn) *transaction.Txn {
	txn := m.transactions.GetByConn(conn)
	if txn == nil {
		if _, ok := m.stale[conn]; ok {
			txn = m.stale[conn]
			//tracer.Message("found stale txn")
		} else {
			panic("no ongoing transactions")
		}
	}
	return txn
}

// Sanity check that there is no ongoing follower write transaction on
// this node for the given database.
func (m *Methods) checkNoFollowerTxnExists(tracer *trace.Tracer, conn *sqlite3.SQLiteConn) {
	name := m.connections.FilenameOfLeader(conn)
	if txn := m.transactions.GetByConn(m.connections.Follower(name)); txn != nil {
		tracer.Panic("detected follower write transaction %s", txn)
	}
}

func (m *Methods) markStale(tracer *trace.Tracer, conn *sqlite3.SQLiteConn, txn *transaction.Txn) error {
	//tracer.Message("stale transaction %s", txn)

	m.mu.Lock()
	defer m.mu.Unlock()

	if err := txn.Stale(); err != nil {
		//tracer.Error("transition to stale state failed", err)
		return err
	}

	// Add the transaction to the stale index.
	m.stale[txn.Conn()] = txn

	// Stale transactions instances are not involved anymore in
	// raft-applied commands, so remove them from the registry.
	//tracer.Message("remove stale transaction from registry %s", txn)
	m.transactions.Remove(txn.ID())

	return nil
}

// Mark the transaction as stale and create a surrogate follower transaction
// with its ID.
func (m *Methods) markStaleAndAddFollowerTxn(tracer *trace.Tracer, conn *sqlite3.SQLiteConn, txn *transaction.Txn) error {
	if err := m.markStale(tracer, conn, txn); err != nil {
		return err
	}

	name := m.connections.FilenameOfLeader(txn.Conn())
	return m.addFollowerTxn(tracer, conn, name, txn.ID())
}

// Create a surrogate follower transaction with the given ID.
func (m *Methods) addFollowerTxn(tracer *trace.Tracer, conn *sqlite3.SQLiteConn, name string, id uint64) error {
	//tracer.Message("create surrogate follower transaction")
	txn := m.transactions.AddFollower(m.connections.Follower(name), id)

	txn.Enter()
	defer txn.Exit()

	if err := txn.Begin(); err != nil {
		//tracer.Error("transition to begin state failed", err)
		return err
	}

	return nil
}

// Apply the given command through raft.
func (m *Methods) apply(tracer *trace.Tracer, conn *sqlite3.SQLiteConn, cmd *protocol.Command) error {
	//tracer = tracer.With(trace.String("cmd", cmd.Name()))
	//tracer.Message("apply start")

	data, err := protocol.MarshalCommand(cmd)
	if err != nil {
		return err
	}

	future := m.raft.Apply(data, m.applyTimeout)
	if err := future.Error(); err != nil {
		//tracer.Error("apply error", err)

		// If the node has lost leadership, we return a
		// dedicated error, so clients will typically retry
		// against the new leader.
		if err == raft.ErrNotLeader || err == raft.ErrLeadershipLost {
			return sqlite3.ErrNotLeader
		}

		// Generic replication error.
		return sqlite3.ErrReplication
	}

	//tracer.Message("apply done")
	return nil
}

// Convert a Go error into a SQLite error number.
func errno(err error) sqlite3.ErrNo {
	switch e := err.(type) {
	case sqlite3.ErrNo:
		return e
	default:
		return sqlite3.ErrReplication // Generic replication error.
	}
}
