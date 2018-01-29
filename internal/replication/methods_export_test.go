package replication

import (
	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/internal/trace"
	"github.com/CanonicalLtd/dqlite/internal/transaction"
	"github.com/hashicorp/raft"
)

// Expose the internal connections registry so tests can register existing leader
// connections, that would normally be created by a Driver instance.
func (m *Methods) Connections() *connection.Registry {
	return m.connections
}

// Expose the internal raft instance so tests can change the leadership state
// of the node.
func (m *Methods) Raft() *raft.Raft {
	return m.raft
}

// Expose the internal transactions registry so tests can inspect transactions
// after invoking other Methods APIs.
func (m *Methods) Transactions() *transaction.Registry {
	return m.transactions
}

// Expose the internal tracers so tests can set a writer.
func (m *Methods) Tracers() *trace.Registry {
	return m.tracers
}
