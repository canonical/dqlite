package replication

import (
	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/internal/transaction"
)

// Expose the internal directory so tests can create new leader connections
// pointing to files in this directory.
func (f *FSM) Dir() string {
	return f.dir
}

// Expose the internal connections registry so tests can register existing leader
// connections, that would normally be created by a Driver instance.
func (f *FSM) Connections() *connection.Registry {
	return f.connections
}

// Expose the internal transactions registry so tests can create existing leader
// transactions, that would normally be created by a Methods instance.
func (f *FSM) Transactions() *transaction.Registry {
	return f.transactions
}

// Wait blocks until the command with the given index has been applied.
func (f *FSM) Wait(index uint64) {
	for {
		if <-f.indexes >= index {
			return
		}
	}
}

// Turn on indexes notifications.
func (f *FSM) WithNotify() {
	f.indexes = make(chan uint64, 128)
}
