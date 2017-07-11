package transaction

import (
	"fmt"
	"sync"

	"github.com/dqlite/go-sqlite3x"
	"github.com/mattn/go-sqlite3"
	"github.com/pborman/uuid"
)

// Registry is a dqlite node-level data stracture that tracks all
// write transactions happening in sqlite connections in leader
// replication mode and all write transactions happening in sqlite
// connections in follower replication.
type Registry struct {
	mu   sync.RWMutex    // Serialize access to internal state
	txns map[string]*Txn // Transactions by ID

	// Flag indicating whether to skip checking that a new
	// transaction started as leader or follower is actually
	// associated to a connection in leader or follower
	// replication mode. This value should always be true except
	// during some unit tests that can't perform the check since
	// the sqlite3_replication_mode API requres the btree lock to
	// be held, and there's no public API for acquiring it.
	skipCheckReplicationMode bool

	// Flag indicating whether transactions state transitions
	// should actually callback the relevant SQLite APIs. Some
	// tests need set this flag to true because there's no public
	// API to acquire the WAL read lock in leader connections.
	dryRun bool
}

// NewRegistry creates a new registry.
func NewRegistry() *Registry {
	return &Registry{
		txns: map[string]*Txn{},
	}
}

// AddLeader adds a new transaction to the registry. The given connection is
// assumed to be in leader replication mode. The new transaction will
// be assigned a unique ID.
func (r *Registry) AddLeader(conn *sqlite3.SQLiteConn) *Txn {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.checkReplicationMode(conn, sqlite3x.ReplicationModeLeader)

	id := uuid.NewRandom().String()
	return r.add(conn, id, true)
}

// AddFollower adds a new transaction to the registry. The given
// connection is assumed to be in follower replication mode. The new
// transaction will be associated with the given transaction ID, which
// should match the one of the leader transaction that initiated the
// write.
func (r *Registry) AddFollower(conn *sqlite3.SQLiteConn, id string) *Txn {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.checkReplicationMode(conn, sqlite3x.ReplicationModeFollower)

	return r.add(conn, id, false)
}

// GetByID returns the transaction with the given ID, if it exists.
func (r *Registry) GetByID(id string) *Txn {
	r.mu.Lock()
	defer r.mu.Unlock()

	txn, _ := r.txns[id]
	return txn
}

// GetByConn returns the transaction with the given connection, if it exists.
func (r *Registry) GetByConn(conn *sqlite3.SQLiteConn) *Txn {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.getByConn(conn)
}

// Remove deletes the transaction with the given ID.
func (r *Registry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.txns[id]; !ok {
		panic(fmt.Sprintf("attempt to remove unregistered transaction %s", id))
	}

	delete(r.txns, id)
}

// SkipCheckReplicationMode switches the flag controlling whether to
// check that connections are actually in the expected replication
// mode. This should only be used by tests.
func (r *Registry) SkipCheckReplicationMode(flag bool) {
	r.skipCheckReplicationMode = flag
}

// DryRun makes transactions only transition between states, without
// actually invoking the relevant SQLite APIs. This should only be
// used by tests.
func (r *Registry) DryRun() {
	r.dryRun = true
}

func (r *Registry) add(conn *sqlite3.SQLiteConn, id string, isLeader bool) *Txn {
	// Sanity check that the same connection hasn't been registered
	// already. Iterating is fast since there will always be few
	// write transactions active at given time.
	if txn := r.getByConn(conn); txn != nil {
		panic(fmt.Sprintf(
			"a transaction for this connection is already registered with ID %s", txn.ID()))
	}

	txn := newTxn(conn, id, isLeader, r.dryRun)
	r.txns[id] = txn
	return txn
}

// Check that the given connection is actually in the given
// replication mode.
func (r *Registry) checkReplicationMode(conn *sqlite3.SQLiteConn, mode sqlite3x.Replication) {
	if r.skipCheckReplicationMode {
		return
	}

	actualMode, err := sqlite3x.ReplicationMode(conn)
	if err != nil {
		panic(fmt.Sprintf("failed to check replication mode: %v", err))
	}
	if actualMode != mode {
		panic("connection not in expected replication mode")
	}
}

func (r *Registry) getByConn(conn *sqlite3.SQLiteConn) *Txn {
	for _, txn := range r.txns {
		if txn.Conn() == conn {
			return txn
		}
	}
	return nil
}
