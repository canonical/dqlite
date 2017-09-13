package dqlite

import (
	"database/sql/driver"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/CanonicalLtd/go-sqlite3x"
	"github.com/CanonicalLtd/raft-membership"
	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"

	"github.com/CanonicalLtd/dqlite/internal/command"
	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/replication"
	"github.com/CanonicalLtd/dqlite/transaction"
)

// Driver manages a node partecipating to a dqlite replicated cluster.
type Driver struct {
	logger       *log.Logger                 // Log messages go here
	raft         *raft.Raft                  // Underlying raft engine
	addr         string                      // Address of this node
	logs         *raftboltdb.BoltStore       // Store for raft logs
	peers        raft.PeerStore              // Store of raft peers, used by the engine
	membership   raftmembership.Changer      // API to join the raft cluster
	connections  *connection.Registry        // Connections registry
	methods      sqlite3x.ReplicationMethods // SQLite replication hooks
	mu           sync.Mutex                  // For serializing critical sections
	inititalized chan struct{}
}

// NewDriver creates a new node of a dqlite cluster, which also imlements the driver.Driver
// interface.
func NewDriver(config *Config) (*Driver, error) {
	if err := config.ensureDir(); err != nil {
		return nil, err
	}

	// Logging
	logger := config.Logger
	if logger == nil {
		logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	sqlite3x.LogConfig(logger)

	// Replication
	connections := connection.NewRegistry(config.Dir)
	transactions := transaction.NewRegistry()
	fsm := replication.NewFSM(logger, connections, transactions)

	// XXX TODO: find a way to perform mode checks on fresh follower
	//           connections that have not yet begun.
	transactions.SkipCheckReplicationMode(true)

	// Logs store
	logs, err := raftboltdb.New(raftboltdb.Options{
		Path:        filepath.Join(config.Dir, "raft.db"),
		BoltOptions: &bolt.Options{Timeout: config.SetupTimeout},
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create raft logs store")
	}

	// Peer store
	peers := raft.NewJSONPeers(config.Dir, config.Transport)

	// Notifications about leadership changes (acquired or lost). We use
	// a reasonably large buffer here to make sure we don't block raft.
	leadership := make(chan bool, 1024)

	// Raft
	raft, err := newRaft(config, fsm, logs, peers, leadership)
	if err != nil {
		return nil, err
	}

	methods := replication.NewMethods(logger, raft, connections, transactions)

	driver := &Driver{
		logger:       logger,
		raft:         raft,
		addr:         config.Transport.LocalAddr(),
		logs:         logs,
		peers:        peers,
		membership:   config.MembershipChanger,
		connections:  connections,
		methods:      methods,
		inititalized: make(chan struct{}, 0),
	}

	if config.MembershipRequests != nil {
		// This goroutine will normally terminate as soon as the
		// config.MembershipRequests channel gets closed, which will
		// happen as soon as raft it's shutdown, because this causes
		// the transport to be closed which in turn should close this
		// requests channel (like the rafthttp.Layer transport does).
		go raftmembership.HandleChangeRequests(raft, config.MembershipRequests)
	}

	return driver, nil
}

// IsLoneNode returns whether this node is a "lone" one, meaning that has no
// peers yet.
func (d *Driver) IsLoneNode() (bool, error) {
	return isLoneNode(d.peers, d.addr)
}

// Join a dqlite cluster by contacting the given address.
func (d *Driver) Join(address string, timeout time.Duration) error {
	if err := d.membership.Join(address, timeout); err != nil {
		return errors.Wrap(err, "failed to join dqlite cluster")
	}
	return nil
}

// Open starts a new connection to a SQLite database. If this node is not the
// leader, or the leader is unknown and this node doesn't get elected within a
// certain timeout (10 seconds by default), an error will be returned.
//
// dqlite adds the following query parameters to those used by SQLite and
// go-sqlite3:
//
//   _leadership_timeout=N
//     Maximum number of milliseconds to wait for this node to be elected
//     leader, in case there's currently no known leader. It defaults to 10000.
//
//   _initialize_timeout=N
//     Maximum number of milliseconds to wait for this node to initialize the
//     database by applying all needed raft log entries. It defaults to 30000.
//
// All other parameters are passed verbatim to go-slite3 and then SQLite.
func (d *Driver) Open(name string) (driver.Conn, error) {
	// Validate the given data source string.
	dsn, err := connection.NewDSN(name)
	if err != nil {
		return nil, errors.Wrap(err, "invalid DSN string")
	}

	// Poll until the leader is known, or the open timeout expires.
	//
	// XXX TODO: use an exponential backoff relative to the timeout? Or even
	//           figure out how to reliably be notified of leadership change?
	pollInterval := 50 * time.Millisecond
	for remaining := dsn.LeadershipTimeout; remaining >= 0; remaining -= pollInterval {
		if d.raft.Leader() != "" {
			break
		}
		// Sleep for the minimum value between pollInterval and remaining
		select {
		case <-time.After(pollInterval):
		case <-time.After(remaining):
		}
	}
	if d.raft.State() != raft.Leader {
		return nil, sqlite3.Error{Code: sqlite3x.ErrNotLeader}
	}

	// Wait until all pending logs are applied.
	err = d.raft.Barrier(dsn.InitializeTimeout).Error()
	if err != nil {
		if err == raft.ErrLeadershipLost {
			return nil, sqlite3.Error{Code: sqlite3x.ErrNotLeader}
		}
		return nil, errors.Wrap(err, "failed to apply pending logs")
	}

	// Acquire the lock and check if a follower connection is already open
	// for this filename, if not open one with the Open raft command.
	d.mu.Lock()
	if !d.connections.HasFollower(dsn.Filename) {
		data, err := command.Marshal(command.NewOpen(dsn.Filename))
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal raft open log")
		}
		if err := d.raft.Apply(data, dsn.LeadershipTimeout).Error(); err != nil {
			return nil, errors.Wrap(err, "failed to apply raft open log")
		}
	}
	d.mu.Unlock()

	sqliteConn, err := d.connections.OpenLeader(dsn, d.methods)
	if err != nil {
		return nil, err
	}

	conn := &Conn{
		connections: d.connections,
		sqliteConn:  sqliteConn,
	}

	return conn, err
}

// AutoCheckpoint sets how many frames the WAL file of a database
// should have before a replicated checkpoint is automatically
// attempted. A value of 1000 (the default if not set) is usually
// works for most application.
func (d *Driver) AutoCheckpoint(n int) {
	if n < 0 {
		panic("can't set a negative auto-checkpoint threshold")
	}
	d.connections.AutoCheckpoint(n)
}

// Shutdown the the node.
func (d *Driver) Shutdown() error {
	if err := d.raft.Shutdown().Error(); err != nil {
		return errors.Wrap(err, "failed to shutdown raft")
	}
	if err := d.logs.Close(); err != nil {
		return errors.Wrap(err, "failed to close logs store")
	}

	return nil
}

// Conn implements the sql.Conn interface.
type Conn struct {
	connections *connection.Registry
	sqliteConn  *sqlite3.SQLiteConn // Raw SQLite connection using the Go bindings
}

// Prepare returns a prepared statement, bound to this connection.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.sqliteConn.Prepare(query)
}

// Exec executes the given query.
func (c *Conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return c.sqliteConn.Exec(query, args)
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
func (c *Conn) Close() error {
	return c.connections.CloseLeader(c.sqliteConn)
}

// Begin starts and returns a new transaction.
func (c *Conn) Begin() (driver.Tx, error) {
	return c.sqliteConn.Begin()
}
