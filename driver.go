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

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/internal/replication"
	"github.com/CanonicalLtd/dqlite/internal/transaction"
)

// Driver manages a node partecipating to a dqlite replicated cluster.
type Driver struct {
	logger       *log.Logger            // Log messages go here
	raft         *raft.Raft             // Underlying raft engine
	addr         string                 // Address of this node
	logs         *raftboltdb.BoltStore  // Store for raft logs
	peers        raft.PeerStore         // Store of raft peers, used by the engine
	membership   raftmembership.Changer // API to join the raft cluster
	connections  *connection.Registry   // Connections registry
	methods      *replication.Methods   // SQLite replication hooks
	mu           sync.Mutex             // For serializing critical sections
	inititalized chan struct{}
}

// NewDriver creates a new node of a dqlite cluster, which also implements the driver.Driver
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
	connections := connection.NewRegistryLegacy(config.Dir)
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

// Open starts a new connection to a SQLite database.
//
// The given name must be a pure file name without any directory segment,
// dqlite will connect to a database with that name in its data directory.
//
// Query parameters are always valid except for "mode=memory".
//
// If this node is not the leader, or the leader is unknown an ErrNotLeader
// error is returned.
func (d *Driver) Open(name string) (driver.Conn, error) {
	// Validate the given data source string.
	dsn, err := connection.NewDSN(name)
	if err != nil {
		return nil, errors.Wrap(err, "invalid DSN string")
	}

	// TODO: this currently set the timeout driver-wide
	d.methods.ApplyTimeout(dsn.LeadershipTimeout)

	sqliteConn, err := connection.OpenLeader(filepath.Join(d.connections.Dir(), dsn.Encode()), d.methods, 1000)
	if err != nil {
		return nil, err
	}
	d.connections.AddLeader(dsn.Filename, sqliteConn)

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
	connections       *connection.Registry
	sqliteConn        *sqlite3.SQLiteConn // Raw SQLite connection using the Go bindings
	leadershipTimeout time.Duration
	barrierTimeout    time.Duration
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
	c.connections.DelLeader(c.sqliteConn)
	return connection.CloseLeader(c.sqliteConn)
}

// Begin starts and returns a new transaction.
func (c *Conn) Begin() (driver.Tx, error) {
	return c.sqliteConn.Begin()
}
