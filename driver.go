package dqlite

import (
	"database/sql/driver"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dqlite/dqlite/connection"
	"github.com/dqlite/dqlite/replication"
	"github.com/dqlite/dqlite/transaction"
	"github.com/dqlite/go-sqlite3x"
	"github.com/dqlite/raft-membership"
	"github.com/hashicorp/raft"
	"github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
)

const (
	// Maximum time to wait for a single-mode peer to become
	// leader or a new follower peer to join the cluster.
	bootstrapTimeout = 30 * time.Second
)

// Driver manages a node partecipating to a DQLite replicated cluster.
type Driver struct {
	logger       *log.Logger                 // Log messages go here
	raft         *raft.Raft                  // Underlying raft engine
	connections  *connection.Registry        // Connections registry
	methods      sqlite3x.ReplicationMethods // SQLite replication hooks
	inititalized chan struct{}
}

// NewDriver creates a new node of a DQLite cluster, which also imlements the driver.Driver
// interface.
func NewDriver(config *Config, join string) (*Driver, error) {
	if err := config.ensureDir(); err != nil {
		return nil, err
	}

	// Logging
	logger := config.Logger
	if logger == nil {
		logger = NewLogger(os.Stdout, "INFO", log.LstdFlags)
	}
	sqlite3x.LogConfig(logger)

	// Replication
	connections := connection.NewRegistry(config.Dir)
	transactions := transaction.NewRegistry()
	fsm := replication.NewFSM(logger, connections, transactions)

	// XXX TODO: find a way to perform mode checks on fresh
	// follower connections that have not yet begun.
	transactions.SkipCheckReplicationMode(true)

	// XXX TODO: remove hard-coded database
	if err := addDatabase(connections); err != nil {
		return nil, err
	}

	// Raft
	raft, err := newRaft(config, join, fsm, logger)
	if err != nil {
		return nil, err
	}

	methods := replication.NewMethods(logger, raft, connections, transactions)

	driver := &Driver{
		logger:       logger,
		raft:         raft,
		connections:  connections,
		methods:      methods,
		inititalized: make(chan struct{}, 0),
	}

	if config.MembershipRequests != nil {
		go raftmembership.HandleChangeRequests(raft, config.MembershipRequests)
	}

	return driver, nil
}

// Open starts a new connection to a SQLite database.
func (d *Driver) Open(database string) (driver.Conn, error) {
	sqliteConn, err := d.connections.OpenLeader(database, d.methods)
	if err != nil {
		return nil, err
	}

	conn := &Conn{
		waitLeadership: d.WaitLeadership,
		connections:    d.connections,
		sqliteConn:     sqliteConn,
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

// WaitLeadership blocks until the node acquires raft leadership and
// any outstanding log is applied.
func (d *Driver) WaitLeadership() error {
	if d.isLeader() {
		return nil
	}
	for {
		if <-d.raft.LeaderCh() {
			err := d.raft.Barrier(120 * time.Second).Error()
			if err == raft.ErrLeadershipLost {
				continue
			}
			return errors.Wrap(err, "failed to apply pending logs")
		}
	}
}

// Shutdown the the node.
func (d *Driver) Shutdown() error {
	return d.raft.Shutdown().Error()
}

// Returns true if this peer currently think it's the leader.
func (d *Driver) isLeader() bool {
	return d.raft.State() == raft.Leader
}

// Conn implements the sql.Conn interface.
type Conn struct {
	waitLeadership func() error
	connections    *connection.Registry
	sqliteConn     *sqlite3.SQLiteConn // Raw SQLite connection using the Go bindings
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
	if err := c.waitLeadership(); err != nil {
		return nil, err
	}
	return c.sqliteConn.Begin()
}

func addDatabase(connections *connection.Registry) error {
	dir := connections.Dir()
	if err := filepath.Walk(dir, func(path string, f os.FileInfo, err error) error {
		if strings.HasPrefix(f.Name(), "test.db") {
			return os.Remove(path)
		}
		return nil
	}); err != nil {
		return errors.Wrap(err, "failed to purge existing database files")
	}

	dsn, _ := connection.NewDSN("test.db")
	connections.Add("test.db", dsn)
	if err := connections.OpenFollower("test.db"); err != nil {
		return errors.Wrap(err, "failed to open follower connection")
	}

	return nil
}
