package dqlite

import (
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/CanonicalLtd/go-sqlite3x"
	"github.com/hashicorp/raft"
	"github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/internal/log"
	"github.com/CanonicalLtd/dqlite/internal/replication"
	"github.com/CanonicalLtd/dqlite/internal/transaction"
)

// Driver manages a node partecipating to a dqlite replicated cluster.
type Driver struct {
	logger         *log.Logger          // Log messages.
	dir            string               // Database files live here.
	autoCheckpoint int                  // WAL auto-checkpoint size threshold.
	connections    *connection.Registry // Connections registry.
	methods        *replication.Methods // SQLite replication hooks.
	barrier        barrier              // Used to make sure the FSM is in sync with the logs.
	mu             sync.Mutex           // For serializing critical sections.
}

// NewDriver creates a new node of a dqlite cluster, which also implements the driver.Driver
// interface.
func NewDriver(dir string, factory RaftFactory, options ...Option) (*Driver, error) {
	if err := ensureDir(dir); err != nil {
		return nil, err
	}

	o := newOptions()
	for _, option := range options {
		option(o)

	}

	logger := log.New(o.logFunc, log.Trace)
	sqlite3x.LogConfig(func(code int, message string) {
		o.logFunc(log.Error, fmt.Sprintf("[%d] %s", code, message))
	})

	// Replication
	connections := connection.NewRegistry()
	transactions := transaction.NewRegistry()
	fsm := replication.NewFSM(logger, dir, connections, transactions)

	// Raft
	raft, err := factory(fsm)
	if err != nil {
		return nil, errors.Wrap(err, "failed to start raft")
	}

	// Replication methods
	methods := replication.NewMethods(raft, logger, connections, transactions)
	methods.ApplyTimeout(o.applyTimeout)

	barrier := func() error {
		if raft.State() != raftLeader {
			return sqlite3x.ErrNotLeader
		}
		if fsm.Index() == raft.LastIndex() {
			return nil
		}
		if err := raft.Barrier(o.barrierTimeout).Error(); err != nil {
			return errors.Wrap(err, "FSM out of sync")
		}
		return nil
	}

	driver := &Driver{
		logger:      logger.Augment("driver"),
		dir:         dir,
		connections: connections,
		barrier:     barrier,
		methods:     methods,
	}

	return driver, nil
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

	uri := filepath.Join(d.dir, dsn.Encode())
	sqliteConn, err := connection.OpenLeader(uri, d.methods, d.autoCheckpoint)
	if err != nil {
		return nil, err
	}
	d.connections.AddLeader(dsn.Filename, sqliteConn)
	d.logger.Tracef("add leader %d", d.connections.Serial(sqliteConn))

	conn := &Conn{
		barrier:     d.barrier,
		connections: d.connections,
		sqliteConn:  sqliteConn,
	}

	return conn, err
}

// Conn implements the sql.Conn interface.
type Conn struct {
	barrier     barrier
	connections *connection.Registry
	sqliteConn  *sqlite3.SQLiteConn // Raw SQLite connection using the Go bindings
}

// Prepare returns a prepared statement, bound to this connection.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	if err := c.barrier(); err != nil {
		return nil, err
	}
	driverStmt, err := c.sqliteConn.Prepare(query)
	if err != nil {
		return nil, err
	}
	stmt := &Stmt{
		barrier:    c.barrier,
		sqliteStmt: driverStmt.(*sqlite3.SQLiteStmt),
	}
	return stmt, err
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
	if err := c.barrier(); err != nil {
		return nil, err
	}
	driverTx, err := c.sqliteConn.Begin()
	if err != nil {
		return nil, err
	}
	tx := &Tx{
		barrier:  c.barrier,
		sqliteTx: driverTx.(*sqlite3.SQLiteTx),
	}
	return tx, nil
}

// Tx is a transaction.
type Tx struct {
	barrier  barrier
	sqliteTx *sqlite3.SQLiteTx
}

// Commit the transaction.
func (tx *Tx) Commit() error {
	if err := tx.barrier(); err != nil {
		return err
	}
	return tx.sqliteTx.Commit()
}

// Rollback the transaction.
func (tx *Tx) Rollback() error {
	if err := tx.barrier(); err != nil {
		return err
	}
	return tx.sqliteTx.Rollback()
}

// Stmt is a prepared statement. It is bound to a Conn and not
// used by multiple goroutines concurrently.
type Stmt struct {
	barrier    barrier
	sqliteStmt *sqlite3.SQLiteStmt
}

// Close closes the statement.
func (s *Stmt) Close() error {
	return s.sqliteStmt.Close()
}

// NumInput returns the number of placeholder parameters.
func (s *Stmt) NumInput() int {
	return s.sqliteStmt.NumInput()
}

// Exec executes a query that doesn't return rows, such
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	if err := s.barrier(); err != nil {
		return nil, err
	}
	return s.sqliteStmt.Exec(args)
}

// Query executes a query that may return rows, such as a
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	if err := s.barrier(); err != nil {
		return nil, err
	}
	return s.sqliteStmt.Query(args)
}

// A function used to make sure that our FSM is up-to-date with the latest Raft
// index.
type barrier func() error

const raftLeader = raft.Leader
