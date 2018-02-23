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

package dqlite

import (
	"database/sql/driver"
	"io/ioutil"
	"log"
	"path/filepath"
	"sync"
	"time"

	"github.com/CanonicalLtd/go-sqlite3"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/internal/replication"
	"github.com/CanonicalLtd/dqlite/internal/trace"
)

// Driver manages a node partecipating to a dqlite replicated cluster.
type Driver struct {
	dir            string               // Database files live here.
	connections    *connection.Registry // Connections registry.
	tracers        *trace.Registry      // Tracers regitry.
	methods        *replication.Methods // SQLite replication hooks.
	logger         *log.Logger          // Logger to use.
	barrier        barrier              // Used to make sure the FSM is in sync with the logs.
	mu             sync.Mutex           // For serializing critical sections.
	autoCheckpoint int
}

// DriverConfig holds configuration options for a dqlite SQL Driver.
type DriverConfig struct {
	Logger         *log.Logger   // Logger to use to emit messages.
	BarrierTimeout time.Duration // Maximum amount of time to wait for the FSM to catch  up with the latest log.
	ApplyTimeout   time.Duration // Maximum amount of time to wait for a raft FSM command to be applied.
	AutoCheckpoint int
}

// NewDriver creates a new node of a dqlite cluster, which also implements the driver.Driver
// interface.
//
// The 'fsm' instance must be the same one that was passed to raft.NewRaft for
// creating the 'raft' instance.
func NewDriver(fsm raft.FSM, raft *raft.Raft, config DriverConfig) (*Driver, error) {
	fsmi, ok := fsm.(*replication.FSM)
	if !ok {
		panic("fsm is not a dqlite FSM")
	}
	if err := ensureDir(fsmi.Dir()); err != nil {
		return nil, err
	}

	if config.Logger == nil {
		config.Logger = log.New(ioutil.Discard, "", 0)
		//config.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	if config.BarrierTimeout == 0 {
		config.BarrierTimeout = time.Minute
	}
	if config.ApplyTimeout == 0 {
		config.ApplyTimeout = 10 * time.Second
	}

	//sqlite3.LogConfig(func(code int, message string) {
	//config.Logger.Printf("[ERR] %s (%d)", message, code)
	//})

	// Replication methods
	methods := replication.NewMethods(fsmi, raft)
	methods.ApplyTimeout(config.ApplyTimeout)

	barrier := func() error {
		if raft.State() != raftLeader {
			return sqlite3.ErrNotLeader
		}
		if fsmi.Index() == raft.LastIndex() {
			return nil
		}
		if err := raft.Barrier(config.BarrierTimeout).Error(); err != nil {
			if err == raftErrLeadershipLost {
				return sqlite3.ErrNotLeader
			}
			return errors.Wrap(err, "FSM out of sync")
		}
		return nil
	}

	driver := &Driver{
		dir:            fsmi.Dir(),
		connections:    fsmi.Connections(),
		tracers:        fsmi.Tracers(),
		logger:         config.Logger,
		barrier:        barrier,
		methods:        methods,
		autoCheckpoint: config.AutoCheckpoint,
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
func (d *Driver) Open(uri string) (driver.Conn, error) {
	// Validate the given data source string.
	filename, query, err := connection.ParseURI(uri)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid URI %s", uri)
	}

	uri = filepath.Join(d.dir, connection.EncodeURI(filename, query))
	sqliteConn, err := connection.OpenLeader(uri, d.methods, d.autoCheckpoint)
	if err != nil {
		return nil, err
	}

	d.connections.AddLeader(filename, sqliteConn)
	d.tracers.Add(replication.TracerName(d.connections, sqliteConn))

	conn := &Conn{
		barrier:     d.barrier,
		connections: d.connections,
		tracers:     d.tracers,
		sqliteConn:  sqliteConn,
	}

	return conn, err
}

// Conn implements the sql.Conn interface.
type Conn struct {
	barrier     barrier
	connections *connection.Registry
	tracers     *trace.Registry
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

// Exec may return ErrSkip.
//
// Deprecated: Drivers should implement ExecerContext instead (or additionally).
func (c *Conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if err := c.barrier(); err != nil {
		return nil, err
	}
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
	if c.sqliteConn == nil {
		return nil // Idempotency
	}

	c.tracers.Remove(replication.TracerName(c.connections, c.sqliteConn))
	c.connections.DelLeader(c.sqliteConn)

	if err := connection.CloseLeader(c.sqliteConn); err != nil {
		return err
	}
	c.sqliteConn = nil
	return nil
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

var raftErrLeadershipLost = raft.ErrLeadershipLost
