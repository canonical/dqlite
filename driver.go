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
	"context"
	"database/sql/driver"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/CanonicalLtd/go-sqlite3"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/dqlite/internal/protocol"
	"github.com/CanonicalLtd/dqlite/internal/registry"
	"github.com/CanonicalLtd/dqlite/internal/replication"
	"github.com/CanonicalLtd/dqlite/internal/transaction"
)

// Driver manages a node partecipating to a dqlite replicated cluster.
type Driver struct {
	registry            *registry.Registry   // Internal registry.
	raft                *raft.Raft           // Raft instance
	methods             *replication.Methods // SQLite replication hooks.
	logger              *log.Logger          // Logger to use.
	checkpointThreshold uint64               // Minimum number of frames before checkpointing
	mu                  sync.RWMutex         // For serializing checkpoints (TODO: make this per-database)

}

// DriverConfig holds configuration options for a dqlite SQL Driver.
type DriverConfig struct {
	Logger              *log.Logger   // Logger to use to emit messages.
	BarrierTimeout      time.Duration // Maximum amount of time to wait for the FSM to catch up with logs.
	ApplyTimeout        time.Duration // Maximum amount of time to wait for a raft FSM command to be applied.
	CheckpointThreshold uint64        // Minimum number of WAL frames before performing a checkpoint.
}

// NewDriver creates a new node of a dqlite cluster, which also implements the driver.Driver
// interface.
//
// The Registry instance must be the same one that was passed to NewFSM to
// build the raft.FSM which in turn got passed to raft.NewRaft for creating the
// raft instance.
func NewDriver(r *Registry, raft *raft.Raft, config DriverConfig) (*Driver, error) {
	registry := (*registry.Registry)(r)
	if err := ensureDir(registry.Dir()); err != nil {
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
	if config.CheckpointThreshold == 0 {
		config.CheckpointThreshold = 1000 // Same as SQLite default
	}

	//sqlite3.LogConfig(func(code int, message string) {
	//config.Logger.Printf("[ERR] %s (%d)", message, code)
	//})

	// Replication methods
	methods := replication.NewMethods(registry, raft)
	methods.ApplyTimeout(config.ApplyTimeout)

	driver := &Driver{
		registry:            registry,
		raft:                raft,
		logger:              config.Logger,
		methods:             methods,
		checkpointThreshold: config.CheckpointThreshold,
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

	d.registry.Lock()
	defer d.registry.Unlock()

	uri = filepath.Join(d.registry.Dir(), connection.EncodeURI(filename, query))
	sqliteConn, err := connection.OpenLeader(uri, d.methods)
	if err != nil {
		return nil, err
	}

	conn := &Conn{
		driver:     d,
		registry:   d.registry,
		filename:   filename,
		raft:       d.raft,
		sqliteConn: sqliteConn,
	}

	if n := len(d.registry.ConnLeaders(filename)); n == 0 {
		// This is the first connection against this database, let's
		// spawn a checkpointing goroutine.
		ctx, cancel := context.WithCancel(context.Background())
		conn.cancelCheckpoint = cancel
		timeout := 10 * time.Second // TODO: make this configurable
		go func() {
			for {
				timer := time.After(timeout)
				select {
				case <-ctx.Done():
					return
				case <-timer:
					conn.checkpoint()
				}
			}
		}()
	}

	d.registry.ConnLeaderAdd(filename, sqliteConn)

	return conn, err
}

// Leader returns the address of the current raft leader, if any.
func (d *Driver) Leader() string {
	return string(d.raft.Leader())
}

// Servers returns the addresses of all current raft servers. It returns an
// error if this server is not the leader.
func (d *Driver) Servers() ([]string, error) {
	if d.raft.State() != raft.Leader {
		// TODO: convert this to grpcsql/cluster.ErrDriverNotLeader
		return nil, raft.ErrNotLeader
	}

	future := d.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}
	configuration := future.Configuration()

	servers := make([]string, len(configuration.Servers))
	for i, server := range configuration.Servers {
		// TODO: add support for raft.ServerAddressProvider
		servers[i] = string(server.Address)
	}

	return servers, nil
}

// Recover tries to recover a transaction that errored because leadership was
// lost at commit time.
//
// If this driver is the newly elected leader and a quorum was reached for the
// lost commit command, recovering will succeed and the transaction can safely
// be considered committed.
func (d *Driver) Recover(token uint64) error {
	if d.raft.State() != raft.Leader {
		return raft.ErrNotLeader
	}

	// Apply a barrier to be sure we caught up with logs.
	timeout := 10 * time.Second // TODO: make this configurable?
	if err := d.raft.Barrier(timeout).Error(); err != nil {
		return err
	}

	d.registry.Lock()
	defer d.registry.Unlock()

	if !d.registry.TxnCommittedFind(token) {
		return fmt.Errorf("not found")
	}

	return nil
}

// Conn implements the sql.Conn interface.
type Conn struct {
	driver     *Driver
	registry   *registry.Registry
	raft       *raft.Raft
	filename   string
	sqliteConn *sqlite3.SQLiteConn // Raw SQLite connection using the Go bindings

	cancelCheckpoint context.CancelFunc
}

func (c *Conn) barrier() error {
	if c.raft.State() != raftLeader {
		return sqlite3.Error{
			Code:         sqlite3.ErrIoErr,
			ExtendedCode: sqlite3.ErrIoErrNotLeader,
		}
	}
	c.registry.Lock()
	index := c.registry.Index()
	c.registry.Unlock()
	if index == c.raft.LastIndex() {
		return nil
	}
	timeout := time.Minute // TODO: make this configurable
	if err := c.raft.Barrier(timeout).Error(); err != nil {
		if err == raftErrLeadershipLost {
			return sqlite3.Error{
				Code:         sqlite3.ErrIoErr,
				ExtendedCode: sqlite3.ErrIoErrNotLeader,
			}
		}
		return errors.Wrap(err, "FSM out of sync")
	}
	// XXX TODO: figure out how to tell the difference between
	//           an FSM command log index and an internal raft log
	//           index.
	// Spin a bit to make sure the FSM actually applied the last
	// log.
	// timeout := time.After(time.Second) // Make this timeout configurable?
	// for {
	// 	select {
	// 	case <-timeout:
	// 		return fmt.Errorf("FSM out of sync")
	// 	default:
	// 	}
	// 	registry.Lock()
	// 	index := registry.Index()
	// 	registry.Unlock()
	// 	if index == raft.LastIndex() {
	// 		return nil
	// 	}
	// }
	return nil

}

func (c *Conn) checkpoint() error {
	if c.raft.State() != raftLeader {
		return sqlite3.Error{
			Code:         sqlite3.ErrIoErr,
			ExtendedCode: sqlite3.ErrIoErrNotLeader,
		}
	}

	c.driver.mu.Lock()
	defer c.driver.mu.Unlock()

	// Read the current size of the WAL.
	stat, err := os.Stat(filepath.Join(c.driver.registry.Dir(), c.filename+"-wal"))
	if err != nil {
		return nil
	}

	c.driver.registry.Lock()
	c.driver.registry.FramesReset()
	c.driver.registry.FramesIncrease(uint64(stat.Size()) / 4096) // TODO: frame size should not be hard-coded
	frames := c.driver.registry.Frames()
	c.driver.registry.Unlock()

	// Check if we crossed the checkpoint threshold
	if frames < c.driver.checkpointThreshold {
		return nil
	}

	cmd := protocol.NewCheckpoint(c.filename)
	data, err := protocol.MarshalCommand(cmd)
	if err != nil {
		return err
	}
	timeout := time.Second // TODO make this configurable
	err = c.raft.Apply(data, timeout).Error()
	if err != nil {
		return err
	}
	return nil
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
		conn:       c,
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

	c.registry.Lock()
	defer c.registry.Unlock()

	// Invalidate any pending transaction
	if txn := c.registry.TxnByConn(c.sqliteConn); txn != nil {
		if txn.State() == transaction.Pending {
			// Followers don't know about this transaction, let's
			// just purge it.
			c.registry.TxnDel(txn.ID())
		} else {
			// We need to create a surrogate follower, in order to
			// undo this transaction across all nodes.
			txn = c.registry.TxnFollowerSurrogate(txn)
			txn.Frames(true, &sqlite3.ReplicationFramesParams{IsCommit: 0})
		}
	}

	c.registry.ConnLeaderDel(c.sqliteConn)

	if err := c.sqliteConn.Close(); err != nil {
		return err
	}
	c.sqliteConn = nil

	return nil
}

// Begin starts and returns a new transaction.
func (c *Conn) Begin() (driver.Tx, error) {
	c.driver.mu.RLock()
	if err := c.barrier(); err != nil {
		c.driver.mu.RUnlock()
		return nil, err
	}

	driverTx, err := c.sqliteConn.Begin()
	if err != nil {
		c.driver.mu.RUnlock()
		return nil, err
	}
	tx := &Tx{
		conn:     c,
		sqliteTx: driverTx.(*sqlite3.SQLiteTx),
	}

	return tx, nil
}

// Tx is a transaction.
type Tx struct {
	conn     *Conn
	sqliteTx *sqlite3.SQLiteTx
}

// Commit the transaction.
func (tx *Tx) Commit() error {
	defer tx.conn.driver.mu.RUnlock()
	if err := tx.conn.barrier(); err != nil {
		return err
	}
	return tx.sqliteTx.Commit()
}

// Rollback the transaction.
func (tx *Tx) Rollback() error {
	defer tx.conn.driver.mu.RUnlock()
	if err := tx.conn.barrier(); err != nil {
		return err
	}
	return tx.sqliteTx.Rollback()
}

// Token returns the internal ID for this transaction, that can be passed to
// driver.Recover() in case the commit fails because of lost leadership.
func (tx *Tx) Token() uint64 {
	tx.conn.registry.Lock()
	defer tx.conn.registry.Unlock()

	return tx.conn.registry.TxnLastID(tx.conn.sqliteConn)
}

// Stmt is a prepared statement. It is bound to a Conn and not
// used by multiple goroutines concurrently.
type Stmt struct {
	conn       *Conn
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
	if err := s.conn.barrier(); err != nil {
		return nil, err
	}
	return s.sqliteStmt.Exec(args)
}

// Query executes a query that may return rows, such as a
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	if err := s.conn.barrier(); err != nil {
		return nil, err
	}
	return s.sqliteStmt.Query(args)
}

// A function used to make sure that our FSM is up-to-date with the latest Raft
// index.
type barrier func() error

const raftLeader = raft.Leader

var raftErrLeadershipLost = raft.ErrLeadershipLost
