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

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/CanonicalLtd/dqlite"
	"github.com/CanonicalLtd/go-sqlite3"
	"github.com/CanonicalLtd/raft-http"
	"github.com/CanonicalLtd/raft-membership"
	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/pkg/errors"
)

var data = flag.String("data", "", "directory to save SQLite databases and Raft data in")
var join = flag.String("join", "", "address of an existing node in the cluster (or none for leader)")
var addr = flag.String("addr", "127.0.0.1:9990", "raft HTTP gateway other address")
var debug = flag.Bool("debug", false, "enable debug logging")
var forever = flag.Bool("forever", false, "run forever, without crashing at a random time")

func main() {
	flag.Parse()

	if *data == "" {
		fmt.Fprintf(os.Stderr, "error: the -data option is required (see -help)\n")
		os.Exit(2)
	}

	node := newNode(*data, *addr, *join, *debug)
	defer node.Stop()
	if err := node.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "error: failed to start node: %v\n", err)
		os.Exit(2)
	}

	// Insert data into the DB, either forever for a random time between 5
	// and 25 (depending on the -forever command line switch).
	if *forever {
		node.InsertForever()
	} else {
		go node.InsertForever()
		randomSleep(5, 25)
	}
}

type node struct {
	logger   *log.Logger
	addr     string
	join     string
	debug    bool
	listener net.Listener
	handler  *rafthttp.Handler
	dir      string
	timeout  time.Duration
	notifyCh <-chan bool
	raft     *raft.Raft
	driver   *dqlite.Driver
	db       *sql.DB
}

func newNode(dir string, addr string, join string, debug bool) *node {
	return &node{
		logger:  log.New(os.Stdout, addr+" ", log.Ltime|log.Lmicroseconds),
		addr:    addr,
		dir:     dir,
		join:    join,
		timeout: 10 * time.Second,
	}
}

func (n *node) Driver() *dqlite.Driver {
	return n.driver
}

func (n *node) Addr() string {
	return n.listener.Addr().String()
}

func (n *node) Start() (err error) {
	if n.listener, err = net.Listen("tcp", n.addr); err != nil {
		return errors.Wrap(err, "failed to listen to random port")
	}
	n.handler = rafthttp.NewHandler()
	go http.Serve(n.listener, n.handler)

	config := dqlite.DriverConfig{
		//dqlite.LogFunc(logFunc),
		BarrierTimeout:      n.timeout,
		CheckpointThreshold: 100,
	}
	registry := dqlite.NewRegistry(n.dir)
	fsm := dqlite.NewFSM(registry)
	raft, err := n.makeRaft(fsm)
	if err != nil {
		return errors.Wrap(err, "failed to start raft")
	}
	if n.driver, err = dqlite.NewDriver(registry, raft, config); err != nil {
		return errors.Wrap(err, "failed to create dqlite driver")
	}
	sql.Register("dqlite", n.driver)

	// Open a database backed by DQLite, and use at most one connection.
	if n.db, err = sql.Open("dqlite", "test.db"); err != nil {
		return errors.Wrap(err, "failed to open database")
	}
	n.db.SetMaxOpenConns(1)
	n.db.SetMaxIdleConns(1)

	return nil
}

func (n *node) Stop() {
	if n.listener != nil {
		if err := n.listener.Close(); err != nil {
			n.logger.Fatalf("failed to close listener: %v", err)
		}
	}
	if n.raft != nil {
		if err := n.raft.Shutdown().Error(); err != nil {
			n.logger.Fatalf("failed to shutdown raft: %v", err)
		}
	}
	n.logger.Printf("[INFO] demo: exit")
}

// Use the out dqlite database to insert new rows to a single-column table
// until we fail or exit.
func (n *node) InsertForever() {
	for {
		if n.raft.State() != raft.Leader {
			if !<-n.notifyCh {
				continue
			}
		}

		// Start the transaction.
		tx, err := n.db.Begin()
		if err != nil {
			n.logger.Fatalf("[FATAL] demo: begin failed: %v", err)
		}

		// Ensure our test table is there.
		if _, err := tx.Exec("CREATE TABLE IF NOT EXISTS test (n INT)"); err != nil {
			handleTxError(n.logger, tx, err, false)
			continue
		}

		// Insert a batch of rows.
		offset := insertedCount(n.logger, tx)
		failed := false
		for i := 0; i < 50; i++ {
			if _, err := tx.Exec("INSERT INTO test (n) VALUES(?)", i+offset); err != nil {
				handleTxError(n.logger, tx, err, false)
				failed = true
				break
			}
			// Sleep a tiny bit between each insert to make it more likely that we get
			// terminated in the middle of a transaction.
			randomSleep(0.010, 0.025)
		}
		if failed {
			// We're not the leader, wait a bit and try again
			randomSleep(0.250, 0.500)
			continue
		}

		reportProgress(n.logger, tx)

		// Commit
		if err := tx.Commit(); err != nil {
			handleTxError(n.logger, tx, err, true)
			continue
		}

		// Sleep a little bit to simulate a pause in the service's
		// activity and give a chance to snapshot.
		randomSleep(0.25, 0.5)
	}
}

func (n *node) makeRaft(fsm raft.FSM) (*raft.Raft, error) {
	logger := n.logger
	if !n.debug {
		logger = log.New(ioutil.Discard, "", 0)
	}

	// Create a raft configuration with default values.
	config := raft.DefaultConfig()
	config.SnapshotInterval = 12 * time.Second
	config.SnapshotThreshold = 512
	config.Logger = logger
	config.LocalID = raft.ServerID(n.Addr())

	// Setup a notify channel to be notified about leadership changes.
	notifyCh := make(chan bool, 128)
	config.NotifyCh = notifyCh
	n.notifyCh = notifyCh

	// Check if this is a brand new cluster.
	isNewCluster := !pathExists(filepath.Join(n.dir, "raft.db"))

	// Create a boltdb-based logs store.
	logs, err := raftboltdb.New(raftboltdb.Options{
		Path:        filepath.Join(n.dir, "raft.db"),
		BoltOptions: &bolt.Options{Timeout: n.timeout},
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create bolt store for raft logs")
	}

	// Create an HTTP-tunneled transport.
	addr := n.listener.Addr()
	layer := rafthttp.NewLayer("/", addr, n.handler, rafthttp.NewDialTCP())
	transport := raft.NewNetworkTransportWithLogger(layer, 2, n.timeout, logger)

	// Create a file snapshot store.
	snaps, err := raft.NewFileSnapshotStoreWithLogger(n.dir, 2, logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create file snapshot store")
	}

	// If we are the seed node and it's a new cluster, bootstrap it.
	if isNewCluster && n.join == "" {
		configuration := raft.Configuration{}
		configuration.Servers = []raft.Server{{
			ID:      config.LocalID,
			Address: raft.ServerAddress(n.Addr()),
		}}
		err := raft.BootstrapCluster(config, logs, logs, snaps, transport, configuration)
		if err != nil {
			return nil, errors.Wrap(err, "failed to bootstrap cluster")
		}
	}

	// Start raft.
	n.raft, err = raft.NewRaft(config, fsm, logs, logs, snaps, transport)
	if err != nil {
		return nil, errors.Wrap(err, "failed to start raft")
	}

	// Process Raft connections over HTTP.
	go raftmembership.HandleChangeRequests(n.raft, n.handler.Requests())

	// If we are not yet part of the cluster and we are being given a
	// bootstrap node to join, let's join.
	if isNewCluster && n.join != "" {
		var err error
		for i := 0; i < 10; i++ {
			if err = layer.Join(config.LocalID, raft.ServerAddress(n.join), n.timeout); err == nil {
				break
			}
			n.logger.Printf("[INFO] demo: retry to join cluster: %v", err)
			time.Sleep(time.Second)
		}
		if err != nil {
			n.logger.Fatalf("[ERR] demo: failed to join cluster: %v", err)
		}
	}

	return n.raft, nil
}

// Handle a transaction error. All errors are fatal except ones due to
// lost leadership, in which case we rollback and try again.
func handleTxError(logger *log.Logger, tx *sql.Tx, err error, isCommit bool) {
	if err, ok := err.(sqlite3.Error); ok {
		if err.ExtendedCode == sqlite3.ErrIoErrNotLeader || err.ExtendedCode == sqlite3.ErrIoErrLeadershipLost {
			if isCommit {
				// Commit failures are automatically
				// rolled back by SQLite, so rolling
				// back again would be an error. See
				// also #2.
				return
			}
			if err := tx.Rollback(); err != nil {
				if err, ok := err.(sqlite3.Error); ok {
					if err.ExtendedCode == sqlite3.ErrIoErrNotLeader || err.ExtendedCode == sqlite3.ErrIoErrLeadershipLost {
						return
					}
				}
				logger.Fatalf("[ERR] demo: rollback failed %v", err)
			}
			return
		}
	}
	logger.Fatalf("[ERR] demo: transaction failed: %v", err)
}

// Return the number of rows inserted so far
func insertedCount(logger *log.Logger, tx *sql.Tx) int {
	rows, err := tx.Query("SELECT COUNT(n) FROM test")
	if err != nil {
		logger.Fatalf("[ERR] demo: select count failed: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		logger.Fatal("[ERR] demo: select count returned no rows")
	}
	var count int
	if err := rows.Scan(&count); err != nil {
		logger.Fatalf("[ERR] demo: scanning failed: %v", err)
	}
	return count
}

// Log about the progress that has been maded
func reportProgress(logger *log.Logger, tx *sql.Tx) {
	rows, err := tx.Query("SELECT n FROM test")
	if err != nil {
		logger.Fatalf("[ERR] demo: select failed: %v", err)
	}
	defer rows.Close()
	inserted := []int{}
	for rows.Next() {
		var n int
		if err := rows.Scan(&n); err != nil {
			logger.Fatalf("[ERR] demo: scan failed: %v", err)
		}
		inserted = append(inserted, n)
	}
	missing := 0
	for i := range inserted {
		if inserted[i] != i {
			missing++
		}
	}
	logger.Printf("[INFO] demo: %d rows inserted, %d values missing", len(inserted), missing)
}

// Randomly sleep at least min seconds and at most max.
func randomSleep(min float64, max float64) {
	milliseconds := (min*1000 + math.Ceil(rand.Float64()*(max-min)*1000))
	time.Sleep(time.Duration(milliseconds) * time.Millisecond)
}

func pathExists(name string) bool {
	if _, err := os.Lstat(name); err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}
