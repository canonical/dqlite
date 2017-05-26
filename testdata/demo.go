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
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/dqlite/dqlite"
	"github.com/dqlite/go-sqlite3x"
	"github.com/dqlite/raft-http"
	"github.com/mattn/go-sqlite3"
)

var data = flag.String("data", "", "directory to save SQLite databases and Raft data in")
var join = flag.String("join", "", "address of an existing node in the cluster (or none for starting a new cluster)")
var port = flag.Int("port", 9990, "local port to use for the raft HTTP gateway")
var debug = flag.Bool("debug", false, "enable debug logging")

func main() {
	// Parse and validate command line flags.
	flag.Parse()
	if *data == "" {
		log.Fatal("[ERR] demo: the -data options is required (see -help)")
	}
	level := "NONE"
	if *debug {
		level = "DEBUG"
	}
	logger := dqlite.NewLogger(os.Stderr, level, log.LstdFlags)

	// Spawn an HTTP server that will act as our Raft transport
	// for the DQLite cluster. In a real-world web service you'll
	// want to route the Raft HTTP handler to some specific path,
	// not "/".
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *port))
	if err != nil {
		log.Fatalf("[ERR] demo: failed to listen to port %d: %v", port, err)
	}
	handler := rafthttp.NewHandler()
	server := &http.Server{Handler: handler}
	go server.Serve(listener)
	addr := listener.Addr()

	prefix := fmt.Sprintf("%s: ", addr)
	logger.SetPrefix(prefix)

	log.SetPrefix(prefix)
	log.Printf("[INFO] demo: start")

	// Create a new DQLite driver for this node and register it
	// using the sql package.
	config := dqlite.NewHTTPConfig(*data, handler, "/", addr, logger)
	driver, err := dqlite.NewDriver(config, *join)
	if err != nil {
		log.Fatalf("[ERR] demo: failed to start DQLite driver: %v", err)
	}
	driver.AutoCheckpoint(20)
	sql.Register("dqlite", driver)

	// Open a database backed by DQLite, and use at most one connection.
	db, err := sql.Open("dqlite", "test.db")
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)

	// Start crunching.
	go insertForever(db)

	randomSleep(5, 25)
	log.Printf("[INFO] demo: exit")
}

// Use the given database to insert new rows to a single-column table
// until we fail or exit.
func insertForever(db *sql.DB) {
	for {
		// Start the transaction.
		tx, err := db.Begin()
		if err != nil {
			log.Fatalf("begin failed: %v", err)
		}

		// Ensure our test table is there.
		if _, err := tx.Exec("CREATE TABLE IF NOT EXISTS test (n INT)"); err != nil {
			handleTxError(tx, err)
			continue
		}

		reportProgress(tx)

		// Insert a batch of rows.
		offset := insertedCount(tx)
		failed := false
		for i := 0; i < 50; i++ {
			if _, err := tx.Exec("INSERT INTO test (n) VALUES(?)", i+offset); err != nil {
				handleTxError(tx, err)
				failed = true
				break
			}
			// Sleep a tiny bit between each insert to make it more likely that we get
			// terminated in the middle of a transaction.
			randomSleep(0.010, 0.025)
		}
		if failed {
			continue
		}

		// Commit
		if err := tx.Commit(); err != nil {
			handleTxError(tx, err)
			continue
		}
		// Sleep a little bit to simulate a pause in the service's
		// activity and give a chance to snapshot.
		randomSleep(0.25, 0.5)
	}
}

// Handle a transaction error. All errors are fatal except ones due to
// lost leadership, in which case we rollback and try again.
func handleTxError(tx *sql.Tx, err error) {
	if err, ok := err.(sqlite3.Error); ok {
		if err.Code == sqlite3x.ErrNotLeader {
			if err := tx.Rollback(); err != nil {
				log.Fatalf("[ERR] demo: rolloback failed %v", err)
			}
			return
		}
	}
	log.Fatalf("[ERR] demo: transaction failed: %v", err)
}

// Log about the progress that has been maded
func reportProgress(tx *sql.Tx) {
	rows, err := tx.Query("SELECT n FROM test")
	if err != nil {
		log.Fatalf("[ERR] demo: select failed: %v", err)
	}
	defer rows.Close()
	inserted := []int{}
	for rows.Next() {
		var n int
		if err := rows.Scan(&n); err != nil {
			log.Fatalf("[ERR] demo: scan failed: %v", err)
		}
		inserted = append(inserted, n)
	}
	missing := 0
	for i := range inserted {
		if inserted[i] != i {
			missing++
		}
	}
	log.Printf("[INFO] demo: %d rows inserted, %d values missing", len(inserted), missing)
}

// Return the number of rows inserted so far
func insertedCount(tx *sql.Tx) int {
	rows, err := tx.Query("SELECT COUNT(n) FROM test")
	if err != nil {
		log.Fatalf("[ERR] demo: select count failed: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		log.Fatal("[ERR] demo: select count returned no rows")
	}
	var count int
	if err := rows.Scan(&count); err != nil {
		log.Fatalf("[ERR] demo: scanning failed: %v", err)
	}
	return count
}

// Randomly sleep at least min seconds and at most max.
func randomSleep(min float64, max float64) {
	milliseconds := (min*1000 + math.Ceil(rand.Float64()*(max-min)*1000))
	time.Sleep(time.Duration(milliseconds) * time.Millisecond)
}
