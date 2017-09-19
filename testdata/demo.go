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

	"github.com/CanonicalLtd/dqlite"
	"github.com/CanonicalLtd/go-sqlite3x"
	"github.com/CanonicalLtd/raft-http"
	"github.com/mattn/go-sqlite3"
)

var data = flag.String("data", "", "directory to save SQLite databases and Raft data in")
var join = flag.String("join", "", "address of an existing node in the cluster (or none for starting a new cluster)")
var addr = flag.String("addr", "127.0.0.1:9990", "address to listen to for the raft HTTP gateway other demo nodes will connect to (via initially -join)")
var debug = flag.Bool("debug", false, "enable debug logging")
var forever = flag.Bool("forever", false, "run forever, without crashing at a random time between 5 and 25 seconds ")

var logger *log.Logger
var db *sql.DB

func main() {
	// Parse and validate command line flags.
	parseCommandLine()

	// Open our sql.DB using the dqlite driver.
	openDB()

	// Start inserting data into the DB.
	useDB()

}

func parseCommandLine() {
	flag.Parse()

	if *data == "" {
		fmt.Fprintf(os.Stderr, "error: the -data option is required (see -help)\n")
		os.Exit(2)
	}

	level := "INFO"
	origins := []string{"demo"}

	if *debug {
		level = "DEBUG"
		origins = append(origins, "dqlite", "raft", "raft-net")
	}

	writer := dqlite.NewLogFilter(os.Stderr, level, origins)
	logger = log.New(writer, "", log.LstdFlags)
}

func openDB() {
	// Spawn an HTTP server that will act as our Raft transport
	// for the DQLite cluster. In a real-world web service you'll
	// want to route the Raft HTTP handler to some specific path,
	// not "/".
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		logger.Fatalf("[ERR] demo: failed to listen to address %s: %v", *addr, err)
	}
	handler := rafthttp.NewHandler()
	go http.Serve(listener, handler)

	addr := listener.Addr()
	logger.SetPrefix(fmt.Sprintf("%s: ", addr))
	logger.Printf("[INFO] demo: open DB")

	// Create a new DQLite driver for this node and register it
	// using the sql package.
	config := dqlite.NewHTTPConfig(*data, handler, "/", addr, logger)
	config.EnableSingleNode = *join == ""
	driver, err := dqlite.NewDriver(config)
	if err != nil {
		logger.Fatalf("[ERR] demo: failed to start DQLite driver: %v", err)
	}
	driver.AutoCheckpoint(20)
	sql.Register("dqlite", driver)

	// Join the cluster if we're being told so in the command line, and we
	// yet a lone node.
	isLone, err := driver.IsLoneNode()
	if err != nil {
		logger.Fatalf("[ERR] demo: failed to establish we're lone: %v", err)
	}
	if *join != "" && isLone {
		if err := driver.Join(*join, 5*time.Second); err != nil {
			logger.Fatalf("[ERR] demo: failed to join cluster: %v", err)
		}
	}

	// Open a database backed by DQLite, and use at most one connection.
	if db, err = sql.Open("dqlite", "test.db"); err != nil {
		logger.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)
}

func useDB() {
	// Insert data into the DB, either forever for a random time between 5
	// and 25 (depending on the -forever command line switch).
	if *forever {
		insertForever()
	} else {
		go insertForever()
		randomSleep(5, 25)
	}
	logger.Printf("[INFO] demo: exit")
}

// Use the given database to insert new rows to a single-column table
// until we fail or exit.
func insertForever() {
	for {
		// Start the transaction.
		tx, err := db.Begin()
		if err != nil {
			logger.Fatalf("[FATAL] demo: begin failed: %v", err)
		}

		// Ensure our test table is there.
		if _, err := tx.Exec("CREATE TABLE IF NOT EXISTS test (n INT)"); err != nil {
			handleTxError(tx, err, false)
			// We're not the leader, wait a bit and try again
			randomSleep(0.250, 0.500)
			continue
		}

		// Insert a batch of rows.
		offset := insertedCount(tx)
		failed := false
		for i := 0; i < 50; i++ {
			if _, err := tx.Exec("INSERT INTO test (n) VALUES(?)", i+offset); err != nil {
				handleTxError(tx, err, false)
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

		reportProgress(tx)

		// Commit
		if err := tx.Commit(); err != nil {
			handleTxError(tx, err, true)
			continue
		}
		// Sleep a little bit to simulate a pause in the service's
		// activity and give a chance to snapshot.
		randomSleep(0.25, 0.5)
	}
}

// Handle a transaction error. All errors are fatal except ones due to
// lost leadership, in which case we rollback and try again.
func handleTxError(tx *sql.Tx, err error, isCommit bool) {
	if err, ok := err.(sqlite3.Error); ok {
		if err.Code == sqlite3x.ErrNotLeader {
			if isCommit {
				// Commit failures are automatically
				// rolled back by SQLite, so rolling
				// back again would be an error. See
				// also #2.
				return
			}
			if err := tx.Rollback(); err != nil {
				logger.Fatalf("[ERR] demo: rollback failed %v", err)
			}
			return
		}
	}
	logger.Fatalf("[ERR] demo: transaction failed: %v", err)
}

// Log about the progress that has been maded
func reportProgress(tx *sql.Tx) {
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

// Return the number of rows inserted so far
func insertedCount(tx *sql.Tx) int {
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

// Randomly sleep at least min seconds and at most max.
func randomSleep(min float64, max float64) {
	milliseconds := (min*1000 + math.Ceil(rand.Float64()*(max-min)*1000))
	time.Sleep(time.Duration(milliseconds) * time.Millisecond)
}
