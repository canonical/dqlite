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

// Create a new cluster for 3 dqlite drivers exposed over gRPC. Return 3 sql.DB
// instances backed gRPC SQL drivers, each one trying to connect to one of the
// 3 dqlite drivers over gRPC, in a round-robin fashion.

package dqlite_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/CanonicalLtd/dqlite"
	"github.com/CanonicalLtd/raft-test"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_DatabaseSQL(t *testing.T) {
	db, _, cleanup := newDB(t)
	defer cleanup()

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.Exec(`
CREATE TABLE test  (n INT, s TEXT);
CREATE TABLE test2 (n INT, t DATETIME DEFAULT CURRENT_TIMESTAMP)
`)
	require.NoError(t, err)

	stmt, err := tx.Prepare("INSERT INTO test(n, s) VALUES(?, ?)")
	require.NoError(t, err)

	_, err = stmt.Exec(int64(123), "hello")
	require.NoError(t, err)

	require.NoError(t, stmt.Close())

	_, err = tx.Exec("INSERT INTO test2(n) VALUES(?)", int64(456))
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	tx, err = db.Begin()
	require.NoError(t, err)

	rows, err := tx.Query("SELECT n, s FROM test")
	require.NoError(t, err)

	for rows.Next() {
		var n int64
		var s string

		require.NoError(t, rows.Scan(&n, &s))

		assert.Equal(t, int64(123), n)
		assert.Equal(t, "hello", s)
	}

	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	rows, err = tx.Query("SELECT n, t FROM test2")
	require.NoError(t, err)

	for rows.Next() {
		var n int64
		var s time.Time

		require.NoError(t, rows.Scan(&n, &s))

		assert.Equal(t, int64(456), n)
	}

	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	require.NoError(t, tx.Rollback())

	require.NoError(t, db.Close())
}

func TestIntegration_LargeQuery(t *testing.T) {
	db, _, cleanup := newDB(t)
	defer cleanup()

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.Exec("CREATE TABLE test  (n INT)")
	require.NoError(t, err)

	stmt, err := tx.Prepare("INSERT INTO test(n) VALUES(?)")
	require.NoError(t, err)

	for i := 0; i < 255; i++ {
		_, err = stmt.Exec(int64(i))
		require.NoError(t, err)
	}

	require.NoError(t, stmt.Close())

	require.NoError(t, tx.Commit())

	tx, err = db.Begin()
	require.NoError(t, err)

	rows, err := tx.Query("SELECT n FROM test")
	require.NoError(t, err)

	columns, err := rows.Columns()
	require.NoError(t, err)

	assert.Equal(t, []string{"n"}, columns)

	for i := 0; rows.Next(); i++ {
		var n int64

		require.NoError(t, rows.Scan(&n))

		assert.Equal(t, int64(i), n)
	}

	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	require.NoError(t, tx.Rollback())

	require.NoError(t, db.Close())
}

func TestIntegration_NotLeader(t *testing.T) {
	db, control, cleanup := newDB(t)
	defer cleanup()

	tx, err := db.Begin()
	require.NoError(t, err)

	control.Depose()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = tx.PrepareContext(ctx, "CREATE TABLE test (n INT)")
	require.Equal(t, driver.ErrBadConn, err)
}

func newDB(t *testing.T) (*sql.DB, *rafttest.Control, func()) {
	n := 3

	listeners := make([]net.Listener, n)
	servers := make([]dqlite.ServerInfo, n)
	for i := range listeners {
		listeners[i] = newListener(t)
		servers[i].Address = listeners[i].Addr().String()
	}

	control, cleanup := newServers(t, listeners)

	store, err := dqlite.DefaultServerStore(":memory:")
	require.NoError(t, err)

	require.NoError(t, store.Set(context.Background(), servers))

	log := testingLogFunc(t)
	driver, err := dqlite.NewDriver(store, dqlite.WithLogFunc(log))
	require.NoError(t, err)

	driverName := fmt.Sprintf("dqlite-integration-test-%d", driversCount)
	sql.Register(driverName, driver)

	driversCount++

	db, err := sql.Open(driverName, "test.db")
	require.NoError(t, err)

	return db, control, cleanup
}

func newServers(t *testing.T, listeners []net.Listener) (*rafttest.Control, func()) {
	t.Helper()

	n := len(listeners)
	cleanups := make([]func(), 0)

	// Create the dqlite registries and FSMs.
	registries := make([]*dqlite.Registry, n)
	fsms := make([]raft.FSM, n)

	for i := range registries {
		id := strconv.Itoa(i)
		registries[i] = dqlite.NewRegistry(id)
		fsms[i] = dqlite.NewFSM(registries[i])
	}

	// Create the raft cluster using the dqlite FSMs.
	rafts, control := rafttest.Cluster(t, fsms, rafttest.Transport(func(i int) raft.Transport {
		address := raft.ServerAddress(listeners[i].Addr().String())
		_, transport := raft.NewInmemTransport(address)
		return transport
	}))
	control.Elect("0")

	for id := range rafts {
		r := rafts[id]
		i, err := strconv.Atoi(string(id))
		require.NoError(t, err)

		log := testingLogFunc(t)

		server, err := dqlite.NewServer(
			r, registries[i], listeners[i],
			dqlite.WithServerLogFunc(log))
		require.NoError(t, err)

		cleanups = append(cleanups, func() {
			require.NoError(t, server.Close())
		})

	}

	cleanup := func() {
		control.Close()
		for _, f := range cleanups {
			f()
		}
	}

	return control, cleanup
}

var driversCount = 0
