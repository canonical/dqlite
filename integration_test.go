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
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_DatabaseSQL(t *testing.T) {
	driver, cleanup := newDriver(t)
	defer cleanup()

	sql.Register("dqlite-database-sql", driver)

	db, err := sql.Open("dqlite-database-sql", "test.db")
	require.NoError(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.Exec(
		"CREATE TABLE test (n INT, s TEXT); CREATE TABLE test2 (n INT)",
	)
	require.NoError(t, err)

	stmt, err := tx.Prepare("INSERT INTO test(n, s) VALUES(?, ?)")
	require.NoError(t, err)

	_, err = stmt.Exec(int64(123), "hello")
	require.NoError(t, err)

	require.NoError(t, stmt.Close())
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

	rows, err = tx.Query("SELECT n FROM test2")
	require.NoError(t, err)

	for rows.Next() {
	} // TODO: at the moment the test panics without this loop.

	require.NoError(t, rows.Close())

	require.NoError(t, tx.Rollback())

	require.NoError(t, db.Close())
}

/*
import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/CanonicalLtd/dqlite"
	"github.com/CanonicalLtd/raft-test"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Foo(t *testing.T) {
	_, cleanup := newCluster(t)
	defer cleanup()
}

func newCluster(t *testing.T) ([]*sql.DB, func()) {
	drivers, _, driversCleanup := newDrivers(t)

	dbs, dbsCleanup := newDBs(t, func(i int) driver.Driver {
		return drivers[i]
	})

	cleanup := func() {
		dbsCleanup()
		driversCleanup()
	}

	return dbs, cleanup
}

vfunc newDBs(t *testing.T, driverFactory func(int) driver.Driver) ([]*sql.DB, func()) {
	dbs := make([]*sql.DB, 3)

	for i := range dbs {
		driver := driverFactory(i)
		driverName := fmt.Sprintf("dqlite-integration-test-%d", driversCount)
		driversCount++
		sql.Register(driverName, driver)

		db, err := sql.Open(driverName, "test.db")
		require.NoError(t, err)
		dbs[i] = db
	}

	cleanup := func() {
		for i := range dbs {
			require.NoError(t, dbs[i].Close())
		}
	}

	return dbs, cleanup
}

func newDrivers(t *testing.T) ([]driver.Driver, map[raft.ServerID]*raft.Raft, func()) {
	// Temporary dqlite data dir.
	dir, err := ioutil.TempDir("", "dqlite-integration-test-")
	assert.NoError(t, err)

	// Create the dqlite Registries and FSMs.
	registries := make([]*dqlite.Registry, 3)
	fsms := make([]raft.FSM, 3)
	for i := range fsms {
		registries[i] = dqlite.NewRegistry(filepath.Join(dir, strconv.Itoa(i)))
		fsms[i] = dqlite.NewFSM(registries[i])
	}

	// Create the raft cluster using the dqlite FSMs.
	rafts, control := rafttest.Cluster(t, fsms)
	control.Elect("0")

	// Create the dqlite drivers.
	drivers := make([]driver.Driver, 3)
	for i := range fsms {
		config := dqlite.DriverConfig{Logger: newTestingLogger(t, i)}
		id := raft.ServerID(strconv.Itoa(i))
		driver, err := dqlite.NewDriver(registries[i], rafts[id], config)
		require.NoError(t, err)
		drivers[i] = driver
	}

	cleanup := func() {
		control.Close()
		_, err := os.Stat(dir)
		if err != nil {
			assert.True(t, os.IsNotExist(err))
		} else {
			assert.NoError(t, os.RemoveAll(dir))
		}
	}

	return drivers, rafts, cleanup
}

var driversCount = 0
*/
