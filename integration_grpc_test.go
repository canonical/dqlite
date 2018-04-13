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

package dqlite_test

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/CanonicalLtd/go-grpc-sql"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test the integration with the go-grpc-sql package.
func TestExposeDriverOverGrpc(t *testing.T) {
	dbs, cleanup := newGrpcCluster(t)
	defer cleanup()

	// Create a table using the first db.
	db := dbs[0]
	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec("CREATE TABLE test (n INT)")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Insert a row in the created table using the second db.
	db = dbs[1]
	tx, err = db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec("INSERT INTO test VALUES(123)")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Read the inserted row using the third db.
	db = dbs[2]
	tx, err = db.Begin()
	require.NoError(t, err)
	rows, err := tx.Query("SELECT n FROM test")
	require.NoError(t, err)
	require.True(t, rows.Next())
	var n int
	require.NoError(t, rows.Scan(&n))
	assert.Equal(t, 123, n)
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.NoError(t, tx.Commit())
}

// Test concurrent leader connections and transactions over gRPC.
func TestGrpcConcurrency(t *testing.T) {
	dbs, cleanup := newGrpcCluster(t)
	defer cleanup()

	// Create a table using the first db.
	db := dbs[0]
	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec("CREATE TABLE test (n INT)")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	n := 3
	wg := sync.WaitGroup{}
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			db := dbs[i]
			tx, err := db.Begin()
			require.NoError(t, err)
			_, err = tx.Exec("INSERT INTO test VALUES(?)", i)
			require.NoError(t, err)
			require.NoError(t, tx.Commit())
			wg.Done()
		}(i)
	}
	wg.Wait()
}

// Create a new cluster for 3 dqlite drivers exposed over gRPC. Return 3 sql.DB
// instances backed gRPC SQL drivers, each one trying to connect to one of the
// 3 dqlite drivers over gRPC, in a round-robin fashion.
func newGrpcCluster(t *testing.T) ([]*sql.DB, func()) {
	drivers, rafts, driversCleanup := newDrivers(t)

	// Create the gRPC SQL servers and dialers. The dialer will fail if the
	// raft instance associated with their server is not the leader.
	servers := make([]*grpc.Server, 3)
	dialers := make([]grpcsql.Dialer, 3)
	for i, driver := range drivers {
		id := raft.ServerID(strconv.Itoa(i))
		rft := rafts[id]
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)
		server := grpcsql.NewServer(driver)
		go server.Serve(listener)
		servers[i] = server
		dialers[i] = func() (*grpc.ClientConn, error) {
			if rft.State() != raft.Leader {
				return nil, fmt.Errorf("not leader")
			}
			return grpc.Dial(listener.Addr().String(), grpc.WithInsecure())
		}
	}

	// Create a dialer which will try to connect to the others in
	// round-robin, until one succeeds or a timeout expires.
	dialer := func() (*grpc.ClientConn, error) {
		var lastErr error
		remaining := 5 * time.Second
		for remaining > 0 {
			for i, dialer := range dialers {
				t.Logf("dialing backend %d", i)
				conn, err := dialer()
				if err == nil {
					return conn, err
				}
				lastErr = err
			}
			time.Sleep(250 * time.Millisecond)
			remaining -= 250 * time.Millisecond
		}
		return nil, lastErr
	}

	dbs, dbsCleanup := newDBs(t, func(i int) driver.Driver {
		return grpcsql.NewDriver(dialer)
	})

	cleanup := func() {
		dbsCleanup()
		for i := range servers {
			servers[i].Stop()
		}
		driversCleanup()
	}

	return dbs, cleanup
}
