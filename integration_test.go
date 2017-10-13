package dqlite_test

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/CanonicalLtd/dqlite"
	"github.com/CanonicalLtd/go-grpc-sql"
	"github.com/CanonicalLtd/raft-test"
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

// Create a new cluster for 3 dqlite drivers exposed over gRPC. Return 3 sql.DB
// instances backed gRPC SQL drivers, each one trying to connect to one of the
// 3 dqlite drivers over gRPC, in a round-robin fashion.
func newGrpcCluster(t *testing.T) ([]*sql.DB, func()) {
	// Temporary dqlite data dir.
	dir, err := ioutil.TempDir("", "dqlite-integration-test-")
	assert.NoError(t, err)

	// Create the dqlite FSMs.
	fsms := make([]raft.FSM, 3)
	for i := range fsms {
		fsms[i] = dqlite.NewFSM(filepath.Join(dir, strconv.Itoa(i)))
	}

	// Create the raft cluster using the dqlite FSMs.
	rafts, raftsCleanup := rafttest.Cluster(t, fsms)

	// Create the dqlite drivers.
	drivers := make([]driver.Driver, 3)
	for i := range fsms {
		logFunc := func(level, message string) {
			t.Logf("[%s] %d: %s", level, i, message)
		}
		driver, err := dqlite.NewDriver(fsms[i], rafts[i], dqlite.LogFunc(logFunc))
		require.NoError(t, err)
		drivers[i] = driver
	}

	// Create the gRPC SQL servers and dialers. The dialer will fail if the
	// raft instance associated with their server is not the leader.
	servers := make([]*grpc.Server, 3)
	dialers := make([]grpcsql.Dialer, 3)
	for i, driver := range drivers {
		rft := rafts[i]
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
	dbs := make([]*sql.DB, 3)

	for i := range dbs {
		grpcDriver := grpcsql.NewDriver(dialer)
		grpcDriverName := fmt.Sprintf("dqlite-integration-test-%d", i)
		sql.Register(grpcDriverName, grpcDriver)

		db, err := sql.Open(grpcDriverName, "test.db")
		require.NoError(t, err)
		dbs[i] = db
	}

	cleanup := func() {
		for i := range dbs {
			require.NoError(t, dbs[i].Close())
		}
		for i := range servers {
			servers[i].Stop()
		}
		raftsCleanup()
		require.NoError(t, os.RemoveAll(dir))
	}

	return dbs, cleanup
}
