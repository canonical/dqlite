package connection_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Open a connection in leader replication mode.
func TestOpenLeader(t *testing.T) {
	dir, cleanup := newDir()
	defer cleanup()

	methods := sqlite3.NoopReplicationMethods()
	conn, err := connection.OpenLeader(filepath.Join(dir, "test.db"), methods)
	require.NoError(t, err)
	require.NotNil(t, conn)

	defer conn.Close()

	_, err = conn.Exec("CREATE TABLE test (n INT)", nil)
	require.NoError(t, err)

	// The journal mode is set to WAL.
	_, err = os.Stat(filepath.Join(dir, "test.db-shm"))
	require.NoError(t, err)

	info, err := os.Stat(filepath.Join(dir, "test.db-wal"))
	require.NotEqual(t, int64(0), info.Size())
}

// Open a connection in follower replication mode.
func TestOpenFollower(t *testing.T) {
	dir, cleanup := newDir()
	defer cleanup()

	conn, err := connection.OpenFollower(filepath.Join(dir, "test.db"))
	require.NoError(t, err)
	require.NotNil(t, conn)

	_, err = conn.Exec("CREATE TABLE test (n INT)", nil)
	require.EqualError(t, err, "database is in follower replication mode: main")
}

// Possible failure modes.
func TestOpenFollower_Error(t *testing.T) {
	cases := []struct {
		title string
		dsn   string
		err   string
	}{
		{
			`non existing dsn`,
			"/non/existing/dsn.db",
			"open error",
		},
	}
	for _, c := range cases {
		conn, err := connection.OpenFollower(c.dsn)
		assert.Nil(t, conn)
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), c.err)
	}
}

// Create a new temporary directory.
func newDir() (string, func()) {
	dir, err := ioutil.TempDir("", "dqlite-connection-test-")
	if err != nil {
		panic(fmt.Sprintf("could not create temporary dir: %v", err))
	}

	cleanup := func() {
		if err := os.RemoveAll(dir); err != nil {
			panic(fmt.Sprintf("could not remove temporary dir: %v", err))
		}
	}

	return dir, cleanup
}
