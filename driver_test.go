package dqlite_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/CanonicalLtd/dqlite"
	"github.com/CanonicalLtd/raft-test"
	"github.com/hashicorp/raft"
	"github.com/mpvl/subtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Using invalid paths in Config.Dir results in an error.
func TestNewDriver_DirErrors(t *testing.T) {
	cases := []struct {
		title string
		dir   string // Dir to pass to the new driver.
		error string // Expected message
	}{
		{
			`no path given at all`,
			"",
			"no data dir provided in config",
		},
		{
			`non-existing path that can't be created`,
			"/cant/create/anything/here/",
			"failed to create data dir",
		},
		{
			`path that can't be accessed`,
			"/proc/1/root/",
			"failed to access data dir",
		},
		{
			`path that is not a directory`,
			"/etc/fstab",
			"data dir '/etc/fstab' is not a directory",
		},
	}
	for _, c := range cases {
		subtest.Run(t, c.title, func(t *testing.T) {
			driver, err := dqlite.NewDriver(c.dir, nil)
			assert.Nil(t, driver)
			require.Error(t, err)
			assert.Contains(t, err.Error(), c.error)
		})
	}
}

// If there's a problem starting raft, an error is returned.
func TestNewDriver_RaftErrors(t *testing.T) {
	dir, cleanup := newDir(t)
	defer cleanup()

	factory := func(raft.FSM) (*raft.Raft, error) {
		return nil, fmt.Errorf("boom")
	}
	driver, err := dqlite.NewDriver(dir, factory)
	assert.Nil(t, driver)
	assert.EqualError(t, err, "failed to start raft: boom")
}

// If the data directory does not exist, it is created automatically.
func TestNewDriver_CreateDir(t *testing.T) {
	dir, cleanup := newDir(t)
	defer cleanup()

	dir = filepath.Join(dir, "does", "not", "exist")
	factory := func(raft.FSM) (*raft.Raft, error) {
		return &raft.Raft{}, nil
	}
	_, err := dqlite.NewDriver(dir, factory)
	assert.NoError(t, err)
}

func TestDriver_SQLiteLogging(t *testing.T) {
	output := bytes.NewBuffer(nil)
	logFunc := func(level, message string) {
		output.WriteString(fmt.Sprintf("%s %s\n", level, message))
	}

	driver, cleanup := newDriver(t, dqlite.LogFunc(logFunc))
	defer cleanup()

	conn, err := driver.Open("test.db")
	require.NoError(t, err)

	_, err = conn.Prepare("CREATE FOO")
	require.Error(t, err)
	assert.Contains(t, output.String(), `ERROR [1] near "FOO": syntax error`)
}

func TestDriver_OpenClose(t *testing.T) {
	driver, cleanup := newDriver(t)
	defer cleanup()

	conn, err := driver.Open("test.db")
	require.NoError(t, err)
	assert.NoError(t, conn.Close())
}

func TestStmt_Exec(t *testing.T) {
	driver, cleanup := newDriver(t)
	defer cleanup()

	conn, err := driver.Open("test.db")
	require.NoError(t, err)
	defer conn.Close()

	stmt, err := conn.Prepare("CREATE TABLE foo (n INT)")
	require.NoError(t, err)
	_, err = stmt.Exec(nil)
	assert.NoError(t, err)
}

func TestStmt_Query(t *testing.T) {
	driver, cleanup := newDriver(t)
	defer cleanup()

	conn, err := driver.Open("test.db")
	require.NoError(t, err)
	defer conn.Close()

	stmt, err := conn.Prepare("SELECT name FROM sqlite_master")
	require.NoError(t, err)
	assert.Equal(t, 0, stmt.NumInput())
	rows, err := stmt.Query(nil)
	assert.NoError(t, err)
	defer rows.Close()

}

func TestTx_Commit(t *testing.T) {
	driver, cleanup := newDriver(t)
	defer cleanup()

	conn, err := driver.Open("test.db")
	require.NoError(t, err)
	defer conn.Close()

	tx, err := conn.Begin()
	require.NoError(t, err)
	assert.NoError(t, tx.Commit())
}

func TestTx_Rollback(t *testing.T) {
	driver, cleanup := newDriver(t)
	defer cleanup()

	conn, err := driver.Open("test.db")
	require.NoError(t, err)
	defer conn.Close()

	tx, err := conn.Begin()
	require.NoError(t, err)
	assert.NoError(t, tx.Rollback())
}

// Create a new test dqlite.Driver.
func newDriver(t *testing.T, options ...dqlite.Option) (*dqlite.Driver, func()) {
	dir, dirCleanup := newDir(t)
	factory, factoryCleanup := newFactory(t)

	driver, err := dqlite.NewDriver(dir, factory, options...)
	require.NoError(t, err)

	cleanup := func() {
		factoryCleanup()
		dirCleanup()
	}

	return driver, cleanup
}

// Create a new test directory and return it, along with a function that can be
// used to remove it.
func newDir(t *testing.T) (string, func()) {
	dir, err := ioutil.TempDir("", "dqlite-driver-test-")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	cleanup := func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatalf("failed to cleanup temp dir: %v", err)
		}
	}
	return dir, cleanup
}

// Create a new RaftFactory, using a single node raft.
func newFactory(t *testing.T) (dqlite.RaftFactory, func()) {
	rafts := make([]*raft.Raft, 1)
	factory := func(fsm raft.FSM) (*raft.Raft, error) {
		raft := rafttest.Node(t, fsm)
		rafts[0] = raft
		return raft, nil
	}
	cleanup := func() {
		raft := rafts[0]
		if raft == nil {
			return
		}
		require.NoError(t, raft.Shutdown().Error())
	}
	return factory, cleanup
}
