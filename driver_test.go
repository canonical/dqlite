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
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
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
			driver, err := dqlite.NewDriver(dqlite.NewFSM(c.dir), nil, dqlite.DriverConfig{})
			assert.Nil(t, driver)
			require.Error(t, err)
			assert.Contains(t, err.Error(), c.error)
		})
	}
}

// Passing a raft.FSM that is not a dqlite FSM results in panic.
func TestNewDriver_WrongFSMConcreteType(t *testing.T) {
	f := func() {
		dqlite.NewDriver(rafttest.FSM(), nil, dqlite.DriverConfig{})
	}
	assert.PanicsWithValue(t, "fsm is not a dqlite FSM", f)
}

func TestNewDriver_CreateDir(t *testing.T) {
	dir, cleanup := newDir(t)
	defer cleanup()

	dir = filepath.Join(dir, "does", "not", "exist")
	_, err := dqlite.NewDriver(dqlite.NewFSM(dir), &raft.Raft{}, dqlite.DriverConfig{})
	assert.NoError(t, err)
}

func DISABLE_TestDriver_SQLiteLogging(t *testing.T) {
	output := bytes.NewBuffer(nil)
	logger := log.New(output, "", 0)
	config := dqlite.DriverConfig{Logger: logger}

	driver, cleanup := newDriverWithConfig(t, config)
	defer cleanup()

	conn, err := driver.Open("test.db")
	require.NoError(t, err)

	_, err = conn.Prepare("CREATE FOO")
	require.Error(t, err)
	assert.Contains(t, output.String(), `[ERR] near "FOO": syntax error (1)`)
}

func TestDriver_OpenClose(t *testing.T) {
	driver, cleanup := newDriver(t)
	defer cleanup()

	conn, err := driver.Open("test.db")
	require.NoError(t, err)
	assert.NoError(t, conn.Close())
}

func TestDriver_OpenInvalidURI(t *testing.T) {
	driver, cleanup := newDriver(t)
	defer cleanup()

	conn, err := driver.Open("/foo/test.db")
	assert.Nil(t, conn)
	assert.EqualError(t, err, "invalid URI /foo/test.db: directory segments are invalid")
}

func TestDriver_OpenEror(t *testing.T) {
	dir, cleanup := newDir(t)
	defer cleanup()

	fsm := dqlite.NewFSM(dir)
	raft, cleanup := rafttest.Node(t, fsm)
	defer cleanup()
	config := dqlite.DriverConfig{}

	driver, err := dqlite.NewDriver(fsm, raft, config)
	require.NoError(t, err)
	require.NoError(t, os.RemoveAll(dir))

	conn, err := driver.Open("test.db")
	assert.Nil(t, conn)

	expected := fmt.Sprintf("open error for %s: unable to open database file", filepath.Join(dir, "test.db"))
	assert.EqualError(t, err, expected)
}

func TestDriver_NotLeader(t *testing.T) {
	cases := []struct {
		title string
		f     func(*testing.T, *dqlite.Conn) error
	}{
		{
			`open`,
			func(t *testing.T, conn *dqlite.Conn) error {
				_, err := conn.Prepare("CREATE TABLE foo (n INT)")
				return err
			},
		},
		{
			`exec`,
			func(t *testing.T, conn *dqlite.Conn) error {
				_, err := conn.Exec("CREATE TABLE foo (n INT)", nil)
				return err
			},
		},
		{
			`begin`,
			func(t *testing.T, conn *dqlite.Conn) error {
				_, err := conn.Begin()
				return err
			},
		},
	}

	for _, c := range cases {
		t.Run(c.title, func(t *testing.T) {
			dir, cleanup := newDir(t)
			defer cleanup()

			fsm1 := dqlite.NewFSM(dir)
			fsm2 := dqlite.NewFSM(dir)
			rafts, control := rafttest.Cluster(t, []raft.FSM{fsm1, fsm2}, rafttest.Latency(1000.0))
			defer control.Close()

			config := dqlite.DriverConfig{}

			driver, err := dqlite.NewDriver(fsm1, rafts[0], config)
			require.NoError(t, err)

			conn, err := driver.Open("test.db")
			require.NoError(t, err)

			err = c.f(t, conn.(*dqlite.Conn))
			assert.EqualError(t, err, "attempt to write on a non-leader replicated node")
		})
	}
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
func newDriver(t *testing.T) (*dqlite.Driver, func()) {
	config := dqlite.DriverConfig{Logger: newTestingLogger(t, 0)}
	return newDriverWithConfig(t, config)
}

// Create a new test dqlite.Driver with custom configuration.
func newDriverWithConfig(t *testing.T, config dqlite.DriverConfig) (*dqlite.Driver, func()) {
	dir, dirCleanup := newDir(t)

	fsm := dqlite.NewFSM(dir)
	raft, raftCleanup := rafttest.Node(t, fsm)

	driver, err := dqlite.NewDriver(fsm, raft, config)
	require.NoError(t, err)

	cleanup := func() {
		raftCleanup()
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
		_, err := os.Stat(dir)
		if err != nil {
			assert.True(t, os.IsNotExist(err))
		} else {
			assert.NoError(t, os.RemoveAll(dir))
		}
	}
	return dir, cleanup
}

// Return a logger forwarding its output to the test logger.
func newTestingLogger(t *testing.T, n int) *log.Logger {
	return log.New(rafttest.TestingWriter(t), fmt.Sprintf("%d: ", n), log.LstdFlags)
}
