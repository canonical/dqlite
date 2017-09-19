package dqlite_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/CanonicalLtd/dqlite"
	"github.com/hashicorp/raft"
	"github.com/mpvl/subtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Using invalid paths in Config.Dir results in an error.
func TestNewDriver_DirErrors(t *testing.T) {
	cases := []struct {
		title   string
		config  *dqlite.Config // Configuration to use for the new driver
		message string         // Expected message
	}{
		{
			`no path given at all`,
			&dqlite.Config{},
			"no data dir provided in config",
		},
		{
			`non-existing path that can't be created`,
			&dqlite.Config{Dir: "/cant/create/anything/here/"},
			"failed to create data dir",
		},
		{
			`path that can't be accessed`,
			&dqlite.Config{Dir: "/proc/1/root/"},
			"failed to access data dir",
		},
		{
			`path that is not a directory`,
			&dqlite.Config{Dir: "/etc/fstab"},
			"data dir '/etc/fstab' is not a directory",
		},
	}
	for _, c := range cases {
		subtest.Run(t, c.message, func(t *testing.T) {
			driver, err := dqlite.NewDriver(c.config)
			assert.Nil(t, driver)
			require.Error(t, err)
			assert.Contains(t, err.Error(), c.message)
		})
	}
}

// If there's a problem starting raft, an error is returned.
func TestNewDriver_RaftErrors(t *testing.T) {
	cases := []struct {
		title   string
		tweak   func(*dqlite.Config) // Function used to tweak the base test config object
		message string               // Expected error message
	}{
		{
			`invalid bolt store file`,
			func(c *dqlite.Config) {
				// Make raft.db a symlink to a file that does not exist, to trigger a
				// bolt open error.
				os.Symlink("/non/existing/file/path", filepath.Join(c.Dir, "raft.db"))
			},
			"failed to create raft logs store",
		},
		{
			`invalid snapshot dir`,
			func(c *dqlite.Config) {
				// Make "snapshots" a file, so NewFileSnapshotStoreWithLogger will
				// fail.
				err := ioutil.WriteFile(filepath.Join(c.Dir, "snapshots"), []byte(""), 0644)
				require.NoError(t, err)
			},
			"failed to create snapshot store",
		},
		{
			`invalid raft parameter`,
			func(c *dqlite.Config) {
				c.HeartbeatTimeout = time.Microsecond
			},
			"failed to start raft",
		},
	}
	for _, c := range cases {
		subtest.Run(t, c.message, func(t *testing.T) {
			dir, cleanup := newDir()
			defer cleanup()

			config := &dqlite.Config{Dir: dir}
			c.tweak(config)

			driver, err := dqlite.NewDriver(config)
			assert.Nil(t, driver)
			require.Error(t, err)
			assert.Contains(t, err.Error(), c.message)
		})
	}
}

// If the data directory does not exist, it is created automatically.
func TestNewDriver_CreateDir(t *testing.T) {
	dir, cleanup := newDir()
	defer cleanup()

	_, transport := raft.NewInmemTransport("")

	config := &dqlite.Config{
		Dir:                filepath.Join(dir, "does", "not", "exist"),
		HeartbeatTimeout:   5 * time.Millisecond,
		ElectionTimeout:    5 * time.Millisecond,
		CommitTimeout:      5 * time.Millisecond,
		LeaderLeaseTimeout: 5 * time.Millisecond,
		Transport:          transport,
	}

	_, err := dqlite.NewDriver(config)
	assert.NoError(t, err)
}

func TestDriver_JoinError(t *testing.T) {
	config, cleanup := newConfig()
	defer cleanup()

	config.MembershipChanger = &failingMembershipChanger{}

	driver, err := dqlite.NewDriver(config)
	require.NoError(t, err)

	err = driver.Join("1.2.3.4", time.Microsecond)
	assert.Error(t, err, "failed to join dqlite cluster: boom")
}

func TestDriver_JoinSuccess(t *testing.T) {
	config, cleanup := newConfig()
	defer cleanup()

	config.MembershipChanger = &succeedingMembershipChanger{}

	driver, err := dqlite.NewDriver(config)
	require.NoError(t, err)

	err = driver.Join("1.2.3.4", time.Microsecond)
	assert.NoError(t, err)
}

// Implementation of raftmembership.Changer that always fails.
type failingMembershipChanger struct {
}

func (c *failingMembershipChanger) Join(addr string, timeout time.Duration) error {
	return fmt.Errorf("boom")
}

func (c *failingMembershipChanger) Leave(addr string, timeout time.Duration) error {
	return fmt.Errorf("boom")
}

// Implementation of raftmembership.Changer that always succeeds.
type succeedingMembershipChanger struct {
}

func (c *succeedingMembershipChanger) Join(addr string, timeout time.Duration) error {
	return nil
}

func (c *succeedingMembershipChanger) Leave(addr string, timeout time.Duration) error {
	return nil
}

func TestDriver_SQLiteLogging(t *testing.T) {
	config, cleanup := newConfig()
	defer cleanup()

	output := bytes.NewBuffer(nil)
	config.Logger = log.New(output, "", log.Lmicroseconds)

	driver, err := dqlite.NewDriver(config)
	require.NoError(t, err)

	conn, err := driver.Open("test.db")
	require.NoError(t, err)

	_, err = conn.(*dqlite.Conn).Exec("CREATE FOO", nil)
	require.Error(t, err)
	assert.Contains(t, output.String(), `[ERR] sqlite: near "FOO": syntax error (1)`)
}

func TestDriver_IsLoneNode(t *testing.T) {
	driver, cleanup := newDriver()
	defer cleanup()

	isLone, err := driver.IsLoneNode()
	if err != nil {
		t.Fatal(err)
	}
	if !isLone {
		t.Error("expected node to have no peers")
	}
}

func TestDriver_ExecStatementLeadershipTimeout(t *testing.T) {
	driver, cleanup := newDriver()
	defer cleanup()

	conn, err := driver.Open("test.db?_leadership_timeout=1")
	require.NoError(t, err)

	_, err = conn.(*dqlite.Conn).Exec("CREATE TABLE foo (n INT)", nil)
	if err == nil {
		t.Error("expected Exec to timeout and fail")
	}
}

func TestDriver_ExecStatement(t *testing.T) {
	driver, cleanup := newDriver()
	defer cleanup()

	conn, err := driver.Open("test.db")
	require.NoError(t, err)

	_, err = conn.(*dqlite.Conn).Exec("CREATE TABLE foo (n INT)", nil)
	require.NoError(t, err)
}

// Create a new dqlite.Driver and return it along with a function that can be
// used to cleanup any relevant state.
func newDriver() (*dqlite.Driver, func()) {
	config, configCleanup := newConfig()

	driver, err := dqlite.NewDriver(config)
	if err != nil {
		configCleanup()
		panic(fmt.Sprintf("failed to create driver: %v", err))
	}

	cleanup := func() {
		defer configCleanup()
		if err := driver.Shutdown(); err != nil {
			panic(fmt.Sprintf("failed to shutdown driver: %v", err))
		}
	}

	return driver, cleanup
}

// Create a new test Config object with a few sane defaults, such as a temp
// directory and an in-memory transport. Return the config object along with a
// cleanup function to remove the temporary dir.
func newConfig() (*dqlite.Config, func()) {
	dir, cleanup := newDir()

	_, transport := raft.NewInmemTransport("0")

	config := &dqlite.Config{
		Dir:                dir,
		Logger:             log.New(os.Stdout, "", log.Ltime|log.Lmicroseconds),
		EnableSingleNode:   true,
		HeartbeatTimeout:   5 * time.Millisecond,
		ElectionTimeout:    5 * time.Millisecond,
		CommitTimeout:      5 * time.Millisecond,
		LeaderLeaseTimeout: 5 * time.Millisecond,
		Transport:          transport,
	}

	return config, cleanup
}

// Create a new test directory and return it, along with a function that can be
// used to remove it.
func newDir() (string, func()) {
	dir, err := ioutil.TempDir("", "dqlite-driver-test-")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	cleanup := func() {
		if err := os.RemoveAll(dir); err != nil {
			panic(fmt.Sprintf("failed to cleanup temp dir: %v", err))
		}
	}
	return dir, cleanup
}
