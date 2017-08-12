package dqlite_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/CanonicalLtd/dqlite"
	"github.com/hashicorp/raft"
)

func TestNewDriver_Errors(t *testing.T) {
	cases := []struct {
		name      string
		newConfig func() *dqlite.Config
		join      string
		want      string
	}{
		{"no dir", newConfigWithNoPath, "", "no data dir provided in config"},
		{"make dir error", newConfigWithDirThatCantBeMade, "", "failed to create data dir"},
		{"dir access error", newConfigWithDirThatCantBeAccessed, "", "failed to access data dir"},
		{"dir is regular file", newConfigWithDirThatIsRegularFile, "", "data dir '/etc/fstab' is not a directory"},
		{"get peers", newConfigWithCorruptedJSONPeersFile, "", "failed to get current raft peers"},
		{"raft params", newConfigWithInvalidRaftParams, "", "failed to start raft"},
		{"join fails", newConfigWithFailingJoinAddress, "1.2.3.4", "failed to join the cluster"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			config := c.newConfig()
			defer os.RemoveAll(config.Dir)
			driver, err := dqlite.NewDriver(config, c.join)
			if driver != nil {
				t.Error("driver is not nil")
			}
			if err == nil {
				t.Fatal("no error was returned")
			}
			got := err.Error()
			if !strings.HasPrefix(got, c.want) {
				t.Fatalf("expected error to start with\n%q\ngot\n%q", c.want, got)
			}
		})
	}
}

func newConfigWithNoPath() *dqlite.Config {
	return &dqlite.Config{}
}

func newConfigWithDirThatCantBeMade() *dqlite.Config {
	return &dqlite.Config{
		Dir: "/should/not/be/able/to/create/anything/here/",
	}
}

func newConfigWithDirThatCantBeAccessed() *dqlite.Config {
	return &dqlite.Config{
		Dir: "/proc/1/root/",
	}
}

func newConfigWithDirThatIsRegularFile() *dqlite.Config {
	return &dqlite.Config{
		Dir: "/etc/fstab",
	}
}

func newConfigWithCorruptedJSONPeersFile() *dqlite.Config {
	dir, err := ioutil.TempDir("", "go-dqlite-")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	if err := ioutil.WriteFile(filepath.Join(dir, "peers.json"), []byte("foo"), 0755); err != nil {
		panic(fmt.Sprintf("failed to write peers.json: %v", err))
	}
	return &dqlite.Config{
		Dir: dir,
	}
}

func newConfigWithInvalidRaftParams() *dqlite.Config {
	dir, err := ioutil.TempDir("", "go-dqlite-")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	return &dqlite.Config{
		Dir:              dir,
		HeartbeatTimeout: time.Microsecond,
	}
}

func newConfigWithFailingJoinAddress() *dqlite.Config {
	dir, err := ioutil.TempDir("", "go-dqlite-")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	_, transport := raft.NewInmemTransport("")
	return &dqlite.Config{
		Dir:                dir,
		MembershipChanger:  &failingMembershipChanger{},
		HeartbeatTimeout:   5 * time.Millisecond,
		ElectionTimeout:    5 * time.Millisecond,
		CommitTimeout:      5 * time.Millisecond,
		LeaderLeaseTimeout: 5 * time.Millisecond,
		Transport:          transport,
	}
}

func newConfigWithLeaderTimeout() *dqlite.Config {
	dir, err := ioutil.TempDir("", "go-dqlite-")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	_, transport := raft.NewInmemTransport("")
	return &dqlite.Config{
		Dir:                dir,
		Transport:          transport,
		MembershipChanger:  &succeedingMembershipChanger{},
		HeartbeatTimeout:   5 * time.Millisecond,
		ElectionTimeout:    5 * time.Millisecond,
		CommitTimeout:      5 * time.Millisecond,
		LeaderLeaseTimeout: 5 * time.Millisecond,
		Logger:             log.New(bytes.NewBuffer(nil), "", 0),
		SetupTimeout:       time.Microsecond,
	}
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
	node := newNode()
	defer node.Cleanup()

	conn := node.Conn()
	statement := "CREATE FOO"
	if _, err := conn.Exec(statement, nil); err == nil {
		t.Fatalf("statement '%s' did not fail", statement)
	}
	const want = `[ERR] sqlite: near "FOO": syntax error (1)`
	got := node.Output.String()
	if !strings.Contains(got, want) {
		t.Errorf("%q\ndoes not contain\n%q", got, want)
	}
}

func TestDriver_ExecStatement(t *testing.T) {
	node := newNode()
	defer node.Cleanup()

	conn := node.Conn()
	statement := "CREATE TABLE foo (n INT)"
	if _, err := conn.Exec(statement, nil); err != nil {
		t.Errorf("Failed to execute SQL '%s': %s", statement, err)
	}
}

// Captures a single dqlite.Driver instance along with its
// dependencies.
type node struct {
	Transport raft.LoopbackTransport
	Output    *bytes.Buffer
	Config    *dqlite.Config
	Driver    *dqlite.Driver
}

// Create a new dqlite node.
func newNode() *node {
	dir, err := ioutil.TempDir("", "go-dqlite-")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}

	_, transport := raft.NewInmemTransport("")

	output := bytes.NewBuffer(nil)

	config := &dqlite.Config{
		Dir:                dir,
		Transport:          transport,
		SetupTimeout:       1 * time.Second,
		HeartbeatTimeout:   5 * time.Millisecond,
		ElectionTimeout:    5 * time.Millisecond,
		CommitTimeout:      5 * time.Millisecond,
		LeaderLeaseTimeout: 5 * time.Millisecond,
		Logger:             log.New(output, "", log.Lmicroseconds),
	}

	driver, err := dqlite.NewDriver(config, "")
	if err != nil {
		panic(fmt.Sprintf("failed to create driver: %v", err))
	}

	return &node{
		Transport: transport,
		Output:    output,
		Config:    config,
		Driver:    driver,
	}
}

// Cleanup all the state associated with the node.
func (n *node) Cleanup() {
	if err := n.Driver.Shutdown(); err != nil {
		panic(fmt.Sprintf("failed to shutdown driver: %v", err))
	}
	if err := os.RemoveAll(n.Config.Dir); err != nil {
		panic(fmt.Sprintf("failed to remove data dir: %v", err))
	}
}

// Return a new dqlite.Conn created using the node's dqlite.Driver.
func (n *node) Conn() *dqlite.Conn {
	n.Driver.WaitLeadership()
	conn, err := n.Driver.Open("test.db")
	if err != nil {
		panic(fmt.Sprintf("failed to create connection: %v", err))
	}
	return conn.(*dqlite.Conn)
}
