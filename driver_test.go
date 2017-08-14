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
	"github.com/CanonicalLtd/raft-membership"
	"github.com/hashicorp/raft"
)

func TestNewDriver_Errors(t *testing.T) {
	cases := []struct {
		newConfig func() *dqlite.Config
		want      string
	}{
		{newConfigWithNoPath, "no data dir provided in config"},
		{newConfigWithDirThatCantBeMade, "failed to create data dir"},
		{newConfigWithDirThatCantBeAccessed, "failed to access data dir"},
		{newConfigWithDirThatIsRegularFile, "data dir '/etc/fstab' is not a directory"},
		{newConfigWithInvalidBoltStoreFile, "failed to create raft store"},
		{newConfigWithInvalidSnapshotsDir, "failed to create snapshot store"},
		{newConfigWithInvalidRaftParams, "failed to start raft"},
	}
	for _, c := range cases {
		t.Run(c.want, func(t *testing.T) {
			config := c.newConfig()
			defer os.RemoveAll(config.Dir)
			driver, err := dqlite.NewDriver(config)
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
	if err := ioutil.WriteFile(filepath.Join(dir, "peers.json"), []byte("foo"), 0644); err != nil {
		panic(fmt.Sprintf("failed to write peers.json: %v", err))
	}

	_, transport := raft.NewInmemTransport("1")

	return &dqlite.Config{
		Dir:              dir,
		Transport:        transport,
		EnableSingleNode: true,
	}
}

func newConfigWithInvalidBoltStoreFile() *dqlite.Config {
	dir, err := ioutil.TempDir("", "go-dqlite-")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}

	// Make raft.db a symlink to a file that does not exist, to trigger a
	// bolt open error.
	os.Symlink("/non/existing/file/path", filepath.Join(dir, "raft.db"))

	return &dqlite.Config{
		Dir: dir,
	}
}

func newConfigWithInvalidSnapshotsDir() *dqlite.Config {
	dir, err := ioutil.TempDir("", "go-dqlite-")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}

	// Make "snapshots" a file, so NewFileSnapshotStoreWithLogger will
	// fail.
	if err := ioutil.WriteFile(filepath.Join(dir, "snapshots"), []byte(""), 0644); err != nil {
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

func TestDriver_JoinError(t *testing.T) {
	config := newConfigWithMembershipChanger(&failingMembershipChanger{})
	driver, err := dqlite.NewDriver(config)
	if err != nil {
		t.Fatal(err)
	}

	err = driver.Join("1.2.3.4", time.Microsecond)

	if err == nil {
		t.Fatal("no error returned")
	}
	want := "failed to join dqlite cluster: boom"
	got := err.Error()
	if got != want {
		t.Errorf("expected error '%v', got '%v'", want, got)
	}
}

func TestDriver_JoinSuccess(t *testing.T) {
	config := newConfigWithMembershipChanger(&succeedingMembershipChanger{})
	driver, err := dqlite.NewDriver(config)
	if err != nil {
		t.Fatal(err)
	}

	if err := driver.Join("1.2.3.4", time.Microsecond); err != nil {
		t.Fatalf("joining failed: %v", err)
	}
}

func newConfigWithMembershipChanger(changer raftmembership.Changer) *dqlite.Config {
	dir, err := ioutil.TempDir("", "go-dqlite-")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	_, transport := raft.NewInmemTransport("")

	return &dqlite.Config{
		Dir:                dir,
		MembershipChanger:  changer,
		HeartbeatTimeout:   5 * time.Millisecond,
		ElectionTimeout:    5 * time.Millisecond,
		CommitTimeout:      5 * time.Millisecond,
		LeaderLeaseTimeout: 5 * time.Millisecond,
		Transport:          transport,
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

func TestDriver_IsLoneNode(t *testing.T) {
	node := newNode()
	isLone, err := node.Driver.IsLoneNode()
	if err != nil {
		t.Fatal(err)
	}
	if !isLone {
		t.Error("expected node to have no peers")
	}
}

func TestDriver_OpenErrorLeadershipTimeout(t *testing.T) {
	node := newNode()
	defer node.Cleanup()

	conn, err := node.Driver.Open("test.db?_leadership_timeout=1")
	if conn != nil {
		t.Error("expected Open to timeout and return a nil connection")
	}
	if err == nil {
		t.Error("expected Open to timeout and fail")
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
		EnableSingleNode:   true,
		HeartbeatTimeout:   5 * time.Millisecond,
		ElectionTimeout:    5 * time.Millisecond,
		CommitTimeout:      5 * time.Millisecond,
		LeaderLeaseTimeout: 5 * time.Millisecond,
		Logger:             log.New(output, "", log.Lmicroseconds),
	}

	driver, err := dqlite.NewDriver(config)
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
	conn, err := n.Driver.Open("test.db")
	if err != nil {
		panic(fmt.Sprintf("failed to create connection: %v", err))
	}
	return conn.(*dqlite.Conn)
}
