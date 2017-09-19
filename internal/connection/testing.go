package connection

import (
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/CanonicalLtd/go-sqlite3x"
	"github.com/mattn/go-sqlite3"
)

// NewTestDSN returns a DSN pointing to a database filename called 'test.db'.
func NewTestDSN() *Params {
	dsn, err := NewDSN("test.db")
	if err != nil {
		panic(fmt.Sprintf("failed to create test DSN: %v", err))
	}
	return dsn
}

// NewTempRegistry returns a registry backed by a temporary directory.
func NewTempRegistry() *Registry {
	path, err := ioutil.TempDir("", "dqlite-connection-")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp directory for registry: %v", err))

	}
	return NewRegistryLegacy(path)
}

// NewTempRegistryWithDatabase returns a temp registry with a
// registered database named 'test', that uses the test DSN and that
// has its follower connection open.
func NewTempRegistryWithDatabase() *Registry {
	registry := NewTempRegistry()

	path := filepath.Join(registry.Dir(), "test.db")
	conn, err := OpenFollower(path)
	if err != nil {
		panic(fmt.Sprintf("failed to open follower connection: %v", err))
	}
	registry.AddFollower("test.db", conn)

	return registry
}

// NewTempRegistryWithLeader returns a temp registry with a
// registered database named 'test', that uses the test DSN, that
// has its follower connection open and new leader connection
// created with the given replication methods.
func NewTempRegistryWithLeader() (*Registry, *sqlite3.SQLiteConn) {
	registry := NewTempRegistryWithDatabase()

	methods := sqlite3x.PassthroughReplicationMethods()
	dsn := NewTestDSN()
	conn, err := OpenLeader(filepath.Join(registry.Dir(), dsn.Filename), methods, 1000)
	if err != nil {
		panic(fmt.Sprintf("failed to open leader: %v", err))
	}
	registry.AddLeader(dsn.Filename, conn)

	return registry, conn
}
