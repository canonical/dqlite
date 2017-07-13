package connection

import (
	"fmt"
	"io/ioutil"

	"github.com/dqlite/go-sqlite3x"
	"github.com/mattn/go-sqlite3"
)

// NewTestDSN returns a DSN pointing to a database filename called 'test.db'.
func NewTestDSN() *DSN {
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
	return NewRegistry(path)
}

// NewTempRegistryWithDatabase returns a temp registry with a
// registered database named 'test', that uses the test DSN and that
// has its follower connection open.
func NewTempRegistryWithDatabase() *Registry {
	registry := NewTempRegistry()
	registry.Add("test", NewTestDSN())

	if err := registry.OpenFollower("test"); err != nil {
		panic(fmt.Sprintf("failed to open follower connection: %v", err))
	}

	return registry
}

// NewTempRegistryWithLeader returns a temp registry with a
// registered database named 'test', that uses the test DSN, that
// has its follower connection open and new leader connection
// created with the given replication methods.
func NewTempRegistryWithLeader() (*Registry, *sqlite3.SQLiteConn) {
	registry := NewTempRegistryWithDatabase()

	methods := sqlite3x.PassthroughReplicationMethods()
	conn, err := registry.OpenLeader("test", methods)
	if err != nil {
		panic(fmt.Sprintf("failed to open leader connection: %v", err))
	}

	return registry, conn
}
