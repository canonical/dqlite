package connection_test

import (
	"fmt"
	"log"
	"strings"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/go-sqlite3x"
)

func ExampleRegistry() {
	// Create a registry backed by a temporary directory.
	registry := connection.NewTempRegistry()
	defer registry.Purge()

	// Create a SQLite connection in leader replication mode.
	dsn, err := connection.NewDSN("test.db?_busy_timeout=10")
	if err != nil {
		log.Fatalf("failed to parse DSN string: %v", err)
	}
	methods := sqlite3x.PassthroughReplicationMethods()
	leader, err := registry.OpenLeader(dsn, methods)
	if err != nil {
		log.Fatalf("failed to open leader connection: %v", err)
	}

	// Create a SQLite connection in follower replication mode.
	err = registry.OpenFollower("test.db")
	if err != nil {
		log.Fatalf("failed to open follower connection: %v", err)
	}

	// Output:
	// true
	// test.db
	// 1
	// true
	// [test.db]
	// true
	// true
	fmt.Println(strings.HasPrefix(registry.Dir(), "/tmp/"))
	fmt.Println(registry.NameByLeader(leader))
	fmt.Println(len(registry.Leaders("test.db")))
	fmt.Println(registry.Leaders("test.db")[0] == leader)
	fmt.Println(registry.AllNames())
	fmt.Println(registry.HasFollower("test.db"))
	fmt.Println(registry.Follower("test.db") != nil)
}
