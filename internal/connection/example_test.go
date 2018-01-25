package connection_test

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/go-sqlite3"
)

func Example() {
	// Create a temporary directory.
	dir, err := ioutil.TempDir("", "dqlite-connection-example-")
	if err != nil {
		log.Fatalf("failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create a connection in leader replication mode.
	methods := sqlite3.PassthroughReplicationMethods()
	uri := filepath.Join(dir, "test.db")
	leader, err := connection.OpenLeader(uri, methods, 1000)
	if err != nil {
		log.Fatalf("failed to open leader connection: %v", err)
	}
	defer connection.CloseLeader(leader)

	// Create a connection in follower replication mode.
	follower, err := connection.OpenFollower(uri)
	if err != nil {
		log.Fatalf("failed to open follower connection: %v", err)
	}

	// Register the connections.
	registry := connection.NewRegistry()
	registry.AddLeader("test.db", leader)
	registry.AddFollower("test.db", follower)

	// Output:
	// test.db
	// 1
	// true
	// [test.db]
	// true
	// true
	fmt.Println(registry.FilenameOfLeader(leader))
	fmt.Println(len(registry.Leaders("test.db")))
	fmt.Println(registry.Leaders("test.db")[0] == leader)
	fmt.Println(registry.FilenamesOfFollowers())
	fmt.Println(registry.HasFollower("test.db"))
	fmt.Println(registry.Follower("test.db") == follower)
}
