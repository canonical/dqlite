package connection_test

import (
	"fmt"
	"log"

	"github.com/CanonicalLtd/dqlite/internal/connection"
)

// Create a DSN object from a string.
func ExampleDSN() {
	dsn, err := connection.NewDSN("test.db?mode=rw")
	if err != nil {
		log.Fatalf("failed to parse DSN string: %v", err)
	}
	// Output:
	// test.db
	// mode=rw
	// test.db?mode=rw
	fmt.Println(dsn.Filename)
	fmt.Println(dsn.Query)
	fmt.Println(dsn.Encode()) // Excludes dqlite-specific parameters
}
