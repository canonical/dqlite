package connection_test

import (
	"fmt"
	"log"

	"github.com/CanonicalLtd/dqlite/internal/connection"
)

// Parse and encode dqlite-compatbile URIs.
func ExampleURI() {
	filename, query, err := connection.ParseURI("test.db?mode=rw")
	if err != nil {
		log.Fatalf("failed to parse connection string: %v", err)
	}
	// Output:
	// test.db
	// mode=rw
	// test.db?mode=rw
	fmt.Println(filename)
	fmt.Println(query)
	fmt.Println(connection.EncodeURI(filename, query))
}
