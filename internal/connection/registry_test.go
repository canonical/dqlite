package connection_test

import (
	"fmt"

	"testing"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

// Add a new leader connection to the registry.
func TestRegistry_AddLeader(t *testing.T) {
	conn := newConn()
	registry := connection.NewRegistry()
	registry.AddLeader("test.db", conn)
	assert.Equal(t, []*sqlite3.SQLiteConn{conn}, registry.Leaders("test.db"))
	assert.Equal(t, "test.db", registry.FilenameOfLeader(conn))
}

// Delete a leader from to the registry.
func TestRegistry_DelLeader(t *testing.T) {
	conn := newConn()
	registry := connection.NewRegistry()
	registry.AddLeader("test.db", conn)
	registry.DelLeader(conn)
	assert.Equal(t, []*sqlite3.SQLiteConn{}, registry.Leaders("test.db"))
}

// Delete a non registered leader causes a panic.
func TestRegistry_DelLeaderNotRegistered(t *testing.T) {
	conn := newConn()
	registry := connection.NewRegistry()
	f := func() { registry.DelLeader(conn) }
	assert.PanicsWithValue(t, "no such leader connection registered", f)
}

// FilenameOfLeader panics if passed a non-registered connection.
func TestRegistry_FilenameOfLeaderNonRegisteredConn(t *testing.T) {
	conn := newConn()
	registry := connection.NewRegistry()
	f := func() { registry.FilenameOfLeader(conn) }
	assert.PanicsWithValue(t, "no database for the given connection", f)
}

// Add a new follower connection to the registry.
func TestRegistry_AddFollower(t *testing.T) {
	conn := newConn()
	registry := connection.NewRegistry()
	registry.AddFollower("test.db", conn)
	assert.Equal(t, []string{"test.db"}, registry.FilenamesOfFollowers())
}

// AddFollower panics if the given filename already has a registered follower
// connection.
func TestRegistry_AddFollowerAlreadyRegistered(t *testing.T) {
	conn := newConn()
	registry := connection.NewRegistry()
	registry.AddFollower("test.db", conn)
	f := func() { registry.AddFollower("test.db", conn) }
	assert.PanicsWithValue(t, "follower connection for 'test.db' already registered", f)
}

// DelFollower a removes a follower connection from the registry.
func TestRegistry_DelFollower(t *testing.T) {
	conn := newConn()
	registry := connection.NewRegistry()
	registry.AddFollower("test.db", conn)
	registry.DelFollower("test.db")
	assert.Equal(t, []string{}, registry.FilenamesOfFollowers())
}

// DelFollower panics if the given filename is not associated with a registered
// follower connection.
func TestRegistry_DelFollowerAlreadyRegistered(t *testing.T) {
	registry := connection.NewRegistry()
	f := func() { registry.DelFollower("test.db") }
	assert.PanicsWithValue(t, "follower connection for 'test.db' is not registered", f)
}

// Follower panics if no follower with the given filename is registered.
func TestRegistry_FollowerNotRegistered(t *testing.T) {
	registry := connection.NewRegistry()
	f := func() { registry.Follower("test.db") }
	assert.PanicsWithValue(t, "no follower connection for 'test.db'", f)
}

// Create a new sqlite connection against a memory database.
func newConn() *sqlite3.SQLiteConn {
	driver := &sqlite3.SQLiteDriver{}
	conn, err := driver.Open(":memory:")
	if err != nil {
		panic(fmt.Errorf("failed to open in-memory database: %v", err))
	}
	return conn.(*sqlite3.SQLiteConn)
}
