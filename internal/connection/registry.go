package connection

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/CanonicalLtd/go-sqlite3"
)

// Registry is a dqlite node-level data structure that tracks all
// SQLite connections opened on the node, either in leader replication
// mode or follower replication mode.
type Registry struct {
	mu        sync.RWMutex                   // Serialize access to internal state.
	leaders   map[*sqlite3.SQLiteConn]string // Map leader connections to database filenames.
	followers map[string]*sqlite3.SQLiteConn // Map database filenames to follower connections.
	serial    map[*sqlite3.SQLiteConn]uint64 // Map a connection to its serial number.
}

// NewRegistry creates a new connections registry.
func NewRegistry() *Registry {
	return &Registry{
		leaders:   map[*sqlite3.SQLiteConn]string{},
		followers: map[string]*sqlite3.SQLiteConn{},
		serial:    map[*sqlite3.SQLiteConn]uint64{},
	}
}

// AddLeader adds a new leader connection to the registry.
func (r *Registry) AddLeader(filename string, conn *sqlite3.SQLiteConn) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.leaders[conn] = filename
	r.addConn(conn)
}

// DelLeader removes the given leader connection from the registry.
func (r *Registry) DelLeader(conn *sqlite3.SQLiteConn) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.leaders[conn]; !ok {
		panic("no such leader connection registered")
	}

	delete(r.leaders, conn)
	r.delConn(conn)
}

// Leaders returns all open leader connections for the database with
// the given filename.
func (r *Registry) Leaders(filename string) []*sqlite3.SQLiteConn {
	r.mu.RLock()
	defer r.mu.RUnlock()

	conns := []*sqlite3.SQLiteConn{}
	for conn := range r.leaders {
		if r.leaders[conn] == filename {
			conns = append(conns, conn)
		}
	}
	return conns
}

// FilenameOfLeader returns the filename of the database associated with the
// given leader connection.
//
// If conn is not a registered leader connection, this method will panic.
func (r *Registry) FilenameOfLeader(conn *sqlite3.SQLiteConn) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	name, ok := r.leaders[conn]
	if !ok {
		panic("no database for the given connection")
	}
	return name
}

// AddFollower adds a new follower connection to the registry.
//
// If a follower connection for the database with the given filename is already
// registered, this method panics.
func (r *Registry) AddFollower(filename string, conn *sqlite3.SQLiteConn) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.followers[filename]; ok {
		panic(fmt.Sprintf("follower connection for '%s' already registered", filename))
	}

	r.followers[filename] = conn
	r.addConn(conn)
}

// DelFollower removes the follower registered against the database with the
// given filename.
func (r *Registry) DelFollower(filename string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	conn, ok := r.followers[filename]
	if !ok {
		panic(fmt.Sprintf("follower connection for '%s' is not registered", filename))
	}

	delete(r.followers, filename)
	r.delConn(conn)
}

// FilenamesOfFollowers returns the filenames for all databases which currently
// have registered follower connections.
func (r *Registry) FilenamesOfFollowers() []string {
	names := []string{}
	for name := range r.followers {
		names = append(names, name)
	}
	return names
}

// HasFollower checks whether the registry has a follower connection registered
// against the database with the given filename.
func (r *Registry) HasFollower(filename string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.followers[filename]
	return ok
}

// Follower returns the follower connection used to replicate the
// database identified by the given filename.
//
// If there's no follower connection registered for the database with the given
// filename, this method panics.
func (r *Registry) Follower(filename string) *sqlite3.SQLiteConn {
	r.mu.RLock()
	defer r.mu.RUnlock()

	conn, ok := r.followers[filename]
	if !ok {
		panic(fmt.Sprintf("no follower connection for '%s'", filename))
	}
	return conn
}

// Serial returns a serial number uniquely identifying the given registered
// connection.
func (r *Registry) Serial(conn *sqlite3.SQLiteConn) uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	serial, ok := r.serial[conn]

	if !ok {
		panic("connection is not registered")
	}

	return serial
}

// Dump the content of the registry, useful for debugging.
func (r *Registry) Dump() string {
	buffer := bytes.NewBuffer(nil)
	fmt.Fprintf(buffer, "leaders:\n")
	for conn, name := range r.leaders {
		fmt.Fprintf(buffer, "-> %d: %s\n", r.Serial(conn), name)
	}
	fmt.Fprintf(buffer, "followers:\n")
	for name, conn := range r.followers {
		fmt.Fprintf(buffer, "-> %d: %s\n", r.Serial(conn), name)
	}
	return buffer.String()
}

// Add a new connection (either leader or follower) to the registry and assign
// it a serial number.
func (r *Registry) addConn(conn *sqlite3.SQLiteConn) {
	if serial, ok := r.serial[conn]; ok {
		panic(fmt.Sprintf("connection is already registered with serial %d", serial))
	}

	atomic.AddUint64(&serial, 1)
	r.serial[conn] = serial
}

// Delete a connection (either leader or follower) from the registry
func (r *Registry) delConn(conn *sqlite3.SQLiteConn) {
	if _, ok := r.serial[conn]; !ok {
		panic("connection has no serial assigned")
	}

	delete(r.serial, conn)
}

// Monotonic counter for identifying connections for tracing and debugging
// purposes.
var serial uint64
