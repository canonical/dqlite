package connection

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/CanonicalLtd/go-sqlite3x"
	"github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
)

// Registry is a dqlite node-level data structure that tracks all
// SQLite connections opened on the node, either in leader replication
// mode or follower replication mode.
type Registry struct {
	mu        sync.RWMutex                   // Serialize access to internal state.
	leaders   map[*sqlite3.SQLiteConn]string // Leader connections to database filenames.
	followers map[string]*sqlite3.SQLiteConn // Database filenames to follower connections.
	serial    map[*sqlite3.SQLiteConn]uint64 // Map a connection to its serial number.

	dir string // Directory where we store database files.
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
func (r *Registry) DelLeader(conn *sqlite3.SQLiteConn) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.leaders[conn]; !ok {
		panic("no such leader connection registered")
	}

	delete(r.leaders, conn)
	r.delConn(conn)

	return nil
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

// NewRegistryLegacy creates a new connections registry, managing
// connections against database files in the given directory.
func NewRegistryLegacy(dir string) *Registry {
	return &Registry{
		dir:       dir,
		leaders:   map[*sqlite3.SQLiteConn]string{},
		followers: map[string]*sqlite3.SQLiteConn{},
		serial:    map[*sqlite3.SQLiteConn]uint64{},
	}
}

// Dir is the directory where databases are kept.
func (r *Registry) Dir() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.dir
}

// Backup a single database using the given leader connection. It
// returns two slices of data, one the content of the backup database
// and one is the current content of the WAL file.
func (r *Registry) Backup(name string) ([]byte, []byte, error) {
	sourceConn, err := r.open(name)
	if err != nil {
		return nil, nil, err
	}
	defer sourceConn.Close()

	backupConn, err := r.openBackup(name)
	if err != nil {
		return nil, nil, err
	}
	for _, path := range []string{
		sqlite3x.DatabaseFilename(backupConn),
		sqlite3x.WalFilename(backupConn),
		sqlite3x.ShmFilename(backupConn),
	} {
		defer os.Remove(path)
	}
	defer backupConn.Close()

	backup, err := backupConn.Backup("main", sourceConn, "main")
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to init backup database")
	}

	done, err := backup.Step(-1)
	backup.Close()
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to backup database")
	}
	if !done {
		return nil, nil, fmt.Errorf("database backup not complete")
	}

	database, err := r.readDatabaseContent(sourceConn)
	if err != nil {
		return nil, nil, err
	}

	wal, err := r.readWalContent(backupConn)
	if err != nil {
		return nil, nil, err
	}

	return database, wal, nil
}

// Restore the given database and WAL backups.
func (r *Registry) Restore(name string, database []byte, wal []byte) error {
	if err := r.writeDatabaseContent(name, database); err != nil {
		return err
	}
	if err := r.writeWalContent(name, wal); err != nil {
		return err
	}
	return nil
}

// Purge removes all database files in our directory, including the
// directory itself.
func (r *Registry) Purge() error {
	for conn := range r.leaders {
		r.DelLeader(conn)
		if err := CloseLeader(conn); err != nil {
			return err
		}
	}
	for name, conn := range r.followers {
		r.DelFollower(name)
		if err := conn.Close(); err != nil {
			return err
		}
	}
	return os.RemoveAll(r.dir)
}

// Open returns a new SQLite connection to a database in our
// directory, configured with WAL journaling (automatic checkpoints
// are disabled and the WAL always kept persistent after connections
// close). The given DSN will be tracked in the registry and
// associated with the connection.
func (r *Registry) open(dsn string) (*sqlite3.SQLiteConn, error) {
	driver := &sqlite3.SQLiteDriver{}
	conn, err := driver.Open(filepath.Join(r.dir, dsn))
	if err != nil {
		return nil, err
	}
	// Convert driver.Conn interface to concrete sqlite3.SQLiteConn.
	sqliteConn := conn.(*sqlite3.SQLiteConn)

	// Ensure journal mode is set to WAL
	if err := sqlite3x.JournalModePragma(sqliteConn, sqlite3x.JournalWal); err != nil {
		return nil, err
	}

	// Ensure we don't truncate the WAL on exit.
	if err := sqlite3x.JournalSizeLimitPragma(sqliteConn, -1); err != nil {
		return nil, err
	}

	if err := sqlite3x.DatabaseNoCheckpointOnClose(sqliteConn); err != nil {
		return nil, err
	}

	return sqliteConn, nil
}

// Open a new database connection against a temporary backup database
// file named against the given DSN.
func (r *Registry) openBackup(name string) (*sqlite3.SQLiteConn, error) {
	// Create a temporary file using the source DSN filename as prefix.
	tempFile, err := ioutil.TempFile(r.dir, name)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create temp file for backup")
	}
	tempFile.Close()

	backupConn, err := r.open(path.Base(tempFile.Name()))
	if err != nil {
		os.Remove(tempFile.Name())
		return nil, errors.Wrap(err, "failed to open backup database")
	}
	return backupConn, nil
}

// Read the current content of the database file associated with the given
// connection.
func (r *Registry) readDatabaseContent(conn *sqlite3.SQLiteConn) ([]byte, error) {
	path := sqlite3x.DatabaseFilename(conn)
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to read database content at %s", path))
	}
	return data, nil
}

// Read the current content of the WAL associated with the given
// connection.
func (r *Registry) readWalContent(conn *sqlite3.SQLiteConn) ([]byte, error) {
	path := sqlite3x.WalFilename(conn)
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to read WAL content at %s", path))
	}
	return data, nil
}

// Write the the content of a database backup to the DSN filename associated
// with the given identifier.
func (r *Registry) writeDatabaseContent(name string, database []byte) error {
	path := filepath.Join(r.Dir(), name)
	if err := ioutil.WriteFile(path, database, 0600); err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to write database content at %s", path))
	}
	return nil
}

// Write the the content of a WAL backup to the DSN filename associated
// with the given identifier.
func (r *Registry) writeWalContent(name string, wal []byte) error {
	path := filepath.Join(r.Dir(), name+"-wal")
	if err := ioutil.WriteFile(path, wal, 0600); err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to write wal content at %s", path))
	}
	return nil
}

// Monotonic counter for identifying connections for tracing and debugging
// purposes.
var serial uint64
