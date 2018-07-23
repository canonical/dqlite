package bindings

/*
#include <stdlib.h>
#include <sqlite3.h>

// Wrapper around sqlite3_db_config() for invoking the
// SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE opcode, since there's no way to use C
// varargs from Go.
static int sqlite3__config_no_ckpt_on_close(sqlite3 *db, int value, int *pValue) {
  return sqlite3_db_config(db, SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, value, pValue);
}

*/
import "C"
import (
	"database/sql/driver"
	"io"
	"unsafe"

	"github.com/pkg/errors"
)

// Open modes.
const (
	OpenReadWrite = C.SQLITE_OPEN_READWRITE
	OpenReadOnly  = C.SQLITE_OPEN_READONLY
	OpenCreate    = C.SQLITE_OPEN_CREATE
)

// Conn is a Go wrapper around a sqlite3 database instance.
type Conn C.sqlite3

// OpenFollower is a wrapper around Open that opens connection in follower
// replication mode, and sets any additional dqlite-related options.
func OpenFollower(name string, vfs string) (*Conn, error) {
	flags := OpenReadWrite | OpenCreate

	db, err := open(name, flags, vfs)
	if err != nil {
		return nil, err
	}

	rc := C.sqlite3_wal_replication_follower(db, walReplicationSchema)
	if rc != C.SQLITE_OK {
		err := codeToError(rc)
		return nil, errors.Wrap(err, "failed to set follower mode")
	}

	return (*Conn)(unsafe.Pointer(db)), nil
}

// OpenLeader is a wrapper around open that opens connection in leader
// replication mode, and sets any additional dqlite-related options.
func OpenLeader(name string, vfs string, replication string) (*Conn, error) {
	flags := OpenReadWrite | OpenCreate

	db, err := open(name, flags, vfs)
	if err != nil {
		return nil, err
	}

	creplication := C.CString(replication)
	defer C.free(unsafe.Pointer(creplication))

	rc := C.sqlite3_wal_replication_leader(db, walReplicationSchema, creplication, unsafe.Pointer(db))
	if rc != C.SQLITE_OK {
		err := codeToError(rc)
		return nil, errors.Wrap(err, "failed to set leader mode")
	}

	return (*Conn)(unsafe.Pointer(db)), nil
}

// Open a SQLite connection.
func Open(name string, vfs string) (*Conn, error) {
	flags := OpenReadWrite | OpenCreate

	db, err := open(name, flags, vfs)
	if err != nil {
		return nil, err
	}

	var value C.int
	rc := C.sqlite3__config_no_ckpt_on_close(db, 1, &value)
	if rc != C.SQLITE_OK {
		err := lastError(db)
		C.sqlite3_close_v2(db)
		return nil, errors.Wrap(err, "failed to disable checkpoint on close")
	}

	return (*Conn)(unsafe.Pointer(db)), nil
}

// Open a SQLite connection, setting anything that is common between leader and
// follower connections.
func open(name string, flags int, vfs string) (db *C.sqlite3, err error) {
	// Open the database.
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	cvfs := C.CString(vfs)
	defer C.free(unsafe.Pointer(cvfs))

	rc := C.sqlite3_open_v2(cname, &db, C.int(flags), cvfs)
	if rc != C.SQLITE_OK {
		err = errors.Wrap(lastError(db), "failed to open database")
		return
	}

	defer func() {
		if err != nil {
			C.sqlite3_close_v2(db)
		}
	}()

	var errmsg *C.char

	// Set the page size. TODO: make page size configurable?
	sql := C.CString("PRAGMA page_size=4096")
	defer C.free(unsafe.Pointer(sql))

	rc = C.sqlite3_exec(db, sql, nil, nil, &errmsg)
	if rc != C.SQLITE_OK {
		err = errors.Wrap(lastError(db), "failed to exec PRAGMA page_size")
		return
	}

	// Disable syncs.
	sql = C.CString("PRAGMA synchronous=OFF")
	defer C.free(unsafe.Pointer(sql))

	rc = C.sqlite3_exec(db, sql, nil, nil, &errmsg)
	if rc != C.SQLITE_OK {
		err = errors.Wrap(lastError(db), "failed to exec PRAGMA synchronous")
		return
	}

	// Set WAL journaling.
	sql = C.CString("PRAGMA journal_mode=WAL")
	defer C.free(unsafe.Pointer(sql))

	rc = C.sqlite3_exec(db, sql, nil, nil, &errmsg)
	if rc != C.SQLITE_OK {
		err = errors.Wrap(lastError(db), "failed to exec PRAGMA journal_mode")
		return
	}

	return
}

// Close the connection.
func (c *Conn) Close() error {
	db := (*C.sqlite3)(unsafe.Pointer(c))

	rc := C.sqlite3_close(db)
	if rc != C.SQLITE_OK {
		return lastError(db)
	}

	return nil
}

// Filename of the underlying database file.
func (c *Conn) Filename() string {
	db := (*C.sqlite3)(unsafe.Pointer(c))

	return C.GoString(C.sqlite3_db_filename(db, walReplicationSchema))
}

// Query the database.
func (c *Conn) Query(query string) (*Rows, error) {
	db := (*C.sqlite3)(unsafe.Pointer(c))

	var stmt *C.sqlite3_stmt
	var tail *C.char

	sql := C.CString(query)
	defer C.free(unsafe.Pointer(sql))

	rc := C.sqlite3_prepare(db, sql, -1, &stmt, &tail)
	if rc != C.SQLITE_OK {
		return nil, lastError(db)
	}

	rows := &Rows{db: db, stmt: stmt}

	return rows, nil
}

// Exec executes a query.
func (c *Conn) Exec(query string) error {
	db := (*C.sqlite3)(unsafe.Pointer(c))

	var errmsg *C.char

	sql := C.CString(query)
	defer C.free(unsafe.Pointer(sql))

	rc := C.sqlite3_exec(db, sql, nil, nil, &errmsg)
	if rc != C.SQLITE_OK {
		return lastError(db)
	}

	return nil
}

// Rows represents a result set.
type Rows struct {
	db   *C.sqlite3
	stmt *C.sqlite3_stmt
}

// Next fetches the next row of a result set.
func (r *Rows) Next(values []driver.Value) error {
	rc := C.sqlite3_step(r.stmt)
	if rc == C.SQLITE_DONE {
		rc = C.sqlite3_finalize(r.stmt)
		if rc != C.SQLITE_OK {
			return lastError(r.db)
		}
		return io.EOF
	}
	if rc != C.SQLITE_ROW {
		return lastError(r.db)
	}

	for i := range values {
		values[i] = int64(C.sqlite3_column_int64(r.stmt, C.int(i)))
	}

	return nil
}
