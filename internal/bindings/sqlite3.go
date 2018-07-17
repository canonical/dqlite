package bindings

/*
#cgo linux LDFLAGS: -lsqlite3

#include <stdlib.h>
#include <sqlite3.h>

// WAL replication trampolines.
int walReplicationBegin(int handle, sqlite3 *db);
int walReplicationAbort(int handle, sqlite3 *db);
int walReplicationFrames(int handle, sqlite3 *db,
      int, int, sqlite3_wal_replication_frame*, unsigned, int);
int walReplicationUndo(int handle, sqlite3 *db);
int walReplicationEnd(int handle, sqlite3 *db);

// Wal replication methods.
static int sqlite3__wal_replication_begin(sqlite3_wal_replication *r, void *arg)
{
  int handle = *(int*)(r->pAppData);
  sqlite3 *db = (sqlite3*)(arg);
  return walReplicationBegin(handle, db);
}

static int sqlite3__wal_replication_abort(sqlite3_wal_replication *r, void *arg)
{
  int handle = *(int*)(r->pAppData);
  sqlite3 *db = (sqlite3*)(arg);
  return walReplicationAbort(handle, db);
}

static int sqlite3__wal_replication_frames(sqlite3_wal_replication *r, void *arg,
  int szPage, int nFrame, sqlite3_wal_replication_frame *aFrame,
  unsigned nTruncate, int isCommit)
{
  int handle = *(int*)(r->pAppData);
  sqlite3 *db = (sqlite3*)(arg);
  return walReplicationFrames(handle, db, szPage, nFrame, aFrame, nTruncate, isCommit);
}

static int sqlite3__wal_replication_undo(sqlite3_wal_replication *r, void *arg)
{
  int handle = *(int*)(r->pAppData);
  sqlite3 *db = (sqlite3*)(arg);
  return walReplicationUndo(handle, db);
}

static int sqlite3__wal_replication_end(sqlite3_wal_replication *r, void *arg)
{
  int handle = *(int*)(r->pAppData);
  sqlite3 *db = (sqlite3*)(arg);
  return walReplicationEnd(handle, db);
}

static int sqlite3__wal_replication_register(char *name, int handle){
  sqlite3_wal_replication *replication;
  void *ctx;
  int rc;

  replication = (sqlite3_wal_replication*)sqlite3_malloc(sizeof(sqlite3_wal_replication));
  if (replication == NULL) {
    return SQLITE_NOMEM;
  }

  ctx = (void*)sqlite3_malloc(sizeof(int));
  if (ctx == NULL) {
    sqlite3_free(replication);
    return SQLITE_NOMEM;
  }
  *(int*)(ctx) = handle;

  replication->iVersion = 1;
  replication->zName    = (const char*)(name);
  replication->pAppData = ctx;
  replication->xBegin   = sqlite3__wal_replication_begin;
  replication->xAbort   = sqlite3__wal_replication_abort;
  replication->xFrames  = sqlite3__wal_replication_frames;
  replication->xUndo    = sqlite3__wal_replication_undo;
  replication->xEnd     = sqlite3__wal_replication_end;

  rc = sqlite3_wal_replication_register(replication, 0);

  return rc;
}

static int sqlite3__wal_replication_unregister(char *name, int *handle) {
  int rc;

  sqlite3_wal_replication *replication = sqlite3_wal_replication_find(name);
  if (replication == NULL) {
    return SQLITE_ERROR;
  }

  *handle = *(int*)(replication->pAppData);

  rc = sqlite3_wal_replication_unregister(replication);
  if (rc != SQLITE_OK) {
    return rc;
  }

  sqlite3_free(replication->pAppData);
  free((char*)(replication->zName));
  sqlite3_free(replication);

  return SQLITE_OK;
}

*/
import "C"
import (
	"database/sql/driver"
	"fmt"
	"io"
	"unsafe"

	"github.com/pkg/errors"
)

// SQLite constants.
const (
	OpenReadWrite = C.SQLITE_OPEN_READWRITE
	OpenReadOnly  = C.SQLITE_OPEN_READONLY
	OpenCreate    = C.SQLITE_OPEN_CREATE

	Integer = C.SQLITE_INTEGER
	Float   = C.SQLITE_FLOAT
	Text    = C.SQLITE_TEXT
	Blob    = C.SQLITE_BLOB
	Null    = C.SQLITE_NULL

	ErrError               = C.SQLITE_ERROR
	ErrInternal            = C.SQLITE_INTERNAL
	ErrInterrupt           = C.SQLITE_INTERRUPT
	ErrBusy                = C.SQLITE_BUSY
	ErrIoErr               = C.SQLITE_IOERR
	ErrIoErrNotLeader      = C.SQLITE_IOERR_NOT_LEADER
	ErrIoErrLeadershipLost = C.SQLITE_IOERR_LEADERSHIP_LOST
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
		return nil, fmt.Errorf("failed to set follower mode")
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
		return nil, fmt.Errorf("failed to set leader mode")
	}

	return (*Conn)(unsafe.Pointer(db)), nil
}

// Error holds information about a SQLite error.
type Error struct {
	Code         int
	ExtendedCode int
	Message      string
}

func (e Error) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return C.GoString(C.sqlite3_errstr(C.int(e.ExtendedCode)))
}

func lastError(db *C.sqlite3) Error {
	return Error{
		Code:         int(C.sqlite3_errcode(db)),
		ExtendedCode: int(C.sqlite3_extended_errcode(db)),
		Message:      C.GoString(C.sqlite3_errmsg(db)),
	}
}

// Open a SQLite connection.
func Open(name string, vfs string) (*Conn, error) {
	flags := OpenReadWrite | OpenCreate

	db, err := open(name, flags, vfs)
	if err != nil {
		return nil, err
	}

	return (*Conn)(unsafe.Pointer(db)), nil
}

// Open a SQLite connection, setting anything that is common between leader and
// follower connections.
func open(name string, flags int, vfs string) (*C.sqlite3, error) {
	// Open the database.
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	cvfs := C.CString(vfs)
	defer C.free(unsafe.Pointer(cvfs))

	var db *C.sqlite3
	rc := C.sqlite3_open_v2(cname, &db, C.int(flags), cvfs)
	if rc != C.SQLITE_OK {
		return nil, errors.Wrap(lastError(db), "failed to open database")
	}

	var errmsg *C.char

	// Set the page size. TODO: make page size configurable?
	sql := C.CString("PRAGMA page_size=4096")
	defer C.free(unsafe.Pointer(sql))

	rc = C.sqlite3_exec(db, sql, nil, nil, &errmsg)
	if rc != C.SQLITE_OK {
		return nil, errors.Wrap(lastError(db), "failed to exec PRAGMA page_size")
	}

	// Disable syncs.
	sql = C.CString("PRAGMA synchronous=OFF")
	defer C.free(unsafe.Pointer(sql))

	rc = C.sqlite3_exec(db, sql, nil, nil, &errmsg)
	if rc != C.SQLITE_OK {
		return nil, errors.Wrap(lastError(db), "failed to exec PRAGMA synchronous")
	}

	// Set WAL journaling.
	sql = C.CString("PRAGMA journal_mode=WAL")
	defer C.free(unsafe.Pointer(sql))

	rc = C.sqlite3_exec(db, sql, nil, nil, &errmsg)
	if rc != C.SQLITE_OK {
		return nil, errors.Wrap(lastError(db), "failed to exec PRAGMA journal_mode")
	}

	return db, nil
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

type Rows struct {
	db   *C.sqlite3
	stmt *C.sqlite3_stmt
}

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

// WalReplicationFollower switches the given sqlite connection to follower WAL
// replication mode. In this mode no regular operation is possible, and the
// connection should be driven with the WalReplicationFrames, and
// WalReplicationUndo APIs.
func (c *Conn) WalReplicationFollower() error {
	db := (*C.sqlite3)(unsafe.Pointer(c))

	rc := C.sqlite3_wal_replication_follower(db, walReplicationSchema)
	if rc != C.SQLITE_OK {
		return lastError(db)
	}

	return nil
}

// WalReplicationFrames writes the given batch of frames to the write-ahead log
// linked to the given connection.
//
// This method must be called with a "follower" connection, meant to replicate
// the "leader" one.
func (c *Conn) WalReplicationFrames(info WalReplicationFrameInfo) error {
	db := (*C.sqlite3)(unsafe.Pointer(c))

	rc := C.sqlite3_wal_replication_frames(
		db, walReplicationSchema, info.isBegin, info.szPage, info.nFrame,
		info.aPgno, info.aPage, info.nTruncate, info.isCommit)
	if rc != C.SQLITE_OK {
		return lastError(db)
	}

	return nil
}

// WalReplicationUndo rollbacks a write transaction in the given sqlite
// connection. This should be called with a "follower" connection, meant to
// replicate the "leader" one.
func (c *Conn) WalReplicationUndo() error {
	db := (*C.sqlite3)(unsafe.Pointer(c))

	rc := C.sqlite3_wal_replication_undo(db, walReplicationSchema)
	if rc != C.SQLITE_OK {
		return lastError(db)
	}
	return nil
}

// WalCheckpoint triggers a WAL checkpoint on the given database attached to the
// connection. See https://sqlite.org/c3ref/wal_checkpoint_v2.html
func (c *Conn) WalCheckpoint(schema string, mode WalCheckpointMode) (int, int, error) {
	db := (*C.sqlite3)(unsafe.Pointer(c))

	var size C.int
	var ckpt C.int
	var err error

	// Convert to C types
	zDb := C.CString(schema)
	defer C.free(unsafe.Pointer(zDb))

	rc := C.sqlite3_wal_checkpoint_v2(db, zDb, C.int(mode), &size, &ckpt)
	if rc != 0 {
		return -1, -1, lastError(db)
	}

	return int(size), int(ckpt), err
}

// WalCheckpointMode defines all valid values for the "checkpoint mode" parameter
// of the WalCheckpointV2 API. See https://sqlite.org/c3ref/wal_checkpoint_v2.html.
type WalCheckpointMode int

// WAL checkpoint modes
const (
	WalCheckpointPassive  = WalCheckpointMode(C.SQLITE_CHECKPOINT_PASSIVE)
	WalCheckpointFull     = WalCheckpointMode(C.SQLITE_CHECKPOINT_FULL)
	WalCheckpointRestart  = WalCheckpointMode(C.SQLITE_CHECKPOINT_RESTART)
	WalCheckpointTruncate = WalCheckpointMode(C.SQLITE_CHECKPOINT_TRUNCATE)
)

// PageNumber identifies a single database or WAL page.
type PageNumber C.unsigned

// FrameNumber identifies a single frame in the WAL.
type FrameNumber C.unsigned

// WalReplicationFrameList holds information about a single batch of WAL frames
// that are being dispatched for replication by a leader connection.
//
// They map to the parameters of the sqlite3_wal_replication.xFrames API
type WalReplicationFrameList struct {
	szPage    C.int
	nFrame    C.int
	aFrame    *C.sqlite3_wal_replication_frame
	nTruncate C.uint
	isCommit  C.int
}

// PageSize returns the page size of this batch of WAL frames.
func (l *WalReplicationFrameList) PageSize() int {
	return int(l.szPage)
}

// Len returns the number of WAL frames in this batch.
func (l *WalReplicationFrameList) Len() int {
	return int(l.nFrame)
}

// Truncate returns the size of the database in pages after this batch of WAL
// frames is applied.
func (l *WalReplicationFrameList) Truncate() uint {
	return uint(l.nTruncate)
}

// Frame returns information about the i'th frame in the batch.
func (l *WalReplicationFrameList) Frame(i int) (unsafe.Pointer, PageNumber, FrameNumber) {
	pFrame := (*C.sqlite3_wal_replication_frame)(unsafe.Pointer(
		uintptr(unsafe.Pointer(l.aFrame)) +
			unsafe.Sizeof(*l.aFrame)*uintptr(i),
	))
	return pFrame.pBuf, PageNumber(pFrame.pgno), FrameNumber(pFrame.iPrev)
}

// IsCommit returns whether this batch of WAL frames concludes a transaction.
func (l *WalReplicationFrameList) IsCommit() bool {
	return l.isCommit > 0
}

// WalReplication implements the interface required by sqlite3_wal_replication
type WalReplication interface {
	// Begin a new write transaction. The implementation should check
	// that the database is eligible for starting a replicated write
	// transaction (e.g. this node is the leader), and perform internal
	// state changes as appropriate.
	Begin(*Conn) int

	// Abort a write transaction. The implementation should clear any
	// state previously set by the Begin hook.
	Abort(*Conn) int

	// Write new frames to the write-ahead log. The implementation should
	// broadcast this write to other nodes and wait for a quorum.
	Frames(*Conn, WalReplicationFrameList) int

	// Undo a write transaction. The implementation should broadcast
	// this event to other nodes and wait for a quorum. The return code
	// is currently ignored by SQLite.
	Undo(*Conn) int

	// End a write transaction. The implementation should update its
	// internal state and be ready for a new transaction.
	End(*Conn) int
}

// FindWalReplication finds the replication implementation with the given name,
// if one is registered.
func FindWalReplication(name string) WalReplication {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	r := C.sqlite3_wal_replication_find(cname)
	if r == nil {
		return nil
	}

	handle := *(*C.int)(r.pAppData)

	return walReplicationHandles[handle]
}

// RegisterWalReplication registers a WalReplication implementation under the
// given name.
func RegisterWalReplication(name string, replication WalReplication) error {
	handle := walReplicationHandlesSerial

	walReplicationHandles[handle] = replication

	walReplicationHandlesSerial++

	rc := C.sqlite3__wal_replication_register(C.CString(name), handle)
	if rc != C.SQLITE_OK {
		return fmt.Errorf("registration failed: %d", rc)
	}

	return nil
}

// UnregisterWalReplication unregisters the given WalReplication
// implementation.
func UnregisterWalReplication(name string) error {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	var handle C.int
	rc := C.sqlite3__wal_replication_unregister(cname, &handle)
	if rc != C.SQLITE_OK {
		return fmt.Errorf("unregistration failed: %d", rc)
	}

	// Cleanup the entry
	delete(walReplicationHandles, handle)

	return nil
}

// WalReplicationFrameInfo information about a single batch of WAL frames that
// are being replicated by a follower connection.
type WalReplicationFrameInfo struct {
	isBegin   C.int
	szPage    C.int
	nFrame    C.int
	aPgno     *C.unsigned
	aPage     unsafe.Pointer
	nTruncate C.uint
	isCommit  C.int
}

// IsBegin sets the C isBegin parameter for sqlite3_wal_replication_frames.
func (i *WalReplicationFrameInfo) IsBegin(flag bool) {
	if flag {
		i.isBegin = C.int(1)
	} else {
		i.isBegin = C.int(0)
	}
}

// PageSize sets the C szPage parameter for sqlite3_wal_replication_frames.
func (i *WalReplicationFrameInfo) PageSize(size int) {
	i.szPage = C.int(size)
}

// Len sets the C nFrame parameter for sqlite3_wal_replication_frames.
func (i *WalReplicationFrameInfo) Len(n int) {
	i.nFrame = C.int(n)
}

// Pages sets the C aPgno and aPage parameters for sqlite3_wal_replication_frames.
func (i *WalReplicationFrameInfo) Pages(numbers []PageNumber, data unsafe.Pointer) {
	i.aPgno = (*C.unsigned)(&numbers[0])
	i.aPage = data
}

// Truncate sets the nTruncate parameter for sqlite3_wal_replication_frames.
func (i *WalReplicationFrameInfo) Truncate(truncate uint) {
	i.nTruncate = C.unsigned(truncate)
}

// IsCommit sets the isCommit parameter for sqlite3_wal_replication_frames.
func (i *WalReplicationFrameInfo) IsCommit(flag bool) {
	if flag {
		i.isCommit = C.int(1)
	} else {
		i.isCommit = C.int(0)
	}
}

func (i *WalReplicationFrameInfo) IsCommitGet() bool {
	return i.isCommit > 0
}

//export walReplicationBegin
func walReplicationBegin(handle C.int, db *C.sqlite3) C.int {
	replication := walReplicationHandles[handle]

	return C.int(replication.Begin((*Conn)(unsafe.Pointer(db))))
}

//export walReplicationAbort
func walReplicationAbort(handle C.int, db *C.sqlite3) C.int {
	replication := walReplicationHandles[handle]

	return C.int(replication.Abort((*Conn)(unsafe.Pointer(db))))
}

//export walReplicationFrames
func walReplicationFrames(
	handle C.int,
	db *C.sqlite3,
	szPage C.int,
	nFrame C.int,
	aFrame *C.sqlite3_wal_replication_frame,
	nTruncate C.uint,
	isCommit C.int,
) C.int {
	replication := walReplicationHandles[handle]

	list := WalReplicationFrameList{
		szPage:    szPage,
		nFrame:    nFrame,
		aFrame:    aFrame,
		nTruncate: nTruncate,
		isCommit:  isCommit,
	}

	return C.int(replication.Frames((*Conn)(unsafe.Pointer(db)), list))
}

//export walReplicationUndo
func walReplicationUndo(handle C.int, db *C.sqlite3) C.int {
	replication := walReplicationHandles[handle]

	return C.int(replication.Undo((*Conn)(unsafe.Pointer(db))))
}

//export walReplicationEnd
func walReplicationEnd(handle C.int, db *C.sqlite3) C.int {
	replication := walReplicationHandles[handle]

	return C.int(replication.End((*Conn)(unsafe.Pointer(db))))
}

// Map C.int to WalReplication instances to avoid passing Go pointers to C.
//
// We do not protect this map with a lock since typically just one long-lived
// WalReplication instance should be registered (except for unit tests).
var walReplicationHandlesSerial C.int
var walReplicationHandles = map[C.int]WalReplication{}

// Hard-coded main schema name.
//
// TODO: support replicating also attached databases.
var walReplicationSchema = C.CString("main")
