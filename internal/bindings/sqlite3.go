package bindings

/*
#cgo linux LDFLAGS: -lsqlite3

#include <sqlite3.h>
*/
import "C"

// SQLite constants.
const (
	DbOpenReadWrite = C.SQLITE_OPEN_READWRITE
	DbOpenCreate    = C.SQLITE_OPEN_CREATE
)
