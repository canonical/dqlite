package bindings

/*
#cgo linux LDFLAGS: -lsqlite3

#include <sqlite3.h>
*/
import "C"

// SQLite constants.
const (
	DbOpenReadWrite = C.SQLITE_OPEN_READWRITE
	DbOpenReadOnly  = C.SQLITE_OPEN_READONLY
	DbOpenCreate    = C.SQLITE_OPEN_CREATE

	Integer = C.SQLITE_INTEGER
	Float   = C.SQLITE_FLOAT
	Text    = C.SQLITE_TEXT
	Blob    = C.SQLITE_BLOB
	Null    = C.SQLITE_NULL
)
