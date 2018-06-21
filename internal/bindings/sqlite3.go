package bindings

/*
#cgo linux LDFLAGS: -lsqlite3

#include <sqlite3.h>

static void config() {
  sqlite3_config(0);
}
*/
import "C"

// Hello api.
func Hello() {
	C.config()
}
