package bindings

/*
#include <stdlib.h>

#include <sqlite3.h>
#include <dqlite.h>
*/
import "C"
import (
	"unsafe"
)

// Vfs is a Go wrapper arround dqlite's in-memory VFS implementation.
type Vfs C.sqlite3_vfs

// NewVfs registers an in-memory VFS instance under the given name.
func NewVfs(name string) (*Vfs, error) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	vfs := C.dqlite_vfs_create(cname)
	if vfs == nil {
		return nil, codeToError(C.DQLITE_NOMEM)
	}

	rc := C.sqlite3_vfs_register(vfs, 0)
	if rc != 0 {
		return nil, codeToError(rc)
	}

	return (*Vfs)(unsafe.Pointer(vfs)), nil
}

// Close unregisters this in-memory VFS instance.
func (v *Vfs) Close() {
	vfs := (*C.sqlite3_vfs)(unsafe.Pointer(v))
	C.sqlite3_vfs_unregister(vfs)
	C.dqlite_vfs_destroy(vfs)
}

// Name returns the registration name of the vfs.
func (v *Vfs) Name() string {
	vfs := (*C.sqlite3_vfs)(unsafe.Pointer(v))

	return C.GoString(vfs.zName)
}

// Content returns the content of the given filename.
func (v *Vfs) Content(filename string) ([]byte, error) {
	vfs := (*C.sqlite3_vfs)(unsafe.Pointer(v))

	cfilename := C.CString(filename)
	defer C.free(unsafe.Pointer(cfilename))

	var buf *C.uint8_t
	var n C.size_t

	rc := C.dqlite_file_read(vfs.zName, cfilename, &buf, &n)
	if rc != 0 {
		return nil, Error{Code: int(rc)}
	}

	content := C.GoBytes(unsafe.Pointer(buf), C.int(n))

	C.sqlite3_free(unsafe.Pointer(buf))

	return content, nil
}

// Restore the content of the given filename.
func (v *Vfs) Restore(filename string, bytes []byte) error {
	vfs := (*C.sqlite3_vfs)(unsafe.Pointer(v))

	cfilename := C.CString(filename)
	defer C.free(unsafe.Pointer(cfilename))

	buf := (*C.uint8_t)(unsafe.Pointer(&bytes[0]))
	n := C.size_t(len(bytes))

	rc := C.dqlite_file_write(vfs.zName, cfilename, buf, n)
	if rc != 0 {
		return Error{Code: int(rc & 0xff)}
	}

	return nil
}
