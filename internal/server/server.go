package server

/*
#cgo linux LDFLAGS: -ldqlite

#include <stdio.h>
#include <assert.h>

#include <dqlite.h>

static dqlite* dqliteCreateWrapper() {
  int     err;
  dqlite *d;

  err = dqlite_create(stdout, &d);
  assert( !err );

  err = dqlite_init(d);
  assert( !err );

  return d;
}

*/
import "C"
import (
	"fmt"
	"net"
	"runtime"
)

// Server wraps a C dqlite instance.
type Server struct {
	dqlite *C.dqlite
	stopCh chan struct{}
}

// New creates a new dqlite server.
func New() (*Server, error) {
	server := &Server{
		dqlite: C.dqliteCreateWrapper(),
		stopCh: make(chan struct{}),
	}

	go server.run()

	return server, nil
}

// Handle a new connection.
func (s *Server) Handle(conn net.Conn) error {
	file, err := conn.(*net.TCPConn).File()
	if err != nil {
		return err
	}

	fd := file.Fd()

	var errmsg *C.char
	var rc C.int

	rc = C.dqlite_handle(s.dqlite, C.int(fd), &errmsg)
	if rc != 0 {
		return fmt.Errorf(C.GoString(errmsg))
	}

	conn.Write([]byte{0x39, 0xea, 0x93, 0xbf})

	return nil
}

// Stop the server.
func (s *Server) Stop() error {
	var errmsg *C.char
	var rc C.int

	rc = C.dqlite_stop(s.dqlite, &errmsg)
	if rc != 0 {
		return fmt.Errorf(C.GoString(errmsg))
	}

	<-s.stopCh

	return nil
}

func (s *Server) run() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	C.dqlite_run(s.dqlite)
	close(s.stopCh)
}
