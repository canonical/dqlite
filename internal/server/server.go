package server

/*
#cgo linux LDFLAGS: -ldqlite

#include <stdio.h>
#include <assert.h>

#include <dqlite.h>

const char *test__cluster_leader(void *ctx)
{
  return "127.0.0.1:666";
}

const char **test__cluster_servers(void *ctx)
{
	static const char *addresses[] = {
		"1.2.3.4:666",
		"5.6.7.8:666",
		NULL,
	};

	return addresses;
}

static dqlite_cluster test__cluster = {
  0,
  test__cluster_leader,
  test__cluster_servers,
  0,
};

dqlite_cluster* test_cluster()
{
	return &test__cluster;
}

*/
import "C"
import (
	"fmt"
	"net"
	"os"
	"runtime"
)

// Server wraps a C dqlite instance.
type Server struct {
	service *C.dqlite_service
	stopCh  chan struct{}
}

// New creates a new dqlite server.
func New() (*Server, error) {
	log := os.Stdout.Fd()

	service := C.dqlite_service_alloc()
	if service == nil {
		return nil, fmt.Errorf("out of memory")
	}

	file := C.fdopen(C.int(log), C.CString("w"))
	if file == nil {
		return nil, fmt.Errorf("out of memory")
	}

	C.dqlite_service_init(service, file, C.test_cluster())

	server := &Server{
		service: service,
		stopCh:  make(chan struct{}),
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

	rc = C.dqlite_service_handle(s.service, C.int(fd), &errmsg)
	if rc != 0 {
		return fmt.Errorf(C.GoString(errmsg))
	}

	return nil
}

// Stop the server.
func (s *Server) Stop() error {
	var errmsg *C.char
	var rc C.int

	rc = C.dqlite_service_stop(s.service, &errmsg)
	if rc != 0 {
		return fmt.Errorf(C.GoString(errmsg))
	}

	<-s.stopCh

	C.dqlite_service_close(s.service)
	C.dqlite_service_free(s.service)

	return nil
}

func (s *Server) run() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	C.dqlite_service_run(s.service)
	close(s.stopCh)
}

// Protocol version.
const Protocol = uint64(C.DQLITE_PROTOCOL_VERSION)
