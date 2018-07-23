package bindings

/*
#include <stdlib.h>

#include <dqlite.h>
#include <sqlite3.h>


*/
import "C"
import (
	"fmt"
	"net"
	"os"
	"unsafe"

	"github.com/CanonicalLtd/dqlite/internal/logging"
	"github.com/pkg/errors"
)

// ProtocolVersion is the latest dqlite server protocol version.
const ProtocolVersion = uint64(C.DQLITE_PROTOCOL_VERSION)

// Request types.
const (
	RequestLeader    = C.DQLITE_REQUEST_LEADER
	RequestClient    = C.DQLITE_REQUEST_CLIENT
	RequestHeartbeat = C.DQLITE_REQUEST_HEARTBEAT
	RequestOpen      = C.DQLITE_REQUEST_OPEN
	RequestPrepare   = C.DQLITE_REQUEST_PREPARE
	RequestExec      = C.DQLITE_REQUEST_EXEC
	RequestQuery     = C.DQLITE_REQUEST_QUERY
	RequestFinalize  = C.DQLITE_REQUEST_FINALIZE
	RequestExecSQL   = C.DQLITE_REQUEST_EXEC_SQL
	RequestQuerySQL  = C.DQLITE_REQUEST_QUERY_SQL
)

// Response types.
const (
	ResponseFailure = C.DQLITE_RESPONSE_FAILURE
	ResponseServer  = C.DQLITE_RESPONSE_SERVER
	ResponseWelcome = C.DQLITE_RESPONSE_WELCOME
	ResponseServers = C.DQLITE_RESPONSE_SERVERS
	ResponseDb      = C.DQLITE_RESPONSE_DB
	ResponseStmt    = C.DQLITE_RESPONSE_STMT
	ResponseResult  = C.DQLITE_RESPONSE_RESULT
	ResponseRows    = C.DQLITE_RESPONSE_ROWS
	ResponseEmpty   = C.DQLITE_RESPONSE_EMPTY
)

// Server is a Go wrapper arround dqlite_server.
type Server C.dqlite_server

// Init initializes dqlite global state.
func Init() error {
	var errmsg *C.char

	rc := C.dqlite_init(&errmsg)
	if rc != 0 {
		return fmt.Errorf("%s (%d)", C.GoString(errmsg), rc)
	}
	return nil
}

// NewServer creates a new Server instance.
func NewServer(cluster Cluster) (*Server, error) {
	server := C.dqlite_server_alloc()
	if server == nil {
		err := codeToError(C.SQLITE_NOMEM)
		return nil, errors.Wrap(err, "failed to allocate server object")
	}

	clusterHandle := clusterRegister(cluster)

	clusterTrampoline := clusterAlloc()
	if clusterTrampoline == nil {
		err := codeToError(C.SQLITE_NOMEM)
		return nil, errors.Wrap(err, "failed to allocate cluster object")
	}
	clusterInit(clusterTrampoline, clusterHandle)

	rc := C.dqlite_server_init(server, clusterTrampoline)
	if rc != 0 {
		clusterUnregister(clusterHandle)
		clusterFree(clusterTrampoline)
		C.dqlite_server_free(server)

		err := codeToError(rc)
		return nil, errors.Wrap(err, "failed to initialize server")
	}

	return (*Server)(unsafe.Pointer(server)), nil
}

// Close the server releasing all used resources.
func (s *Server) Close() {
	server := (*C.dqlite_server)(unsafe.Pointer(s))

	cluster := C.dqlite_server_cluster(server)
	handle := clusterHandle(cluster)

	clusterUnregister(handle)
	clusterFree(cluster)

	if logger := C.dqlite_server_logger(server); logger != nil {
		loggerUnregister(uintptr(logger.ctx))
		C.sqlite3_free(unsafe.Pointer(logger))
	}

	C.dqlite_server_close(server)
	C.dqlite_server_free(server)
}

// SetLogFunc sets the server logging function.
func (s *Server) SetLogFunc(f logging.Func) {
	server := (*C.dqlite_server)(unsafe.Pointer(s))

	logger := newLogger(f)

	rc := C.dqlite_server_config(server, C.DQLITE_CONFIG_LOGGER, unsafe.Pointer(logger))
	if rc != 0 {
		// Setting the logger should never fail.
		panic("failed to set logger")
	}
}

// Run the server.
//
// After this method is called it's possible to invoke Handle().
func (s *Server) Run() error {
	server := (*C.dqlite_server)(unsafe.Pointer(s))

	var errmsg *C.char

	rc := C.dqlite_server_run(server)
	if rc != 0 {
		return fmt.Errorf(C.GoString(errmsg))
	}

	return nil
}

// Ready waits for the server to be ready to handle connections.
func (s *Server) Ready() bool {
	server := (*C.dqlite_server)(unsafe.Pointer(s))

	return C.dqlite_server_ready(server) == 1
}

// Handle a new connection.
func (s *Server) Handle(conn net.Conn) error {
	server := (*C.dqlite_server)(unsafe.Pointer(s))

	file, err := conn.(*net.TCPConn).File()
	if err != nil {
		return err
	}

	fd := file.Fd()

	var errmsg *C.char

	rc := C.dqlite_server_handle(server, C.int(fd), &errmsg)
	if rc != 0 {
		defer C.sqlite3_free(unsafe.Pointer(errmsg))
		if rc == C.DQLITE_STOPPED {
			return ErrServerStopped
		}
		return fmt.Errorf(C.GoString(errmsg))
	}

	// TODO: this is a hack to prevent the GC from closing the file.
	files = append(files, file)

	return nil
}

var files = []*os.File{}

// Stop the server.
func (s *Server) Stop() error {
	server := (*C.dqlite_server)(unsafe.Pointer(s))

	var errmsg *C.char

	rc := C.dqlite_server_stop(server, &errmsg)
	if rc != 0 {
		return fmt.Errorf(C.GoString(errmsg))
	}

	return nil
}

// ErrServerStopped is returned by Server.Handle() is the server was stopped.
var ErrServerStopped = fmt.Errorf("server was stopped")
