package bindings

/*
#cgo linux LDFLAGS: -ldqlite

#include <assert.h>
#include <stdlib.h>

#include <dqlite.h>
#include <sqlite3.h>

struct dqliteCluster {
  int    handle;          // Entry of the Go clusterHandles map
  char  *leader;          // Hold the last string returned by xLeader.
  char **addresses;       // Hold the last string array returned by xServers.
  dqlite_cluster cluster; // Cluster methods implementation.
};

static struct dqliteCluster *dqliteClusterAlloc() {
  struct dqliteCluster *c;

  c = (struct dqliteCluster*)malloc(sizeof(*c));

  return c;
}

static void dqliteClusterFree(struct dqliteCluster *c)
{
  assert(c != NULL);

  free(c);
}

// Go land callback for xLeader.
char *dqliteClusterLeaderCb(int handle);

static const char* dqliteClusterLeader(void *ctx) {
  struct dqliteCluster *c;
  char *leader;

  assert(ctx != NULL);

  c = (struct dqliteCluster*)ctx;

  c->leader = dqliteClusterLeaderCb(c->handle);

  return (const char*)c->leader;
}

// Go land callback for xServers.
int dqliteClusterServersCb(int handle, char ***addresses);

static int dqliteClusterServers(void *ctx, const char ***addresses) {
  int err;
  struct dqliteCluster *c;

  assert(ctx != NULL);

  c = (struct dqliteCluster*)ctx;

  err = dqliteClusterServersCb(c->handle, &c->addresses);
  if (err != 0) {
    assert(c->addresses == NULL);
    *addresses = NULL;
    return err;
  }

  *addresses = (const char**)c->addresses;

  return 0;
}

// Go land callback for xLeader.
int dqliteClusterRecoverCb(int handle, uint64_t txToken);

static int dqliteClusterRecover(void *ctx, uint64_t tx_token) {
  struct dqliteCluster *c;

  assert(ctx != NULL);

  c = (struct dqliteCluster*)ctx;

  return dqliteClusterRecoverCb(c->handle, tx_token);
}

static void dqliteClusterInit(struct dqliteCluster *c, int handle)
{
  assert(c != NULL);

  c->handle = handle;
  c->leader = NULL;
  c->addresses = NULL;

  c->cluster.ctx = (void*)c;
  c->cluster.xLeader = dqliteClusterLeader;
  c->cluster.xServers = dqliteClusterServers;
  c->cluster.xRecover = dqliteClusterRecover;
}

static void dqliteClusterClose(struct dqliteCluster *c)
{
  assert(c != NULL);

  if (c->leader != NULL) {
    free(c->leader);
  }

  if (c->addresses != NULL) {
    int i;

    for (i = 0; *(c->addresses + i) != NULL; i++) {
      free(*c->addresses + i);
    }

    free(c->addresses);
  }
}

static int dqliteServerInit(dqlite_server *s, int fd, int cluster_handle)
{
  int err;
  FILE *file;
  struct dqliteCluster *cluster;

  assert(s != NULL);

  file = fdopen(fd, "w");
  if (file == NULL) {
    return DQLITE_ERROR;
  }

  cluster = dqliteClusterAlloc();
  if (cluster == NULL) {
    return DQLITE_NOMEM;
  }

  dqliteClusterInit(cluster, cluster_handle);

  err = dqlite_server_init(s, file, &cluster->cluster);
  if (err != 0) {
    return err;
  }

  return 0;
}

static void dqliteServerClose(dqlite_server *s) {
  struct dqliteCluster *cluster;

  assert(s != NULL);

  cluster = (struct dqliteCluster*)dqlite_server_cluster(s)->ctx;

  dqliteClusterClose(cluster);
  dqliteClusterFree(cluster);
}

*/
import "C"
import (
	"fmt"
	"net"
	"os"
	"unsafe"
)

// ServerProtocolVersion is the latest dqlite server protocol version.
const ServerProtocolVersion = uint64(C.DQLITE_PROTOCOL_VERSION)

// Request types.
const (
	ServerRequestLeader    = C.DQLITE_REQUEST_LEADER
	ServerRequestClient    = C.DQLITE_REQUEST_CLIENT
	ServerRequestHeartbeat = C.DQLITE_REQUEST_HEARTBEAT
	ServerRequestOpen      = C.DQLITE_REQUEST_OPEN
	ServerRequestPrepare   = C.DQLITE_REQUEST_PREPARE
	ServerRequestExec      = C.DQLITE_REQUEST_EXEC
	ServerRequestQuery     = C.DQLITE_REQUEST_QUERY
	ServerRequestFinalize  = C.DQLITE_REQUEST_FINALIZE
	ServerRequestExecSQL   = C.DQLITE_REQUEST_EXEC_SQL
	ServerRequestQuerySQL  = C.DQLITE_REQUEST_QUERY_SQL
)

// Response types.
const (
	ServerResponseServer  = C.DQLITE_RESPONSE_SERVER
	ServerResponseWelcome = C.DQLITE_RESPONSE_WELCOME
	ServerResponseServers = C.DQLITE_RESPONSE_SERVERS
	ServerResponseDbError = C.DQLITE_RESPONSE_DB_ERROR
	ServerResponseDb      = C.DQLITE_RESPONSE_DB
	ServerResponseStmt    = C.DQLITE_RESPONSE_STMT
	ServerResponseResult  = C.DQLITE_RESPONSE_RESULT
	ServerResponseRows    = C.DQLITE_RESPONSE_ROWS
	ServerResponseEmpty   = C.DQLITE_RESPONSE_EMPTY
)

// Server is a Go wrapper arround server
type Server C.dqlite_server

// Init initializes dqlite global state.
func Init() error {
	var errmsg *C.char

	rc := C.dqlite_init(&errmsg)
	if rc != 0 {
		return fmt.Errorf("%s (%d)", errmsg, rc)
	}
	return nil
}

// NewServer creates a new Server instance.
func NewServer(file *os.File, cluster Cluster) (*Server, error) {
	server := C.dqlite_server_alloc()
	if server == nil {
		return nil, fmt.Errorf("out of memory")
	}

	handle := clusterHandlesSerial

	clusterHandles[handle] = cluster

	clusterHandlesSerial++

	rc := C.dqliteServerInit(server, C.int(file.Fd()), handle)
	if rc != 0 {
		C.dqliteServerClose(server)
		C.dqlite_server_free(server)
		return nil, fmt.Errorf("failed to initialize server: %d", rc)
	}

	return (*Server)(unsafe.Pointer(server)), nil
}

// Close the server releasing all used resources.
func (s *Server) Close() {
	server := (*C.dqlite_server)(unsafe.Pointer(s))
	C.dqliteServerClose(server)
}

// Free the memory allocated for the given server.
func (s *Server) Free() {
	server := (*C.dqlite_server)(unsafe.Pointer(s))
	C.dqlite_server_free(server)
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

	return nil
}

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

// Cluster implements the interface required by dqlite_cluster.
type Cluster interface {
	// Return the address of the current cluster leader, if any. If not
	// empty, the address string must a be valid network IP or hostname,
	// that clients can use to connect to a dqlite service.
	Leader() string

	// If this driver is the current leader of the cluster, return the
	// addresses of all other servers. Each address must be a valid IP or
	// host name name, that clients can use to connect to the relevant
	// dqlite service , in case the current leader is deposed and a new one
	// is elected.
	//
	// If this driver is not the current leader of the cluster, an error
	// implementing the Error interface below and returning true in
	// NotLeader() must be returned.
	Servers() ([]string, error)

	Recover(token uint64) error
}

// A ClusterError represents a cluster-related error.
type ClusterError interface {
	error
	NotLeader() bool // Is the error due to the server not being the leader?
}

//export dqliteClusterLeaderCb
func dqliteClusterLeaderCb(handle C.int) *C.char {
	cluster := clusterHandles[handle]

	return C.CString(cluster.Leader())
}

//export dqliteClusterServersCb
func dqliteClusterServersCb(handle C.int, out ***C.char) C.int {
	cluster := clusterHandles[handle]

	addresses, err := cluster.Servers()
	if err != nil {
		*out = nil
		// TODO: return an appropriate error code based on err
		return C.int(-1)
	}

	n := C.size_t(len(addresses)) + 1

	*out = (**C.char)(C.malloc(n))

	if *out == nil {
		return C.DQLITE_NOMEM
	}

	size := unsafe.Sizeof(*out)
	for i := C.size_t(0); i < n; i++ {
		address := (**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(*out)) + size*uintptr(i)))

		if i == n-1 {
			*address = nil
		} else {
			*address = C.CString(addresses[i])
		}
	}

	return C.int(0)
}

//export dqliteClusterRecoverCb
func dqliteClusterRecoverCb(handle C.int, txToken C.uint64_t) C.int {
	cluster := clusterHandles[handle]

	err := cluster.Recover(uint64(txToken))
	if err != nil {
		return C.int(-1)
	}

	return C.int(0)
}

// Map C.int to Cluster instances to avoid passing Go pointers to C.
//
// We do not protect this map with a lock since typically just one long-lived
// Cluster instance should be registered (expect for unit tests).
var clusterHandlesSerial C.int
var clusterHandles = map[C.int]Cluster{}
