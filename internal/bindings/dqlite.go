package bindings

/*
#cgo linux LDFLAGS: -ldqlite

#include <assert.h>
#include <stdlib.h>

#include <dqlite.h>
#include <sqlite3.h>

// Trampoline between a C dqlite_cluster instance and a Go
// bindings.Cluster instance.
struct dqliteCluster {
  int    handle;      // Entry of the Go clusterHandles map
  char  *replication; // Hold the last string returned by xReplication
  char  *leader;      // Hold the last string returned by xLeader.
  char **addresses;   // Hold the last string array returned by xServers.
  dqlite_cluster cluster;
};

// Constructor.
static struct dqliteCluster *dqliteClusterAlloc() {
  struct dqliteCluster *c;

  c = (struct dqliteCluster*)malloc(sizeof(*c));

  return c;
}

// Destructor.
static void dqliteClusterFree(struct dqliteCluster *c)
{
  assert(c != NULL);

  free(c);
}

// Go land callback for xReplication.
char *dqliteClusterReplicationCb(int handle);

// Implementation of xReplication.
static const char *dqliteClusterReplication(void *ctx) {
  int err;
  struct dqliteCluster *c;

  assert(ctx != NULL);

  c = (struct dqliteCluster*)ctx;

  // Save the C string allocated by Go because we'll want to
  // free it when this dqliteCluster instance gets destroyed.
  c->replication = dqliteClusterReplicationCb(c->handle);

  return (const char*)c->replication;
}

// Go land callback for xLeader.
char *dqliteClusterLeaderCb(int handle);

// Implementation of xLeader.
static const char* dqliteClusterLeader(void *ctx) {
  struct dqliteCluster *c;
  char *leader;

  assert(ctx != NULL);

  c = (struct dqliteCluster*)ctx;

  // Save the C string allocated by Go because we'll want to
  // free it when this dqliteCluster instance gets destroyed.
  c->leader = dqliteClusterLeaderCb(c->handle);

  return (const char*)c->leader;
}

// Go land callback for xServers.
int dqliteClusterServersCb(int handle, char ***addresses);

// Implementation of xServers.
static int dqliteClusterServers(void *ctx, const char ***addresses) {
  int err;
  struct dqliteCluster *c;

  assert(ctx != NULL);

  c = (struct dqliteCluster*)ctx;

  // Save the C string array allocated by Go because we'll want to
  // free it when this dqliteCluster instance gets destroyed.
  err = dqliteClusterServersCb(c->handle, &c->addresses);
  if (err != 0) {
    assert(c->addresses == NULL);
    *addresses = NULL;
    return err;
  }

  *addresses = (const char**)c->addresses;

  return 0;
}

// Go land callback for xRegister.
void dqliteClusterRegisterCb(int handle, sqlite3 *db);

// Implementation of xRegister.
static void dqliteClusterRegister(void *ctx, sqlite3 *db) {
  struct dqliteCluster *c;

  assert(ctx != NULL);

  c = (struct dqliteCluster*)ctx;

  dqliteClusterRegisterCb(c->handle, db);
}

// Go land callback for xUnregister.
void dqliteClusterUnregisterCb(int handle, sqlite3 *db);

// Implementation of xUnregister.
static void dqliteClusterUnregister(void *ctx, sqlite3 *db) {
  struct dqliteCluster *c;

  assert(ctx != NULL);

  c = (struct dqliteCluster*)ctx;

  dqliteClusterUnregisterCb(c->handle, db);
}

// Go land callback for xBarrier.
int dqliteClusterBarrierCb(int handle);

// Implementation of xBarrier.
static int dqliteClusterBarrier(void *ctx) {
  struct dqliteCluster *c;

  assert(ctx != NULL);

  c = (struct dqliteCluster*)ctx;

  return dqliteClusterBarrierCb(c->handle);
}

// Go land callback for xRecover.
int dqliteClusterRecoverCb(int handle, uint64_t txToken);

// Go land callback for xRecover.
static int dqliteClusterRecover(void *ctx, uint64_t tx_token) {
  struct dqliteCluster *c;

  assert(ctx != NULL);

  c = (struct dqliteCluster*)ctx;

  return dqliteClusterRecoverCb(c->handle, tx_token);
}

// Initializer.
static void dqliteClusterInit(struct dqliteCluster *c, int handle)
{
  assert(c != NULL);

  c->handle = handle;
  c->replication = NULL;
  c->leader = NULL;
  c->addresses = NULL;

  c->cluster.ctx = (void*)c; // The context is the wrapper itself.
  c->cluster.xReplication = dqliteClusterReplication;
  c->cluster.xLeader = dqliteClusterLeader;
  c->cluster.xServers = dqliteClusterServers;
  c->cluster.xRegister = dqliteClusterRegister;
  c->cluster.xUnregister = dqliteClusterUnregister;
  c->cluster.xBarrier = dqliteClusterBarrier;
  c->cluster.xRecover = dqliteClusterRecover;
}

// Closer.
static void dqliteClusterClose(struct dqliteCluster *c)
{
  assert(c != NULL);

  if (c->replication != NULL) {
    free(c->replication);
  }

  if (c->leader != NULL) {
    free(c->leader);
  }

  if (c->addresses != NULL) {
    int i;

    for (i = 0; *(c->addresses + i) != NULL; i++) {
      free(*(c->addresses + i));
    }

    free(c->addresses);
  }
}

// Wrapper around dqlite_server_init. It's needed in order to create a
// dqliteCluster instance that holds a handle to the Go cluster
// implementation.
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
	ResponseDbError = C.DQLITE_RESPONSE_DB_ERROR
	ResponseDb      = C.DQLITE_RESPONSE_DB
	ResponseStmt    = C.DQLITE_RESPONSE_STMT
	ResponseResult  = C.DQLITE_RESPONSE_RESULT
	ResponseRows    = C.DQLITE_RESPONSE_ROWS
	ResponseEmpty   = C.DQLITE_RESPONSE_EMPTY
)

// Special data types for time values.
const (
	UnixTime = C.DQLITE_UNIXTIME
	ISO8601  = C.DQLITE_ISO8601
)

// Vfs is a Go wrapper arround dqlite's in-memory VFS implementation.
type Vfs C.sqlite3_vfs

// NewVfs registers an in-memory VFS instance under the given name.
func NewVfs(name string) (*Vfs, error) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	var vfs *C.sqlite3_vfs

	err := C.dqlite_vfs_register(cname, &vfs)
	if err != 0 {
		return nil, fmt.Errorf("failure (%d)", err)
	}

	return (*Vfs)(unsafe.Pointer(vfs)), nil
}

// Close unregisters this in-memory VFS instance.
func (v *Vfs) Close() {
	vfs := (*C.sqlite3_vfs)(unsafe.Pointer(v))
	C.dqlite_vfs_unregister(vfs)
}

// Name returns the registration name of the vfs.
func (v *Vfs) Name() string {
	vfs := (*C.sqlite3_vfs)(unsafe.Pointer(v))

	return C.GoString(vfs.zName)
}

}

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
func NewServer(file *os.File, cluster Cluster) (*Server, error) {
	server := C.dqlite_server_alloc()
	if server == nil {
		return nil, fmt.Errorf("out of memory")
	}

	// Register the cluster implementation pass its handle to
	// dqliteServerInit.
	handle := clusterRegister(cluster)

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

// Cluster implements the interface required by dqlite_cluster.
type Cluster interface {
	// Return the registration name of the WAL replication implementation.
	Replication() string

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

	Register(*Conn)
	Unregister(*Conn)

	Barrier() error

	Recover(token uint64) error
}

// A ClusterError represents a cluster-related error.
type ClusterError interface {
	error
	NotLeader() bool // Is the error due to the server not being the leader?
}

//export dqliteClusterReplicationCb
func dqliteClusterReplicationCb(handle C.int) *C.char {
	cluster := clusterHandles[handle]

	return C.CString(cluster.Replication())
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

//export dqliteClusterRegisterCb
func dqliteClusterRegisterCb(handle C.int, db *C.sqlite3) {
	cluster := clusterHandles[handle]

	cluster.Register((*Conn)(unsafe.Pointer(db)))
}

//export dqliteClusterUnregisterCb
func dqliteClusterUnregisterCb(handle C.int, db *C.sqlite3) {
	cluster := clusterHandles[handle]

	cluster.Unregister((*Conn)(unsafe.Pointer(db)))
}

//export dqliteClusterBarrierCb
func dqliteClusterBarrierCb(handle C.int) C.int {
	cluster := clusterHandles[handle]

	if err := cluster.Barrier(); err != nil {
		if err, ok := err.(Error); ok {
			return C.int(err.ExtendedCode)
		}

		// Return a generic error.
		return C.SQLITE_ERROR
	}

	return 0
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
// Cluster instance should be registered (except for unit tests).
var clusterHandlesSerial C.int
var clusterHandles = map[C.int]Cluster{}

func clusterRegister(cluster Cluster) C.int {
	handle := clusterHandlesSerial

	clusterHandles[handle] = cluster
	clusterHandlesSerial++

	return handle
}
