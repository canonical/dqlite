package bindings

/*
#include <assert.h>
#include <stdlib.h>

#include <dqlite.h>

// Go land callbacks for dqlite_cluster methods.
char *clusterLeaderCb(int handle);
int clusterServersCb(int handle, dqlite_server_info **servers);
void clusterRegisterCb(int handle, sqlite3 *db);
void clusterUnregisterCb(int handle, sqlite3 *db);
int clusterBarrierCb(int handle);
int clusterRecoverCb(int handle, uint64_t txToken);
int clusterCheckpointCb(int handle, sqlite3 *db);

// Custom state used as context for dqlite_cluster trampoline objects.
typedef struct dqlite__cluster_state {
  int    handle;               // Entry of the Go clusterHandles map
  char  *leader;               // Hold the last string returned by xLeader.
  dqlite_server_info *servers; // Hold the last array returned by xServers.
} dqlite__cluster_state;

// Allocate a trampoline dqlite_cluster object whose methods forward calls to
// Go functions.
static dqlite_cluster *dqlite__cluster_alloc() {
  dqlite_cluster *c;
  struct dqlite__cluster_state *state;

  c = sqlite3_malloc(sizeof *c);
  if (c == NULL) {
    return NULL;
  }

  state = sqlite3_malloc(sizeof *state);
  if (state == NULL) {
    sqlite3_free(c);
    return NULL;
  }

  c->ctx = state;

  return c;
}

// Implementation of xLeader.
static const char* dqlite__cluster_leader(void *ctx) {
  struct dqlite__cluster_state *state;
  char *leader;

  assert(ctx != NULL);

  state = ctx;

  // Free the previous value.
  if (state->leader != NULL) {
    free(state->leader);
  }

  // Save the C string allocated by Go because we'll want to
  // free it when this cluster instance gets destroyed.
  state->leader = clusterLeaderCb(state->handle);

  return (const char*)state->leader;
}

// Implementation of xServers.
static int dqlite__cluster_servers(void *ctx, dqlite_server_info *servers[]) {
  struct dqlite__cluster_state *state;
  int err;

  assert(ctx != NULL);

  state= ctx;

  // Free the previous value.
  if (state->servers != NULL) {
    free(state->servers);
  }

  // Save the C string array allocated by Go because we'll want to
  // free it when this cluster instance gets destroyed.
  err = clusterServersCb(state->handle, &state->servers);

  *servers = state->servers;

  return err;
}

// Implementation of xRegister.
static void dqlite__cluster_register(void *ctx, sqlite3 *db) {
  struct dqlite__cluster_state *state;

  assert(ctx != NULL);

  state = ctx;

  clusterRegisterCb(state->handle, db);
}

// Implementation of xUnregister.
static void dqlite__cluster_unregister(void *ctx, sqlite3 *db) {
  struct dqlite__cluster_state *state;

  assert(ctx != NULL);

  state = ctx;

  clusterUnregisterCb(state->handle, db);
}

// Implementation of xBarrier.
static int dqlite__cluster_barrier(void *ctx) {
  struct dqlite__cluster_state *state;

  assert(ctx != NULL);

  state = ctx;

  return clusterBarrierCb(state->handle);
}

// Implementation of of xRecover.
static int dqlite__cluster_recover(void *ctx, uint64_t tx_token) {
  struct dqlite__cluster_state *state;

  assert(ctx != NULL);

  state = ctx;

  return clusterRecoverCb(state->handle, tx_token);
}

// Implementation of of xCheckpoint.
static int dqlite__cluster_checkpoint(void *ctx, sqlite3 *db) {
  struct dqlite__cluster_state *state;

  assert(ctx != NULL);

  state = ctx;

  return clusterCheckpointCb(state->handle, db);
}

// Initializer.
static void dqlite__cluster_init(dqlite_cluster *c, int handle)
{
  struct dqlite__cluster_state *state;

  assert(c != NULL);

  state = c->ctx;

  state->handle = handle;
  state->leader = NULL;
  state->servers = NULL;

  c->xLeader = dqlite__cluster_leader;
  c->xServers = dqlite__cluster_servers;
  c->xRegister = dqlite__cluster_register;
  c->xUnregister = dqlite__cluster_unregister;
  c->xBarrier = dqlite__cluster_barrier;
  c->xRecover = dqlite__cluster_recover;
  c->xCheckpoint = dqlite__cluster_checkpoint;
}

// Destructor.
static void dqlite__cluster_free(dqlite_cluster *c)
{
  struct dqlite__cluster_state *state;

  assert(c != NULL);

  state = c->ctx;

  if (state->leader != NULL) {
    free(state->leader);
  }

  if (state->servers != NULL) {
    free(state->servers);
  }

  sqlite3_free(c->ctx);
  sqlite3_free(c);
}

*/
import "C"
import (
	"unsafe"
)

// ServerInfo is the Go equivalent of dqlite_server_info.
type ServerInfo struct {
	ID      uint64
	Address string
}

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
	Servers() ([]ServerInfo, error)

	Register(*Conn)
	Unregister(*Conn)

	Barrier() error

	Recover(token uint64) error

	Checkpoint(*Conn) error
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

func clusterUnregister(handle C.int) {
	delete(clusterHandles, handle)
}

// Create a new dqlite_cluster C object for forwarding calls to a given Go
// implementation.
func clusterAlloc() *C.dqlite_cluster {
	return C.dqlite__cluster_alloc()
}

// Initialize a dqlite_cluster, making forward methods to the Go Cluster
// implementation associated with the given handle.
func clusterInit(c *C.dqlite_cluster, handle C.int) {
	C.dqlite__cluster_init(c, handle)
}

// Returns the handle associated with the given dqlite_cluster object.
func clusterHandle(c *C.dqlite_cluster) C.int {
	state := (*C.dqlite__cluster_state)(c.ctx)
	return state.handle
}

// Release resources associated with the given C cluster object.
func clusterFree(cluster *C.dqlite_cluster) {
	C.dqlite__cluster_free(cluster)
}

//export clusterLeaderCb
func clusterLeaderCb(handle C.int) *C.char {
	cluster := clusterHandles[handle]

	return C.CString(cluster.Leader())
}

//export clusterServersCb
func clusterServersCb(handle C.int, out **C.dqlite_server_info) C.int {
	cluster := clusterHandles[handle]

	servers, err := cluster.Servers()
	if err != nil {
		*out = nil
		return C.int(ErrorCode(err))
	}

	n := C.size_t(len(servers)) + 1

	size := unsafe.Sizeof(C.dqlite_server_info{})
	*out = (*C.dqlite_server_info)(C.malloc(n * C.size_t(size)))

	if *out == nil {
		return C.SQLITE_NOMEM
	}

	for i := C.size_t(0); i < n; i++ {
		server := (*C.dqlite_server_info)(unsafe.Pointer(uintptr(unsafe.Pointer(*out)) + size*uintptr(i)))

		if i == n-1 {
			server.id = 0
			server.address = nil
		} else {
			server.id = C.uint64_t(servers[i].ID)
			server.address = C.CString(servers[i].Address)
		}
	}

	return C.int(0)
}

//export clusterRegisterCb
func clusterRegisterCb(handle C.int, db *C.sqlite3) {
	cluster := clusterHandles[handle]

	cluster.Register((*Conn)(unsafe.Pointer(db)))
}

//export clusterUnregisterCb
func clusterUnregisterCb(handle C.int, db *C.sqlite3) {
	cluster := clusterHandles[handle]

	cluster.Unregister((*Conn)(unsafe.Pointer(db)))
}

//export clusterBarrierCb
func clusterBarrierCb(handle C.int) C.int {
	cluster := clusterHandles[handle]

	if err := cluster.Barrier(); err != nil {
		return C.int(ErrorCode(err))
	}

	return 0
}

//export clusterRecoverCb
func clusterRecoverCb(handle C.int, txToken C.uint64_t) C.int {
	cluster := clusterHandles[handle]

	err := cluster.Recover(uint64(txToken))
	if err != nil {
		return C.int(ErrorCode(err))
	}

	return C.int(0)
}

//export clusterCheckpointCb
func clusterCheckpointCb(handle C.int, db *C.sqlite3) C.int {
	cluster := clusterHandles[handle]

	err := cluster.Checkpoint((*Conn)(unsafe.Pointer(db)))
	if err != nil {
		return C.int(ErrorCode(err))
	}

	return C.int(0)
}
