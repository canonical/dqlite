#ifndef DQLITE_H
#define DQLITE_H

#include <sqlite3.h>
#include <stddef.h>

/**
 * Error codes.
 */
#define DQLITE_ERROR 1  /* Generic error */
#define DQLITE_MISUSE 2 /* Library used incorrectly */
#define DQLITE_NOMEM 3  /* A malloc() failed */

/**
 * Dqlite node handle.
 *
 * Opaque handle to a single dqlite node that can serve database requests from
 * connected clients and exchanges data replication messages with other dqlite
 * nodes.
 */
typedef struct dqlite_node dqlite_node;

/**
 * Hold the value of a dqlite node ID. Guaranteed to be at least 64-bit long.
 */
typedef unsigned long long dqlite_node_id;

/**
 * Create a new dqlite node object.
 *
 * The @id argument a is positive number that identifies this particular dqlite
 * node in the cluster. Each dqlite node part of the same cluster must be
 * created with a different ID. The very first node, used to bootstrap a new
 * cluster, must have ID #1. Every time a node is started again, it must be
 * passed the same ID.
 *
 * The @address argument is the network address that clients or other nodes in
 * the cluster must use to connect to this dqlite node. If no custom connect
 * function is going to be set using dqlite_node_set_connect_func(), then the
 * format of the string must be "<HOST>:<PORT>", where <HOST> is an IPv4/IPv6
 * address or a DNS name, and <PORT> is a port number. Otherwise if a custom
 * connect function is used, then the format of the string must by whatever the
 * custom connect function accepts.
 *
 * The @data_dir argument the file system path where the node should store its
 * durable data, such as Raft log entries containing WAL frames of the SQLite
 * databases being replicated.
 *
 * No reference to the memory pointed to by @address and @data_dir is kept by
 * the dqlite library, so any memory associated with them can be released after
 * the function returns.
 */
int dqlite_node_create(dqlite_node_id id,
		       const char *address,
		       const char *data_dir,
		       dqlite_node **n);

/**
 * Destroy a dqlite node object.
 *
 * This will release all memory that was allocated by the node. If
 * dqlite_node_start() was successfully invoked, then dqlite_node_stop() must be
 * invoked before destroying the node.
 */
void dqlite_node_destroy(dqlite_node *n);

/**
 * Instruct the dqlite node to bind a network address when starting, and
 * listening for incoming client connections.
 *
 * The given address might match the one passed to @dqlite_node_create or be a
 * different one (for example if the application wants to proxy it).
 *
 * The format of the @address argument must be either "<HOST>:<PORT>", where
 * <HOST> is an IPv4/IPv6 address or a DNS name and <PORT> is a port number, or
 * "@<PATH>", where <PATH> is an abstract Unix socket path. The special string
 * "@" can be used to automatically select an available abstract Unix socket
 * path, which can then be retrieved with dqlite_node_get_bind_address().

 * If an abstract Unix socket is used the dqlite node will accept only
 * connections originating from the same process.
 *
 * No reference to the memory pointed to by @address is kept, so any memory
 * associated with them can be released after the function returns.
 *
 * This function must be called before calling dqlite_node_start().
 */
int dqlite_node_set_bind_address(dqlite_node *n, const char *address);

/**
 * Get the network address that the dqlite node is using to accept incoming
 * connections.
 */
const char *dqlite_node_get_bind_address(dqlite_node *n);

/**
 * Set a custom connect function.
 *
 * The function should block until a network connection with the dqlite node at
 * the given @address is established, or an error occurs.
 *
 * In case of success, the file descriptor of the connected socket must be saved
 * into the location pointed by the @fd argument. The socket must be either a
 * TCP or a Unix socket.
 *
 * This function must be called before calling dqlite_node_start().
 */
int dqlite_node_set_connect_func(dqlite_node *n,
				 int (*f)(void *arg,
					  const char *address,
					  int *fd),
				 void *arg);

/**
 * Set the average one-way network latency, expressed in nanoseconds.
 *
 * This value is used internally by dqlite to decide how frequently the leader
 * node should send heartbeats to other nodes in order to maintain its
 * leadership, and how long other nodes should wait before deciding that the
 * leader has died and initiate a failover.
 *
 * This function must be called before calling dqlite_node_start().
 */
int dqlite_node_set_network_latency(dqlite_node *n,
				    unsigned long long nanoseconds);

/**
 * Set the failure domain associated with this node.
 *
 * This is effectively a tag applied to the node and that can be inspected later
 * with the "Describe node" client request.
 */
int dqlite_node_set_failure_domain(dqlite_node *n, unsigned long long code);

/**
 * Start a dqlite node.
 *
 * A background thread will be spawned which will run the node's main loop. If
 * this function returns successfully, the dqlite node is ready to accept new
 * connections.
 */
int dqlite_node_start(dqlite_node *n);

/**
 * Stop a dqlite node.
 *
 * The background thread running the main loop will be notified and the node
 * will not accept any new client connections. Once inflight requests are
 * completed, open client connections get closed and then the thread exits.
 */
int dqlite_node_stop(dqlite_node *n);

struct dqlite_node_info
{
	dqlite_node_id id;
	const char *address;
};
typedef struct dqlite_node_info dqlite_node_info;

/**
 * Force recovering a dqlite node which is part of a cluster whose majority of
 * nodes have died, and therefore has become unavailable.
 *
 * In order for this operation to be safe you must follow these steps:
 *
 * 1. Make sure no dqlite node in the cluster is running.
 *
 * 2. Identify all dqlite nodes that have survived and that you want to be part
 *    of the recovered cluster.
 *
 * 3. Among the survived dqlite nodes, find the one with the most up-to-date
 *    raft term and log.
 *
 * 4. Invoke @dqlite_node_recover exactly one time, on the node you found in
 *    step 3, and pass it an array of #dqlite_node_info filled with the IDs and
 *    addresses of the survived nodes, including the one being recovered.
 *
 * 5. Copy the data directory of the node you ran @dqlite_node_recover on to all
 *    other non-dead nodes in the cluster, replacing their current data
 *    directory.
 *
 * 6. Restart all nodes.
 */
int dqlite_node_recover(dqlite_node *n, dqlite_node_info infos[], int n_info);

/**
 * Return a human-readable description of the last error occurred.
 */
const char *dqlite_node_errmsg(dqlite_node *n);

/**
 * Generate a unique ID for the given address.
 */
dqlite_node_id dqlite_generate_node_id(const char *address);

/**
 * Initialize the given SQLite VFS interface object with dqlite's custom
 * implementation, which can be used for replication.
 */
int dqlite_vfs_init(sqlite3_vfs *vfs, const char *name);

/**
 * Release all memory used internally by a SQLite VFS object that was
 * initialized using @qlite_vfs_init.
 */
void dqlite_vfs_close(sqlite3_vfs *vfs);

/**
 * A single WAL frame to be replicated.
 */
struct dqlite_vfs_frame
{
	unsigned long page_number; /* Database page number. */
	void *data;                /* Content of the database page. */
};
typedef struct dqlite_vfs_frame dqlite_vfs_frame;

/**
 * Check if the last call to sqlite3_step() has triggered a write transaction on
 * the database with the given filename. In that case acquire a WAL write lock
 * to prevent further write transactions, and return all new WAL frames
 * generated by the transaction. These frames are meant to be replicated across
 * nodes and then actually added to the WAL with dqlite_vfs_apply() once a
 * quorum is reached. If a quorum is not reached within a given time, then
 * dqlite_vfs_abort() can be used to abort and release the WAL write lock.
 */
int dqlite_vfs_poll(sqlite3_vfs *vfs,
		    const char *filename,
		    dqlite_vfs_frame **frames,
		    unsigned *n);

/**
 * Add to the WAL all frames that were generated by a write transaction
 * triggered by sqlite3_step() and that were obtained via dqlite_vfs_poll().
 *
 * This interface is designed to match the typical use case of a node receiving
 * the frames by sequentially reading a byte stream from a network socket and
 * passing the data to this routine directly without any copy or futher
 * allocation, possibly except for integer encoding/decoding.
 */
int dqlite_vfs_apply(sqlite3_vfs *vfs,
		     const char *filename,
		     unsigned n,
		     unsigned long *page_numbers,
		     void *frames);

/**
 * Abort a pending write transaction that was triggered by sqlite3_step() and
 * whose frames were obtained via dqlite_vfs_poll().
 *
 * This should be called if the transaction could not be safely replicated. In
 * particular it will release the write lock acquired by dqlite_vfs_poll().
 */
int dqlite_vfs_abort(sqlite3_vfs *vfs, const char *filename);

/**
 * Return a snapshot of the main database file and of the WAL file.
 */
int dqlite_vfs_snapshot(sqlite3_vfs *vfs,
			const char *filename,
			void **data,
			size_t *n);

/**
 * Restore a snapshot of the main database file and of the WAL file.
 */
int dqlite_vfs_restore(sqlite3_vfs *vfs,
		       const char *filename,
		       const void *data,
		       size_t n);

#endif /* DQLITE_H */
