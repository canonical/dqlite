#ifndef DQLITE_H
#define DQLITE_H

#include <sqlite3.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifndef DQLITE_API
#define DQLITE_API __attribute__((visibility("default")))
#endif

/* XXX */
#define DQLITE_VISIBLE_TO_TESTS DQLITE_API

/**
 * Version.
 */
#define DQLITE_VERSION_MAJOR 1
#define DQLITE_VERSION_MINOR 14
#define DQLITE_VERSION_RELEASE 0
#define DQLITE_VERSION_NUMBER                                            \
	(DQLITE_VERSION_MAJOR * 100 * 100 + DQLITE_VERSION_MINOR * 100 + \
	 DQLITE_VERSION_RELEASE)

DQLITE_API int dqlite_version_number(void);

/**
 * Hold the value of a dqlite node ID. Guaranteed to be at least 64-bit long.
 */
typedef unsigned long long dqlite_node_id;

typedef struct dqlite_server dqlite_server;
typedef struct dqlite dqlite;
typedef struct dqlite_stmt dqlite_stmt;

/**
 * Signature of a custom callback used to establish network connections
 * to dqlite servers.
 *
 * @arg is a user data parameter, copied from the third argument of
 * dqlite_server_set_connect_func. @addr is a (borrowed) abstract address
 * string, as passed to dqlite_server_create or dqlite_server_set_auto_join. @fd
 * is an address where a socket representing the connection should be stored.
 * The callback should return zero if a connection was established successfully
 * or nonzero if the attempt failed.
 */
typedef int (*dqlite_connect_func)(void *arg, const char *addr, int *fd);

/* The following dqlite_server functions return zero on success or nonzero on
 * error. More specific error codes may be specified in the future. */

/**
 * Start configuring a dqlite server.
 *
 * The server will not start running until dqlite_server_start is called. @path
 * is the path to a directory where the server (and attached client) will store
 * its persistent state; the directory must exist. A pointer to the new server
 * object is stored in @server on success.
 *
 * Whether or not this function succeeds, you should call dqlite_server_destroy
 * to release resources owned by the server object.
 *
 * No reference to @path is kept after this function returns.
 */
DQLITE_API int dqlite_server_create(const char *path, dqlite_server **server);

/**
 * Set the abstract address of this server.
 *
 * This function must be called when the server starts for the first time, and
 * is a no-op when the server is restarting. The abstract address is recorded in
 * the Raft log and passed to the connect function on each server (see
 * dqlite_server_set_connect_func). The server will also bind to this address to
 * listen for incoming connections from clients and other servers, unless
 * dqlite_server_set_bind_address is used. For the address syntax accepted by
 * the default connect function (and for binding/listening), see
 * dqlite_server_set_bind_address.
 */
DQLITE_API int dqlite_server_set_address(dqlite_server *server,
					 const char *address);

/**
 * Turn on or off automatic bootstrap for this server.
 *
 * The bootstrap server should be the first to start up. It automatically
 * becomes the leader in the first term, and is responsible for adding all other
 * servers to the cluster configuration. There must be exactly one bootstrap
 * server in each cluster. After the first startup, the bootstrap server is no
 * longer special and this function is a no-op.
 */
DQLITE_API int dqlite_server_set_auto_bootstrap(dqlite_server *server, bool on);

/**
 * Declare the addresses of existing servers in the cluster, which should
 * already be running.
 *
 * The server addresses declared with this function will not be used unless
 * @server is starting up for the first time; after the first startup, the list
 * of servers stored on disk will be used instead. (It is harmless to call this
 * function unconditionally.)
 */
DQLITE_API int dqlite_server_set_auto_join(dqlite_server *server,
					   const char *const *addrs,
					   unsigned n);

/**
 * Configure @server to listen on the address @addr for incoming connections
 * (from clients and other servers).
 *
 * If no bind address is configured with this function, the abstract address
 * passed to dqlite_server_create will be used. The point of this function is to
 * support decoupling the abstract address from the networking implementation
 * (for example, if a proxy is going to be used).
 *
 * @addr must use one of the following formats:
 *
 * 1. "<HOST>"
 * 2. "<HOST>:<PORT>"
 * 3. "@<PATH>"
 *
 * Where <HOST> is a numeric IPv4/IPv6 address, <PORT> is a port number, and
 * <PATH> is an abstract Unix socket path. The port number defaults to 8080 if
 * not specified. In the second form, if <HOST> is an IPv6 address, it must be
 * enclosed in square brackets "[]". In the third form, if <PATH> is empty, the
 * implementation will automatically select an available abstract Unix socket
 * path.
 *
 * If an abstract Unix socket is used, the server will accept only
 * connections originating from the same process.
 */
DQLITE_API int dqlite_server_set_bind_address(dqlite_server *server,
					      const char *addr);

/**
 * Configure the function that this server will use to connect to other servers.
 *
 * The same function will be used by the server's attached client to establish
 * connections to all servers in the cluster. @arg is a user data parameter that
 * will be passed to all invocations of the connect function.
 */
DQLITE_API int dqlite_server_set_connect_func(dqlite_server *server,
					      dqlite_connect_func f,
					      void *arg);

/**
 * Start running the server.
 *
 * Once this function returns successfully, the server will be ready to accept
 * client requests using the functions below.
 */
DQLITE_API int dqlite_server_start(dqlite_server *server);

/**
 * Get the ID of the server.
 *
 * This will return 0 (an invalid ID) if the server has not been started.
 */
DQLITE_API dqlite_node_id dqlite_server_get_id(dqlite_server *server);

/**
 * Hand over the server's privileges to other servers.
 *
 * This is intended to be called before dqlite_server_stop. The server will try
 * to surrender leadership and voting rights to other nodes in the cluster, if
 * applicable. This avoids some disruptions that can result when a privileged
 * server stops suddenly.
 */
DQLITE_API int dqlite_server_handover(dqlite_server *server);

/**
 * Stop the server.
 *
 * The server will stop processing requests from client or other servers. To
 * smooth over some possible disruptions to the cluster, call
 * dqlite_server_handover before this function. After this function returns
 * (successfully or not), you should call dqlite_server_destroy to free
 * resources owned by the server.
 */
DQLITE_API int dqlite_server_stop(dqlite_server *server);

/**
 * Free resources owned by the server.
 *
 * You should always call this function to finalize a server created with
 * dqlite_server_create, whether or not that function returned successfully.
 * If the server has been successfully started with dqlite_server_start,
 * then you must stop it with dqlite_server_stop before calling this function.
 */
DQLITE_API void dqlite_server_destroy(dqlite_server *server);

/* The following functions (up to but not including dqlite_node_create) mimic
 * the core of the sqlite3.h API and use SQLite result codes. dqlite tries to
 * return result codes that match what SQLite would return in the same
 * situation, but in some cases will return the less specific SQLITE_ERROR
 * instead. Certain dqlite-specific error conditions have their own codes that
 * are compatible with the SQLite set: */

#define SQLITE_IOERR_NOT_LEADER (SQLITE_IOERR | (40 << 8))
#define SQLITE_IOERR_LEADERSHIP_LOST (SQLITE_IOERR | (41 << 8))

/* When some version of dqlite returns SQLITE_ERROR under certain
 * circumstances, the developers reserve the right to switch to a more specific
 * error code in future releases without announcing a breaking change. */

/**
 * Open a database connection on the dqlite cluster.
 *
 * This request will be transparently forwarded to the cluster leader as needed.
 *
 * This is the analogue of sqlite3_open. @name is the name of the database
 * to open, which will be created if it does not exist. All servers in the
 * cluster share a "namespace" for databases.
 */
DQLITE_API int dqlite_open(dqlite_server *server,
			   const char *name,
			   dqlite **db);

/**
 * Create a prepared statement to be executed on the cluster.
 *
 * This is the analogue of sqlite3_prepare_v2.
 */
DQLITE_API int dqlite_prepare(dqlite *db,
			      const char *sql,
			      int sql_len,
			      dqlite_stmt **stmt,
			      const char **tail);

/**
 * Execute a prepared statement for one "step".
 *
 * This is the analogue of sqlite3_step.
 */
DQLITE_API int dqlite_step(dqlite_stmt *stmt);

/**
 * Restore a prepared statement to the state preceding any calls to dqlite_step.
 *
 * This is the analogue of sqlite3_reset. It can be called on a prepared
 * statement at any point in its lifecycle.
 */
DQLITE_API int dqlite_reset(dqlite_stmt *stmt);

/**
 * Unbind all parameters from a prepared statement.
 *
 * This is a purely local operation that does not involve communication with any
 * server. It it the analogue of sqlite3_clear_bindings.
 */
DQLITE_API int dqlite_clear_bindings(dqlite_stmt *stmt);

/**
 * Release all resources associated with a prepared statement.
 *
 * This ends the statement's lifecycle, rendering it invalid for further use. It
 * is the analogue of sqlite3_finalize.
 */
DQLITE_API int dqlite_finalize(dqlite_stmt *stmt);

/**
 * Prepare a SQL string, run it to completion, and finalize it.
 *
 * This is the analogue of sqlite3_exec.
 */
DQLITE_API int dqlite_exec(dqlite *db,
			   const char *sql,
			   int (*cb)(void *, int, char **, char **),
			   void *cb_data,
			   char **errmsg);

/**
 * Close a database after all associated prepared statements have been
 * finalized.
 *
 * This is analogous to sqlite3_close (note, not sqlite3_close_v2). In
 * particular, it will fail with SQLITE_BUSY if some dqlite_stmt objects
 * associated with this database have not yet been finalized.
 */
DQLITE_API int dqlite_close(dqlite *db);

/**
 * Bind parameters to a prepared statement.
 *
 * Parameter binding is a purely local operation, involving no communication
 * with any server.
 *
 * These functions are analogous to the sqlite3_bind family of functions.
 */
DQLITE_API int dqlite_bind_blob64(dqlite_stmt *stmt,
				  int index,
				  const void *blob,
				  uint64_t blob_len,
				  void (*dealloc)(void *));
DQLITE_API int dqlite_bind_double(dqlite_stmt *stmt, int index, double val);
DQLITE_API int dqlite_bind_int64(dqlite_stmt *stmt, int index, int64_t val);
DQLITE_API int dqlite_bind_null(dqlite_stmt *stmt, int index);
DQLITE_API int dqlite_bind_text64(dqlite_stmt *stmt,
				  int index,
				  const char *text,
				  uint64_t text_len,
				  void (*dealloc)(void *),
				  unsigned char encoding);

/**
 * Retrieve values from a table row associated with a prepared statement.
 *
 * Column retrieval is a purely local operation, involving no communication with
 * any server.
 *
 * These functions are analogous to the sqlite3_column family of functions.
 */
DQLITE_API const void *dqlite_column_blob(dqlite_stmt *stmt, int index);
DQLITE_API double dqlite_column_double(dqlite_stmt *stmt, int index);
DQLITE_API int64_t dqlite_column_int64(dqlite_stmt *stmt, int index);
DQLITE_API const unsigned char *dqlite_column_text(dqlite_stmt *stmt,
						   int index);
DQLITE_API int dqlite_column_bytes(dqlite_stmt *stmt, int index);
DQLITE_API int dqlite_column_type(dqlite_stmt *stmt, int index);

/* TODO: currently we have #include <sqlite.h> at the top of this file,
 * to support the dqlite_vfs functions below. Once those functions are
 * removed, we should also remove the #include and just copy the definitions
 * of the SQLite result codes here. */

/**
 * Error codes.
 *
 * These are used only with the dqlite_node family of functions.
 */
enum {
	DQLITE_ERROR = 1, /* Generic error */
	DQLITE_MISUSE,    /* Library used incorrectly */
	DQLITE_NOMEM      /* A malloc() failed */
};

/**
 * Dqlite node handle.
 *
 * Opaque handle to a single dqlite node that can serve database requests from
 * connected clients and exchanges data replication messages with other dqlite
 * nodes.
 */
typedef struct dqlite_node dqlite_node;

/**
 * Create a new dqlite node object.
 *
 * The @id argument a is positive number that identifies this particular dqlite
 * node in the cluster. Each dqlite node part of the same cluster must be
 * created with a different ID. The very first node, used to bootstrap a new
 * cluster, must have ID #1. Every time a node is started again, it must be
 * passed the same ID.

 * The @address argument is the network address that clients or other nodes in
 * the cluster must use to connect to this dqlite node. If no custom connect
 * function is going to be set using dqlite_node_set_connect_func(), then the
 * format of the string must be "<HOST>" or "<HOST>:<PORT">, where <HOST> is a
 * numeric IPv4/IPv6 address and <PORT> is a port number. The port number
 * defaults to 8080 if not specified. If a port number is specified with an
 * IPv6 address, the address must be enclosed in square brackets "[]".
 *
 * If a custom connect function is used, then the format of the string must by
 * whatever the custom connect function accepts.
 *
 * The @data_dir argument the file system path where the node should store its
 * durable data, such as Raft log entries containing WAL frames of the SQLite
 * databases being replicated.
 *
 * No reference to the memory pointed to by @address and @data_dir is kept by
 * the dqlite library, so any memory associated with them can be released after
 * the function returns.
 *
 * Even if an error is returned, the caller should call dqlite_node_destroy()
 * on the dqlite_node* value pointed to by @n, and calling dqlite_node_errmsg()
 * with that value will return a valid error string. (In some cases *n will be
 * set to NULL, but dqlite_node_destroy() and dqlite_node_errmsg() will handle
 * this gracefully.)
 */
DQLITE_API int dqlite_node_create(dqlite_node_id id,
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
DQLITE_API void dqlite_node_destroy(dqlite_node *n);

/**
 * Instruct the dqlite node to bind a network address when starting, and
 * listening for incoming client connections.
 *
 * The given address might match the one passed to @dqlite_node_create or be a
 * different one (for example if the application wants to proxy it).
 *
 * The format of the @address argument must be one of
 *
 * 1. "<HOST>"
 * 2. "<HOST>:<PORT>"
 * 3. "@<PATH>"
 *
 * Where <HOST> is a numeric IPv4/IPv6 address, <PORT> is a port number, and
 * <PATH> is an abstract Unix socket path. The port number defaults to 8080 if
 * not specified. In the second form, if <HOST> is an IPv6 address, it must be
 * enclosed in square brackets "[]". In the third form, if <PATH> is empty, the
 * implementation will automatically select an available abstract Unix socket
 * path, which can then be retrieved with dqlite_node_get_bind_address().
 *
 * If an abstract Unix socket is used the dqlite node will accept only
 * connections originating from the same process.
 *
 * No reference to the memory pointed to by @address is kept, so any memory
 * associated with them can be released after the function returns.
 *
 * This function must be called before calling dqlite_node_start().
 */
DQLITE_API int dqlite_node_set_bind_address(dqlite_node *n,
					    const char *address);

/**
 * Get the network address that the dqlite node is using to accept incoming
 * connections.
 */
DQLITE_API const char *dqlite_node_get_bind_address(dqlite_node *n);

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
DQLITE_API int dqlite_node_set_connect_func(dqlite_node *n,
					    int (*f)(void *arg,
						     const char *address,
						     int *fd),
					    void *arg);

/**
 * DEPRECATED - USE `dqlite_node_set_network_latency_ms`
 * Set the average one-way network latency, expressed in nanoseconds.
 *
 * This value is used internally by dqlite to decide how frequently the leader
 * node should send heartbeats to other nodes in order to maintain its
 * leadership, and how long other nodes should wait before deciding that the
 * leader has died and initiate a failover.
 *
 * This function must be called before calling dqlite_node_start().
 */
DQLITE_API int dqlite_node_set_network_latency(dqlite_node *n,
					       unsigned long long nanoseconds);

/**
 * Set the average one-way network latency, expressed in milliseconds.
 *
 * This value is used internally by dqlite to decide how frequently the leader
 * node should send heartbeats to other nodes in order to maintain its
 * leadership, and how long other nodes should wait before deciding that the
 * leader has died and initiate a failover.
 *
 * This function must be called before calling dqlite_node_start().
 *
 * Latency should not be 0 or larger than 3600000 milliseconds.
 */
DQLITE_API int dqlite_node_set_network_latency_ms(dqlite_node *t,
						  unsigned milliseconds);

/**
 * Set the failure domain associated with this node.
 *
 * This is effectively a tag applied to the node and that can be inspected later
 * with the "Describe node" client request.
 */
DQLITE_API int dqlite_node_set_failure_domain(dqlite_node *n,
					      unsigned long long code);

/**
 * Set the snapshot parameters for this node.
 *
 * This function determines how frequently a node will snapshot the state
 * of the database and how many raft log entries will be kept around after
 * a snapshot has been taken.
 *
 * `snapshot_threshold` : Determines the frequency of taking a snapshot, the
 * lower the number, the higher the frequency.
 *
 * `snapshot_trailing` : Determines the amount of log entries kept around after
 * taking a snapshot. Lowering this number decreases disk and memory footprint
 * but increases the chance of having to send a full snapshot (instead of a
 * number of log entries to a node that has fallen behind.
 *
 * This function must be called before calling dqlite_node_start().
 */
DQLITE_API int dqlite_node_set_snapshot_params(dqlite_node *n,
					       unsigned snapshot_threshold,
					       unsigned snapshot_trailing);

/**
 * Set the block size used for performing disk IO when writing raft log segments
 * to disk. @size is limited to a list of preset values.
 *
 * This function must be called before calling dqlite_node_start().
 */
DQLITE_API int dqlite_node_set_block_size(dqlite_node *n, size_t size);

/**
 * WARNING: This is an experimental API.
 *
 * By default dqlite holds the SQLite database file and WAL in memory. By
 * enabling disk-mode, dqlite will hold the SQLite database file on-disk while
 * keeping the WAL in memory. Has to be called after `dqlite_node_create` and
 * before `dqlite_node_start`.
 */
DQLITE_API int dqlite_node_enable_disk_mode(dqlite_node *n);

/**
 * Set the target number of voting nodes for the cluster.
 *
 * If automatic role management is enabled, the cluster leader will attempt to
 * promote nodes to reach the target. If automatic role management is disabled,
 * this has no effect.
 *
 * The default target is 3 voters.
 */
DQLITE_API int dqlite_node_set_target_voters(dqlite_node *n, int voters);

/**
 * Set the target number of standby nodes for the cluster.
 *
 * If automatic role management is enabled, the cluster leader will attempt to
 * promote nodes to reach the target. If automatic role management is disabled,
 * this has no effect.
 *
 * The default target is 0 standbys.
 */
DQLITE_API int dqlite_node_set_target_standbys(dqlite_node *n, int standbys);

/**
 * Enable automatic role management on the server side for this node.
 *
 * When automatic role management is enabled, servers in a dqlite cluster will
 * autonomously (without client intervention) promote and demote each other
 * to maintain a specified number of voters and standbys, taking into account
 * the health, failure domain, and weight of each server.
 *
 * By default, no automatic role management is performed.
 */
DQLITE_API int dqlite_node_enable_role_management(dqlite_node *n);

/**
 * Start a dqlite node.
 *
 * A background thread will be spawned which will run the node's main loop. If
 * this function returns successfully, the dqlite node is ready to accept new
 * connections.
 */
DQLITE_API int dqlite_node_start(dqlite_node *n);

/**
 * Attempt to hand over this node's privileges to other nodes in preparation
 * for a graceful shutdown.
 *
 * Specifically, if this node is the cluster leader, this will cause another
 * voting node (if one exists) to be elected leader; then, if this node is a
 * voter, another non-voting node (if one exists) will be promoted to voter, and
 * then this node will be demoted to spare.
 *
 * This function returns 0 if all privileges were handed over successfully,
 * and nonzero otherwise. Callers can continue to dqlite_node_stop immediately
 * after this function returns (whether or not it succeeded), or include their
 * own graceful shutdown logic before dqlite_node_stop.
 */
DQLITE_API int dqlite_node_handover(dqlite_node *n);

/**
 * Stop a dqlite node.
 *
 * The background thread running the main loop will be notified and the node
 * will not accept any new client connections. Once inflight requests are
 * completed, open client connections get closed and then the thread exits.
 */
DQLITE_API int dqlite_node_stop(dqlite_node *n);

struct dqlite_node_info
{
	dqlite_node_id id;
	const char *address;
};
typedef struct dqlite_node_info dqlite_node_info;

/* Defined to be an extensible struct, future additions to this struct should be
 * 64-bits wide and 0 should not be used as a valid value. */
struct dqlite_node_info_ext
{
	uint64_t size; /* The size of this struct */
	uint64_t id;   /* dqlite_node_id */
	uint64_t address;
	uint64_t dqlite_role;
};
typedef struct dqlite_node_info_ext dqlite_node_info_ext;
#define DQLITE_NODE_INFO_EXT_SZ_ORIG 32U /* (4 * 64) / 8 */

/**
 * !!! Deprecated, use `dqlite_node_recover_ext` instead which also includes
 * dqlite roles. !!!
 *
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
DQLITE_API int dqlite_node_recover(dqlite_node *n,
				   dqlite_node_info infos[],
				   int n_info);

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
 * 4. Invoke @dqlite_node_recover_ext exactly one time, on the node you found in
 *    step 3, and pass it an array of #dqlite_node_info filled with the IDs,
 *    addresses and roles of the survived nodes, including the one being
 *    recovered.
 *
 * 5. Copy the data directory of the node you ran @dqlite_node_recover_ext on to
 *    all other non-dead nodes in the cluster, replacing their current data
 *    directory.
 *
 * 6. Restart all nodes.
 */
DQLITE_API int dqlite_node_recover_ext(dqlite_node *n,
				       dqlite_node_info_ext infos[],
				       int n_info);

/**
 * Return a human-readable description of the last error occurred.
 */
DQLITE_API const char *dqlite_node_errmsg(dqlite_node *n);

/**
 * Generate a unique ID for the given address.
 */
DQLITE_API dqlite_node_id dqlite_generate_node_id(const char *address);

/**
 * This function is DEPRECATED and will be removed in a future major release.
 *
 * Initialize the given SQLite VFS interface object with dqlite's custom
 * implementation, which can be used for replication.
 */
DQLITE_API int dqlite_vfs_init(sqlite3_vfs *vfs, const char *name);

DQLITE_API int dqlite_vfs_enable_disk(sqlite3_vfs *vfs);

/**
 * This function is DEPRECATED and will be removed in a future major release.
 *
 * Release all memory used internally by a SQLite VFS object that was
 * initialized using @qlite_vfs_init.
 */
DQLITE_API void dqlite_vfs_close(sqlite3_vfs *vfs);

/**
 * This type is DEPRECATED and will be removed in a future major release.
 *
 * A single WAL frame to be replicated.
 */
struct dqlite_vfs_frame
{
	unsigned long page_number; /* Database page number. */
	void *data;                /* Content of the database page. */
};
typedef struct dqlite_vfs_frame dqlite_vfs_frame;

/**
 * This function is DEPRECATED and will be removed in a future major release.
 *
 * Check if the last call to sqlite3_step() has triggered a write transaction on
 * the database with the given filename. In that case acquire a WAL write lock
 * to prevent further write transactions, and return all new WAL frames
 * generated by the transaction. These frames are meant to be replicated across
 * nodes and then actually added to the WAL with dqlite_vfs_apply() once a
 * quorum is reached. If a quorum is not reached within a given time, then
 * dqlite_vfs_abort() can be used to abort and release the WAL write lock.
 */
DQLITE_API int dqlite_vfs_poll(sqlite3_vfs *vfs,
			       const char *filename,
			       dqlite_vfs_frame **frames,
			       unsigned *n);

/**
 * This function is DEPRECATED and will be removed in a future major release.
 *
 * Add to the WAL all frames that were generated by a write transaction
 * triggered by sqlite3_step() and that were obtained via dqlite_vfs_poll().
 *
 * This interface is designed to match the typical use case of a node receiving
 * the frames by sequentially reading a byte stream from a network socket and
 * passing the data to this routine directly without any copy or futher
 * allocation, possibly except for integer encoding/decoding.
 */
DQLITE_API int dqlite_vfs_apply(sqlite3_vfs *vfs,
				const char *filename,
				unsigned n,
				unsigned long *page_numbers,
				void *frames);

/**
 * This function is DEPRECATED and will be removed in a future major release.
 *
 * Abort a pending write transaction that was triggered by sqlite3_step() and
 * whose frames were obtained via dqlite_vfs_poll().
 *
 * This should be called if the transaction could not be safely replicated. In
 * particular it will release the write lock acquired by dqlite_vfs_poll().
 */
DQLITE_API int dqlite_vfs_abort(sqlite3_vfs *vfs, const char *filename);

/**
 * This function is DEPRECATED and will be removed in a future major release.
 *
 * Return a snapshot of the main database file and of the WAL file.
 */
DQLITE_API int dqlite_vfs_snapshot(sqlite3_vfs *vfs,
				   const char *filename,
				   void **data,
				   size_t *n);

/**
 * This type is DEPRECATED and will be removed in a future major release.
 *
 * A data buffer.
 */
struct dqlite_buffer
{
	void *base; /* Pointer to the buffer data. */
	size_t len; /* Length of the buffer. */
};

/**
 * This function is DEPRECATED and will be removed in a future major release.
 *
 * Return a shallow snapshot of the main database file and of the WAL file.
 * Expects a bufs array of size x + 1, where x is obtained from
 * `dqlite_vfs_num_pages`.
 */
DQLITE_API int dqlite_vfs_shallow_snapshot(sqlite3_vfs *vfs,
					   const char *filename,
					   struct dqlite_buffer bufs[],
					   unsigned n);

/**
 * This function is DEPRECATED and will be removed in a future major release.
 */
DQLITE_API int dqlite_vfs_snapshot_disk(sqlite3_vfs *vfs,
					const char *filename,
					struct dqlite_buffer bufs[],
					unsigned n);

/**
 * This function is DEPRECATED and will be removed in a future major release.
 *
 * Return the number of database pages (excluding WAL).
 */
DQLITE_API int dqlite_vfs_num_pages(sqlite3_vfs *vfs,
				    const char *filename,
				    unsigned *n);

/**
 * This function is DEPRECATED and will be removed in a future major release.
 *
 * Restore a snapshot of the main database file and of the WAL file.
 */
DQLITE_API int dqlite_vfs_restore(sqlite3_vfs *vfs,
				  const char *filename,
				  const void *data,
				  size_t n);

/**
 * This function is DEPRECATED and will be removed in a future major release.
 *
 * Restore a snapshot of the main database file and of the WAL file.
 */
DQLITE_API int dqlite_vfs_restore_disk(sqlite3_vfs *vfs,
				       const char *filename,
				       const void *data,
				       size_t main_size,
				       size_t wal_size);
#endif /* DQLITE_H */
