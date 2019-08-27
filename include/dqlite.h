#ifndef DQLITE_H
#define DQLITE_H

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
int dqlite_node_create(unsigned id,
		       const char *address,
		       const char *data_dir,
		       dqlite_node **t);

/**
 * Destroy a dqlite node object.
 *
 * This will release all memory that was allocated by the node. If
 * dqlite_node_start() was successfully invoked, then dqlite_node_stop() must be
 * invoked before destroying the node.
 */
void dqlite_node_destroy(dqlite_node *t);

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
int dqlite_node_set_bind_address(dqlite_node *t, const char *address);

/**
 * Get the network address that the dqlite node is using to accept incoming
 * connections.
 */
const char * dqlite_node_get_bind_address(dqlite_node *t);

/**
 * Set a custom connect function.
 *
 * The function should block until a connection with the dqlite node identified
 * by the given @id and @address is established, or an error occurs.
 *
 * In case of success, the file descriptor of the connected socket must be saved
 * into the location pointed by the @fd argument. The socket must be either a
 * TCP or a Unix socket.
 */
int dqlite_node_set_connect_func(
    dqlite_node *t,
    int (*f)(void *arg, unsigned id, const char *address, int *fd),
    void *arg);

/**
 * Start a dqlite node.
 *
 * A background thread will be spawned which will run the node's main loop. If
 * this function returns successfully, the dqlite node is ready to accept new
 * connections.
 */
int dqlite_node_start(dqlite_node *t);

/**
 * Stop a dqlite node.
 *
 * The background thread running the main loop will be notified and the node
 * will not accept any new client connections. Once inflight requests are
 * completed, open client connections get closed and then the thread exits.
 */
int dqlite_node_stop(dqlite_node *t);

#endif /* DQLITE_H */
