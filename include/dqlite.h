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
 * Dqlite task handle.
 *
 * Opaque handle to a dqlite background task that the user application process
 * should run in order to handle database client requests and data replication.
 */
typedef struct dqlite_task dqlite_task;

/**
 * Create a new dqlite task object.
 *
 * The @server_id argument a is positive number that identifies this particular
 * dqlite task in the cluster. Each dqlite task part of the same cluster must be
 * created with a different server ID. The very first task, used to bootstrap a
 * new cluster, must have ID #1. Every time a task is started again, it must be
 * passed the same ID.
 *
 * The @advertise_address argument is a string identifying the network address
 * that other tasks in the cluster should use to connect to this dqlite. The
 * format of string must be "<HOST>:<PORT>", where <HOST> is an IPv4/IPv6
 * address or a DNS name, and <PORT> is a port number. Applications can use
 * dqlite_task_set_connect_func() to customize how the connection gets
 * established. The default is to connect via plain TCP.
 *
 * The @data_dir argument the file system path where the task should store its
 * durable data, such as Raft log entries containing the Write-Ahead log pages
 * of the SQLite databases being replicated.
 *
 * No reference to the memory pointed to by @advertise_address and @data_dir is
 * kept, so any memory associated with them can be released after the function
 * returns.
 */
int dqlite_task_create(unsigned server_id,
		       const char *advertise_address,
		       const char *data_dir,
		       dqlite_task **t);

/**
 * Destroy a dqlite task object.
 *
 * This will release all resources that were allocated by the task. If
 * dqlite_task_start() was successfully invoked, then dqlite_task_stop() must be
 * invoked before destroying the task.
 */
void dqlite_task_destroy(dqlite_task *t);

/**
 * Set the address that this dqlite task should bind to in order to accept
 * connections. It might differ from the @advertise_address passed to
 * @dqlite_task_create if the application wants to act as proxy for incoming
 * connections.
 *
 * Address must be of the form "@<PATH>", which means that the task will bind an
 * abstract Unix socket with the given <PATH>. The task will accept connections
 * only from the same process its running in.
 *
 * No reference to the memory pointed to by @address is kept, so any memory
 * associated with them can be released after the function returns.
 *
 * This function must be called before calling dqlite_task_start().
 */
int dqlite_task_set_bind_address(dqlite_task *t, const char *address);

/**
 * Set a custom connect function.
 *
 * The function should block until a connection with the dqlite task identified
 * by the given @id and @address is established, or an error occurs.
 *
 * In case of success, the file descriptor of the connected socket must be saved
 * into the location pointed by the @fd argument. The socket must be either a
 * TCP or a Unix socket.
 */
int dqlite_task_set_connect_func(
    dqlite_task *t,
    int (*f)(void *arg, unsigned id, const char *address, int *fd),
    void *arg);

/**
 * Start a dqlite task in the background.
 *
 * A thread will be spawned which will run the task's main loop. If this
 * function returns successfully, the dqlite task is ready to accept new
 * connections.
 */
int dqlite_task_start(dqlite_task *t);

/**
 * Stop a dqlite task.
 *
 * The background thread running the thread will be notified of this stop
 * request. At that point the task will stop accepting new connections or
 * request. All inflight requests will be completed and then the thread will
 * exit.
 *
 * If this function returns successfully it means that the thread was gracefully
 * stopped and no error occurred.
 */
int dqlite_task_stop(dqlite_task *t);

#endif /* DQLITE_H */
