#ifndef DQLITE_H
#define DQLITE_H

#include <stdint.h>
#include <stdio.h>

#include <sqlite3.h>

/* #ifdef __cplusplus */
/* extern "C" { */
/* #endif */

/* Error codes */
#define DQLITE_ERROR 1
#define DQLITE_NOMEM 2
#define DQLITE_PROTO 3
#define DQLITE_PARSE 4
#define DQLITE_OVERFLOW 5
#define DQLITE_EOM 6    /* End of message */
#define DQLITE_ENGINE 7 /* A SQLite error occurred */
#define DQLITE_NOTFOUND 8
#define DQLITE_STOPPED 9 /* The server was stopped */

/* Current protocol version */
#define DQLITE_PROTOCOL_VERSION 0x86104dd760433fe5

/* Request types */
#define DQLITE_REQUEST_LEADER 0
#define DQLITE_REQUEST_CLIENT 1
#define DQLITE_REQUEST_HEARTBEAT 2
#define DQLITE_REQUEST_OPEN 3
#define DQLITE_REQUEST_PREPARE 4
#define DQLITE_REQUEST_EXEC 5
#define DQLITE_REQUEST_QUERY 6
#define DQLITE_REQUEST_FINALIZE 7
#define DQLITE_REQUEST_EXEC_SQL 8
#define DQLITE_REQUEST_QUERY_SQL 9
#define DQLITE_REQUEST_INTERRUPT 10

/* Response types */
#define DQLITE_RESPONSE_FAILURE 0
#define DQLITE_RESPONSE_SERVER 1
#define DQLITE_RESPONSE_WELCOME 2
#define DQLITE_RESPONSE_SERVERS 3
#define DQLITE_RESPONSE_DB 4
#define DQLITE_RESPONSE_STMT 5
#define DQLITE_RESPONSE_RESULT 6
#define DQLITE_RESPONSE_ROWS 7
#define DQLITE_RESPONSE_EMPTY 8

/* Special datatypes */
#define DQLITE_UNIXTIME 9
#define DQLITE_ISO8601 10
#define DQLITE_BOOLEAN 11

/* Log levels */
#define DQLITE_LOG_DEBUG 0
#define DQLITE_LOG_INFO 1
#define DQLITE_LOG_WARN 2
#define DQLITE_LOG_ERROR 3

/* Config opcodes */
#define DQLITE_CONFIG_LOGGER 0
#define DQLITE_CONFIG_VFS 1
#define DQLITE_CONFIG_WAL_REPLICATION 2
#define DQLITE_CONFIG_HEARTBEAT_TIMEOUT 3
#define DQLITE_CONFIG_PAGE_SIZE 4
#define DQLITE_CONFIG_CHECKPOINT_THRESHOLD 5
#define DQLITE_CONFIG_METRICS 6

/* Special value indicating that a batch of rows is over, but there are more. */
#define DQLITE_RESPONSE_ROWS_PART 0xeeeeeeeeeeeeeeee

/* Special value indicating that the result set is complete. */
#define DQLITE_RESPONSE_ROWS_DONE 0xffffffffffffffff

/* Initialize SQLite global state with values specific to dqlite
 *
 * This API must be called exactly once before any other SQLite or dqlite API
 * call is invoked in a process.
 */
int dqlite_init(const char **ermsg);

/* Interface implementing logging functionality */
typedef struct dqlite_logger {
	void *ctx;
	void (*xLogf)(void *ctx, int level, const char *format, ...);
} dqlite_logger;

/* Interface implementing cluster-related functionality */
typedef struct dqlite_server_info {
	uint64_t    id;
	const char *address;
} dqlite_server_info;

/* The memory returned by a method of the cluster interface must be valid until
 * the next invokation of the same method. */
typedef struct dqlite_cluster {
	void *ctx;
	const char *(*xLeader)(void *ctx);
	int (*xServers)(void *ctx, dqlite_server_info *servers[]);
	void (*xRegister)(void *ctx, sqlite3 *db);
	void (*xUnregister)(void *ctx, sqlite3 *db);
	int (*xBarrier)(void *ctx);
	int (*xRecover)(void *ctx, uint64_t tx_token);
	int (*xCheckpoint)(void *ctx, sqlite3 *db);
} dqlite_cluster;

/* Handle connections from dqlite clients */
typedef struct dqlite__server dqlite_server;

/* Allocate and initialize a dqlite server instance. */
int dqlite_server_create(dqlite_cluster *cluster, dqlite_server **out);

/* Destroy and deallocate a dqlite server instance. */
void dqlite_server_destroy(dqlite_server *s);

/* Set a config option on a dqlite server
 *
 * This API must be called after dqlite_server_init and before
 * dqlite_server_run.
 */
int dqlite_server_config(dqlite_server *s, int op, void *arg);

/* Start a dqlite server.
 *
 * In case of error, a human-readable message describing the failure can be
 * obtained using dqlite_server_errmsg.
 */
int dqlite_server_run(dqlite_server *s);

/* Wait until a dqlite server is ready and can handle connections.
**
** Returns 1 if the server has been successfully started, 0 otherwise.
**
** This is a thread-safe API, but must be invoked before any call to
** dqlite_server_stop or dqlite_server_handle.
*/
int dqlite_server_ready(dqlite_server *s);

/* Stop a dqlite server and wait for it to shutdown.
**
** This is a thread-safe API.
**
** In case of error, the caller must invoke sqlite3_free
** against the returned errmsg.
*/
int dqlite_server_stop(dqlite_server *s, char **errmsg);

/* Start handling a new connection.
**
** This is a thread-safe API.
**
** In case of error, the caller must invoke sqlite3_free
** against the returned errmsg.
*/
int dqlite_server_handle(dqlite_server *s, int socket, char **errrmsg);

/* Return a message describing the most recent error occurred.
 *
 * This is API not thread-safe.
 *
 * The memory holding returned string is managed by the dqlite_server object
 * internally, and will be valid until dqlite_server_close is invoked. However,
 * the message contained in the string itself might change if another error
 * occurs in the meantime.
 */
const char *dqlite_server_errmsg(dqlite_server *s);

/* Return the dqlite_cluster object used to initialize the server */
dqlite_cluster *dqlite_server_cluster(dqlite_server *s);

/* Return the dqlite_logger object the server is using, if any was
 * configured. */
dqlite_logger *dqlite_server_logger(dqlite_server *s);

/* Allocate and initialize an in-memory dqlite VFS object, configured with the
 * given registration name.
 *
 * A copy of the provided name will be made, so clients can free it after the
 * function returns. */
sqlite3_vfs *dqlite_vfs_create(const char *name, dqlite_logger *logger);

/* Destroy and deallocate an in-memory dqlite VFS object. */
void dqlite_vfs_destroy(sqlite3_vfs *vfs);

/* Read the content of a file, using the VFS implementation registered under the
 * given name. Used to take database snapshots using the dqlite in-memory
 * VFS. */
int dqlite_file_read(const char *vfs_name,
                     const char *filename,
                     uint8_t **  buf,
                     size_t *    len);

/* Write the content of a file, using the VFS implementation registered under
 * the given name. Used to restore database snapshots against the dqlite
 * in-memory VFS. If the file already exists, it's overwritten. */
int dqlite_file_write(const char *vfs_name,
                      const char *filename,
                      uint8_t *   buf,
                      size_t      len);

#endif /* DQLITE_H */
