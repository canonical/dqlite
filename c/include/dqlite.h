#ifndef DQLITE_H
#define DQLITE_H

#include <stdio.h>
#include <stdint.h>

/* #ifdef __cplusplus */
/* extern "C" { */
/* #endif */

#if defined(__cplusplus) || (defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901L)
#define DQLITE_INLINE static inline
#else
#define DQLITE_INLINE static
#endif

/* Error codes */
#define DQLITE_OK       0
#define DQLITE_ERROR    1
#define DQLITE_NOMEM    2
#define DQLITE_PROTO    3
#define DQLITE_PARSE    4
#define DQLITE_OVERFLOW 5
#define DQLITE_EOM      6 /* End of message */
#define DQLITE_ENGINE   7 /* A SQLite error occurred */
#define DQLITE_NOTFOUND 8

/* Config op codes */
#define DQLITE_CONFIG_VFS

/* Current protocol version */
#define DQLITE_PROTOCOL_VERSION 0x86104dd760433fe5

/* Request types */
#define DQLITE_REQUEST_LEADER    0
#define DQLITE_REQUEST_CLIENT    1
#define DQLITE_REQUEST_HEARTBEAT 2
#define DQLITE_REQUEST_OPEN      3
#define DQLITE_REQUEST_PREPARE   4
#define DQLITE_REQUEST_EXEC      5
#define DQLITE_REQUEST_QUERY     6
#define DQLITE_REQUEST_FINALIZE  7

/* Response types */
#define DQLITE_RESPONSE_SERVER   0
#define DQLITE_RESPONSE_WELCOME  1
#define DQLITE_RESPONSE_SERVERS  2
#define DQLITE_RESPONSE_DB_ERROR 3
#define DQLITE_RESPONSE_DB       4
#define DQLITE_RESPONSE_STMT     5
#define DQLITE_RESPONSE_RESULT   6
#define DQLITE_RESPONSE_ROWS     7
#define DQLITE_RESPONSE_EMPTY    8

/* Interface implementing cluster-related functionality */
typedef struct dqlite_cluster {
	void *ctx;
	const char *(*xLeader)(void *ctx);
	int         (*xServers)(void *ctx, const char ***addresses);
	int         (*xRecover)(void *ctx, uint64_t tx_token);
} dqlite_cluster;

/* Handle connections from dqlite clients */
typedef struct dqlite__server dqlite_server;

/* Allocate and free a dqlite server */
dqlite_server *dqlite_server_alloc();
void dqlite_server_free(dqlite_server *s);

/* Initialize and release resources used by a dqlite server */
int dqlite_server_init(dqlite_server *s, FILE *log, dqlite_cluster *cluster);
void dqlite_server_close(dqlite_server *s);

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

/* Stop a dqlite server.
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
const char* dqlite_server_errmsg(dqlite_server *s);

/* Return the dqlite_cluster object used to initialize the server */
dqlite_cluster *dqlite_server_cluster(dqlite_server *s);

#endif /* DQLITE_H */
