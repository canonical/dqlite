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

/* Request types */
#define DQLITE_HELO      0
#define DQLITE_HEARTBEAT 1
#define DQLITE_OPEN      2
#define DQLITE_PREPARE   3
#define DQLITE_EXEC      4
#define DQLITE_QUERY     5
#define DQLITE_FINALIZE  6

/* Response types */
#define DQLITE_WELCOME  0
#define DQLITE_SERVERS  1
#define DQLITE_DB_ERROR 2
#define DQLITE_DB       3
#define DQLITE_STMT     4
#define DQLITE_RESULT   5
#define DQLITE_ROWS     6
#define DQLITE_EMPTY    7

#define DQLITE_PROTOCOL_VERSION 0x86104dd760433fe5

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
