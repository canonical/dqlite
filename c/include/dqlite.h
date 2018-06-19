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

/* Response types */
#define DQLITE_WELCOME  0
#define DQLITE_SERVERS  1
#define DQLITE_DB_ERROR 2
#define DQLITE_DB       3
#define DQLITE_STMT     4
#define DQLITE_RESULT   5
#define DQLITE_ROWS     6

#define DQLITE_PROTOCOL_VERSION 0x86104dd760433fe5

typedef struct dqlite__service dqlite_service;
typedef struct dqlite_cluster {
	void *ctx;
	const char  *(*xLeader)(void*);
	const char **(*xServers)(void*);
	int          (*xRecover)(void*, uint64_t);
} dqlite_cluster;

dqlite_service *dqlite_service_alloc();
void dqlite_service_free(dqlite_service *s);

int dqlite_service_init(dqlite_service *s, FILE *log, dqlite_cluster *cluster);
void dqlite_service_close(dqlite_service *s);

int dqlite_service_run(dqlite_service *s);

/* Stop a dqlite service.
**
** This is a thread-safe API.
**
** In case of error, the caller must invoke sqlite3_free
** against the returned errmsg.
*/
int dqlite_service_stop(dqlite_service *s, char **errmsg);

/* Start handling a new connection.
**
** This is a thread-safe API.
**
** In case of error, the caller must invoke sqlite3_free
** against the returned errmsg.
*/
int dqlite_service_handle(dqlite_service *s, int socket, char **errrmsg);

const char* dqlite_service_errmsg(dqlite_service*);

#endif /* DQLITE_H */
