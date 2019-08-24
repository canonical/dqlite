#ifndef DQLITE_H
#define DQLITE_H

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

#include <sqlite3.h>
#include <uv.h>

/* Error codes */
enum { DQLITE_BADSOCKET = 1,
       DQLITE_MISUSE,
       DQLITE_NOMEM,
       DQLITE_PROTO,
       DQLITE_PARSE,
       DQLITE_OVERFLOW,
       DQLITE_EOM,      /* End of message */
       DQLITE_INTERNAL, /* A SQLite error occurred */
       DQLITE_NOTFOUND,
       DQLITE_STOPPED, /* The server was stopped */
       DQLITE_CANTBOOTSTRAP };

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
#define DQLITE_REQUEST_CONNECT 11
#define DQLITE_REQUEST_JOIN 12
#define DQLITE_REQUEST_PROMOTE 13
#define DQLITE_REQUEST_REMOVE 14
#define DQLITE_REQUEST_DUMP 15
#define DQLITE_REQUEST_CLUSTER 16

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
#define DQLITE_RESPONSE_FILES 9

/* Special datatypes */
#define DQLITE_UNIXTIME 9
#define DQLITE_ISO8601 10
#define DQLITE_BOOLEAN 11

/* Log levels */
enum { DQLITE_DEBUG = 0, DQLITE_INFO, DQLITE_WARN, DQLITE_ERROR };

/* State codes. */
enum { DQLITE_UNAVAILABLE, DQLITE_FOLLOWER, DQLITE_CANDIDATE, DQLITE_LEADER };

/* Config opcodes */
#define DQLITE_CONFIG_WATCHER 5

/* Special value indicating that a batch of rows is over, but there are more. */
#define DQLITE_RESPONSE_ROWS_PART 0xeeeeeeeeeeeeeeee

/* Special value indicating that the result set is complete. */
#define DQLITE_RESPONSE_ROWS_DONE 0xffffffffffffffff

/* Handle connections from dqlite clients */
typedef struct dqlite_task dqlite_task;

int dqlite_task_create(unsigned server_id,
		       const char *advertise_address,
		       const char *data_dir,
		       dqlite_task **t);

void dqlite_task_destroy(dqlite_task *t);

int dqlite_task_set_bind_address(dqlite_task *t, const char *address);

int dqlite_task_set_connect_func(
    dqlite_task *t,
    int (*f)(void *arg, unsigned id, const char *address, int *fd),
    void *arg);

/* Allocate and initialize a dqlite server instance. */
int dqlite_task_start(dqlite_task *t);

/* Function to be notified about state changes. */
typedef void (*dqlite_watch)(void *data, int old_state, int new_state);

/* Set a config option on a dqlite server
 *
 * This API must be called after dqlite_init and before
 * dqlite_run.
 */
int dqlite_config(dqlite_task *t, int op, ...);

/* Stop a dqlite server.
**
** This is a thread-safe API.
**
** In case of error, the caller must invoke sqlite3_free
** against the returned errmsg.
*/
int dqlite_task_stop(dqlite_task *t);

#endif /* DQLITE_H */
