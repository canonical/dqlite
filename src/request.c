#include <assert.h>
#include <stdint.h>
#include <string.h>

#include "request.h"
#include "schema.h"

/* Set the maximum data size to 1M, to prevent eccessive memmory allocation
 * in case of client bugs
 *
 * TODO: relax this limit since it prevents inserting large blobs.
 */
#define DQLITE_REQUEST_MAX_DATA_SIZE 1048576

DQLITE__SCHEMA_IMPLEMENT(dqlite__request_leader, DQLITE__REQUEST_SCHEMA_LEADER);
DQLITE__SCHEMA_IMPLEMENT(dqlite__request_client, DQLITE__REQUEST_SCHEMA_CLIENT);
DQLITE__SCHEMA_IMPLEMENT(dqlite__request_heartbeat,
                         DQLITE__REQUEST_SCHEMA_HEARTBEAT);
DQLITE__SCHEMA_IMPLEMENT(dqlite__request_open, DQLITE__REQUEST_SCHEMA_OPEN);
DQLITE__SCHEMA_IMPLEMENT(dqlite__request_prepare,
                         DQLITE__REQUEST_SCHEMA_PREPARE);
DQLITE__SCHEMA_IMPLEMENT(dqlite__request_exec, DQLITE__REQUEST_SCHEMA_EXEC);
DQLITE__SCHEMA_IMPLEMENT(dqlite__request_query, DQLITE__REQUEST_SCHEMA_QUERY);
DQLITE__SCHEMA_IMPLEMENT(dqlite__request_finalize,
                         DQLITE__REQUEST_SCHEMA_FINALIZE);
DQLITE__SCHEMA_IMPLEMENT(dqlite__request_exec_sql,
                         DQLITE__REQUEST_SCHEMA_EXEC_SQL);
DQLITE__SCHEMA_IMPLEMENT(dqlite__request_query_sql,
                         DQLITE__REQUEST_SCHEMA_QUERY_SQL);
DQLITE__SCHEMA_IMPLEMENT(dqlite__request_interrupt,
                         DQLITE__REQUEST_SCHEMA_INTERRUPT);

DQLITE__SCHEMA_HANDLER_IMPLEMENT(dqlite__request, DQLITE__REQUEST_SCHEMA_TYPES);
