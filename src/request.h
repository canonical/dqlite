#ifndef DQLITE_REQUEST_H
#define DQLITE_REQUEST_H

#include <stdint.h>

#include "../include/dqlite.h"

#include "schema.h"

/*
 * Request types.
 */

#define DQLITE__REQUEST_SCHEMA_LEADER(X, ...) X(uint64, __unused__, __VA_ARGS__)

#define DQLITE__REQUEST_SCHEMA_CLIENT(X, ...) X(uint64, id, __VA_ARGS__)

#define DQLITE__REQUEST_SCHEMA_HEARTBEAT(X, ...)                               \
	X(uint64, timestamp, __VA_ARGS__)

#define DQLITE__REQUEST_SCHEMA_OPEN(X, ...)                                    \
	X(text, name, __VA_ARGS__)                                             \
	X(uint64, flags, __VA_ARGS__)                                          \
	X(text, vfs, __VA_ARGS__)

#define DQLITE__REQUEST_SCHEMA_PREPARE(X, ...)                                 \
	X(uint64, db_id, __VA_ARGS__)                                          \
	X(text, sql, __VA_ARGS__)

#define DQLITE__REQUEST_SCHEMA_EXEC(X, ...)                                    \
	X(uint32, db_id, __VA_ARGS__)                                          \
	X(uint32, stmt_id, __VA_ARGS__)

#define DQLITE__REQUEST_SCHEMA_QUERY(X, ...)                                   \
	X(uint32, db_id, __VA_ARGS__)                                          \
	X(uint32, stmt_id, __VA_ARGS__)

#define DQLITE__REQUEST_SCHEMA_FINALIZE(X, ...)                                \
	X(uint32, db_id, __VA_ARGS__)                                          \
	X(uint32, stmt_id, __VA_ARGS__)

#define DQLITE__REQUEST_SCHEMA_EXEC_SQL(X, ...)                                \
	X(uint64, db_id, __VA_ARGS__)                                          \
	X(text, sql, __VA_ARGS__)

#define DQLITE__REQUEST_SCHEMA_QUERY_SQL(X, ...)                               \
	X(uint64, db_id, __VA_ARGS__)                                          \
	X(text, sql, __VA_ARGS__)

#define DQLITE__REQUEST_SCHEMA_INTERRUPT(X, ...) X(uint64, db_id, __VA_ARGS__)

DQLITE__SCHEMA_DEFINE(dqlite__request_leader, DQLITE__REQUEST_SCHEMA_LEADER);
DQLITE__SCHEMA_DEFINE(dqlite__request_client, DQLITE__REQUEST_SCHEMA_CLIENT);
DQLITE__SCHEMA_DEFINE(dqlite__request_heartbeat,
                      DQLITE__REQUEST_SCHEMA_HEARTBEAT);
DQLITE__SCHEMA_DEFINE(dqlite__request_open, DQLITE__REQUEST_SCHEMA_OPEN);
DQLITE__SCHEMA_DEFINE(dqlite__request_prepare, DQLITE__REQUEST_SCHEMA_PREPARE);
DQLITE__SCHEMA_DEFINE(dqlite__request_query, DQLITE__REQUEST_SCHEMA_QUERY);
DQLITE__SCHEMA_DEFINE(dqlite__request_exec, DQLITE__REQUEST_SCHEMA_EXEC);
DQLITE__SCHEMA_DEFINE(dqlite__request_finalize,
                      DQLITE__REQUEST_SCHEMA_FINALIZE);
DQLITE__SCHEMA_DEFINE(dqlite__request_exec_sql,
                      DQLITE__REQUEST_SCHEMA_EXEC_SQL);
DQLITE__SCHEMA_DEFINE(dqlite__request_query_sql,
                      DQLITE__REQUEST_SCHEMA_QUERY_SQL);
DQLITE__SCHEMA_DEFINE(dqlite__request_interrupt,
                      DQLITE__REQUEST_SCHEMA_INTERRUPT);

#define DQLITE__REQUEST_SCHEMA_TYPES(X, ...)                                   \
	X(DQLITE_REQUEST_LEADER, dqlite__request_leader, leader, __VA_ARGS__)  \
	X(DQLITE_REQUEST_CLIENT, dqlite__request_client, client, __VA_ARGS__)  \
	X(DQLITE_REQUEST_HEARTBEAT,                                            \
	  dqlite__request_heartbeat,                                           \
	  heartbeat,                                                           \
	  __VA_ARGS__)                                                         \
	X(DQLITE_REQUEST_OPEN, dqlite__request_open, open, __VA_ARGS__)        \
	X(DQLITE_REQUEST_PREPARE,                                              \
	  dqlite__request_prepare,                                             \
	  prepare,                                                             \
	  __VA_ARGS__)                                                         \
	X(DQLITE_REQUEST_EXEC, dqlite__request_exec, exec, __VA_ARGS__)        \
	X(DQLITE_REQUEST_QUERY, dqlite__request_query, query, __VA_ARGS__)     \
	X(DQLITE_REQUEST_FINALIZE,                                             \
	  dqlite__request_finalize,                                            \
	  finalize,                                                            \
	  __VA_ARGS__)                                                         \
	X(DQLITE_REQUEST_EXEC_SQL,                                             \
	  dqlite__request_exec_sql,                                            \
	  exec_sql,                                                            \
	  __VA_ARGS__)                                                         \
	X(DQLITE_REQUEST_QUERY_SQL,                                            \
	  dqlite__request_query_sql,                                           \
	  query_sql,                                                           \
	  __VA_ARGS__)                                                         \
	X(DQLITE_REQUEST_INTERRUPT,                                            \
	  dqlite__request_interrupt,                                           \
	  interrupt,                                                           \
	  __VA_ARGS__)

DQLITE__SCHEMA_HANDLER_DEFINE(dqlite__request, DQLITE__REQUEST_SCHEMA_TYPES);

#endif /* DQLITE_REQUEST_H */
