#ifndef DQLITE_RESPONSE_H
#define DQLITE_RESPONSE_H

#include "schema.h"

/* The size of pre-allocated response buffer. This should generally fit in
 * a single IP packet, given typical MTU sizes */
#define DQLITE__RESPONSE_BUF_SIZE 1024

#define DQLITE__RESPONSE_SCHEMA_FAILURE(X, ...)                                     \
	X(uint64, code, __VA_ARGS__)                                                \
	X(text, message, __VA_ARGS__)

#define DQLITE__RESPONSE_SCHEMA_SERVER(X, ...) X(text, address, __VA_ARGS__)

#define DQLITE__RESPONSE_SCHEMA_WELCOME(X, ...)                                     \
	X(uint64, heartbeat_timeout, __VA_ARGS__)

#define DQLITE__RESPONSE_SCHEMA_SERVERS(X, ...) X(servers, servers, __VA_ARGS__)

#define DQLITE__RESPONSE_SCHEMA_DB(X, ...)                                          \
	X(uint32, id, __VA_ARGS__)                                                  \
	X(uint32, __pad__, __VA_ARGS__)

#define DQLITE__RESPONSE_SCHEMA_STMT(X, ...)                                        \
	X(uint32, db_id, __VA_ARGS__)                                               \
	X(uint32, id, __VA_ARGS__)                                                  \
	X(uint64, params, __VA_ARGS__)

#define DQLITE__RESPONSE_SCHEMA_RESULT(X, ...)                                      \
	X(uint64, last_insert_id, __VA_ARGS__)                                      \
	X(uint64, rows_affected, __VA_ARGS__)

#define DQLITE__RESPONSE_SCHEMA_ROWS(X, ...) X(uint64, eof, __VA_ARGS__)

#define DQLITE__RESPONSE_SCHEMA_EMPTY(X, ...) X(uint64, __unused__, __VA_ARGS__)

DQLITE__SCHEMA_DEFINE(dqlite__response_failure, DQLITE__RESPONSE_SCHEMA_FAILURE);
DQLITE__SCHEMA_DEFINE(dqlite__response_server, DQLITE__RESPONSE_SCHEMA_SERVER);
DQLITE__SCHEMA_DEFINE(dqlite__response_welcome, DQLITE__RESPONSE_SCHEMA_WELCOME);
DQLITE__SCHEMA_DEFINE(dqlite__response_servers, DQLITE__RESPONSE_SCHEMA_SERVERS);
DQLITE__SCHEMA_DEFINE(dqlite__response_db, DQLITE__RESPONSE_SCHEMA_DB);
DQLITE__SCHEMA_DEFINE(dqlite__response_stmt, DQLITE__RESPONSE_SCHEMA_STMT);
DQLITE__SCHEMA_DEFINE(dqlite__response_result, DQLITE__RESPONSE_SCHEMA_RESULT);
DQLITE__SCHEMA_DEFINE(dqlite__response_rows, DQLITE__RESPONSE_SCHEMA_ROWS);
DQLITE__SCHEMA_DEFINE(dqlite__response_empty, DQLITE__RESPONSE_SCHEMA_EMPTY);

#define DQLITE__RESPONSE_SCHEMA_TYPES(X, ...)                                       \
	X(DQLITE_RESPONSE_FAILURE, dqlite__response_failure, failure, __VA_ARGS__)  \
	X(DQLITE_RESPONSE_SERVER, dqlite__response_server, server, __VA_ARGS__)     \
	X(DQLITE_RESPONSE_WELCOME, dqlite__response_welcome, welcome, __VA_ARGS__)  \
	X(DQLITE_RESPONSE_SERVERS, dqlite__response_servers, servers, __VA_ARGS__)  \
	X(DQLITE_RESPONSE_DB, dqlite__response_db, db, __VA_ARGS__)                 \
	X(DQLITE_RESPONSE_STMT, dqlite__response_stmt, stmt, __VA_ARGS__)           \
	X(DQLITE_RESPONSE_RESULT, dqlite__response_result, result, __VA_ARGS__)     \
	X(DQLITE_RESPONSE_ROWS, dqlite__response_rows, rows, __VA_ARGS__)           \
	X(DQLITE_RESPONSE_EMPTY, dqlite__response_empty, empty, __VA_ARGS__)

DQLITE__SCHEMA_HANDLER_DEFINE(dqlite__response, DQLITE__RESPONSE_SCHEMA_TYPES);

#endif /* DQLITE_RESPONSE_H */
