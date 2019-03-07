#ifndef DQLITE_RESPONSE_H
#define DQLITE_RESPONSE_H

#include "schema.h"

/* The size of pre-allocated response buffer. This should generally fit in
 * a single IP packet, given typical MTU sizes */
#define RESPONSE__BUF_SIZE 1024

#define RESPONSE__SCHEMA_FAILURE(X, ...) \
	X(uint64, code, __VA_ARGS__)     \
	X(text, message, __VA_ARGS__)

#define RESPONSE__SCHEMA_SERVER(X, ...) X(text, address, __VA_ARGS__)

#define RESPONSE__SCHEMA_WELCOME(X, ...) \
	X(uint64, heartbeat_timeout, __VA_ARGS__)

#define RESPONSE__SCHEMA_SERVERS(X, ...) X(servers, servers, __VA_ARGS__)

#define RESPONSE__SCHEMA_DB(X, ...) \
	X(uint32, id, __VA_ARGS__)  \
	X(uint32, __pad__, __VA_ARGS__)

#define RESPONSE__SCHEMA_STMT(X, ...) \
	X(uint32, db_id, __VA_ARGS__) \
	X(uint32, id, __VA_ARGS__)    \
	X(uint64, params, __VA_ARGS__)

#define RESPONSE__SCHEMA_RESULT(X, ...)        \
	X(uint64, last_insert_id, __VA_ARGS__) \
	X(uint64, rows_affected, __VA_ARGS__)

#define RESPONSE__SCHEMA_ROWS(X, ...) X(uint64, eof, __VA_ARGS__)

#define RESPONSE__SCHEMA_EMPTY(X, ...) X(uint64, __unused__, __VA_ARGS__)

SCHEMA__DEFINE(response_failure, RESPONSE__SCHEMA_FAILURE);
SCHEMA__DEFINE(response_server, RESPONSE__SCHEMA_SERVER);
SCHEMA__DEFINE(response_welcome, RESPONSE__SCHEMA_WELCOME);
SCHEMA__DEFINE(response_servers, RESPONSE__SCHEMA_SERVERS);
SCHEMA__DEFINE(response_db, RESPONSE__SCHEMA_DB);
SCHEMA__DEFINE(response_stmt, RESPONSE__SCHEMA_STMT);
SCHEMA__DEFINE(response_result, RESPONSE__SCHEMA_RESULT);
SCHEMA__DEFINE(response_rows, RESPONSE__SCHEMA_ROWS);
SCHEMA__DEFINE(response_empty, RESPONSE__SCHEMA_EMPTY);

#define RESPONSE__SCHEMA_TYPES(X, ...)                                     \
	X(DQLITE_RESPONSE_FAILURE, response_failure, failure, __VA_ARGS__) \
	X(DQLITE_RESPONSE_SERVER, response_server, server, __VA_ARGS__)    \
	X(DQLITE_RESPONSE_WELCOME, response_welcome, welcome, __VA_ARGS__) \
	X(DQLITE_RESPONSE_SERVERS, response_servers, servers, __VA_ARGS__) \
	X(DQLITE_RESPONSE_DB, response_db, db, __VA_ARGS__)                \
	X(DQLITE_RESPONSE_STMT, response_stmt, stmt, __VA_ARGS__)          \
	X(DQLITE_RESPONSE_RESULT, response_result, result, __VA_ARGS__)    \
	X(DQLITE_RESPONSE_ROWS, response_rows, rows, __VA_ARGS__)          \
	X(DQLITE_RESPONSE_EMPTY, response_empty, empty, __VA_ARGS__)

SCHEMA__HANDLER_DEFINE(response, RESPONSE__SCHEMA_TYPES);

#endif /* DQLITE_RESPONSE_H */
