#ifndef DQLITE_RESPONSE_H_
#define DQLITE_RESPONSE_H_

#include "lib/serialize.h"

#include "schema.h"

/**
 * Response types.
 */

#define RESPONSE_SERVER(X, ...) X(text, address, ##__VA_ARGS__)
#define RESPONSE_WELCOME(X, ...) X(uint64, heartbeat_timeout, ##__VA_ARGS__)
#define RESPONSE_FAILURE(X, ...)       \
	X(uint64, code, ##__VA_ARGS__) \
	X(text, message, ##__VA_ARGS__)
#define RESPONSE_DB(X, ...)          \
	X(uint32, id, ##__VA_ARGS__) \
	X(uint32, __pad__, ##__VA_ARGS__)
#define RESPONSE_STMT(X, ...)           \
	X(uint32, db_id, ##__VA_ARGS__) \
	X(uint32, id, ##__VA_ARGS__)    \
	X(uint64, params, ##__VA_ARGS__)
#define RESPONSE_RESULT(X, ...)                 \
	X(uint64, last_insert_id, ##__VA_ARGS__) \
	X(uint64, rows_affected, ##__VA_ARGS__)

#define RESPONSE__DEFINE(LOWER, UPPER, _) \
	SERIALIZE__DEFINE(response_##LOWER, RESPONSE_##UPPER);

#define RESPONSE__TYPES(X, ...)          \
	X(server, SERVER, __VA_ARGS__)   \
	X(welcome, WELCOME, __VA_ARGS__) \
	X(failure, FAILURE, __VA_ARGS__) \
	X(db, DB, __VA_ARGS__)           \
	X(stmt, STMT, __VA_ARGS__)       \
	X(result, RESULT, __VA_ARGS__)

RESPONSE__TYPES(RESPONSE__DEFINE);

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

SCHEMA__DEFINE(response_failure_, RESPONSE__SCHEMA_FAILURE);
SCHEMA__DEFINE(response_server_, RESPONSE__SCHEMA_SERVER);
SCHEMA__DEFINE(response_welcome_, RESPONSE__SCHEMA_WELCOME);
SCHEMA__DEFINE(response_servers, RESPONSE__SCHEMA_SERVERS);
SCHEMA__DEFINE(response_db_, RESPONSE__SCHEMA_DB);
SCHEMA__DEFINE(response_stmt_, RESPONSE__SCHEMA_STMT);
SCHEMA__DEFINE(response_result_, RESPONSE__SCHEMA_RESULT);
SCHEMA__DEFINE(response_rows, RESPONSE__SCHEMA_ROWS);
SCHEMA__DEFINE(response_empty, RESPONSE__SCHEMA_EMPTY);

#define RESPONSE__SCHEMA_TYPES(X, ...)                                      \
	X(DQLITE_RESPONSE_FAILURE, response_failure_, failure, __VA_ARGS__) \
	X(DQLITE_RESPONSE_SERVER, response_server_, server, __VA_ARGS__)    \
	X(DQLITE_RESPONSE_WELCOME, response_welcome_, welcome, __VA_ARGS__) \
	X(DQLITE_RESPONSE_SERVERS, response_servers, servers, __VA_ARGS__)  \
	X(DQLITE_RESPONSE_DB, response_db_, db, __VA_ARGS__)                \
	X(DQLITE_RESPONSE_STMT, response_stmt_, stmt, __VA_ARGS__)          \
	X(DQLITE_RESPONSE_RESULT, response_result_, result, __VA_ARGS__)    \
	X(DQLITE_RESPONSE_ROWS, response_rows, rows, __VA_ARGS__)           \
	X(DQLITE_RESPONSE_EMPTY, response_empty, empty, __VA_ARGS__)

SCHEMA__HANDLER_DEFINE(response, RESPONSE__SCHEMA_TYPES);

#endif /* DQLITE_RESPONSE_H_ */
