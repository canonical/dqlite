#ifndef RESPONSE_H_
#define RESPONSE_H_

#include "lib/serialize.h"

/**
 * Response types.
 */

#define RESPONSE_SERVER(X, ...)      \
	X(uint64, id, ##__VA_ARGS__) \
	X(text, address, ##__VA_ARGS__)
#define RESPONSE_SERVER_LEGACY(X, ...) X(text, address, ##__VA_ARGS__)
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
#define RESPONSE_STMT_WITH_OFFSET(X, ...) \
	X(uint32, db_id, ##__VA_ARGS__)   \
	X(uint32, id, ##__VA_ARGS__)      \
	X(uint64, params, ##__VA_ARGS__)  \
	X(uint64, offset, ##__VA_ARGS__)
#define RESPONSE_RESULT(X, ...)                  \
	X(uint64, last_insert_id, ##__VA_ARGS__) \
	X(uint64, rows_affected, ##__VA_ARGS__)
#define RESPONSE_ROWS(X, ...) X(uint64, eof, ##__VA_ARGS__)
#define RESPONSE_EMPTY(X, ...) X(uint64, __unused__, ##__VA_ARGS__)
#define RESPONSE_FILES(X, ...) X(uint64, n, ##__VA_ARGS__)
#define RESPONSE_SERVERS(X, ...) X(uint64, n, ##__VA_ARGS__)
#define RESPONSE_METADATA(X, ...)                \
	X(uint64, failure_domain, ##__VA_ARGS__) \
	X(uint64, weight, ##__VA_ARGS__)

#define RESPONSE__DEFINE(LOWER, UPPER, _) \
	SERIALIZE__DEFINE(response_##LOWER, RESPONSE_##UPPER);

#define RESPONSE__TYPES(X, ...)                            \
	X(server, SERVER, __VA_ARGS__)                     \
	X(server_legacy, SERVER_LEGACY, __VA_ARGS__)       \
	X(welcome, WELCOME, __VA_ARGS__)                   \
	X(failure, FAILURE, __VA_ARGS__)                   \
	X(db, DB, __VA_ARGS__)                             \
	X(stmt, STMT, __VA_ARGS__)                         \
	X(stmt_with_offset, STMT_WITH_OFFSET, __VA_ARGS__) \
	X(result, RESULT, __VA_ARGS__)                     \
	X(rows, ROWS, __VA_ARGS__)                         \
	X(empty, EMPTY, __VA_ARGS__)                       \
	X(files, FILES, __VA_ARGS__)                       \
	X(servers, SERVERS, __VA_ARGS__)                   \
	X(metadata, METADATA, __VA_ARGS__)

RESPONSE__TYPES(RESPONSE__DEFINE);

#endif /* RESPONSE_H_ */
