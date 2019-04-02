#ifndef DQLITE_REQUEST_H_
#define DQLITE_REQUEST_H_

#include "lib/serialize.h"

#include "schema.h"

/**
 * Request types.
 */

#define REQUEST_LEADER(X, ...) X(uint64, __unused__, ##__VA_ARGS__)
#define REQUEST_CLIENT(X, ...) X(uint64, id, ##__VA_ARGS__)
#define REQUEST_OPEN(X, ...)             \
	X(text, filename, ##__VA_ARGS__) \
	X(uint64, flags, ##__VA_ARGS__)  \
	X(text, vfs, ##__VA_ARGS__)
#define REQUEST_PREPARE(X, ...)         \
	X(uint64, db_id, ##__VA_ARGS__) \
	X(text, sql, ##__VA_ARGS__)
#define REQUEST_EXEC(X, ...)            \
	X(uint32, db_id, ##__VA_ARGS__) \
	X(uint32, stmt_id, ##__VA_ARGS__)
#define REQUEST_QUERY(X, ...)           \
	X(uint32, db_id, ##__VA_ARGS__) \
	X(uint32, stmt_id, ##__VA_ARGS__)
#define REQUEST_FINALIZE(X, ...)        \
	X(uint32, db_id, ##__VA_ARGS__) \
	X(uint32, stmt_id, ##__VA_ARGS__)
#define REQUEST_EXEC_SQL(X, ...) \
	X(uint64, db_id, ##__VA_ARGS__)    \
	X(text, sql, ##__VA_ARGS__)


#define REQUEST__DEFINE(LOWER, UPPER, _) \
	SERIALIZE__DEFINE(request_##LOWER, REQUEST_##UPPER);

#define REQUEST__TYPES(X, ...)             \
	X(leader, LEADER, __VA_ARGS__)     \
	X(client, CLIENT, __VA_ARGS__)     \
	X(open, OPEN, __VA_ARGS__)         \
	X(prepare, PREPARE, __VA_ARGS__)   \
	X(exec, EXEC, __VA_ARGS__)         \
	X(query, QUERY, __VA_ARGS__)       \
	X(finalize, FINALIZE, __VA_ARGS__) \
	X(exec_sql, EXEC_SQL, __VA_ARGS__)

REQUEST__TYPES(REQUEST__DEFINE);

#define REQUEST__SCHEMA_LEADER(X, ...) X(uint64, __unused__, __VA_ARGS__)

#define REQUEST__SCHEMA_CLIENT(X, ...) X(uint64, id, __VA_ARGS__)

#define REQUEST__SCHEMA_HEARTBEAT(X, ...) X(uint64, timestamp, __VA_ARGS__)

#define REQUEST__SCHEMA_OPEN(X, ...)  \
	X(text, name, __VA_ARGS__)    \
	X(uint64, flags, __VA_ARGS__) \
	X(text, vfs, __VA_ARGS__)

#define REQUEST__SCHEMA_PREPARE(X, ...) \
	X(uint64, db_id, __VA_ARGS__)   \
	X(text, sql, __VA_ARGS__)

#define REQUEST__SCHEMA_EXEC(X, ...)  \
	X(uint32, db_id, __VA_ARGS__) \
	X(uint32, stmt_id, __VA_ARGS__)

#define REQUEST__SCHEMA_QUERY(X, ...) \
	X(uint32, db_id, __VA_ARGS__) \
	X(uint32, stmt_id, __VA_ARGS__)

#define REQUEST__SCHEMA_FINALIZE(X, ...) \
	X(uint32, db_id, __VA_ARGS__)    \
	X(uint32, stmt_id, __VA_ARGS__)

#define REQUEST__SCHEMA_EXEC_SQL(X, ...) \
	X(uint64, db_id, __VA_ARGS__)    \
	X(text, sql, __VA_ARGS__)

#define REQUEST__SCHEMA_QUERY_SQL(X, ...) \
	X(uint64, db_id, __VA_ARGS__)     \
	X(text, sql, __VA_ARGS__)

#define REQUEST__SCHEMA_INTERRUPT(X, ...) X(uint64, db_id, __VA_ARGS__)

SCHEMA__DEFINE(request_leader_, REQUEST__SCHEMA_LEADER);
SCHEMA__DEFINE(request_client_, REQUEST__SCHEMA_CLIENT);
SCHEMA__DEFINE(request_heartbeat, REQUEST__SCHEMA_HEARTBEAT);
SCHEMA__DEFINE(request_open_, REQUEST__SCHEMA_OPEN);
SCHEMA__DEFINE(request_prepare_, REQUEST__SCHEMA_PREPARE);
SCHEMA__DEFINE(request_query_, REQUEST__SCHEMA_QUERY);
SCHEMA__DEFINE(request_exec_, REQUEST__SCHEMA_EXEC);
SCHEMA__DEFINE(request_finalize_, REQUEST__SCHEMA_FINALIZE);
SCHEMA__DEFINE(request_exec_sql_, REQUEST__SCHEMA_EXEC_SQL);
SCHEMA__DEFINE(request_query_sql, REQUEST__SCHEMA_QUERY_SQL);
SCHEMA__DEFINE(request_interrupt, REQUEST__SCHEMA_INTERRUPT);

#define REQUEST__SCHEMA_TYPES(X, ...)                                          \
	X(DQLITE_REQUEST_LEADER, request_leader_, leader, __VA_ARGS__)         \
	X(DQLITE_REQUEST_CLIENT, request_client_, client, __VA_ARGS__)         \
	X(DQLITE_REQUEST_HEARTBEAT, request_heartbeat, heartbeat, __VA_ARGS__) \
	X(DQLITE_REQUEST_OPEN, request_open_, open, __VA_ARGS__)               \
	X(DQLITE_REQUEST_PREPARE, request_prepare_, prepare, __VA_ARGS__)      \
	X(DQLITE_REQUEST_EXEC, request_exec_, exec, __VA_ARGS__)               \
	X(DQLITE_REQUEST_QUERY, request_query_, query, __VA_ARGS__)            \
	X(DQLITE_REQUEST_FINALIZE, request_finalize_, finalize, __VA_ARGS__)    \
	X(DQLITE_REQUEST_EXEC_SQL, request_exec_sql_, exec_sql, __VA_ARGS__)    \
	X(DQLITE_REQUEST_QUERY_SQL, request_query_sql, query_sql, __VA_ARGS__) \
	X(DQLITE_REQUEST_INTERRUPT, request_interrupt, interrupt, __VA_ARGS__)

SCHEMA__HANDLER_DEFINE(request, REQUEST__SCHEMA_TYPES);

#endif /* DQLITE_REQUEST_H_ */
