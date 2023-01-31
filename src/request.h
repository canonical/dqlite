#ifndef REQUEST_H_
#define REQUEST_H_

#include "lib/serialize.h"

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
#define REQUEST_EXEC_SQL(X, ...)        \
	X(uint64, db_id, ##__VA_ARGS__) \
	X(text, sql, ##__VA_ARGS__)
#define REQUEST_QUERY_SQL(X, ...)       \
	X(uint64, db_id, ##__VA_ARGS__) \
	X(text, sql, ##__VA_ARGS__)
#define REQUEST_INTERRUPT(X, ...) X(uint64, db_id, ##__VA_ARGS__)
#define REQUEST_ADD(X, ...)          \
	X(uint64, id, ##__VA_ARGS__) \
	X(text, address, ##__VA_ARGS__)
#define REQUEST_PROMOTE_OR_ASSIGN(X, ...) X(uint64, id, ##__VA_ARGS__)
#define REQUEST_REMOVE(X, ...) X(uint64, id, ##__VA_ARGS__)
#define REQUEST_DUMP(X, ...) X(text, filename, ##__VA_ARGS__)
#define REQUEST_CLUSTER(X, ...) X(uint64, format, ##__VA_ARGS__)
#define REQUEST_TRANSFER(X, ...) X(uint64, id, ##__VA_ARGS__)
#define REQUEST_DESCRIBE(X, ...) X(uint64, format, ##__VA_ARGS__)
#define REQUEST_WEIGHT(X, ...) X(uint64, weight, ##__VA_ARGS__)

#define REQUEST__DEFINE(LOWER, UPPER, _) \
	SERIALIZE__DEFINE(request_##LOWER, REQUEST_##UPPER);

#define REQUEST__TYPES(X, ...)                               \
	X(leader, LEADER, __VA_ARGS__)                       \
	X(client, CLIENT, __VA_ARGS__)                       \
	X(open, OPEN, __VA_ARGS__)                           \
	X(prepare, PREPARE, __VA_ARGS__)                     \
	X(exec, EXEC, __VA_ARGS__)                           \
	X(query, QUERY, __VA_ARGS__)                         \
	X(finalize, FINALIZE, __VA_ARGS__)                   \
	X(exec_sql, EXEC_SQL, __VA_ARGS__)                   \
	X(query_sql, QUERY_SQL, __VA_ARGS__)                 \
	X(interrupt, INTERRUPT, __VA_ARGS__)                 \
	X(add, ADD, __VA_ARGS__)                             \
	X(promote_or_assign, PROMOTE_OR_ASSIGN, __VA_ARGS__) \
	X(remove, REMOVE, __VA_ARGS__)                       \
	X(dump, DUMP, __VA_ARGS__)                           \
	X(cluster, CLUSTER, __VA_ARGS__)                     \
	X(transfer, TRANSFER, __VA_ARGS__)                   \
	X(describe, DESCRIBE, __VA_ARGS__)                   \
	X(weight, WEIGHT, __VA_ARGS__)

REQUEST__TYPES(REQUEST__DEFINE);

#define REQUEST_CONNECT(X, ...)      \
	X(uint64, id, ##__VA_ARGS__) \
	X(text, address, ##__VA_ARGS__)

SERIALIZE__DEFINE(request_connect, REQUEST_CONNECT);

/* Definition of the ASSIGN request that's used only for serialization.
 *
 * The one-field PROMOTE request and the two-field ASSIGN request have the
 * same type tag, so we can't dispatch deserialization based on that field.
 * Instead, we deserialize as the least-common-denominator PROMOTE_OR_ASSIGN
 * and then manually read the second field if appropriate.
 *
 * But when serializing, we can just decide to send an ASSIGN request, so
 * we provide the message definition here for that purpose. */
#define REQUEST_ASSIGN(X, ...)       \
	X(uint64, id, ##__VA_ARGS__) \
	X(uint64, role, ##__VA_ARGS__)

SERIALIZE__DEFINE(request_assign, REQUEST_ASSIGN);

#endif /* REQUEST_H_ */
