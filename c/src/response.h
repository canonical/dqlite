#ifndef DQLITE_RESPONSE_H
#define DQLITE_RESPONSE_H

#include "schema.h"

/* The size of pre-allocated response buffer. This should generally fit in
 * a single IP packet, given typical MTU sizes */
#define DQLITE__RESPONSE_BUF_SIZE 1024

#define DQLITE__RESPONSE_SCHEMA_WELCOME(X, ...)		\
	X(uint64, heartbeat_timeout, __VA_ARGS__)	\
	X(text,   leader,            __VA_ARGS__)

#define DQLITE__RESPONSE_SCHEMA_SERVERS(X, ...)	\
	X(text_list, addresses, __VA_ARGS__)

#define DQLITE__RESPONSE_SCHEMA_DB_ERROR(X, ...)	\
	X(uint64, code,          __VA_ARGS__)		\
	X(uint64, extended_code, __VA_ARGS__)		\
	X(text,   message,       __VA_ARGS__)

#define DQLITE__RESPONSE_SCHEMA_DB(X, ...)	\
	X(uint64, id, __VA_ARGS__)

#define DQLITE__RESPONSE_SCHEMA_STMT(X, ...)	\
	X(uint64, id, __VA_ARGS__)

#define DQLITE__RESPONSE_SCHEMA_ROWS(X, ...)	\
	X(uint64, id,           __VA_ARGS__)	\
	X(uint64, column_count, __VA_ARGS__)

DQLITE__SCHEMA_DEFINE(dqlite__response_welcome,  DQLITE__RESPONSE_SCHEMA_WELCOME);
DQLITE__SCHEMA_DEFINE(dqlite__response_servers,  DQLITE__RESPONSE_SCHEMA_SERVERS);
DQLITE__SCHEMA_DEFINE(dqlite__response_db_error, DQLITE__RESPONSE_SCHEMA_DB_ERROR);
DQLITE__SCHEMA_DEFINE(dqlite__response_db,       DQLITE__RESPONSE_SCHEMA_DB);
DQLITE__SCHEMA_DEFINE(dqlite__response_stmt,     DQLITE__RESPONSE_SCHEMA_STMT);
DQLITE__SCHEMA_DEFINE(dqlite__response_rows,     DQLITE__RESPONSE_SCHEMA_ROWS);

#define DQLITE__RESPONSE_SCHEMA_TYPES(X, ...)				\
	X(DQLITE_WELCOME,  dqlite__response_welcome,  welcome,  __VA_ARGS__) \
	X(DQLITE_SERVERS,  dqlite__response_servers,  servers,  __VA_ARGS__) \
	X(DQLITE_DB_ERROR, dqlite__response_db_error, db_error, __VA_ARGS__) \
	X(DQLITE_DB,       dqlite__response_db,       db,       __VA_ARGS__) \
	X(DQLITE_STMT,     dqlite__response_stmt,     stmt,     __VA_ARGS__) \
	X(DQLITE_ROWS,     dqlite__response_rows,     rows,     __VA_ARGS__)

DQLITE__SCHEMA_ENCODER_DEFINE(dqlite__response, DQLITE__RESPONSE_SCHEMA_TYPES);

#endif /* DQLITE_RESPONSE_H */
