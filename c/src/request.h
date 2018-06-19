#ifndef DQLITE_REQUEST_H
#define DQLITE_REQUEST_H

#include <stdint.h>

#include "dqlite.h"
#include "schema.h"

/*
 * Request types.
 */

#define DQLITE__REQUEST_SCHEMA_HELO(X, ...)	\
	X(uint64, client_id, __VA_ARGS__)

#define DQLITE__REQUEST_SCHEMA_HEARTBEAT(X, ...)	\
	X(uint64, timestamp, __VA_ARGS__)

#define DQLITE__REQUEST_SCHEMA_OPEN(X, ...)	\
	X(text,   name,  __VA_ARGS__)		\
	X(uint64, flags, __VA_ARGS__)		\
	X(text,   vfs,   __VA_ARGS__)

#define DQLITE__REQUEST_SCHEMA_PREPARE(X, ...)	\
	X(uint64, db_id, __VA_ARGS__)		\
	X(text,   sql,  __VA_ARGS__)

DQLITE__SCHEMA_DEFINE(dqlite__request_helo, DQLITE__REQUEST_SCHEMA_HELO);
DQLITE__SCHEMA_DEFINE(dqlite__request_heartbeat, DQLITE__REQUEST_SCHEMA_HEARTBEAT);
DQLITE__SCHEMA_DEFINE(dqlite__request_open, DQLITE__REQUEST_SCHEMA_OPEN);
DQLITE__SCHEMA_DEFINE(dqlite__request_prepare, DQLITE__REQUEST_SCHEMA_PREPARE);

#define DQLITE__REQUEST_SCHEMA_TYPES(X, ...)				\
	X(DQLITE_HELO,      dqlite__request_helo,      helo,      __VA_ARGS__) \
	X(DQLITE_HEARTBEAT, dqlite__request_heartbeat, heartbeat, __VA_ARGS__) \
	X(DQLITE_OPEN,      dqlite__request_open,      open,      __VA_ARGS__) \
	X(DQLITE_PREPARE,   dqlite__request_prepare,   prepare,   __VA_ARGS__)

DQLITE__SCHEMA_DECODER_DEFINE(dqlite__request, DQLITE__REQUEST_SCHEMA_TYPES);

#endif /* DQLITE_REQUEST_H */

