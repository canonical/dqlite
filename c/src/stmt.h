#ifndef DQLITE_STMT_H
#define DQLITE_STMT_H

#include <sqlite3.h>

#include "error.h"
#include "message.h"
#include "registry.h"

/* Hold state for a single open SQLite database */
struct dqlite__stmt {
	size_t        id;    /* Statement ID */
	sqlite3 *     db;    /* Underlying database info */
	sqlite3_stmt *stmt;  /* Underlying SQLite statement handle */
	const char *  tail;  /* Unparsed SQL portion */
	dqlite__error error; /* Last dqlite-specific error */
};

/* Initialize a statement state object */
void dqlite__stmt_init(struct dqlite__stmt *s);

/* Close a statement state object, releasing all associated resources. */
void dqlite__stmt_close(struct dqlite__stmt *s);

/* No-op hash function (hashing is not supported for dqlite__stmt). */
const char *dqlite__stmt_hash(struct dqlite__stmt *stmt);

/* Bind the parameters of the underlying statement by decoding the given
 * message. */
int dqlite__stmt_bind(struct dqlite__stmt *s, struct dqlite__message *message);

int dqlite__stmt_exec(struct dqlite__stmt *s,
                      uint64_t *           last_insert_id,
                      uint64_t *           rows_affected);

/* Step through a query statement and fill the given message with the rows it
 * yields. */
int dqlite__stmt_query(struct dqlite__stmt *s, struct dqlite__message *message);

DQLITE__REGISTRY(dqlite__stmt_registry, dqlite__stmt);

#endif /* DQLITE_STMT_H */
