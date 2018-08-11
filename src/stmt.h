/******************************************************************************
 *
 * APIs to decode parameters and bind them to SQLite statement, and to fetch
 * rows and encode them.
 *
 * The dqlite wire format for a list of parameters to be bound to a statement
 * is divided in header and body. The format of the header is:
 *
 *  8 bits: Number of parameters to bind (min is 1, max is 255).
 *  4 bits: Type code of the 1st parameter to bind.
 *  4 bits: Type code of the 2nd parameter to bind, or 0.
 *  4 bits: Type code of the 3rn parameter to bind, or 0.
 *  ...
 *
 * This repeats until reaching a full 64-bit word. If there are more than 14
 * parameters, the header will grow additional 64-bit words as needed, following
 * the same pattern: a sequence of 4-bit slots with type codes of the parameters
 * to bind, followed by a sequence of zero bits, until word boundary is reached.
 *
 * After the parameters header follows the parameters body, which contain one
 * value for each parameter to bind, following the normal encoding rules.
 *
 * The dqlite wire format for a set of query rows is divided in header and
 * body. The format of the header is:
 *
 *  64 bits: Number of columns in the result set (min is 1).
 *  64 bits: Name of the first column. If the name is longer, additional words
 *           of 64 bits can be used, like for normal string encoding.
 *  ...      If present, name of the 2nd, 3rd, ..., nth column.
 *
 * After the result set header follows the result set body, which is a sequence
 * of zero or more rows. Each row has the following format:
 *
 *  4 bits: Type code of the 1st column of the row.
 *  4 bits: Type code of the 2nd column of row, or 0.
 *  4 bits: Type code of the 2nd column of row, or 0.
 *
 * This repeats until reaching a full 64-bit word. If there are more than 16 row
 * columns, the header will grow additional 64-bit words as needed, following
 * the same pattern. After this row preamble, the values of all columns of the
 * row follow, using the normal dqlite enconding conventions.
 *
 *****************************************************************************/

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

/* No-op hash function (hashing is not supported for dqlite__stmt). This is
 * required by the registry interface. */
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
