#ifndef DQLITE_DB_H
#define DQLITE_DB_H

#include <sqlite3.h>

#include "error.h"
#include "registry.h"
#include "stmt.h"

/* Hold state for a single open SQLite database */
struct dqlite__db {
	/* read-only */
	size_t        id;    /* Database ID */
	dqlite__error error; /* Last error occurred */

	/* private */
	sqlite3 *                    db;      /* Underlying SQLite database */
	struct dqlite__stmt_registry stmts;   /* Registry of prepared statements */
	int                          in_a_tx; /* True if a tx is in progress */
};

/* Initialize a database state object */
void dqlite__db_init(struct dqlite__db *db);

/* Close a database state object, releasing all associated resources. */
void dqlite__db_close(struct dqlite__db *db);

/* Open the underlying db. */
int dqlite__db_open(struct dqlite__db *db,
                    const char *       name,
                    int                flags,
                    const char *       replication);

/* Prepare a statement using the underlying db. */
int dqlite__db_prepare(struct dqlite__db *   db,
                       const char *          sql,
                       struct dqlite__stmt **stmt);

/* Lookup the statement with the given ID. */
struct dqlite__stmt *dqlite__db_stmt(struct dqlite__db *db, uint32_t stmt_id);

/* Finalize a statement. */
int dqlite__db_finalize(struct dqlite__db *db, struct dqlite__stmt *stmt);

/* Begin a transaction. */
int dqlite__db_begin(struct dqlite__db *db);

/* Commit a transaction. */
int dqlite__db_commit(struct dqlite__db *db);

/* Rollback a transaction. */
int dqlite__db_rollback(struct dqlite__db *db);

/* Define the database registry */
DQLITE__REGISTRY(dqlite__db_registry, dqlite__db);

#endif /* DQLITE_DB_H */
