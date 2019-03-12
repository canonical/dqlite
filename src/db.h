#ifndef DQLITE_DB_H
#define DQLITE_DB_H

#include <sqlite3.h>

#include "error.h"
#include "stmt.h"

/* Hold state for a single open SQLite database */
struct db
{
	/* public */
	dqlite_cluster *cluster; /* Cluster API implementation  */

	/* read-only */
	size_t id;	   /* Database ID */
	dqlite__error error; /* Last error occurred */

	/* private */
	sqlite3 *db;		     /* Underlying SQLite database */
	struct stmt__registry stmts; /* Registry of prepared statements */
};

/* Initialize a database state object */
void db__init(struct db *db);

/* Close a database state object, releasing all associated resources. */
void db__close(struct db *db);

/* Open the underlying db. */
int db__open(struct db *db,
	     const char *name,
	     int flags,
	     const char *vfs,
	     uint16_t page_size,
	     const char *wal_replication);

/* Prepare a statement using the underlying db. */
int db__prepare(struct db *db, const char *sql, struct stmt **stmt);

/* Lookup the statement with the given ID. */
struct stmt *db__stmt(struct db *db, uint32_t stmt_id);

/* Finalize a statement. */
int db__finalize(struct db *db, struct stmt *stmt);

#endif /* DQLITE_DB_H */
