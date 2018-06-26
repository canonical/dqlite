#ifndef DQLITE_DB_H
#define DQLITE_DB_H

#include <sqlite3.h>

#include "error.h"
#include "registry.h"
#include "stmt.h"

/* Hold state for a single open SQLite database */
struct dqlite__db {
	sqlite3      *db;                   /* Underlying SQLite database */
	struct dqlite__stmt_registry stmts; /* Registry of prepared statements */
};

void dqlite__db_init(struct dqlite__db *db);
void dqlite__db_close(struct dqlite__db *db);

int dqlite__db_open(
	struct dqlite__db *db,
	const char *name,
	int flags,
	const char *replication);

int dqlite__db_prepare(struct dqlite__db *db, const char *sql, uint32_t *stmt_id);
struct dqlite__stmt *dqlite__db_stmt(struct dqlite__db *db, uint32_t stmt_id);

int dqlite__db_finalize(struct dqlite__db *db, struct dqlite__stmt *stmt, uint32_t stmt_id);

DQLITE__REGISTRY(dqlite__db_registry, dqlite__db);

#endif /* DQLITE_DB_H */
