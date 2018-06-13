#ifndef DQLITE_DB_H
#define DQLITE_DB_H

#include <sqlite3.h>

#include "error.h"
#include "registry.h"

/* Hold state for a single open SQLite database */
struct dqlite__db {
	dqlite__error error;  /* Last error occurred */
	sqlite3* db;          /* Underlying SQLite database */
	int rc;               /* Code of the last SQLite error occurred */
	const char *errmsg;   /* Message of thet last SQLite error occurred */
};

void dqlite__db_init(struct dqlite__db *db);
void dqlite__db_close(struct dqlite__db *db);

int dqlite__db_open(
	struct dqlite__db *db,
	const char *name,
	int flags,
	const char *vfs);

DQLITE__REGISTRY(dqlite__db_registry, dqlite__db);

#endif /* DQLITE_DB_H */
