#ifndef DQLITE_STMT_H
#define DQLITE_STMT_H

#include <sqlite3.h>

#include "registry.h"

/* Hold state for a single open SQLite database */
struct dqlite__stmt {
	sqlite3_stmt      *stmt; /* Underlying SQLite database */
};

void dqlite__stmt_init(struct dqlite__stmt *stmt);
void dqlite__stmt_close(struct dqlite__stmt *stmt);

DQLITE__REGISTRY(dqlite__stmt_registry, dqlite__stmt);

#endif /* DQLITE_STMT_H */
