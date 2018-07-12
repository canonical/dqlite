#ifndef DQLITE_STMT_H
#define DQLITE_STMT_H

#include <sqlite3.h>

#include "error.h"
#include "message.h"
#include "registry.h"

/* Hold state for a single open SQLite database */
struct dqlite__stmt {
	sqlite3       *db;     /* Underlying SQLite database handle */
	sqlite3_stmt  *stmt;   /* Underlying SQLite statement handle */
	const char    *tail;   /* Unparsed SQL portion */
	dqlite__error  error;  /* Last dqlite-specific error */
};

void dqlite__stmt_init(struct dqlite__stmt *s);
void dqlite__stmt_close(struct dqlite__stmt *s);

int dqlite__stmt_bind(
	struct dqlite__stmt *s,
	struct dqlite__message *message,
	int *rc);

int dqlite__stmt_exec(
	struct dqlite__stmt *s,
	uint64_t *last_insert_id,
	uint64_t *rows_affected);

int dqlite__stmt_query(
	struct dqlite__stmt *s,
	struct dqlite__message *message,
	int *rc);

DQLITE__REGISTRY(dqlite__stmt_registry, dqlite__stmt);

#endif /* DQLITE_STMT_H */
