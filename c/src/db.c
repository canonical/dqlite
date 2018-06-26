#include <assert.h>
#include <stddef.h>

#include <sqlite3.h>

#include "db.h"
#include "dqlite.h"
#include "lifecycle.h"
#include "registry.h"
#include "stmt.h"

void dqlite__db_init(struct dqlite__db *db) {
	assert(db != NULL);

	dqlite__lifecycle_init(DQLITE__LIFECYCLE_DB);

	dqlite__stmt_registry_init(&db->stmts);
}

void dqlite__db_close(struct dqlite__db *db) {
	int rc;

	assert(db != NULL);

	dqlite__stmt_registry_close(&db->stmts);

	if (db->db != NULL) {
		rc = sqlite3_close(db->db);

		/* Since we cleanup all existing db resources, SQLite should
		 * never fail, according to the docs. */
		assert(rc == SQLITE_OK);
	}

	dqlite__lifecycle_close(DQLITE__LIFECYCLE_DB);
}

/* TODO: do some validation of the name (e.g. can't begin with a slash) */
int dqlite__db_open(
	struct dqlite__db *db,
	const char *name,
	int flags,
	const char *replication)
{
	int rc;
	sqlite3_stmt *stmt;
	const char *tail;
	const char *vfs;

	assert(db != NULL);
	assert(name != NULL);
	assert(replication != NULL);

	/* The VFS registration name must match the one of the replication
	 * implementation. */
	vfs = replication;

	rc = sqlite3_open_v2(name, &db->db, flags, vfs);
	if (rc != SQLITE_OK)
		return rc;

	/* Set the page size. TODO: make page size configurable? */
	rc = sqlite3_prepare(db->db, "PRAGMA page_size=4096", -1, &stmt, &tail);
	if (rc != SQLITE_OK)
		return rc;

	rc = sqlite3_step(stmt);
	if (rc != SQLITE_DONE)
		return rc;

	rc = sqlite3_finalize(stmt);
	if (rc != SQLITE_OK)
		return rc;

	/* Disable syncs. */
	rc = sqlite3_prepare(db->db, "PRAGMA synchronous=OFF", -1, &stmt, &tail);
	if (rc != SQLITE_OK)
		return rc;

	rc = sqlite3_step(stmt);
	if (rc != SQLITE_DONE)
		return rc;

	rc = sqlite3_finalize(stmt);
	if (rc != SQLITE_OK)
		return rc;

	/* Set WAL journaling. */
	rc = sqlite3_prepare(db->db, "PRAGMA journal_mode=WAL", -1, &stmt, &tail);
	if (rc != SQLITE_OK)
		return rc;

	rc = sqlite3_step(stmt);
	if (rc != SQLITE_ROW)
		return rc;

	rc = sqlite3_finalize(stmt);
	if (rc != SQLITE_OK)
		return rc;

	/* Set WAL replication. */
	rc = sqlite3_wal_replication_leader(db->db, "main", replication, (void*)db->db);
	if (rc != SQLITE_OK)
		return rc;

	return rc;
}

int dqlite__db_prepare(struct dqlite__db *db, const char *sql, uint32_t *stmt_id)
{
	int rc;
	struct dqlite__stmt *stmt;
	size_t i;
	const char *tail;

	assert(db != NULL);
	assert(db->db != NULL);

	assert(sql != NULL);

	rc = dqlite__stmt_registry_add(&db->stmts, &stmt, &i);
	if (rc != 0) {
		assert(rc == DQLITE_NOMEM);
		rc = SQLITE_NOMEM;
		goto err_stmt_add;
	}

	assert(stmt != NULL);

	stmt->db = db->db;

	rc = sqlite3_prepare_v2(db->db, sql, -1, &stmt->stmt, &tail);
	if (rc != SQLITE_OK) {
		goto err_stmt_prepare;
	}

	*stmt_id = (uint32_t)i;

	return SQLITE_OK;

 err_stmt_prepare:
	dqlite__stmt_registry_del(&db->stmts, i);

 err_stmt_add:
	assert(rc != SQLITE_OK);

	return rc;
}

/* Lookup a stmt object by ID */
struct dqlite__stmt *dqlite__db_stmt(struct dqlite__db *db, uint32_t stmt_id)
{
	return dqlite__stmt_registry_get(&db->stmts, stmt_id);
}

int dqlite__db_finalize(struct dqlite__db *db, struct dqlite__stmt *stmt, uint32_t stmt_id)
{
	int err;
	int rc;

	assert(db != NULL);
	assert(stmt != NULL);
	assert(stmt->stmt != NULL);

	rc = sqlite3_finalize(stmt->stmt);

	/* Unset the stmt member, to prevent dqlite__stmt_registry_del from
	 * trying to finalize the statement too */
	stmt->stmt = NULL;

	err = dqlite__stmt_registry_del(&db->stmts, stmt_id);

	/* Deleting the statement from the registry can't fail, because the
	 * given statement ID was obtained with dqlite__db_stmt(). */
	assert(err == 0);

	return rc;
}

DQLITE__REGISTRY_METHODS(dqlite__db_registry, dqlite__db);
