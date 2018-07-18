#include <assert.h>
#include <stddef.h>

#include <sqlite3.h>

#include "../include/dqlite.h"

#include "db.h"
#include "lifecycle.h"
#include "registry.h"
#include "stmt.h"

void dqlite__db_init(struct dqlite__db *db) {
	assert(db != NULL);

	dqlite__lifecycle_init(DQLITE__LIFECYCLE_DB);
	dqlite__error_init(&db->error);
	dqlite__stmt_registry_init(&db->stmts);
}

void dqlite__db_close(struct dqlite__db *db) {
	int rc;

	assert(db != NULL);

	dqlite__stmt_registry_close(&db->stmts);
	dqlite__error_close(&db->error);

	if (db->db != NULL) {
		rc = sqlite3_close(db->db);

		/* Since we cleanup all existing db resources, SQLite should
		 * never fail, according to the docs. */
		assert(rc == SQLITE_OK);
	}

	dqlite__lifecycle_close(DQLITE__LIFECYCLE_DB);
}

int dqlite__db_open(struct dqlite__db *db,
                    const char *       name,
                    int                flags,
                    const char *       replication) {
	const char *vfs;
	int         rc;
	char *      msg;

	assert(db != NULL);
	assert(name != NULL);
	assert(replication != NULL);

	/* The VFS registration name must match the one of the replication
	 * implementation. */
	vfs = replication;

	/* TODO: do some validation of the name (e.g. can't begin with a slash) */
	rc = sqlite3_open_v2(name, &db->db, flags, vfs);
	if (rc != SQLITE_OK) {
		dqlite__error_printf(&db->error, sqlite3_errmsg(db->db));
		return rc;
	}

	/* Enable extended result codes */
	rc = sqlite3_extended_result_codes(db->db, 1);
	if (rc != SQLITE_OK) {
		dqlite__error_printf(&db->error, sqlite3_errmsg(db->db));
		return rc;
	}

	/* Set the page size. TODO: make page size configurable? */
	rc = sqlite3_exec(db->db, "PRAGMA page_size=4096", NULL, NULL, &msg);
	if (rc != SQLITE_OK) {
		dqlite__error_printf(&db->error, "unable to set page size: %s", msg);
		return rc;
	}

	/* Disable syncs. */
	rc = sqlite3_exec(db->db, "PRAGMA synchronous=OFF", NULL, NULL, &msg);
	if (rc != SQLITE_OK) {
		dqlite__error_printf(
		    &db->error, "unable to switch off syncs: %s", msg);
		return rc;
	}

	/* Set WAL journaling. */
	rc = sqlite3_exec(db->db, "PRAGMA journal_mode=WAL", NULL, NULL, &msg);
	if (rc != SQLITE_OK) {
		dqlite__error_printf(&db->error, "unable to set WAL mode: %s", msg);
		return rc;
	}

	/* Set WAL replication. */
	rc = sqlite3_wal_replication_leader(
	    db->db, "main", replication, (void *)db->db);

	if (rc != SQLITE_OK) {
		dqlite__error_printf(&db->error, "unable to set WAL replication");
		return rc;
	}

	return SQLITE_OK;
}

int dqlite__db_prepare(struct dqlite__db *   db,
                       const char *          sql,
                       struct dqlite__stmt **stmt) {
	int err;
	int rc;

	assert(db != NULL);
	assert(db->db != NULL);

	assert(sql != NULL);

	err = dqlite__stmt_registry_add(&db->stmts, stmt);
	if (err != 0) {
		assert(err == DQLITE_NOMEM);
		dqlite__error_oom(&db->error, "unable to register statement");
		return SQLITE_NOMEM;
	}

	assert(stmt != NULL);

	(*stmt)->db = db->db;

	rc = sqlite3_prepare_v2(db->db, sql, -1, &(*stmt)->stmt, &(*stmt)->tail);
	if (rc != SQLITE_OK) {
		dqlite__error_printf(&db->error, sqlite3_errmsg(db->db));
		dqlite__stmt_registry_del(&db->stmts, *stmt);
		return rc;
	}

	return SQLITE_OK;
}

/* Lookup a stmt object by ID */
struct dqlite__stmt *dqlite__db_stmt(struct dqlite__db *db, uint32_t stmt_id) {
	return dqlite__stmt_registry_get(&db->stmts, stmt_id);
}

int dqlite__db_finalize(struct dqlite__db *db, struct dqlite__stmt *stmt) {
	int rc;
	int err;

	assert(db != NULL);
	assert(stmt != NULL);

	if (stmt->stmt != NULL) {
		rc = sqlite3_finalize(stmt->stmt);
		if (rc != SQLITE_OK) {
			dqlite__error_printf(&db->error, sqlite3_errmsg(db->db));
		}

		/* Unset the stmt member, to prevent dqlite__stmt_registry_del from
		 * trying to finalize the statement too */
		stmt->stmt = NULL;
	} else {
		rc = SQLITE_OK;
	}

	err = dqlite__stmt_registry_del(&db->stmts, stmt);

	/* Deleting the statement from the registry can't fail, because the
	 * given statement was obtained with dqlite__db_stmt(). */
	assert(err == 0);

	return rc;
}

DQLITE__REGISTRY_METHODS(dqlite__db_registry, dqlite__db);
