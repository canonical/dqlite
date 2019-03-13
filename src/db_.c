#include <sqlite3.h>

#include "./lib/assert.h"

#include "db_.h"
#include "lifecycle.h"
#include "stmt.h"

/* Default name of the registered sqlite3_vfs implementation to use when opening
 * new connections. */
#define DB__DEFAULT_VFS "volatile"

/* Default name of the registered sqlite3_wal_replication implementation to use
 * to switch new connections to leader replication mode. */
#define DB__DEFAULT_WAL_REPLICATION "dqlite"

/* Wrapper around sqlite3_exec that frees the memory allocated for the error
 * message in case of failure and sets the db's error field
 * appropriately */
static int db__exec(struct db_ *db, const char *sql)
{
	char *msg;
	int rc;

	assert(db != NULL);

	rc = sqlite3_exec(db->db, sql, NULL, NULL, &msg);
	if (rc != SQLITE_OK) {
		assert(msg != NULL);
		sqlite3_free(msg);
		dqlite__error_printf(&db->error, sqlite3_errmsg(db->db));

		return rc;
	}

	return SQLITE_OK;
}

void db__init_(struct db_ *db)
{
	assert(db != NULL);

	db->cluster = NULL;

	dqlite__lifecycle_init(DQLITE__LIFECYCLE_DB);
	dqlite__error_init(&db->error);
	stmt__registry_init(&db->stmts);
}

void db__close_(struct db_ *db)
{
	int rc;

	assert(db != NULL);

	stmt__registry_close(&db->stmts);
	dqlite__error_close(&db->error);

	if (db->db != NULL) {
		rc = sqlite3_close(db->db);

		/* Since we cleanup all existing db resources, SQLite should
		 * never fail, according to the docs. */
		assert(rc == SQLITE_OK);

		if (db->cluster != NULL) {
			/* Notify the cluster implementation about the database
			 * being closed. */
			db->cluster->xUnregister(db->cluster->ctx, db->db);
		}

		db->db = NULL;
	}

	dqlite__lifecycle_close(DQLITE__LIFECYCLE_DB);
}

int db__open(struct db_ *db,
	     const char *name,
	     int flags,
	     const char *vfs,
	     uint16_t page_size,
	     const char *wal_replication)
{
	char pragma[255];
	int rc;

	assert(db != NULL);
	assert(name != NULL);
	assert(page_size > 0);

	if (vfs == NULL) {
		vfs = DB__DEFAULT_VFS;
	}

	if (wal_replication == NULL) {
		wal_replication = DB__DEFAULT_WAL_REPLICATION;
	}

	/* TODO: do some validation of the name (e.g. can't begin with a slash)
	 */
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

	/* Set the page size. */
	sprintf(pragma, "PRAGMA page_size=%d", page_size);
	rc = db__exec(db, pragma);
	if (rc != SQLITE_OK) {
		dqlite__error_wrapf(&db->error, &db->error,
				    "unable to set page size");
		return rc;
	}

	/* Disable syncs. */
	rc = db__exec(db, "PRAGMA synchronous=OFF");
	if (rc != SQLITE_OK) {
		dqlite__error_wrapf(&db->error, &db->error,
				    "unable to switch off syncs");
		return rc;
	}

	/* Set WAL journaling. */
	rc = db__exec(db, "PRAGMA journal_mode=WAL");
	if (rc != SQLITE_OK) {
		dqlite__error_wrapf(&db->error, &db->error,
				    "unable to set WAL mode: %s");
		return rc;
	}

	/* Set WAL replication. */
	rc = sqlite3_wal_replication_leader(db->db, "main", wal_replication,
					    (void *)db->db);

	if (rc != SQLITE_OK) {
		dqlite__error_printf(&db->error,
				     "unable to set WAL replication");
		return rc;
	}

	/* TODO: make setting foreign keys optional. */
	rc = db__exec(db, "PRAGMA foreign_keys=1");
	if (rc != SQLITE_OK) {
		dqlite__error_wrapf(&db->error, &db->error,
				    "unable to set foreign keys checks: %s");
		return rc;
	}

	return SQLITE_OK;
}

int db__prepare(struct db_ *db, const char *sql, struct stmt **stmt)
{
	int err;
	int rc;

	assert(db != NULL);
	assert(db->db != NULL);

	assert(sql != NULL);

	err = stmt__registry_add(&db->stmts, stmt);
	if (err != 0) {
		assert(err == DQLITE_NOMEM);
		dqlite__error_oom(&db->error, "unable to register statement");
		return SQLITE_NOMEM;
	}

	assert(stmt != NULL);

	(*stmt)->db = db->db;

	rc =
	    sqlite3_prepare_v2(db->db, sql, -1, &(*stmt)->stmt, &(*stmt)->tail);
	if (rc != SQLITE_OK) {
		dqlite__error_printf(&db->error, sqlite3_errmsg(db->db));
		stmt__registry_del(&db->stmts, *stmt);
		return rc;
	}

	return SQLITE_OK;
}

/* Lookup a stmt object by ID */
struct stmt *db__stmt(struct db_ *db, uint32_t stmt_id)
{
	return stmt__registry_get(&db->stmts, stmt_id);
}

int db__finalize(struct db_ *db, struct stmt *stmt)
{
	int rc;
	int err;

	assert(db != NULL);
	assert(stmt != NULL);

	if (stmt->stmt != NULL) {
		rc = sqlite3_finalize(stmt->stmt);
		if (rc != SQLITE_OK) {
			dqlite__error_printf(&db->error,
					     sqlite3_errmsg(db->db));
		}

		/* Unset the stmt member, to prevent stmt__registry_del
		 * from trying to finalize the statement too */
		stmt->stmt = NULL;
	} else {
		rc = SQLITE_OK;
	}

	err = stmt__registry_del(&db->stmts, stmt);

	/* Deleting the statement from the registry can't fail, because the
	 * given statement was obtained with db__stmt(). */
	assert(err == 0);

	return rc;
}
