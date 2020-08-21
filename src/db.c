#include <string.h>

#include "../include/dqlite.h"

#include "./lib/assert.h"

#include "db.h"

/* Open a SQLite connection and set it to follower mode. */
static int open_follower_conn(const char *filename,
			      const char *vfs,
			      unsigned page_size,
			      sqlite3 **conn);

void db__init(struct db *db, struct config *config, const char *filename)
{
	db->config = config;
	db->filename = sqlite3_malloc((int)(strlen(filename) + 1));
	assert(db->filename != NULL); /* TODO: return an error instead */
	strcpy(db->filename, filename);
	db->opening = false;
	db->follower = NULL;
	db->tx = NULL;
	db->tx_id = 0;
	QUEUE__INIT(&db->leaders);
}

void db__close(struct db *db)
{
	assert(QUEUE__IS_EMPTY(&db->leaders));
	if (db->follower != NULL) {
		int rc;
		rc = sqlite3_close(db->follower);
		assert(rc == SQLITE_OK);
	}
	if (db->tx != NULL) {
		sqlite3_free(db->tx);
	}
	sqlite3_free(db->filename);
}

int db__open_follower(struct db *db)
{
	int rc;
	assert(db->follower == NULL);
	rc = open_follower_conn(db->filename, db->config->name,
				db->config->page_size, &db->follower);
	if (rc != 0) {
		return rc;
	}
	return 0;
}

int db__create_tx(struct db *db, unsigned long long id, sqlite3 *conn)
{
	assert(db->tx == NULL);
	db->tx = sqlite3_malloc(sizeof *db->tx);
	if (db->tx == NULL) {
		return DQLITE_NOMEM;
	}
	tx__init(db->tx, id, conn);
	return 0;
}

void db__delete_tx(struct db *db)
{
	tx__close(db->tx);
	sqlite3_free(db->tx);
	db->tx = NULL;
}

static int open_follower_conn(const char *filename,
			      const char *vfs,
			      unsigned page_size,
			      sqlite3 **conn)
{
	char pragma[255];
	int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	char *msg = NULL;
	int rc;

	rc = sqlite3_open_v2(filename, conn, flags, vfs);
	if (rc != SQLITE_OK) {
		goto err;
	}

	/* Enable extended result codes */
	rc = sqlite3_extended_result_codes(*conn, 1);
	if (rc != SQLITE_OK) {
		goto err_after_open;
	}

	/* Set the page size. */
	sprintf(pragma, "PRAGMA page_size=%d", page_size);
	rc = sqlite3_exec(*conn, pragma, NULL, NULL, &msg);
	if (rc != SQLITE_OK) {
		goto err_after_open;
	}

	/* Disable syncs. */
	rc = sqlite3_exec(*conn, "PRAGMA synchronous=OFF", NULL, NULL, &msg);
	if (rc != SQLITE_OK) {
		goto err_after_open;
	}

	/* Set WAL journaling. */
	rc = sqlite3_exec(*conn, "PRAGMA journal_mode=WAL", NULL, NULL, &msg);
	if (rc != SQLITE_OK) {
		goto err_after_open;
	}

	rc =
	    sqlite3_db_config(*conn, SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, 1, NULL);
	if (rc != SQLITE_OK) {
		goto err_after_open;
	}

	return 0;

err_after_open:
	sqlite3_close(*conn);
err:
	if (msg != NULL) {
		sqlite3_free(msg);
	}
	return rc;
}
