#include "./lib/assert.h"

#include "follower.h"

/* Open a SQLite connection and set it to follower mode. */
static int open_conn(const char *filename, const char *vfs, sqlite3 **conn);

int follower__init(struct follower *f, const char *vfs, const char *filename)
{
	int rc;
	rc = open_conn(filename, vfs, &f->conn);
	if (rc != 0) {
		return rc;
	}
	return 0;
}

void follower__close(struct follower *f)
{
	int rc;
	rc = sqlite3_close(f->conn);
	assert(rc == 0);
}

static int open_conn(const char *filename, const char *vfs, sqlite3 **conn)
{
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

	/* Switch off automatic WAL checkpoint when a connection is closed. */
	rc =
	    sqlite3_db_config(*conn, SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, 0, NULL);
	if (rc != SQLITE_OK) {
		goto err_after_open;
	}

	/* Set WAL replication. */
	rc = sqlite3_wal_replication_follower(*conn, "main");
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
