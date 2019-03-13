#include <stdio.h>

#include "leader.h"

/* Open a SQLite connection and set it to leader replication mode. */
static int open_conn(const char *filename,
		     const char *vfs,
		     const char *replication,
		     void *replication_arg,
		     unsigned page_size,
		     sqlite3 **conn);

int leader__init(struct leader *l, struct db *db)
{
	int rc;

	l->db = db;
	l->main = co_active();

	rc = open_conn(db->filename, db->options->vfs, db->options->replication,
		       l, db->options->page_size, &l->conn);
	if (rc != 0) {
		return rc;
	}

	QUEUE__PUSH(&db->leaders, &l->queue);

	return 0;
}

void leader__close(struct leader *l)
{
	QUEUE__REMOVE(&l->queue);
	sqlite3_close(l->conn);
}

static int open_conn(const char *filename,
		     const char *vfs,
		     const char *replication,
		     void *replication_arg,
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

	/* Set WAL replication. */
	rc = sqlite3_wal_replication_leader(*conn, "main", replication,
					    replication_arg);

	if (rc != SQLITE_OK) {
		goto err_after_open;
	}

	/* TODO: make setting foreign keys optional. */
	rc = sqlite3_exec(*conn, "PRAGMA foreign_keys=1", NULL, NULL, &msg);
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
