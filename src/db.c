#include <stdint.h>
#include <string.h>

#include "../include/dqlite.h"

#include "./lib/assert.h"

#include "db.h"
#include "tracing.h"

/* Limit taken from sqlite unix vfs. */
#define MAX_PATHNAME 512

/* Open a SQLite connection and set it to follower mode. */
static int open_follower_conn(const char *filename,
			      const char *vfs,
			      unsigned page_size,
			      sqlite3 **conn);

static uint32_t str_hash(const char* name)
{
	const unsigned char *p;
	uint32_t h = 5381U;

	for (p = (const unsigned char *) name; *p != '\0'; p++) {
		h = (h << 5) + h + *p;
	}

	return h;
}

int db__init(struct db *db, struct config *config, const char *filename)
{
	tracef("db init filename=`%s'", filename);
	int rv;

	db->config = config;
	db->cookie = str_hash(filename);
	db->filename = sqlite3_malloc((int)(strlen(filename) + 1));
	if (db->filename == NULL) {
		rv = DQLITE_NOMEM;
		goto err;
	}
	strcpy(db->filename, filename);
	db->path = sqlite3_malloc(MAX_PATHNAME + 1);
	if (db->path == NULL) {
		rv = DQLITE_NOMEM;
		goto err_after_filename_alloc;
	}
	if (db->config->disk) {
		rv = snprintf(db->path, MAX_PATHNAME + 1, "%s/%s",
			      db->config->dir, db->filename);
	} else {
		rv = snprintf(db->path, MAX_PATHNAME + 1, "%s", db->filename);
	}
	if (rv < 0 || rv >= MAX_PATHNAME + 1) {
		goto err_after_path_alloc;
	}

	db->follower = NULL;
	db->tx_id = 0;
	db->read_lock = 0;
	queue_init(&db->leaders);
	return 0;

err_after_path_alloc:
	sqlite3_free(db->path);
err_after_filename_alloc:
	sqlite3_free(db->filename);
err:
	return rv;
}

void db__close(struct db *db)
{
	assert(queue_empty(&db->leaders));
	if (db->follower != NULL) {
		int rc;
		rc = sqlite3_close(db->follower);
		assert(rc == SQLITE_OK);
	}
	sqlite3_free(db->path);
	sqlite3_free(db->filename);
}

int db__open_follower(struct db *db)
{
	int rc;
	assert(db->follower == NULL);
	rc = open_follower_conn(db->path, db->config->name,
				db->config->page_size, &db->follower);
	return rc;
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

	tracef("open follower conn: %s page_size:%u", filename, page_size);
	rc = sqlite3_open_v2(filename, conn, flags, vfs);
	if (rc != SQLITE_OK) {
		tracef("open_v2 failed %d", rc);
		goto err;
	}

	/* Enable extended result codes */
	rc = sqlite3_extended_result_codes(*conn, 1);
	if (rc != SQLITE_OK) {
		goto err;
	}

	/* The vfs, db, gateway, and leader code currently assumes that
	 * each connection will operate on only one DB file/WAL file
	 * pair. Make sure that the client can't use ATTACH DATABASE to
	 * break this assumption. We apply the same limit in openConnection
	 * in leader.c.
	 *
	 * Note, 0 instead of 1 -- apparently the "initial database" is not
	 * counted when evaluating this limit. */
	sqlite3_limit(*conn, SQLITE_LIMIT_ATTACHED, 0);

	/* Set the page size. */
	sprintf(pragma, "PRAGMA page_size=%d", page_size);
	rc = sqlite3_exec(*conn, pragma, NULL, NULL, &msg);
	if (rc != SQLITE_OK) {
		tracef("page_size=%d failed", page_size);
		goto err;
	}

	/* Disable syncs. */
	rc = sqlite3_exec(*conn, "PRAGMA synchronous=OFF", NULL, NULL, &msg);
	if (rc != SQLITE_OK) {
		tracef("synchronous=OFF failed");
		goto err;
	}

	/* Set WAL journaling. */
	rc = sqlite3_exec(*conn, "PRAGMA journal_mode=WAL", NULL, NULL, &msg);
	if (rc != SQLITE_OK) {
		tracef("journal_mode=WAL failed");
		goto err;
	}

	rc =
	    sqlite3_db_config(*conn, SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, 1, NULL);
	if (rc != SQLITE_OK) {
		goto err;
	}

	return 0;

err:
	if (*conn != NULL) {
		sqlite3_close(*conn);
		*conn = NULL;
	}
	if (msg != NULL) {
		sqlite3_free(msg);
	}
	return rc;
}
