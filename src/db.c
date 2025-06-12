#include <stdint.h>
#include <string.h>

#include "../include/dqlite.h"

#include "./lib/assert.h"

#include "db.h"
#include "tracing.h"

/* Limit taken from sqlite unix vfs. */
#define MAX_PATHNAME 512

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
	db->vfs = sqlite3_vfs_find(config->name);
	if (db->vfs == NULL) {
		return DQLITE_MISUSE;
	}
	db->cookie = str_hash(filename);
	db->filename = sqlite3_malloc((int)(strlen(filename) + 1));
	if (db->filename == NULL) {
		return DQLITE_NOMEM;
	}
	strcpy(db->filename, filename);
	db->path = sqlite3_malloc(MAX_PATHNAME + 1);
	if (db->path == NULL) {
		rv = DQLITE_NOMEM;
		goto err_after_filename_alloc;
	}
	if (db->config->disk) {
		rv = snprintf(db->path, MAX_PATHNAME + 1, "%s/%s",
			      db->config->database_dir, db->filename);
	} else {
		rv = snprintf(db->path, MAX_PATHNAME + 1, "%s", db->filename);
	}
	if (rv < 0 || rv >= MAX_PATHNAME + 1) {
		goto err_after_path_alloc;
	}

	db->active_leader = NULL;
	queue_init(&db->pending_queue);
	db->read_lock = 0;
	db->leaders = 0;
	return 0;

err_after_path_alloc:
	sqlite3_free(db->path);
err_after_filename_alloc:
	sqlite3_free(db->filename);
	return rv;
}

void db__close(struct db *db)
{
	assert(db->leaders == 0);
	sqlite3_free(db->path);
	sqlite3_free(db->filename);
}

static int dqlite_authorizer(void *pUserData, int action, const char *third, const char *fourth, const char *fifth, const char *sixth) {
	(void)pUserData;
	(void)fourth;
	(void)fifth;
	(void)sixth;

	if (action == SQLITE_ATTACH) {
		/* Only allow attaching temporary files */
		if (third != NULL && third[0] != '\0') {
			return SQLITE_DENY;
		}
	} else if (action == SQLITE_PRAGMA) {
		if (strcasecmp(third, "journal_mode") == 0 && fourth) {
			/* Block changes to the journal mode:
			 * only WAL mode is supported */
			return SQLITE_DENY;
		}
	}
	return SQLITE_OK;
}

int db__open(struct db *db, sqlite3 **conn)
{
	tracef("open conn: %s page_size:%u", db->path, db->config->page_size);
	char pragma[255];
	int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	char *msg = NULL;
	int rc;

	rc = sqlite3_open_v2(db->path, conn, flags, db->config->name);
	if (rc != SQLITE_OK) {
		tracef("open_v2 failed %d", rc);
		goto err;
	}

	/* Enable extended result codes */
	rc = sqlite3_extended_result_codes(*conn, 1);
	if (rc != SQLITE_OK) {
		tracef("extended codes failed %d", rc);
		goto err;
	}

	/* Set the page size. */
	sprintf(pragma, "PRAGMA page_size=%d", db->config->page_size);
	rc = sqlite3_exec(*conn, pragma, NULL, NULL, &msg);
	if (rc != SQLITE_OK) {
		tracef("page_size=%d failed", db->config->page_size);
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

	rc = sqlite3_exec(*conn, "PRAGMA foreign_keys=1", NULL, NULL, &msg);
	if (rc != SQLITE_OK) {
		tracef("foreign_keys=1 failed");
		goto err;
	}

	rc = sqlite3_wal_autocheckpoint(*conn, 0);
	if (rc != SQLITE_OK) {
		tracef("sqlite3_wal_autocheckpoint off failed %d", rc);
		goto err;
	}

	rc =
	    sqlite3_db_config(*conn, SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, 1, NULL);
	if (rc != SQLITE_OK) {
		goto err;
	}

	/* The vfs, db, gateway, and leader code currently assumes that
	 * each connection will operate on only one DB file/WAL file
	 * pair. Make sure that the client can't use ATTACH DATABASE to
	 * break this assumption.*/
	sqlite3_set_authorizer(*conn, dqlite_authorizer, NULL);

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
