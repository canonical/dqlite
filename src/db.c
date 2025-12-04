#include <sqlite3.h>
#include <stdint.h>
#include <string.h>

#include "../include/dqlite.h"

#include "db.h"
#include "lib/assert.h"
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
	
	sqlite3_vfs *vfs = sqlite3_vfs_find(config->vfs.name);
	if (vfs == NULL) {
		return DQLITE_MISUSE;
	}

	char *db_filename = sqlite3_malloc((int)(strlen(filename) + 1));
	if (db_filename == NULL) {
		return DQLITE_NOMEM;
	}
	strcpy(db_filename, filename);

	*db = (struct db) {
		.config = config,
		.vfs = vfs,
		.filename = db_filename,
		.cookie = str_hash(filename),
	};
	queue_init(&db->pending_queue);
	return DQLITE_OK;
}

void db__close(struct db *db)
{
	dqlite_assert(db->leaders == 0);
	sqlite3_free(db->filename);
}

int db__open(struct db *db, sqlite3 **out)
{
	tracef("open conn: %s page_size:%u", db->filename, db->config->vfs.page_size);

	sqlite3 *conn = NULL;
	int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_EXRESCODE;
	int rv = sqlite3_open_v2(db->filename, &conn, flags, db->config->vfs.name);
	if (rv != SQLITE_OK) {
		tracef("open_v2 failed %d (%d)", rv, sqlite3_system_errno(conn));
		sqlite3_close(conn);
		return rv;
	}
	*out = conn;
	return SQLITE_OK;
}
