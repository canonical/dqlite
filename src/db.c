#include "../include/dqlite.h"

#include "./lib/assert.h"

#include "db.h"
#include "follower.h"

void db__init(struct db *db, struct options *options, const char *filename)
{
	db->options = options;
	db->filename = filename;
	db->follower = NULL;
	QUEUE__INIT(&db->leaders);
}

void db__close(struct db *db)
{
	assert(QUEUE__IS_EMPTY(&db->leaders));
	if (db->follower != NULL) {
		follower__close(db->follower);
		sqlite3_free(db->follower);
	}
}

int db__open_follower(struct db *db)
{
	int rc;
	assert(db->follower == NULL);
	db->follower = sqlite3_malloc(sizeof *db->follower);
	if (db->follower == NULL) {
		rc = DQLITE_NOMEM;
		goto err;
	}
	rc = follower__init(db->follower, db->options->vfs, db->filename);
	if (rc != 0) {
		goto err_after_follower_alloc;
	}
	return 0;

err_after_follower_alloc:
	sqlite3_free(db->follower);
	db->follower = NULL;
err:
	return rc;
}
