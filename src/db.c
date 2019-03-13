#include "./lib/assert.h"

#include "db.h"

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
}
