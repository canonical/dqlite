#include <string.h>

#include "../include/dqlite.h"

#include "./lib/assert.h"

#include "registry.h"

void registry__init(struct registry *r, struct options *options)
{
	r->options = options;
	QUEUE__INIT(&r->dbs);
}

void registry__close(struct registry *r)
{
	while (!QUEUE__IS_EMPTY(&r->dbs)) {
		struct db *db;
		queue *head;
		head = QUEUE__HEAD(&r->dbs);
		QUEUE__REMOVE(head);
		db = QUEUE__DATA(head, struct db, queue);
		db__close(db);
		sqlite3_free(db);
	}
}

int registry__db_get(struct registry *r, const char *filename, struct db **db)
{
	queue *head;
	QUEUE__FOREACH(head, &r->dbs)
	{
		*db = QUEUE__DATA(head, struct db, queue);
		if (strcmp((*db)->filename, filename) == 0) {
			return 0;
		}
	}
	*db = sqlite3_malloc(sizeof **db);
	if (*db == NULL) {
		return DQLITE_NOMEM;
	}
	db__init(*db, r->options, filename);
	QUEUE__PUSH(&r->dbs, &(*db)->queue);
	return 0;
}
