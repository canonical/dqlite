#include <string.h>

#include "../include/dqlite.h"

#include "lib/assert.h"

#include "registry.h"

void registry__init(struct registry *r, struct config *config)
{
	r->config = config;
	queue_init(&r->dbs);
}

void registry__close(struct registry *r)
{
	while (!queue_empty(&r->dbs)) {
		struct db *db;
		queue *head;
		head = queue_head(&r->dbs);
		queue_remove(head);
		db = QUEUE_DATA(head, struct db, queue);
		db__close(db);
		sqlite3_free(db);
	}
}

int registry__db_get(struct registry *r, const char *filename, struct db **db)
{
	queue *head;
	QUEUE_FOREACH(head, &r->dbs)
	{
		*db = QUEUE_DATA(head, struct db, queue);
		if (strcmp((*db)->filename, filename) == 0) {
			return 0;
		}
	}
	*db = sqlite3_malloc(sizeof **db);
	if (*db == NULL) {
		return DQLITE_NOMEM;
	}
	db__init(*db, r->config, filename);
	queue_insert_tail(&r->dbs, &(*db)->queue);
	return 0;
}
