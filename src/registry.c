#include <assert.h>
#include <string.h>

#include "../include/dqlite.h"

#include "registry.h"

void registry__init(struct registry *r, struct config *config)
{
	*r = (struct registry) {
		.config = config,
	};
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
	r->size = 0;
}

int registry__create(struct registry *r, const char *filename, struct db **db)
{
	*db = registry__get(r, filename);
	if (*db != NULL) {
		return DQLITE_OK;
	}

	*db = sqlite3_malloc(sizeof(struct db));
	if (*db == NULL) {
		return DQLITE_NOMEM;
	}
	db__init(*db, r->config, filename);
	queue_insert_tail(&r->dbs, &(*db)->queue);
	r->size++;
	return DQLITE_OK;
}

struct db *registry__get(const struct registry *r, const char *filename)
{
	queue *head;
	QUEUE_FOREACH(head, &r->dbs)
	{
		struct db *db = QUEUE_DATA(head, struct db, queue);
		if (strcmp(db->filename, filename) == 0) {
			return db;
		}
	}
	return NULL;
}
