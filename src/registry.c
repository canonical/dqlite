#include <string.h>

#include "../include/dqlite.h"

#include "lib/assert.h"
#include "registry.h"
#include "vfs.h"

static void registryDeleteHook(void *data, const char *filename)
{
	struct registry *r = data;
	queue *head;
	QUEUE_FOREACH(head, &r->dbs)
	{
		struct db *db = QUEUE_DATA(head, struct db, queue);
		if (strcmp(db->filename, filename) == 0) {
			queue_remove(head);
			db__close(db);
			sqlite3_free(db);
			r->size--;
			return;
		}
	}
}

void registry__init(struct registry *r, struct config *config)
{
	*r = (struct registry) {
		.config = config,
	};
	queue_init(&r->dbs);
	sqlite3_vfs *vfs = sqlite3_vfs_find(config->vfs.name);
	dqlite_assert(vfs != NULL);
	VfsDeleteHook(vfs, registryDeleteHook, r);
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
	sqlite3_vfs *vfs = sqlite3_vfs_find(r->config->vfs.name);
	dqlite_assert(vfs != NULL);
	VfsDeleteHook(vfs, NULL, NULL);
}

int registry__get_or_create(struct registry *r, const char *filename, struct db **db)
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
