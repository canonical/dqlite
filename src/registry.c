#include <string.h>

#include "../include/dqlite.h"

#include "./lib/assert.h"

#include "registry.h"

struct follower
{
	sqlite3 *db;
	queue queue;
};

static const char *follower_filename(struct follower *f)
{
	return sqlite3_db_filename(f->db, "main");
}

static bool follower_has_filename(struct follower *f, const char *filename)
{
	return strcmp(follower_filename(f), filename) == 0;
}

void registry__init(struct registry *r)
{
	QUEUE__INIT(&r->followers);
}

void registry__close(struct registry *r)
{
	while (!QUEUE__IS_EMPTY(&r->followers)) {
		struct follower *f;
		queue *head;
		head = QUEUE__HEAD(&r->followers);
		QUEUE__REMOVE(head);
		f = QUEUE__DATA(head, struct follower, queue);
		sqlite3_free(f);
	}
}

int registry__conn_follower_add(struct registry *r, sqlite3 *db)
{
	struct follower *f;
	const char *filename = sqlite3_db_filename(db, "main");
	assert(registry__conn_follower_get(r, filename) == NULL);
	f = sqlite3_malloc(sizeof *f);
	if (f == NULL) {
		return DQLITE_NOMEM;
	}
	f->db = db;
	QUEUE__PUSH(&r->followers, &f->queue);
	return 0;
}

sqlite3 *registry__conn_follower_get(struct registry *r, const char *filename)
{
	queue *head;
	QUEUE__FOREACH(head, &r->followers)
	{
		struct follower *f = QUEUE__DATA(head, struct follower, queue);
		if (follower_has_filename(f, filename)) {
			return f->db;
		}
	}
	return NULL;
}
