#include <string.h>

#include "../include/dqlite.h"

#include "./lib/assert.h"

#include "registry.h"
#include "follower.h"

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

int registry__conn_follower_add(struct registry *r, sqlite3 *conn)
{
	struct follower *f;
	const char *filename = sqlite3_db_filename(conn, "main");

	assert(registry__conn_follower_get(r, filename) == NULL);

	f = sqlite3_malloc(sizeof *f);
	if (f == NULL) {
		return DQLITE_NOMEM;
	}
	follower__init(f, conn);

	QUEUE__PUSH(&r->followers, &f->queue);

	return 0;
}

sqlite3 *registry__conn_follower_get(struct registry *r, const char *filename)
{
	queue *head;
	QUEUE__FOREACH(head, &r->followers)
	{
		struct follower *f = QUEUE__DATA(head, struct follower, queue);
		if (strcmp(follower__filename(f), filename) == 0) {
			return f->conn;
		}
	}
	return NULL;
}
