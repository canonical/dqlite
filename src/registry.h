#ifndef REGISTRY_H_
#define REGISTRY_H_

#include <stdbool.h>

#include <sqlite3.h>

#include "./lib/queue.h"

struct registry
{
	queue followers;
};

void registry__init(struct registry *r);
void registry__close(struct registry *r);

/**
 * Add a new follower connection to the registry.
 *
 * If a follower connection for the database with the given filename is already
 * registered, abort the process with an assertion error.
 */
int registry__conn_follower_add(struct registry *r, sqlite3 *db);

/**
 * Return the follower connection used to replicate the database identified by
 * the given filename, or NULL.
 */
sqlite3 *registry__conn_follower_get(struct registry *r, const char *filename);

#endif /* REGISTRY_H_*/
