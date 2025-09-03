#ifndef REGISTRY_H_
#define REGISTRY_H_

#include <stdbool.h>

#include <sqlite3.h>

#include "lib/queue.h"

#include "db.h"

struct registry
{
	struct config *config;
	queue dbs;
	size_t size;
};

void registry__init(struct registry *r, struct config *config);
void registry__close(struct registry *r);

/**
 * Returns the db with the given filename. If the db does not exists, it is created.
 */
int registry__get_or_create(struct registry *r, const char *filename, struct db **db);

/**
 * Returns the db with the given filename. If the db does not exists it returns NULL.
 */
struct db *registry__get(const struct registry *r, const char *filename);

/**
 * Returns the number of databases in the registry. 
 */
inline size_t registry__size(const struct registry *r) { return r->size; }

#endif /* REGISTRY_H_*/
