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
};

void registry__init(struct registry *r, struct config *config);
void registry__close(struct registry *r);

/**
 * Get the db with the given filename. If no one is registered, create one.
 */
int registry__db_get(struct registry *r, const char *filename, struct db **db);

/**
 * Get the db whose current transaction matches the given ID.
 */
void registry__db_by_tx_id(struct registry *r, size_t id, struct db **db);

#endif /* REGISTRY_H_*/
