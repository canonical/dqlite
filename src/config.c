#include <stdlib.h>
#include <string.h>

#include <sqlite3.h>

#include "../include/dqlite.h"

#include "./lib/assert.h"

#include "config.h"

/* Default heartbeat timeout in milliseconds.
 *
 * Clients will be disconnected if the server does not receive a heartbeat
 * message within this time. */
#define DEFAULT_HEARTBEAT_TIMEOUT 15000

/* Default database page size in bytes. */
#define DEFAULT_PAGE_SIZE 4096

/* Number of outstanding WAL frames after which a checkpoint is triggered as
 * soon as possible. */
#define DEFAULT_CHECKPOINT_THRESHOLD 1000

int config__init(struct config *c, unsigned id, const char *address)
{
	c->id = id;
	c->address = sqlite3_malloc(strlen(address) + 1);
	if (c->address == NULL) {
		return DQLITE_NOMEM;
	}
	strcpy(c->address, address);
	c->vfs = NULL;
	c->replication = NULL;
	c->heartbeat_timeout = DEFAULT_HEARTBEAT_TIMEOUT;
	c->page_size = DEFAULT_PAGE_SIZE;
	c->checkpoint_threshold = DEFAULT_CHECKPOINT_THRESHOLD;
	return 0;
}

void config__close(struct config *c)
{
	sqlite3_free(c->address);
	if (c->vfs != NULL) {
		sqlite3_free((char *)c->vfs);
	}
	if (c->replication != NULL) {
		sqlite3_free((char *)c->replication);
	}
}

int config__set_vfs(struct config *c, const char *vfs)
{
	assert(vfs != NULL);

	c->vfs = sqlite3_malloc(strlen(vfs) + 1);
	if (c->vfs == NULL) {
		return DQLITE_NOMEM;
	}
	strcpy((char *)c->vfs, vfs);

	return 0;
}

int config__set_replication(struct config *c, const char *replication)
{
	assert(replication != NULL);

	c->replication = sqlite3_malloc(strlen(replication) + 1);
	if (c->replication == NULL) {
		return DQLITE_NOMEM;
	}

	strcpy((char *)c->replication, replication);

	return 0;
}
