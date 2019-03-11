#include <stdlib.h>
#include <string.h>

#include <sqlite3.h>

#include "../include/dqlite.h"

#include "./lib/assert.h"

#include "options.h"

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

void options__init(struct options *o)
{
	assert(o != NULL);
	o->vfs = NULL;
	o->replication = NULL;
	o->heartbeat_timeout = DEFAULT_HEARTBEAT_TIMEOUT;
	o->page_size = DEFAULT_PAGE_SIZE;
	o->checkpoint_threshold = DEFAULT_CHECKPOINT_THRESHOLD;
}

void options__close(struct options *o)
{
	assert(o != NULL);
	if (o->vfs != NULL) {
		sqlite3_free((char *)o->vfs);
	}
	if (o->replication != NULL) {
		sqlite3_free((char *)o->replication);
	}
}

int options__set_vfs(struct options *o, const char *vfs)
{
	assert(o != NULL);
	assert(vfs != NULL);

	o->vfs = sqlite3_malloc(strlen(vfs) + 1);
	if (o->vfs == NULL) {
		return DQLITE_NOMEM;
	}
	strcpy((char *)o->vfs, vfs);

	return 0;
}

int options__set_replication(struct options *o, const char *replication)
{
	assert(o != NULL);
	assert(replication != NULL);

	o->replication = sqlite3_malloc(strlen(replication) + 1);
	if (o->replication == NULL) {
		return DQLITE_NOMEM;
	}

	strcpy((char *)o->replication, replication);

	return 0;
}
