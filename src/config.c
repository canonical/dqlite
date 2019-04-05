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
	int rv;
	c->id = id;
	c->address = sqlite3_malloc(strlen(address) + 1);
	if (c->address == NULL) {
		return DQLITE_NOMEM;
	}
	strcpy(c->address, address);
	c->heartbeat_timeout = DEFAULT_HEARTBEAT_TIMEOUT;
	c->page_size = DEFAULT_PAGE_SIZE;
	c->checkpoint_threshold = DEFAULT_CHECKPOINT_THRESHOLD;
	rv = snprintf(c->name, sizeof c->name, "dqlite-%u", id);
	assert(rv < (int)(sizeof c->name));
	return 0;
}

void config__close(struct config *c)
{
	sqlite3_free(c->address);
}
