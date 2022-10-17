#include <stdlib.h>
#include <string.h>

#include <sqlite3.h>

#include "../include/dqlite.h"

#include "./lib/assert.h"

#include "config.h"
#include "logger.h"

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

/* For generating unique replication/VFS registration names.
 *
 * TODO: make this thread safe. */
static unsigned serial = 1;

int config__init(struct config *c, dqlite_node_id id,
		 const char *address, const char *dir)
{
	int rv;
	c->id = id;
	c->address = sqlite3_malloc((int)strlen(address) + 1);
	if (c->address == NULL) {
		return DQLITE_NOMEM;
	}
	strcpy(c->address, address);
	c->heartbeat_timeout = DEFAULT_HEARTBEAT_TIMEOUT;
	c->page_size = DEFAULT_PAGE_SIZE;
	c->checkpoint_threshold = DEFAULT_CHECKPOINT_THRESHOLD;
	rv = snprintf(c->name, sizeof c->name, "dqlite-%u", serial);
	assert(rv < (int)(sizeof c->name));
	c->logger.data = NULL;
	c->logger.emit = loggerDefaultEmit;
	c->failure_domain = 0;
	c->weight = 0;
	strncpy(c->dir, dir, sizeof(c->dir)-1);
	c->dir[sizeof(c->dir)-1] = '\0';
	c->disk = false;
	serial++;
	return 0;
}

void config__close(struct config *c)
{
	sqlite3_free(c->address);
}
