#include <sqlite3.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "../include/dqlite.h"

#include "config.h"
#include "lib/assert.h"
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

/* For generating unique replication/VFS registration names. */
static _Atomic unsigned serial = 1;

int config__init(struct config *c,
		 dqlite_node_id id,
		 const char *address,
		 const char *raft_dir)
{

	*c = (struct config) {
		.id = id,
		.heartbeat_timeout = DEFAULT_HEARTBEAT_TIMEOUT,
		.vfs = {
			.page_size = DEFAULT_PAGE_SIZE,
			.checkpoint_threshold = DEFAULT_CHECKPOINT_THRESHOLD,
		},
		.logger = {
			.data = NULL,
			.emit = loggerDefaultEmit,
		},
		.failure_domain = 0,
		.weight = 0,
		.allowed_roles = 0,
		.voters = 3,
		.standbys = 0,
		.pool_thread_count = 4,
	};

	c->address = sqlite3_malloc((int)strlen(address) + 1);
	if (c->address == NULL) {
		return DQLITE_NOMEM;
	}
	strcpy(c->address, address);
	
	unsigned vfs_id = atomic_fetch_add_explicit(&serial, 1, memory_order_relaxed);
	int rv = snprintf(c->vfs.name, sizeof c->vfs.name, "dqlite-%u", vfs_id);
	dqlite_assert(rv > 0 && rv < (int)(sizeof c->vfs.name));

	snprintf(c->raft_dir, sizeof(c->raft_dir), "%s", (raft_dir != NULL) ? raft_dir : "");

	return 0;
}

void config__close(struct config *c)
{
	sqlite3_free(c->address);
}
