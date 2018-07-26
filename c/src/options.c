#include <assert.h>
#include <stdlib.h>

#include "options.h"

/* Default name of the regitered sqlite3_vfs implementation to use when opening
 * new connections. */
#define DQLITE__OPTIONS_DEFAULT_VFS "volatile"

/* Default name of the registerd sqlite3_wal_replication implementation to use
 * to switch new connections to leader replication mode. */
#define DQLITE__OPTIONS_DEFAULT_WAL_REPLICATION "dqlite"

/* Default heartbeat timeout in milliseconds.
 *
 * Clients will be disconnected if the server does not receive a heartbeat
 * message within this time. */
#define DQLITE__OPTIONS_DEFAULT_HEARTBEAT_TIMEOUT 15000

/* Default database page size in bytes. */
#define DQLITE__OPTIONS_DEFAULT_PAGE_SIZE 4096

/* Number of outstanding WAL frames after which a checkpoint is triggered as
 * soon as possible. */
#define DQLITE__OPTIONS_CHECKPOINT_THRESHOLD 1000

void dqlite__options_defaults(struct dqlite__options *o) {
	assert(o != NULL);

	o->vfs                  = DQLITE__OPTIONS_DEFAULT_VFS;
	o->wal_replication      = DQLITE__OPTIONS_DEFAULT_WAL_REPLICATION;
	o->heartbeat_timeout    = DQLITE__OPTIONS_DEFAULT_HEARTBEAT_TIMEOUT;
	o->page_size            = DQLITE__OPTIONS_DEFAULT_PAGE_SIZE;
	o->checkpoint_threshold = DQLITE__OPTIONS_CHECKPOINT_THRESHOLD;
}
