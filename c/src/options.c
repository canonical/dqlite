#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include <sqlite3.h>

#include "../include/dqlite.h"

#include "options.h"

/* Default heartbeat timeout in milliseconds.
 *
 * Clients will be disconnected if the server does not receive a heartbeat
 * message within this time. */
#define DQLITE__OPTIONS_DEFAULT_HEARTBEAT_TIMEOUT 15000

/* Default database page size in bytes. */
#define DQLITE__OPTIONS_DEFAULT_PAGE_SIZE 4096

/* Number of outstanding WAL frames after which a checkpoint is triggered as
 * soon as possible. */
#define DQLITE__OPTIONS_DEFAULT_CHECKPOINT_THRESHOLD 1000

void dqlite__options_defaults(struct dqlite__options *o) {
	assert(o != NULL);

	o->vfs                  = NULL;
	o->wal_replication      = NULL;
	o->heartbeat_timeout    = DQLITE__OPTIONS_DEFAULT_HEARTBEAT_TIMEOUT;
	o->page_size            = DQLITE__OPTIONS_DEFAULT_PAGE_SIZE;
	o->checkpoint_threshold = DQLITE__OPTIONS_DEFAULT_CHECKPOINT_THRESHOLD;
}

void dqlite__options_close(struct dqlite__options *o) {
	assert(o != NULL);

	if (o->vfs != NULL) {
		sqlite3_free((char *)o->vfs);
	}

	if (o->wal_replication != NULL) {
		sqlite3_free((char *)o->wal_replication);
	}
}

int dqlite__options_set_vfs(struct dqlite__options *o, const char *vfs) {
	assert(o != NULL);
	assert(vfs != NULL);

	o->vfs = sqlite3_malloc(strlen(vfs) + 1);
	if (o->vfs == NULL) {
		return DQLITE_NOMEM;
	}

	strcpy((char *)o->vfs, vfs);

	return 0;
}

int dqlite__options_set_wal_replication(struct dqlite__options *o,
                                        const char *            wal_replication) {
	assert(o != NULL);
	assert(wal_replication != NULL);

	o->wal_replication = sqlite3_malloc(strlen(wal_replication) + 1);
	if (o->wal_replication == NULL) {
		return DQLITE_NOMEM;
	}

	strcpy((char *)o->wal_replication, wal_replication);

	return 0;
}
