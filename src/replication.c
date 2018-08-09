#ifdef DQLITE_EXPERIMENTAL

#include <assert.h>
#include <stdlib.h>

#include <sqlite3.h>

#include "lifecycle.h"
#include "replication.h"

void dqlite__replication_ctx_init(struct dqlite__replication_ctx *c) {
	assert(c != NULL);

	dqlite__lifecycle_init(DQLITE__LIFECYCLE_REPLICATION);
};

void dqlite__replication_ctx_close(struct dqlite__replication_ctx *c) {
	assert(c != NULL);

	dqlite__lifecycle_close(DQLITE__LIFECYCLE_REPLICATION);
}

int dqlite__replication_begin(sqlite3_wal_replication *r, void *arg) {
	(void)r;
	(void)arg;

	return SQLITE_OK;
}

int dqlite__replication_abort(sqlite3_wal_replication *r, void *arg) {
	(void)r;
	(void)arg;

	return SQLITE_OK;
}

int dqlite__replication_frames(sqlite3_wal_replication *      r,
                               void *                         arg,
                               int                            page_size,
                               int                            n,
                               sqlite3_wal_replication_frame *frames,
                               unsigned                       truncate,
                               int                            commit) {
	(void)arg;
	(void)page_size;
	(void)n;
	(void)frames;
	(void)truncate;
	(void)commit;

	assert(r != NULL);
	assert(r->pAppData != NULL);

	return SQLITE_OK;
}

int dqlite__replication_undo(sqlite3_wal_replication *r, void *arg) {
	(void)r;
	(void)arg;

	return SQLITE_OK;
}

int dqlite__replication_end(sqlite3_wal_replication *r, void *arg) {
	(void)r;
	(void)arg;

	return SQLITE_OK;
}

#endif /* DQLITE_EXPERIMENTAL */
