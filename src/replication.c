#include <stddef.h>

#include <libco.h>
#include <sqlite3.h>

#include "./lib/assert.h"
#include "./lib/logger.h"

#include "replication.h"
#include "leader.h"
#include "queue.h"

/* Set to 1 to enable tracing. */
#if 1
#define tracef(MSG, ...) debugf(r->logger, MSG, ##__VA_ARGS__)
#else
#define tracef(MSG, ...)
#endif

/* Implementation of the sqlite3_wal_replication interface */
struct replication
{
	struct dqlite_logger *logger;
	struct raft *raft;
	queue apply_reqs;
};

int replication__begin(sqlite3_wal_replication *replication, void *arg)
{
	struct replication *r = replication->pAppData;
	struct leader *leader = arg;

	if (raft_state(r->raft) != RAFT_LEADER) {
		return SQLITE_IOERR_NOT_LEADER;
	}

	if (leader->db->follower == NULL) {
	}

	return SQLITE_OK;
}

int replication__abort(sqlite3_wal_replication *r, void *arg)
{
	(void)r;
	(void)arg;

	return SQLITE_OK;
}

int replication__frames(sqlite3_wal_replication *r,
			void *arg,
			int page_size,
			int n,
			sqlite3_wal_replication_frame *frames,
			unsigned truncate,
			int commit)
{
	return SQLITE_OK;
}

int replication__undo(sqlite3_wal_replication *r, void *arg)
{
	(void)r;
	(void)arg;

	return SQLITE_OK;
}

int replication__end(sqlite3_wal_replication *r, void *arg)
{
	(void)r;
	(void)arg;

	return SQLITE_OK;
}

int replication__init(struct sqlite3_wal_replication *replication,
		      struct dqlite_logger *logger,
		      struct raft *raft)
{
	struct replication *r = sqlite3_malloc(sizeof *r);

	if (r == NULL) {
		return DQLITE_NOMEM;
	}

	r->logger = logger;
	r->raft = raft;
	QUEUE__INIT(&r->apply_reqs);

	replication->iVersion = 1;
	replication->pAppData = r;
	replication->xBegin = replication__begin;
	replication->xAbort = replication__abort;
	replication->xFrames = replication__frames;
	replication->xUndo = replication__undo;
	replication->xEnd = replication__end;

	return 0;
}

void replication__close(struct sqlite3_wal_replication *replication)
{
	struct replication *r = replication->pAppData;
	sqlite3_free(r);
}
