#include <stdint.h>
#include <stdio.h>

#include "../include/dqlite.h"

#include "./lib/assert.h"

#include "command.h"
#include "conn.h"
#include "gateway.h"
#include "id.h"
#include "leader.h"
#include "lib/sm.h"
#include "lib/threadpool.h"
#include "server.h"
#include "tracing.h"
#include "utils.h"
#include "vfs.h"

/**
 * State machine for exec requests.
 */
enum {
	EXEC_START,
	EXEC_BARRIER,
	EXEC_STEPPED,
	EXEC_POLLED,
	EXEC_DONE,
	EXEC_FAILED,
	EXEC_NR,
};

static const struct sm_conf exec_states[EXEC_NR] = {
	[EXEC_START] = {
		.name = "start",
		.allowed = BITS(EXEC_BARRIER)|BITS(EXEC_FAILED)|BITS(EXEC_DONE),
		.flags = SM_INITIAL,
	},
	[EXEC_BARRIER] = {
		.name = "barrier",
		.allowed = BITS(EXEC_STEPPED)|BITS(EXEC_FAILED)|BITS(EXEC_DONE),
	},
	[EXEC_STEPPED] = {
		.name = "stepped",
		.allowed = BITS(EXEC_POLLED)|BITS(EXEC_FAILED)|BITS(EXEC_DONE),
	},
	[EXEC_POLLED] = {
		.name = "polled",
		.allowed = BITS(EXEC_FAILED)|BITS(EXEC_DONE),
	},
	[EXEC_DONE] = {
		.name = "done",
		.flags = SM_FINAL,
	},
	[EXEC_FAILED] = {
		.name = "failed",
		.flags = SM_FAILURE|SM_FINAL,
	},
};

static bool exec_invariant(const struct sm *sm, int prev)
{
	(void)sm;
	(void)prev;
	return true;
}

/* Called when a leader exec request terminates and the associated callback can
 * be invoked. */
static void leaderExecDone(struct exec *req)
{
	tracef("leader exec done id:%" PRIu64, req->id);
	req->leader->exec = NULL;
	if (req->status == SQLITE_DONE) {
		sm_move(&req->sm, EXEC_DONE);
	} else {
		sm_fail(&req->sm, EXEC_FAILED, req->status);
	}
	sm_fini(&req->sm);
	if (req->cb != NULL) {
		req->cb(req, req->status);
	}
}

/* Open a SQLite connection and set it to leader replication mode. */
static int openConnection(const char *filename,
			  const char *vfs,
			  unsigned page_size,
			  sqlite3 **conn)
{
	tracef("open connection filename %s", filename);
	char pragma[255];
	int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	char *msg = NULL;
	int rc;

	rc = sqlite3_open_v2(filename, conn, flags, vfs);
	if (rc != SQLITE_OK) {
		tracef("open failed %d", rc);
		goto err;
	}

	/* Enable extended result codes */
	rc = sqlite3_extended_result_codes(*conn, 1);
	if (rc != SQLITE_OK) {
		tracef("extended codes failed %d", rc);
		goto err;
	}

	/* The vfs, db, gateway, and leader code currently assumes that
	 * each connection will operate on only one DB file/WAL file
	 * pair. Make sure that the client can't use ATTACH DATABASE to
	 * break this assumption. We apply the same limit in open_follower_conn
	 * in db.c.
	 *
	 * Note, 0 instead of 1 -- apparently the "initial database" is not
	 * counted when evaluating this limit. */
	sqlite3_limit(*conn, SQLITE_LIMIT_ATTACHED, 0);

	/* Set the page size. */
	sprintf(pragma, "PRAGMA page_size=%d", page_size);
	rc = sqlite3_exec(*conn, pragma, NULL, NULL, &msg);
	if (rc != SQLITE_OK) {
		tracef("page size set failed %d page size %u", rc, page_size);
		goto err;
	}

	/* Disable syncs. */
	rc = sqlite3_exec(*conn, "PRAGMA synchronous=OFF", NULL, NULL, &msg);
	if (rc != SQLITE_OK) {
		tracef("sync off failed %d", rc);
		goto err;
	}

	/* Set WAL journaling. */
	rc = sqlite3_exec(*conn, "PRAGMA journal_mode=WAL", NULL, NULL, &msg);
	if (rc != SQLITE_OK) {
		tracef("wal on failed %d", rc);
		goto err;
	}

	rc = sqlite3_exec(*conn, "PRAGMA wal_autocheckpoint=0", NULL, NULL,
			  &msg);
	if (rc != SQLITE_OK) {
		tracef("wal autocheckpoint off failed %d", rc);
		goto err;
	}

	rc =
	    sqlite3_db_config(*conn, SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, 1, NULL);
	if (rc != SQLITE_OK) {
		tracef("db config failed %d", rc);
		goto err;
	}

	/* TODO: make setting foreign keys optional. */
	rc = sqlite3_exec(*conn, "PRAGMA foreign_keys=1", NULL, NULL, &msg);
	if (rc != SQLITE_OK) {
		tracef("enable foreign keys failed %d", rc);
		goto err;
	}

	return 0;

err:
	if (*conn != NULL) {
		sqlite3_close(*conn);
		*conn = NULL;
	}
	if (msg != NULL) {
		sqlite3_free(msg);
	}
	return rc;
}

/* Whether we need to submit a barrier request because there is no transaction
 * in progress in the underlying database and the FSM is behind the last log
 * index. */
static bool needsBarrier(struct leader *l)
{
	return l->db->tx_id == 0 &&
	       raft_last_applied(l->raft) < raft_last_index(l->raft);
}

int leader__init(struct leader *l, struct db *db, struct raft *raft)
{
	tracef("leader init");
	int rc;
	l->db = db;
	l->raft = raft;
	rc = openConnection(db->path, db->config->name, db->config->page_size,
			    &l->conn);
	if (rc != 0) {
		tracef("open failed %d", rc);
		return rc;
	}

	l->exec = NULL;
	l->inflight = NULL;
	queue_insert_tail(&db->leaders, &l->queue);
	return 0;
}

void leader__close(struct leader *l)
{
	tracef("leader close");
	int rc;
	/* TODO: there shouldn't be any ongoing exec request. */
	if (l->exec != NULL) {
		assert(l->inflight == NULL);
		l->exec->status = SQLITE_ERROR;
		leaderExecDone(l->exec);
	}
	rc = sqlite3_close(l->conn);
	assert(rc == 0);

	queue_remove(&l->queue);
}

/* A checkpoint command that fails to commit is not a huge issue.
 * The WAL will not be checkpointed this time around on these nodes,
 * a new checkpoint command will be issued once the WAL on the leader reaches
 * threshold size again. It's improbable that the WAL in this way could grow
 * without bound, it would mean that apply frames commands commit without
 * issues, while the checkpoint command would somehow always fail to commit. */
static void leaderCheckpointApplyCb(struct raft_apply *req,
				    int status,
				    void *result)
{
	(void)result;
	raft_free(req);
	if (status != 0) {
		tracef("checkpoint apply failed %d", status);
	}
}

/* Attempt to perform a checkpoint on nodes running a version of dqlite that
 * doens't perform autonomous checkpoints. For recent nodes, the checkpoint
 * command will just be a no-op.
 * This function will run after the WAL might have been checkpointed during a
 * call to `apply_frames`.
 * */
static void leaderMaybeCheckpointLegacy(struct leader *l)
{
	tracef("leader maybe checkpoint legacy");
	struct sqlite3_file *wal;
	struct raft_buffer buf;
	struct command_checkpoint command;
	sqlite3_int64 size;
	int rv;

	/* Get the database file associated with this connection */
	rv = sqlite3_file_control(l->conn, "main", SQLITE_FCNTL_JOURNAL_POINTER,
				  &wal);
	assert(rv == SQLITE_OK); /* Should never fail */

	rv = wal->pMethods->xFileSize(wal, &size);
	assert(rv == SQLITE_OK); /* Should never fail */

	/* size of the WAL will be 0 if it has just been checkpointed on this
	 * leader as a result of running apply_frames. */
	if (size != 0) {
		return;
	}

	tracef("issue checkpoint command");

	/* Attempt to perfom a checkpoint across nodes that don't perform
	 * autonomous snapshots. */
	command.filename = l->db->filename;
	rv = command__encode(COMMAND_CHECKPOINT, &command, &buf);
	if (rv != 0) {
		tracef("encode failed %d", rv);
		return;
	}

	struct raft_apply *apply = raft_malloc(sizeof(*apply));
	if (apply == NULL) {
		tracef("raft_malloc - no mem");
		goto err_after_buf_alloc;
	}
#ifdef USE_SYSTEM_RAFT
	rv = raft_apply(l->raft, apply, &buf, 1, leaderCheckpointApplyCb);
#else
	rv = raft_apply(l->raft, apply, &buf, NULL, 1, leaderCheckpointApplyCb);
#endif
	if (rv != 0) {
		tracef("raft_apply failed %d", rv);
		raft_free(apply);
		goto err_after_buf_alloc;
	}

	return;

err_after_buf_alloc:
	raft_free(buf.base);
}

static void leaderApplyFramesCb(struct raft_apply *req,
				int status,
				void *result)
{
	tracef("apply frames cb id:%" PRIu64, idExtract(req->req_id));
	struct apply *apply = req->data;
	struct leader *l = apply->leader;
	if (l == NULL) {
		raft_free(apply);
		return;
	}

	(void)result;

	if (status != 0) {
		tracef("apply frames cb failed status %d", status);
		sqlite3_vfs *vfs = sqlite3_vfs_find(l->db->config->name);
		switch (status) {
			case RAFT_LEADERSHIPLOST:
				l->exec->status = SQLITE_IOERR_LEADERSHIP_LOST;
				break;
			case RAFT_NOSPACE:
				l->exec->status = SQLITE_IOERR_WRITE;
				break;
			case RAFT_SHUTDOWN:
				/* If we got here it means we have manually
				 * fired the apply callback from
				 * gateway__close(). In this case we don't
				 * free() the apply object, since it will be
				 * freed when the callback is fired again by
				 * raft.
				 *
				 * TODO: we should instead make gatewa__close()
				 * itself asynchronous. */
				apply->leader = NULL;
				l->exec->status = SQLITE_ABORT;
				goto finish;
				break;
			default:
				l->exec->status = SQLITE_IOERR;
				break;
		}
		VfsAbort(vfs, l->db->path);
	}

	raft_free(apply);

	if (status == 0) {
		leaderMaybeCheckpointLegacy(l);
	}

finish:
	l->inflight = NULL;
	l->db->tx_id = 0;
	leaderExecDone(l->exec);
}

static int leaderApplyFrames(struct exec *req,
			     dqlite_vfs_frame *frames,
			     unsigned n)
{
	tracef("leader apply frames id:%" PRIu64, req->id);
	struct leader *l = req->leader;
	struct db *db = l->db;
	struct command_frames c;
	struct raft_buffer buf;
	struct apply *apply;
	int rv;

	c.filename = db->filename;
	c.tx_id = 0;
	c.truncate = 0;
	c.is_commit = 1;
	c.frames.n_pages = (uint32_t)n;
	c.frames.page_size = (uint16_t)db->config->page_size;
	c.frames.data = frames;

	apply = raft_malloc(sizeof *req);
	if (apply == NULL) {
		tracef("malloc");
		rv = DQLITE_NOMEM;
		goto err;
	}

	rv = command__encode(COMMAND_FRAMES, &c, &buf);
	if (rv != 0) {
		tracef("encode %d", rv);
		goto err_after_apply_alloc;
	}

	apply->leader = req->leader;
	apply->req.data = apply;
	apply->type = COMMAND_FRAMES;
	idSet(apply->req.req_id, req->id);

#ifdef USE_SYSTEM_RAFT
	rv = raft_apply(l->raft, &apply->req, &buf, 1, leaderApplyFramesCb);
#else
	/* TODO actual WAL slice goes here */
	struct raft_entry_local_data local_data = {};
	rv = raft_apply(l->raft, &apply->req, &buf, &local_data, 1,
			leaderApplyFramesCb);
	sm_relate(&req->sm, &apply->req.sm);
#endif
	if (rv != 0) {
		tracef("raft apply failed %d", rv);
		goto err_after_command_encode;
	}

	db->tx_id = 1;
	l->inflight = apply;

	return 0;

err_after_command_encode:
#ifndef USE_SYSTEM_RAFT
	sm_fini(&apply->req.sm);
#endif
	raft_free(buf.base);
err_after_apply_alloc:
	raft_free(apply);
err:
	assert(rv != 0);
	return rv;
}

static void leaderExecV2(struct exec *req, enum pool_half half)
{
	tracef("leader exec v2 id:%" PRIu64, req->id);
	struct leader *l = req->leader;
	struct db *db = l->db;
	sqlite3_vfs *vfs = sqlite3_vfs_find(db->config->name);
	dqlite_vfs_frame *frames;
	uint64_t size;
	unsigned n;
	unsigned i;
	int rv;

	if (half == POOL_TOP_HALF) {
		req->status = sqlite3_step(req->stmt);
		sm_move(&req->sm, EXEC_STEPPED);
		return;
	} /* else POOL_BOTTOM_HALF => */

	rv = VfsPoll(vfs, db->path, &frames, &n);
	sm_move(&req->sm, EXEC_POLLED);
	if (rv != 0 || n == 0) {
		tracef("vfs poll");
		goto finish;
	}

	/* Check if the new frames would create an overfull database */
	size = VfsDatabaseSize(vfs, db->path, n, db->config->page_size);
	if (size > VfsDatabaseSizeLimit(vfs)) {
		rv = SQLITE_FULL;
		goto abort;
	}

	rv = leaderApplyFrames(req, frames, n);
	if (rv != 0) {
		goto abort;
	}

	for (i = 0; i < n; i++) {
		sqlite3_free(frames[i].data);
	}
	sqlite3_free(frames);
	return;

abort:
	for (i = 0; i < n; i++) {
		sqlite3_free(frames[i].data);
	}
	sqlite3_free(frames);
	VfsAbort(vfs, l->db->path);
finish:
	if (rv != 0) {
		tracef("exec v2 failed %d", rv);
		l->exec->status = rv;
	}
	leaderExecDone(l->exec);
}

#ifdef DQLITE_NEXT

static void exec_top(pool_work_t *w)
{
	struct exec *req = CONTAINER_OF(w, struct exec, work);
	leaderExecV2(req, POOL_TOP_HALF);
}

static void exec_bottom(pool_work_t *w)
{
	struct exec *req = CONTAINER_OF(w, struct exec, work);
	leaderExecV2(req, POOL_BOTTOM_HALF);
}

#endif

static void execBarrierCb(struct barrier *barrier, int status)
{
	tracef("exec barrier cb status %d", status);
	struct exec *req = barrier->data;
	struct leader *l = req->leader;

	sm_move(&req->sm, EXEC_BARRIER);

	if (status != 0) {
		l->exec->status = status;
		leaderExecDone(l->exec);
		return;
	}

#ifdef DQLITE_NEXT
	struct dqlite_node *node = l->raft->data;
	pool_t *pool = !!(pool_ut_fallback()->flags & POOL_FOR_UT)
		? pool_ut_fallback() : &node->pool;
	pool_queue_work(pool, &req->work, l->db->cookie,
			WT_UNORD, exec_top, exec_bottom);
#else
	leaderExecV2(req, POOL_TOP_HALF);
	leaderExecV2(req, POOL_BOTTOM_HALF);
#endif
}

int leader__exec(struct leader *l,
		 struct exec *req,
		 sqlite3_stmt *stmt,
		 uint64_t id,
		 exec_cb cb)
{
	tracef("leader exec id:%" PRIu64, id);
	int rv;
	if (l->exec != NULL) {
		tracef("busy");
		return SQLITE_BUSY;
	}
	l->exec = req;

	req->leader = l;
	req->stmt = stmt;
	req->id = id;
	req->cb = cb;
	req->barrier.data = req;
	req->barrier.cb = NULL;
	req->work = (pool_work_t){};
	sm_init(&req->sm, exec_invariant, NULL, exec_states, "exec",
		EXEC_START);

	rv = leader__barrier(l, &req->barrier, execBarrierCb);
	if (rv != 0) {
		l->exec = NULL;
		return rv;
	}
	return 0;
}

static void raftBarrierCb(struct raft_barrier *req, int status)
{
	tracef("raft barrier cb status %d", status);
	struct barrier *barrier = req->data;
	int rv = 0;
	if (status != 0) {
		if (status == RAFT_LEADERSHIPLOST) {
			rv = SQLITE_IOERR_LEADERSHIP_LOST;
		} else {
			rv = SQLITE_ERROR;
		}
	}
	barrier_cb cb = barrier->cb;
	if (cb == NULL) {
		tracef("barrier cb already fired");
		return;
	}
	barrier->cb = NULL;
	cb(barrier, rv);
}

int leader__barrier(struct leader *l, struct barrier *barrier, barrier_cb cb)
{
	tracef("leader barrier");
	int rv;
	if (!needsBarrier(l)) {
		tracef("not needed");
		cb(barrier, 0);
		return 0;
	}
	barrier->cb = cb;
	barrier->leader = l;
	barrier->req.data = barrier;
	rv = raft_barrier(l->raft, &barrier->req, raftBarrierCb);
	if (rv != 0) {
		tracef("raft barrier failed %d", rv);
		barrier->req.data = NULL;
		barrier->leader = NULL;
		barrier->cb = NULL;
		return rv;
	}
	return 0;
}
