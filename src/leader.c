#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>

#include "../include/dqlite.h"

#include "./lib/assert.h"

#include "command.h"
#include "conn.h"
#include "db.h"
#include "gateway.h"
#include "leader.h"
#include "lib/queue.h"
#include "lib/sm.h"
#include "lib/threadpool.h"
#include "raft.h"
#include "server.h"
#include "tracing.h"
#include "translate.h"
#include "utils.h"
#include "vfs.h"


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

	rc = sqlite3_wal_autocheckpoint(*conn, 0);
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
	return raft_last_applied(l->raft) < raft_last_index(l->raft);
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
	db->leaders++;
	return 0;
}

static bool exec_invariant(const struct sm *sm, int prev)
{
	(void)sm;
	(void)prev;
	return true;
}

void leader__close(struct leader *l)
{
	tracef("leader close");
	int rc;
	// TODO
	if (l->exec != NULL) {
		assert(l->inflight == NULL);
		// exec_v2_abort(l->exec, SQLITE_ERROR);
	}
	rc = sqlite3_close(l->conn);
	assert(rc == 0);

	assert(l->db->leaders > 0);
	l->db->leaders--;
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
	struct sqlite3_file *wal = NULL;
	struct raft_buffer buf;
	struct command_checkpoint command;
	sqlite3_int64 size;
	int rv;

	/* Get the database file associated with this connection */
	rv = sqlite3_file_control(l->conn, "main", SQLITE_FCNTL_JOURNAL_POINTER,
				  &wal);
	assert(rv == SQLITE_OK); /* Should never fail */
	if (wal == NULL || wal->pMethods == NULL) {
		/* This might happen at the beginning of the leader life cycle, 
		 * when no pages have been applied yet. */
		return;
	}
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
	rv = raft_apply(l->raft, apply, &buf, 1, leaderCheckpointApplyCb);
	if (rv != 0) {
		tracef("raft_apply failed %d", rv);
		raft_free(apply);
		goto err_after_buf_alloc;
	}

	return;

err_after_buf_alloc:
	raft_free(buf.base);
}

static int leaderApplyFrames(struct exec *req,
			     dqlite_vfs_frame *frames,
			     unsigned n,
			     raft_apply_cb cb)
{
	PRE(req->leader != NULL);

	tracef("leader apply frames");
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

	rv = command__encode(COMMAND_FRAMES, &c, &buf);
	if (rv != 0) {
		tracef("encode %d", rv);
		goto err_after_apply_alloc;
	}

	apply = raft_malloc(sizeof *req);
	if (apply == NULL) {
		tracef("malloc");
		rv = DQLITE_NOMEM;
		goto err;
	}
	apply->leader = req->leader;
	apply->req.data = apply;
	apply->type = COMMAND_FRAMES;

	rv = raft_apply(l->raft, &apply->req, &buf, 1, cb);
	if (rv != 0) {
		tracef("raft apply failed %d", rv);
		goto err_after_apply_alloc;
	}

	l->inflight = apply;

	return 0;

err_after_apply_alloc:
	raft_free(apply);
err_after_command_encode:
	raft_free(buf.base);
err:
	assert(rv != 0);
	return rv;
}

/**
 * State machine for exec requests.
 */
enum {
	EXEC_INITED,

	EXEC_PREPARE_BARRIER,
	EXEC_PREPARED,

	EXEC_ENQUEUED,

	EXEC_RUN_BARRIER,
	EXEC_RUNNING,
	EXEC_POLLED,

	EXEC_DONE,
	EXEC_NR,
};

#define A(ident) BITS(EXEC_##ident)
#define S(ident, allowed_, flags_) \
	[EXEC_##ident] = { .name = #ident, .allowed = (allowed_), .flags = (flags_) }

static const struct sm_conf exec_states[EXEC_NR] = {
	S(INITED,                 A(PREPARE_BARRIER)|A(RUNNING)|A(PREPARED),     SM_INITIAL),
	S(PREPARE_BARRIER,        A(PREPARED)|A(DONE),                           0),
	S(PREPARED,               A(ENQUEUED)|A(RUN_BARRIER)|A(RUNNING)|A(DONE), 0),
	S(ENQUEUED,               A(RUN_BARRIER)|A(RUNNING)|A(DONE),             0),
	S(RUN_BARRIER,            A(RUNNING)|A(DONE),                            0),
	S(RUNNING,                A(POLLED)|A(DONE),                             0),
	S(POLLED,                 A(DONE),                                       0),
	S(DONE,                   0,                                             SM_FAILURE|SM_FINAL),
};

#undef S
#undef A

// FIXME: exec_abort

static void exec_tick(struct exec *req);

inline static void exec_suspend(struct exec *req) {}

void leader_exec_resume(struct exec *req, int status) {
	PRE(req != NULL);
	PRE(
		sm_state(&req->sm) == EXEC_PREPARE_BARRIER ||
		sm_state(&req->sm) == EXEC_ENQUEUED ||
		sm_state(&req->sm) == EXEC_RUN_BARRIER ||
		sm_state(&req->sm) == EXEC_POLLED
	);
	req->status = status;
	return exec_tick(req);
}

static void exec_barrier_cb(struct raft_barrier *barrier, int status)
{
	PRE(barrier != NULL);
	struct exec *req = CONTAINER_OF(barrier, struct exec, barrier);

	return leader_exec_resume(req, status);
}

static void exec_apply_cb(struct raft_apply *req, int status, void *result) {
	(void)result;

	PRE(req != NULL && req->data != NULL);
	struct apply *apply = req->data;

	PRE(apply->leader != NULL);
	PRE(apply->leader->exec != NULL);

	struct leader *leader = apply->leader;
	struct exec *exec = leader->exec;

	raft_free(apply);

	return leader_exec_resume(exec, status);
}

static void exec_timer_cb(struct raft_timer *timer)
{
	PRE(timer != NULL);
	struct exec *req = CONTAINER_OF(timer, struct exec, timer);

	return leader_exec_resume(req, SQLITE_BUSY);
}

static bool exec_db_busy(struct exec *req)
{
	PRE(req != NULL);
	PRE(req->leader != NULL);
	struct leader *l = req->leader;

	return l->db->active_leader != NULL && l->db->active_leader != l;
}

static bool exec_db_full(sqlite3_vfs *vfs, struct db *db, unsigned nframes)
{
	uint64_t size = VfsDatabaseSize(vfs, db->path, nframes, db->config->page_size);
	return size > VfsDatabaseSizeLimit(vfs);
}

static void exec_tick(struct exec *req) {
	// FIXME: req->status should become sm->rc
	PRE(req != NULL);
	PRE(req->leader != NULL);
	PRE(req->leader->db != NULL);
	struct leader *leader = req->leader;
	struct db *db = leader->db;
	// FIXME this must go in struct db*
	sqlite3_vfs *vfs = sqlite3_vfs_find(db->config->name);

	for (;;) {
		switch (sm_state(&req->sm)) {
		case EXEC_INITED:
			PRE(req->status == 0);
			if (req->stmt == NULL) {
				sm_move(&req->sm, EXEC_PREPARE_BARRIER);
				if (needsBarrier(req->leader)) {
					// FIXME: result code should be SQLITE_XXX
					req->status = raft_barrier(req->leader->raft, &req->barrier, exec_barrier_cb);
					if (req->status == 0) {
						return exec_suspend(req);
					}
				}
			} else {
				sm_move(&req->sm, EXEC_PREPARED);
			}
			break;
		case EXEC_PREPARE_BARRIER:
			if (req->status != 0) {
				sm_move(&req->sm, EXEC_DONE);
			} else {
				req->status = sqlite3_prepare_v2(req->leader->conn, 
					req->sql, -1, &req->stmt, &req->sql);
				if (req->stmt == NULL) {
					sm_move(&req->sm, EXEC_DONE);
				} else {
					sm_move(&req->sm, EXEC_PREPARED);
					POST(req->stmt != NULL);
				}
			}
			break;
		case EXEC_PREPARED:
			PRE(req->status == 0);
			PRE(req->stmt != NULL);
			if (req->work_cb == NULL) {
				/* no work callback, we are done */
				sm_move(&req->sm, EXEC_DONE);
			} else if (sqlite3_stmt_readonly(req->stmt)) {
				/* database in in WAL mode, readers can always proceed */
				sm_move(&req->sm, EXEC_RUNNING);
			} else if (!exec_db_busy(req)) {
				sm_move(&req->sm, EXEC_RUNNING);
				req->leader->db->active_leader = req->leader; 
			} else {
				/* supend as another leader is keeping the database busy,
				 * but also start a timer as this statement should not sit
				 * in the queue for too long. In the case the timer expires
				 * the statement will just fail with SQLITE_BUSY. */
				req->status = raft_timer_start(leader->raft, &req->timer,
					db->config->busy_timeout, 0, exec_timer_cb);
				if (req->status != RAFT_RESULT_OK) {
					sm_move(&req->sm, EXEC_DONE);
					/* given that it wasn't possible to wait for the other leader,
					* the only way out is to report to the user that the db is busy.*/
					req->status = SQLITE_BUSY;
				} else {
					sm_move(&req->sm, EXEC_ENQUEUED);
					queue_insert_tail(&db->pending_queue, &req->queue);
					return exec_suspend(req);
				}
			}
			break;
		case EXEC_ENQUEUED:
			if (req->status != 0) {
				sm_move(&req->sm, EXEC_DONE);
				queue_remove(&req->queue);
			} else {
				sm_move(&req->sm, EXEC_RUN_BARRIER);
				if (UNLIKELY(needsBarrier(req->leader))) {
					// FIXME: result code should be SQLITE_XXX
					req->status = raft_barrier(req->leader->raft, &req->barrier, exec_barrier_cb);
					if (req->status == 0) {
						return exec_suspend(req);
					}
				}
			}
			break;
		case EXEC_RUN_BARRIER:
			if (req->status != 0) {
				sm_move(&req->sm, EXEC_DONE);
				queue_remove(&req->queue);
			} else {
				sm_move(&req->sm, EXEC_RUNNING);
				__attribute__((musttail)) return req->work_cb(req);
			}
			break;
		case EXEC_RUNNING:
			if (req->status != 0) {
				sm_move(&req->sm, EXEC_DONE);
			} else {
				sm_move(&req->sm, EXEC_POLLED);
				req->status = VfsPoll(vfs, db->path, &req->frames.ptr, &req->frames.len);
				if (req->status == 0 && req->frames.len > 0) {
					/* Check if the new frames would create an overfull database */
					if (exec_db_full(vfs, db, req->frames.len)) {
						req->status = SQLITE_FULL;
					} else {
						req->status = leaderApplyFrames(req, req->frames.ptr, req->frames.len, exec_apply_cb);
					}
				} else {
					POST(req->frames.len == 0);
					POST(req->frames.ptr == NULL);
				}
			}
			break;
		case EXEC_POLLED:
			if (req->status != 0) {
				VfsAbort(vfs, leader->db->path);
			}

			for (unsigned i = 0; i < req->frames.len; i++) {
				sqlite3_free(req->frames.ptr[i].data);
			}
			sqlite3_free(req->frames.ptr);
			req->frames.ptr = NULL;
			req->frames.len = 0;

			sm_done(&req->sm, EXEC_DONE, EXEC_DONE, req->status);
			
			break;
		case EXEC_DONE:
			sm_fini(&req->sm);
			__attribute__((musttail)) return req->done_cb(req);
		default:
			POST(false && "impossible!");
		}
	}
}

void leader_exec(struct exec *req,
		exec_work_cb work,
		exec_done_cb done)
{
	PRE(req != NULL);
	PRE(req->leader != NULL);
	PRE(req->leader->exec == NULL);
	PRE(work != NULL);
	PRE(done != NULL);
	PRE(req->stmt != NULL ^ req->sql != NULL);

	req->status = 0;
	req->work_cb = work;
	req->done_cb = done;
	sm_init(&req->sm, exec_invariant, NULL, exec_states, "exec",
		EXEC_INITED);

	return exec_tick(req);
}
