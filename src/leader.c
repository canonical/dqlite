#include <sqlite3.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>

#include "../include/dqlite.h"

#include "command.h"
#include "db.h"
#include "leader.h"
#include "lib/queue.h"
#include "lib/sm.h"
#include "raft.h"
#include "tracing.h"
#include "utils.h"
#include "vfs.h"

#if defined(__has_attribute) && __has_attribute (musttail)
# define TAIL __attribute__ ((musttail))
#else
# define TAIL
#endif

/* Whether we need to submit a barrier request because there is no transaction
 * in progress in the underlying database and the FSM is behind the last log
 * index. */
static bool exec_needs_barrier(struct leader *l)
{
	return raft_last_applied(l->raft) < raft_last_index(l->raft);
}

int leader__init(struct leader *l, struct db *db, struct raft *raft)
{
	tracef("leader init");
	int rc;
	sqlite3 *conn;
	rc = db__open(db, &conn);
	if (rc != 0) {
		tracef("open failed %d", rc);
		return rc;
	}

	*l = (struct leader){
		.db = db,
		.conn = conn,
		.raft = raft,
	};
	queue_init(&l->queue);
	db->leaders++;
	return 0;
}

static bool exec_invariant(const struct sm *sm, int prev)
{
	(void)sm;
	(void)prev;
	return true;
}

static void leader_close(struct leader *leader)
{
	PRE(leader->exec == NULL);
	PRE(leader->db->leaders > 0);
	tracef("leader close");
	sqlite3_interrupt(leader->conn);
	int rc = sqlite3_close(leader->conn);
	assert(rc == 0);
	leader->db->active_leader = NULL;
	leader->db->leaders--;
	leader->close_cb(leader);
}

void leader__close(struct leader *leader, leader_close_cb close_cb)
{
	leader->close_cb = close_cb;
	if (leader->exec == NULL) {
		leader_close(leader);
	}
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
static void leaderMaybeCheckpointLegacy(struct leader *leader)
{
	tracef("leader maybe checkpoint legacy");
	struct sqlite3_file *wal = NULL;
	struct raft_buffer buf;
	struct command_checkpoint command;
	sqlite3_int64 size;
	int rv;

	/* Get the database file associated with this connection */
	rv = sqlite3_file_control(leader->conn, "main", SQLITE_FCNTL_JOURNAL_POINTER,
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
	command.filename = leader->db->filename;
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
	rv = raft_apply(leader->raft, apply, &buf, 1, leaderCheckpointApplyCb);
	if (rv != 0) {
		tracef("raft_apply failed %d", rv);
		raft_free(apply);
		goto err_after_buf_alloc;
	}

	return;

err_after_buf_alloc:
	raft_free(buf.base);
}

/**
 * State machine for exec requests.
 */
enum {
	EXEC_INITED,

	EXEC_PREPARE_BARRIER,
	EXEC_PREPARED,

	EXEC_WAITING,

	EXEC_RUN_BARRIER,
	EXEC_RUNNING,

	EXEC_DONE,
	EXEC_NR,
};

#define A(ident) BITS(EXEC_##ident)
#define S(ident, allowed_, flags_) \
	[EXEC_##ident] = { .name = #ident, .allowed = (allowed_), .flags = (flags_) }

static const struct sm_conf exec_states[EXEC_NR] = {
	S(INITED,                 A(PREPARE_BARRIER)|A(RUNNING)|A(PREPARED)|A(DONE),     SM_INITIAL),
	S(PREPARE_BARRIER,        A(PREPARED)|A(DONE),                                   0),
	S(PREPARED,               A(WAITING)|A(RUN_BARRIER)|A(RUNNING)|A(DONE),          0),
	S(WAITING,                A(RUN_BARRIER)|A(RUNNING)|A(DONE),                     0),
	S(RUN_BARRIER,            A(RUNNING)|A(DONE),                                    0),
	S(RUNNING,                A(DONE),                                               0),
	S(DONE,                   0,                                                     SM_FAILURE|SM_FINAL),
};

#undef S
#undef A

static void exec_tick(struct exec *req);
static int  exec_apply(struct exec *req, dqlite_vfs_frame *pages, unsigned n_pages);
static void exec_barrier_cb(struct raft_barrier *barrier, int status);
static void exec_apply_cb(struct raft_apply *req, int status, void *result);
static void exec_timer_cb(struct raft_timer *timer);
static bool exec_db_full(sqlite3_vfs *vfs, struct db *db, unsigned nframes);
static struct exec* exec_dequeue(struct db *db);

inline static void exec_suspend(struct exec *req) { (void)req; }

void leader_exec(struct leader *leader, 
	struct exec *req,
	exec_work_cb work,
	exec_done_cb done)
{
	PRE((req->stmt != NULL) ^ (req->sql != NULL));
	PRE(req != NULL && req->leader == NULL);
	PRE(leader != NULL);
	PRE(done != NULL);

	req->status = 0;
	req->leader = leader;
	req->work_cb = work;
	req->done_cb = done;
	queue_init(&req->queue);
	sm_init(&req->sm, exec_invariant, NULL, exec_states, "exec",
		EXEC_INITED);

	if (leader->exec != NULL) {
		/* This can only happen if a new exec is issued during a done
		* callback. If the exec statements are part of a transaction 
		* then the only way to proceed is to exec other queries from
		* the same leader until it releases the lock. This means that
		* it is safer to put this query at the beginning of the queue
		* (give this query the precedence) and that it is not necessary
		* to start the timer as a query is about to finish already. */
		queue_insert_head(&leader->db->pending_queue, &req->queue);
		return exec_suspend(req);
	} else {
		return exec_tick(req);
	}
}

void leader_exec_abort(struct exec *req)
{
	PRE(req->status == 0);

	switch (sm_state(&req->sm)) {
	case EXEC_DONE: /* already done */
		return;
	case EXEC_WAITING:
		raft_timer_stop(req->leader->raft, &req->timer);
		queue_remove(&req->queue);
		TAIL return leader_exec_resume(req);
	case EXEC_RUNNING:
		// FIXME: could be a datarace in case off-the-main loop execution
		sqlite3_interrupt(req->leader->conn);
		break;
	}

	leader_exec_result(req, RAFT_CANCELED);
}

void leader_exec_result(struct exec *req, int status)
{
	PRE(req != NULL);

	/* This sets the result to status only if status was an error.
	 * This is part of the best effort cancellation logic: if an
	 * unstoppable request fails, it's better to keep the error
	 * returned from that request.
	 * However, if the request succeeded, but a cancellation was attempted
	 * then we clearly cannot override the error code, as otherwise
	 * the state machine will keep going. */
	if (status != 0) {
		req->status = status;
	}
}

void leader_exec_resume(struct exec *req)
{
	TAIL return exec_tick(req);
}

static int exec_apply(struct exec *req, dqlite_vfs_frame *frames, unsigned nframes)
{
	tracef("leader apply frames");
	PRE(req != NULL);
	PRE(frames != NULL);
	PRE(nframes > 0);

	struct leader *leader = req->leader;
	struct db *db = leader->db;
	struct raft_buffer buf;

	if (exec_db_full(req->leader->db->vfs, req->leader->db, nframes)) {
		return SQLITE_FULL;
	}

	const struct command_frames c = {
		.filename = db->filename,
		.tx_id = 0,
		.truncate = 0,
		.is_commit = 1,
		.frames = {
			.n_pages = (uint32_t)nframes,
			.page_size = (uint16_t)db->config->page_size,
			.data = frames,
		}
	};
	int rv = command__encode(COMMAND_FRAMES, &c, &buf);
	if (rv != 0) {
		tracef("encode %d", rv);
		return rv;
	}

	rv = raft_apply(leader->raft, &req->apply, &buf, 1, exec_apply_cb);
	if (rv != 0) {
		tracef("raft apply failed %d", rv);
		raft_free(buf.base);
		return rv;
	}

	return 0;
}

static struct exec* exec_dequeue(struct db *db) {
	queue *item;
	QUEUE_FOREACH(item, &db->pending_queue)
	{
		struct exec *exec = QUEUE_DATA(item, struct exec, queue);
		if (db->active_leader == NULL || db->active_leader == exec->leader) {
			return exec;
		}
	}
	return NULL;
}

static void exec_tick(struct exec *req)
{
	PRE(req != NULL);
	PRE(req->leader != NULL && req->leader->db != NULL);
	struct leader *leader = req->leader;
	struct db *db = leader->db;

	for (;;) {
		switch (sm_state(&req->sm)) {
		case EXEC_INITED:
			PRE(req->status == 0);
			PRE(leader->exec == NULL);
			leader->exec = req;
			if (leader->close_cb != NULL) {
				/* Close requested. Short-circuit to EXEC_DONE */
				sm_move(&req->sm, EXEC_DONE);
				req->status = RAFT_CANCELED;
			} else if (req->stmt == NULL) {
				sm_move(&req->sm, EXEC_PREPARE_BARRIER);
				if (exec_needs_barrier(leader)) {
					req->status = raft_barrier(leader->raft, &req->barrier, exec_barrier_cb);
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
				int rc = sqlite3_prepare_v2(leader->conn, 
					req->sql, -1, &req->stmt, &req->tail);
				if (rc != 0) {
					sm_move(&req->sm, EXEC_DONE);
					req->status = RAFT_ERROR;
				} else if (req->stmt == NULL) {
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
				sm_move(&req->sm, EXEC_WAITING);
			} else if (db->active_leader == NULL || db->active_leader == leader) {
				sm_move(&req->sm, EXEC_WAITING);
				db->active_leader = leader;
			} else {
				/* Supend as another leader is keeping the database busy,
				 * but also start a timer as this statement should not sit
				 * in the queue for too long. In the case the timer expires
				 * the statement will just fail with RAFT_BUSY. */
				req->status = raft_timer_start(leader->raft, &req->timer,
					db->config->busy_timeout, 0, exec_timer_cb);
				if (req->status != RAFT_OK) {
					sm_move(&req->sm, EXEC_DONE);
				} else {
					sm_move(&req->sm, EXEC_WAITING);
					queue_insert_tail(&db->pending_queue, &req->queue);
					return exec_suspend(req);
				}
			}
			break;
		case EXEC_WAITING:
			raft_timer_stop(leader->raft, &req->timer);
			queue_remove(&req->queue);
			if (req->status != 0) {
				sm_move(&req->sm, EXEC_DONE);
			} else {
				sm_move(&req->sm, EXEC_RUN_BARRIER);
				if (exec_needs_barrier(leader)) {
					req->status = raft_barrier(leader->raft, &req->barrier, exec_barrier_cb);
					if (req->status == 0) {
						return exec_suspend(req);
					}
				}
			}
			break;
		case EXEC_RUN_BARRIER:
			if (req->status != 0) {
				sm_move(&req->sm, EXEC_DONE);
			} else {
				sm_move(&req->sm, EXEC_RUNNING);
				TAIL return req->work_cb(req);
			}
			break;
		case EXEC_RUNNING:
			sm_move(&req->sm, EXEC_DONE);
			if (req->status == RAFT_OK) {
				dqlite_vfs_frame *frames;
				unsigned int nframes;
				/* 
				 * FIXME: If this was a xFileControl:
				 *  - it would be callable through sqlite3_file_control
				 *  - it would set the error for the connection (so, no translation needed here)
				 *  - it would not be necessary to keep a vfs pointer in the db
				 *  - it would not necessary to lookup the database by path every time.
				 */
				int rc = VfsPoll(db->vfs, db->path, &frames, &nframes);
				if (rc == SQLITE_OK && nframes > 0) {
					req->status = exec_apply(req, frames, nframes);
					for (unsigned i = 0; i < nframes; i++) {
						sqlite3_free(frames[i].data);
					}
					sqlite3_free(frames);
					if (req->status == 0) {
						return exec_suspend(req);
					}
				} else if (rc != SQLITE_OK) {
					req->status = RAFT_IOERR;
				}
			}
			break;
		case EXEC_DONE: 
			sm_fini(&req->sm);
			req->leader = NULL;
			req->done_cb(req);
			leader->exec = NULL;
			
			if (db->active_leader == leader) {
				if (sqlite3_txn_state(leader->conn, NULL) != SQLITE_TXN_WRITE) {
					db->active_leader = NULL;
				}
			} else {
				/* It should be impossible to run write transactions without 
				 * keeping the leader busy. */
				POST(sqlite3_txn_state(leader->conn, NULL) != SQLITE_TXN_WRITE);
			}

			req = exec_dequeue(db);

			if (req == NULL) {
				if (leader->close_cb != NULL) {
					leader_close(leader);
				}
				return;
			}

			queue_remove(&req->queue);
			
			if (leader->close_cb != NULL && req->leader != leader) {
				/* Close only if another request for the same leader wasn't
				 * enqueued during the done callback. Otherwise, the leader
				 * will wait a little bit more before closing. */
				leader_close(leader);
			}

			TAIL return exec_tick(req);
		default:
			POST(false && "impossible!");
		}
	}
}

static void exec_barrier_cb(struct raft_barrier *barrier, int status)
{
	PRE(barrier != NULL);
	struct exec *req = CONTAINER_OF(barrier, struct exec, barrier);

	leader_exec_result(req, status); // FIXME(marco6): check if status can only be RAFT_*
	return leader_exec_resume(req);
}

static void exec_timer_cb(struct raft_timer *timer)
{
	PRE(timer != NULL);
	struct exec *req = CONTAINER_OF(timer, struct exec, timer);

	leader_exec_result(req, RAFT_BUSY);
	return leader_exec_resume(req);
}

static void exec_apply_cb(struct raft_apply *apply, int status, void *result)
{
	(void)result;
	struct exec *exec = CONTAINER_OF(apply, struct exec, apply);
	struct leader *leader = exec->leader;
	if (leader) {
		if (status != 0) {
			VfsAbort(leader->db->vfs, leader->db->path);
		} else {
			leaderMaybeCheckpointLegacy(leader);
		}
	}

	// FIXME(marco6) inspect how to always return RAFT_* from this
	leader_exec_result(exec, status);
	return leader_exec_resume(exec);
}

static bool exec_db_full(sqlite3_vfs *vfs, struct db *db, unsigned nframes)
{
	uint64_t size = VfsDatabaseSize(vfs, db->path, nframes, db->config->page_size);
	return size > VfsDatabaseSizeLimit(vfs);
}
