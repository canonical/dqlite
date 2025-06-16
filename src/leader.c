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

#define leader_trace(L, fmt, ...) tracef("[leader %p] "fmt, L, ##__VA_ARGS__)

static bool exec_invariant(const struct sm *sm, int prev);
static void exec_tick(struct exec *req);
static int exec_apply(struct exec *req,
		      const struct vfsTransaction *transaction);
static void exec_prepare_barrier_cb(struct raft_barrier *barrier, int status);
static void exec_run_barrier_cb(struct raft_barrier *barrier, int status);
static void exec_apply_cb(struct raft_apply *req, int status, void *result);
static void exec_timer_cb(struct raft_timer *timer);
static bool is_db_full(sqlite3_vfs *vfs, struct db *db, unsigned nframes);

static struct exec *exec_dequeue(struct db *db);
static void exec_enqueue(struct db *db, struct exec *exec);

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
static inline bool leader_closing(struct leader *leader)
{
	return leader->close_cb != NULL;
}

static void leader_finalize(struct leader *leader)
{
	PRE(leader->exec == NULL && leader->pending == 0);
	PRE(leader->db->leaders > 0);
	tracef("leader close");
	sqlite3_interrupt(leader->conn);
	int rc = sqlite3_close_v2(leader->conn);
	assert(rc == 0);
	if (leader->db->active_leader == leader) {
		leader_trace(leader, "done");
		leader->db->active_leader = NULL;
	}
	leader->db->leaders--;
	leader->close_cb(leader);
}

void leader__close(struct leader *leader, leader_close_cb close_cb)
{
	leader->close_cb = close_cb;
	if (leader->pending == 0) {
		struct db *db = leader->db;
		leader_finalize(leader);

		struct exec *req = exec_dequeue(db);
		if (req == NULL) {
			return;
		}

		PRE(IN(db->active_leader, NULL, req->leader));
		db->active_leader = req->leader;
		return exec_tick(req);
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
 *
 * ┌───────── EXEC_INITED
 * │                │
 * │ stmt != NULL   │stmt == NULL
 * │                ▼
 * │      EXEC_PREPARE_BARRIER
 * │                │
 * │                ▼
 * └───────► EXEC_PREPARED ────────────┐
 *                  │                  │
 *                  │work_cb != NULL   │work_cb == NULL
 *                  ▼                  │
 *         EXEC_WAITING_QUEUE          │
 *                  │                  │
 *                  ▼                  │
 *          EXEC_RUN_BARRIER           │
 *                  │                  │
 *                  ▼                  │
 * ┌────────── EXEC_RUNNING            │
 * │                │                  │
 * │VfsPoll == 0    │VfsPoll > 0       │
 * │                ▼                  │
 * │        EXEC_WAITING_APPLY         │
 * │                │                  │
 * │                ▼                  │
 * └──────────► EXEC_DONE ◄────────────┘
 *
 * All states can also reach `EXEC_DONE` in case of an error.
 * The state machine is suspended in the following states:
 *  - EXEC_PREPARE_BARRIER: if exec_needs_barrier returns true
 *  - EXEC_WAITING_QUEUE: if the statement is not readonly and the db is busy
 *    with another leader
 *  - EXEC_RUN_BARRIER: if exec_needs_barrier returns true; this is necessary
 *    as time might have passed since the request was added to the queue
 *  - EXEC_WAITING_APPLY: always suspended during the raft apply
 */
enum {
	EXEC_INITED,

	EXEC_PREPARE_BARRIER,
	EXEC_PREPARED,

	EXEC_WAITING_QUEUE,

	EXEC_RUN_BARRIER,
	EXEC_RUNNING,
	EXEC_WAITING_APPLY,

	EXEC_DONE,
	EXEC_NR,
};

static const char* exec_state_name(int state) {
	switch (state) {
	case EXEC_INITED:          return "EXEC_INITED";
	case EXEC_PREPARE_BARRIER: return "EXEC_PREPARE_BARRIER";
	case EXEC_PREPARED:        return "EXEC_PREPARED";
	case EXEC_WAITING_QUEUE:   return "EXEC_WAITING_QUEUE";
	case EXEC_RUN_BARRIER:     return "EXEC_RUN_BARRIER";
	case EXEC_RUNNING:         return "EXEC_RUNNING";
	case EXEC_WAITING_APPLY:   return "EXEC_WAITING_APPLY";
	case EXEC_DONE:            return "EXEC_DONE";
	default:                   return "<invalid>";
	}
}

#define A(ident) BITS(EXEC_##ident)
#define S(ident, allowed_, flags_) \
	[EXEC_##ident] = { .name = #ident, .allowed = (allowed_), .flags = (flags_) }

static const struct sm_conf exec_states[EXEC_NR] = {
	S(INITED,                 A(PREPARE_BARRIER)|A(RUNNING)|A(PREPARED)|A(DONE),     SM_INITIAL),
	S(PREPARE_BARRIER,        A(PREPARED)|A(DONE),                                   0),
	S(PREPARED,               A(WAITING_QUEUE)|A(RUN_BARRIER)|A(RUNNING)|A(DONE),    0),
	S(WAITING_QUEUE,          A(RUN_BARRIER)|A(RUNNING)|A(DONE),                     0),
	S(RUN_BARRIER,            A(RUNNING)|A(DONE),                                    0),
	S(RUNNING,                A(WAITING_APPLY)|A(DONE),                              0),
	S(WAITING_APPLY,          A(DONE),                                               0),
	S(DONE,                   0,                                                     SM_FAILURE|SM_FINAL),
};

#undef S
#undef A

#define suspend return

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
	
	bool should_suspend = leader->pending > 0;
	leader->pending++;
	if (should_suspend) {
		/* When dealing with EXEC_SQL and QUERY_SQL requests that have
		 * multiple statements like `BEGIN IMMEDIATE; ROLLBACK`, the
		 * gateway will issue a new exec request for the next statement
		 * during the done callback. If the exec statements are part of
		 * a transaction then the only way to proceed is to exec other
		 * queries from the same leader until it releases the lock. This
		 * means that it is not necessary to start the timer as a query
		 * is about to finish already. */
		return exec_enqueue(leader->db, req);
	} else {
		return exec_tick(req);
	}
}

void leader_exec_abort(struct exec *req)
{
	leader_trace(req->leader, "abort in state %s", exec_state_name(sm_state(&req->sm)));

	switch (sm_state(&req->sm)) {
	case EXEC_DONE: /* already done */
		return;
	case EXEC_RUNNING:
		/* best-effort: there is no guarantee that this will interrupt the query */
		sqlite3_interrupt(req->leader->conn);
		return;
	case EXEC_WAITING_QUEUE:
		/* timers are cancellable, so the request can move on directly. */
		leader_exec_result(req, RAFT_CANCELED);
		TAIL return exec_tick(req);
	}

	/* Raft-related requests canno be cancelled, so the only step that can be taken
	 * is to mark the request as failed and wait for the callback */
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
	PRE(sm_state(&req->sm) == EXEC_RUNNING);
	TAIL return exec_tick(req);
}

static int exec_apply(struct exec *req, const struct vfsTransaction *transaction)
{
	tracef("leader apply frames");
	PRE(req != NULL);
	PRE(transaction->n_pages > 0);
	PRE(transaction->page_numbers != NULL);
	PRE(transaction->pages != NULL);

	struct leader *leader = req->leader;
	struct db *db = leader->db;
	struct raft_buffer buf;

	if (is_db_full(req->leader->db->vfs, req->leader->db, transaction->n_pages)) {
		return SQLITE_FULL;
	}

	const struct command_frames c = {
		.filename = db->filename,
		.tx_id = 0,
		.truncate = 0,
		.is_commit = 1,
		.frames = {
			.n_pages = (uint32_t)transaction->n_pages,
			.page_size = (uint16_t)db->config->page_size,
			.page_numbers = transaction->page_numbers,
			.pages = transaction->pages,
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

static void exec_enqueue(struct db *db, struct exec *req)
{
	if (db->active_leader == req->leader) {
		/* make sure requests from the active leader always come
		 * first as they are the only ones that can proceed. */
		queue_insert_head(&db->pending_queue, &req->queue);
	} else {
		queue_insert_tail(&db->pending_queue, &req->queue);
	}
}

/* exec_dequeue dequeues an executable request from the pending
 * queue of db. A request is considered executable if:
 *  - no leader is holding the database busy;
 *  - the request comes from the leader holding the database busy.*/
static struct exec *exec_dequeue(struct db *db)
{
	if (queue_empty(&db->pending_queue)) {
		return NULL;
	}

	queue *item = queue_head(&db->pending_queue);
	struct exec *req = QUEUE_DATA(item, struct exec, queue);
	if (db->active_leader == NULL || db->active_leader == req->leader) {
		queue_remove(&req->queue);
		queue_init(&req->queue);
		leader_trace(req->leader, "dequeued");
		return req;
	}
	return NULL;
}

static bool exec_invariant(const struct sm *sm, int prev)
{
	(void)prev;
	struct exec *req = CONTAINER_OF(sm, struct exec, sm);

	/* Ensure that only one write request can run at any point of time.
	 * This can be checked by making sure that no progress happen while
	 * enqueued. */
	if (prev != sm_state(sm) && sm_state(sm) != EXEC_WAITING_QUEUE) {
		return CHECK(queue_empty(&req->queue));
	}

	if (sm_state(sm) == EXEC_INITED) {
		return CHECK((req->stmt != NULL) ^ (req->sql != NULL)) &&
		       CHECK(req->status == 0);
	}

	if (IN(sm_state(sm), EXEC_WAITING_QUEUE, EXEC_RUN_BARRIER, EXEC_RUNNING, EXEC_WAITING_APPLY)) {
		return CHECK(req->stmt != NULL);
	}
	
	return true;
}

static void exec_tick(struct exec *req)
{
	PRE(req != NULL);
	PRE(req->leader != NULL && req->leader->db != NULL);
	struct leader *leader = req->leader;
	struct db *db = leader->db;
	struct vfsTransaction transaction;

	for (;;) {
		leader_trace(leader, "exec tick %s (status = %d)",
			     exec_state_name(sm_state(&req->sm)), req->status);
		switch (sm_state(&req->sm)) {
		case EXEC_INITED:
			PRE(leader->exec == NULL);
			leader->exec = req;
			if (leader_closing(leader)) {
				/* Close requested. Short-circuit to EXEC_DONE */
				req->status = RAFT_CANCELED;
				sm_move(&req->sm, EXEC_DONE);
				continue;
			}

			if (req->stmt != NULL) {
				sm_move(&req->sm, EXEC_PREPARED);
				continue;
			}

			if (!exec_needs_barrier(leader)) {
				sm_move(&req->sm, EXEC_PREPARE_BARRIER);
				continue;
			}

			req->status = raft_barrier(leader->raft, &req->barrier, exec_prepare_barrier_cb);
			if (req->status != 0) {
				leader_trace(leader, "barrier failed (status = %d)", req->status);
				sm_move(&req->sm, EXEC_DONE);
				continue;
			}

			leader_trace(leader, "prepare barrier requested");
			sm_move(&req->sm, EXEC_PREPARE_BARRIER);
			suspend;
		case EXEC_PREPARE_BARRIER:
			if (req->status != 0) {
				sm_move(&req->sm, EXEC_DONE);
				continue;
			}

			req->status = sqlite3_prepare_v2(
			    leader->conn, req->sql, -1, &req->stmt, &req->tail);
			if (req->status != 0) {
				req->status = RAFT_ERROR;
				sm_move(&req->sm, EXEC_DONE);
			} else if (req->stmt == NULL) {
				sm_move(&req->sm, EXEC_DONE);
			} else {
				sm_move(&req->sm, EXEC_PREPARED);
			}
			continue;
		case EXEC_PREPARED:
			PRE(req->status == 0);
			if (req->work_cb == NULL) {
				/* no work callback, we are done */
				sm_move(&req->sm, EXEC_DONE);
				continue;
			}
			
			if (sqlite3_stmt_readonly(req->stmt)) {
				/* database in in WAL mode, readers can always proceed */
				sm_move(&req->sm, EXEC_WAITING_QUEUE);
				continue;
			}

			if (IN(db->active_leader, NULL, leader)) {
				db->active_leader = leader;
				leader_trace(leader, "active leader = %p", leader);
				sm_move(&req->sm, EXEC_WAITING_QUEUE);
				continue;
			}

			/* Supend as another leader is keeping the database
			 * busy, but also start a timer as this statement should
			 * not sit in the queue for too long. In the case the
			 * timer expires the statement will just fail with
			 * RAFT_BUSY. */
			req->status = raft_timer_start(
			    leader->raft, &req->timer, db->config->busy_timeout,
			    0, exec_timer_cb);
			if (req->status != RAFT_OK) {
				sm_move(&req->sm, EXEC_DONE);
				continue;
			}
			exec_enqueue(db, req);
			sm_move(&req->sm, EXEC_WAITING_QUEUE);
			suspend;
		case EXEC_WAITING_QUEUE:
			raft_timer_stop(leader->raft, &req->timer);
			queue_remove(&req->queue);
			queue_init(&req->queue);
			if (req->status != 0) {
				sm_move(&req->sm, EXEC_DONE);
				continue;
			}

			if (!exec_needs_barrier(leader)) {
				sm_move(&req->sm, EXEC_RUN_BARRIER);
				continue;
			}

			req->status = raft_barrier(leader->raft, &req->barrier, exec_run_barrier_cb);
			if (req->status != 0) {
				leader_trace(leader, "barrier failed (status = %d)", req->status);
				sm_move(&req->sm, EXEC_DONE);
				continue;
			}

			leader_trace(leader, "requested barrier");
			sm_move(&req->sm, EXEC_RUN_BARRIER);
			suspend;
		case EXEC_RUN_BARRIER:
			if (req->status != 0) {
				sm_move(&req->sm, EXEC_DONE);
				continue;
			}

			leader_trace(leader, "executing query");
			sm_move(&req->sm, EXEC_RUNNING);
			TAIL return req->work_cb(req);
		case EXEC_RUNNING: /* -> EXEC_DONE */
			leader_trace(leader, "executed query on leader (status=%d)", req->status);
			if (req->status != RAFT_OK) {
				sm_move(&req->sm, EXEC_DONE);
				continue;
			}

			/* 
			 * FIXME: If this was a xFileControl:
			 *  - it would be callable through sqlite3_file_control
			 *  - it would set the error for the connection (so, no translation needed here)
			 *  - it would not be necessary to keep a vfs pointer in the db
			 *  - it would not necessary to lookup the database by path every time.
			 */
			int rc = VfsPoll(db->vfs, db->path, &transaction);
			if (rc != SQLITE_OK) {
				leader_trace(leader, "poll failed on leader");
				rc = VfsAbort(leader->db->vfs, leader->db->path);
				assert(rc == SQLITE_OK);
				req->status = RAFT_IOERR;
				sm_move(&req->sm, EXEC_DONE);
				continue;
			}

			leader_trace(leader, "polled connection (%d frames)", transaction.n_pages);
			if (transaction.n_pages == 0) {
				sm_move(&req->sm, EXEC_DONE);
				continue;
			}

			req->status = exec_apply(req, &transaction);
			for (unsigned i = 0; i < transaction.n_pages; i++) {
				sqlite3_free(transaction.pages[i]);
			}
			sqlite3_free(transaction.pages);
			sqlite3_free(transaction.page_numbers);

			if (req->status != 0) {
				sm_move(&req->sm, EXEC_DONE);
				continue;
			}
			sm_move(&req->sm, EXEC_WAITING_APPLY);
			suspend;
		case EXEC_WAITING_APPLY:
			sm_move(&req->sm, EXEC_DONE);
			continue;
		case EXEC_DONE: 
			sm_fini(&req->sm);
			req->leader = NULL;
			req->done_cb(req);

			/* From here on `req` should never be accessed as the `done_cb` might have
			 * released its memory or reused for another request. */
			leader->exec = NULL;
			leader->pending--;
			
			if (db->active_leader == leader) {
				if (sqlite3_txn_state(leader->conn, NULL) != SQLITE_TXN_WRITE) {
					leader_trace(leader, "done");
					db->active_leader = NULL;
				} else {
					leader_trace(leader, "transaction open");
				}
			} else {
				/* It should be impossible to run write transactions without 
				 * keeping the leader busy. */
				POST(sqlite3_txn_state(leader->conn, NULL) != SQLITE_TXN_WRITE);
			}

			if (leader_closing(leader) && leader->pending == 0) {
				leader_finalize(leader);
			}

			req = exec_dequeue(db);
			if (req != NULL) {
				PRE(IN(db->active_leader, NULL, req->leader));
				db->active_leader = req->leader;
				TAIL return exec_tick(req);
			}
			return;
		default:
			IMPOSSIBLE("unknown state");
		}
	}
}

static inline void exec_barrier_cb(struct raft_barrier *barrier, int status, int event)
{
	struct exec *req = CONTAINER_OF(barrier, struct exec, barrier);

	PRE(sm_state(&req->sm) == event);
	leader_exec_result(req, status);
	return exec_tick(req);
}

static void exec_prepare_barrier_cb(struct raft_barrier *barrier, int status)
{
	return exec_barrier_cb(barrier, status, EXEC_PREPARE_BARRIER);
}

static void exec_run_barrier_cb(struct raft_barrier *barrier, int status)
{
	return exec_barrier_cb(barrier, status, EXEC_RUN_BARRIER);
}

static void exec_timer_cb(struct raft_timer *timer)
{
	struct exec *req = CONTAINER_OF(timer, struct exec, timer);

	PRE(sm_state(&req->sm) == EXEC_WAITING_QUEUE);
	leader_exec_result(req, RAFT_BUSY);
	return exec_tick(req);
}

static void exec_apply_cb(struct raft_apply *apply, int status, void *result)
{
	(void)result;
	struct exec *req = CONTAINER_OF(apply, struct exec, apply);
	struct leader *leader = req->leader;
	leader_trace(leader, "query applied (status=%d)", status);
	if (leader) {
		if (status != 0) {
			VfsAbort(leader->db->vfs, leader->db->path);
		} else {
			leaderMaybeCheckpointLegacy(leader);
		}
	}

	PRE(sm_state(&req->sm) == EXEC_WAITING_APPLY);
	// FIXME(marco6): inspect how to always return RAFT_* from this
	leader_exec_result(req, status);
	return exec_tick(req);
}

static bool is_db_full(sqlite3_vfs *vfs, struct db *db, unsigned nframes)
{
	uint64_t size = VfsDatabaseSize(vfs, db->path, nframes, db->config->page_size);
	return size > VfsDatabaseSizeLimit(vfs);
}
