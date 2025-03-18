#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

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

static bool barrier_invariant(const struct sm *sm, int prev)
{
	(void)sm;
	(void)prev;
	return true;
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

/**
 * State machine for exec requests.
 */
enum {
	EXEC_WAITING,
	EXEC_START,
	EXEC_BARRIER,
	EXEC_STEPPED,
	EXEC_POLLED,
	EXEC_APPLIED,
	EXEC_DONE,
	EXEC_FAILED,
	EXEC_NR,
};

#define A(ident) BITS(EXEC_##ident)
#define S(ident, allowed_, flags_) \
	[EXEC_##ident] = { .name = #ident, .allowed = (allowed_), .flags = (flags_) }

static const struct sm_conf exec_states[EXEC_NR] = {
	S(WAITING,  A(START)|A(FAILED)|A(DONE),   SM_INITIAL),
	S(START,    A(BARRIER)|A(FAILED)|A(DONE), 0),
	S(BARRIER,  A(STEPPED)|A(FAILED)|A(DONE), 0),
	S(STEPPED,  A(POLLED)|A(FAILED)|A(DONE),  0),
	S(POLLED,   A(APPLIED)|A(FAILED)|A(DONE), 0),
	S(APPLIED,  A(FAILED)|A(DONE),            0),
	S(DONE,     0,                            SM_FINAL),
	S(FAILED,   0,                            SM_FAILURE|SM_FINAL),
};

#undef S
#undef A

static bool exec_invariant(const struct sm *sm, int prev)
{
	(void)sm;
	(void)prev;
	return true;
}

static void exec_tick(struct exec *, int);
static void exec_suspend(struct exec *);
static void exec_done(struct exec *);
static void exec_abort(struct exec* req, int);

void leader__close(struct leader *l)
{
	tracef("leader close");
	int rc;
	if (l->exec != NULL) {
		assert(l->inflight == NULL);
		exec_abort(l->exec, SQLITE_ERROR);
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

	rv = raft_apply(l->raft, &apply->req, &buf, 1, cb);
	if (rv != 0) {
		tracef("raft apply failed %d", rv);
		goto err_after_command_encode;
	}

	l->inflight = apply;

	return 0;

err_after_command_encode:
	raft_free(buf.base);
err_after_apply_alloc:
	raft_free(apply);
err:
	assert(rv != 0);
	return rv;
}

enum {
	BARRIER_START,
	BARRIER_PASSED,
	BARRIER_DONE,
	BARRIER_FAIL,
	BARRIER_NR,
};

#define A(ident) BITS(BARRIER_##ident)
#define S(ident, allowed_, flags_) \
	[BARRIER_##ident] = { .name = #ident, .allowed = (allowed_), .flags = (flags_) }

static const struct sm_conf barrier_states[BARRIER_NR] = {
	S(START,  A(PASSED)|A(DONE)|A(FAIL), SM_INITIAL),
	S(PASSED, A(DONE)|A(FAIL),           0),
	S(DONE,   0,                         SM_FINAL),
	S(FAIL,   0,                         SM_FINAL|SM_FAILURE),
};

#undef S
#undef A

static void barrier_done(struct barrier *barrier, int status)
{
	PRE(barrier != NULL);
	int state = sm_state(&barrier->sm);
	PRE(state == BARRIER_START || state == BARRIER_PASSED);
	void (*cb)(struct barrier *, int) = barrier->cb;
	PRE(cb != NULL);

	if (status != 0) {
		status = translateRaftErrCode(status);
		sm_fail(&barrier->sm, BARRIER_FAIL, status);
	} else {
		sm_move(&barrier->sm, BARRIER_DONE);
	}
	sm_fini(&barrier->sm);
	/* TODO(cole) uncommment this once the barrier-callback-runs-twice
	 * issue is fixed. */
	/* barrier->req.data = NULL; */
	barrier->leader = NULL;
	barrier->cb = NULL;

	cb(barrier, status);
}

static void barrier_raft_cb(struct raft_barrier *, int);

static void barrier_tick(struct barrier *barrier, int status)
{
	switch (sm_state(&barrier->sm)) {
	case BARRIER_START:
		PRE(status == 0);
		if (needsBarrier(barrier->leader)) {
			status = raft_barrier(barrier->leader->raft, &barrier->req, barrier_raft_cb);
			if (status != 0) {
				return barrier_done(barrier, status);
			}
			return /* suspend */;
		}
		sm_move(&barrier->sm, BARRIER_PASSED);
		__attribute__((fallthrough));
	case BARRIER_PASSED:
		return barrier_done(barrier, status);
	default:
		POST(false && "impossible!");
	}
}

static void barrier_raft_cb(struct raft_barrier *rb, int status)
{
	struct barrier *barrier = rb->data;
	PRE(barrier != NULL);
	/* TODO(marco6) it seems that raft can invoke this callback more than
	 * once, investigate and fix that and then remove this workaround. */
	if (sm_state(&barrier->sm) > BARRIER_START) {
		return;
	}
	sm_move(&barrier->sm, BARRIER_PASSED);
	return barrier_tick(rb->data, status);
}

int leader_barrier_v2(struct leader *l,
		      struct barrier *barrier,
		      barrier_cb cb)
{
	barrier->cb = cb;
	barrier->leader = l;
	barrier->req.data = barrier;
	sm_init(&barrier->sm, barrier_invariant, NULL, barrier_states, "barrier",
		BARRIER_START);
	barrier_tick(barrier, 0);
	return 0;
}

static void exec_abort(struct exec* req, int status) {
	req->status = status;
	req->defer_cb = false;
	return exec_done(req);
}

static void exec_callback_cb(struct raft_timer *timer) {
	struct exec *req = timer->data;
	PRE(req != NULL);

	return exec_done(req);
}

static void exec_done(struct exec *req)
{
	PRE(req->leader != NULL);
	PRE(req->leader->db != NULL);
	struct leader *l = req->leader;
	struct db *db = l->db;

	/* if the pseudo-coroutine never suspended and the callback was executed
	 * directly it would (possibly) create long call chains on the stack which
	 * might contribute to hight stack usage and eventually cause a stack overflow. 
	 * To break this chain it is enough to schedule a callback on the main loop and
	 * suspend the current pseudo-coroutine.
	 * See @handle_exec_sql_next for a pattern which might create problems. */
	if (req->defer_cb) {
		int err = raft_timer_start(l->raft, &req->timer, 0, 0, exec_callback_cb);
		if (err == RAFT_RESULT_OK) {
			return exec_suspend(req);
		}
		/* arriving here means that it is not possible to suspend. The only way out is
		 * to run synchronously and hope that the stack is big enough to hold calls
		 * until an event triggers asyncronicity. It should never happen with a recent
		 * raft_io version. */
		POST(l->raft->io->version < 3);
	} else {
		/* make sure no other event can fire after done is called. */
		raft_timer_stop(l->raft, &req->timer);
	}

	int status = req->status;
	status = status ? status : SQLITE_ERROR;
	if (status == SQLITE_DONE) {
		sm_move(&req->sm, EXEC_DONE);
	} else {
		sm_fail(&req->sm, EXEC_FAILED, status);
	}
	sm_fini(&req->sm);

	bool is_active = db->active_leader == l;
	bool is_busy = sqlite3_txn_state(l->conn, NULL) == SQLITE_TXN_WRITE;

	/* it should never be possible to be in a write transaction 
	 * without being the active leader. */
	POST(!(is_busy && !is_active));
	if (!is_busy && is_active) {
		db->active_leader = NULL;
	}
	l->exec = NULL;
	req->cb(req, status);

	if (db->active_leader != NULL || queue_empty(&db->pending_queue)) {
		return;
	}

	/* The database is not busy now, so it is possible to let the next
	 * pending request run. */
	struct queue *pending = queue_head(&db->pending_queue);
	struct exec *pending_req = QUEUE_DATA(pending, struct exec, queue);
	queue_remove(pending);
	sm_move(&pending_req->sm, EXEC_START);
	exec_tick(pending_req, 0);
}

static void exec_apply_cb(struct raft_apply *, int, void *);

static int exec_apply(struct exec *req)
{
	struct leader *l = req->leader;
	struct db *db = l->db;
	sqlite3_vfs *vfs = sqlite3_vfs_find(db->config->name);
	dqlite_vfs_frame *frames;
	uint64_t size;
	unsigned n;
	unsigned i;
	int rv;

	req->status = sqlite3_step(req->stmt);
	sm_move(&req->sm, EXEC_STEPPED);

	rv = VfsPoll(vfs, db->path, &frames, &n);
	if (rv != 0) {
		goto finish;
	}
	sm_move(&req->sm, EXEC_POLLED);
	if (n == 0) {
		sm_move(&req->sm, EXEC_APPLIED);
		exec_tick(req, 0);
		return 0;
	}

	/* Check if the new frames would create an overfull database */
	size = VfsDatabaseSize(vfs, db->path, n, db->config->page_size);
	if (size > VfsDatabaseSizeLimit(vfs)) {
		rv = SQLITE_FULL;
		goto finish;
	}

	rv = leaderApplyFrames(req, frames, n, exec_apply_cb);
	if (rv != 0) {
		goto finish;
	}

finish:
	for (i = 0; i < n; i++) {
		sqlite3_free(frames[i].data);
	}
	sqlite3_free(frames);
	if (rv != 0) {
		VfsAbort(vfs, l->db->path);
	}
	return rv;
}

static void exec_apply_cb(struct raft_apply *req,
			  int status,
			  void *result)
{
	(void)result;
	struct apply *apply = req->data;
	struct leader *l;
	struct exec *exec;

	l = apply->leader;
	if (l == NULL) {
		raft_free(apply);
		return;
	}

	exec = l->exec;
	PRE(exec != NULL);
	sm_move(&exec->sm, EXEC_APPLIED);
	if (status == RAFT_SHUTDOWN) {
		apply->leader = NULL;
	} else {
		raft_free(apply);
	}
	exec_tick(exec, status);
}

static int exec_status(int r)
{
	PRE(r != 0);
	return r == RAFT_LEADERSHIPLOST ?  SQLITE_IOERR_LEADERSHIP_LOST :
	       r == RAFT_NOSPACE ?  SQLITE_IOERR_WRITE :
	       r == RAFT_SHUTDOWN ?  SQLITE_ABORT :
	       SQLITE_IOERR;
}

static void exec_barrier_cb(struct barrier *barrier, int status)
{
	struct exec *req = barrier->data;
	PRE(req != NULL);
	sm_move(&req->sm, EXEC_BARRIER);
	return exec_tick(req, status);
}

static void exec_timeout_cb(struct raft_timer *timer) {
	struct exec *req = timer->data;
	PRE(req != NULL);
	int err = raft_timer_stop(req->leader->raft, timer);
	assert(err == 0);
	queue_remove(&req->queue);
	return exec_abort(req, SQLITE_BUSY);
}

/**
 * Exec request pseudo-coroutine, encapsulating the whole lifecycle.
 *
 * Exec processing is a sequence of steps, tracked by the embedded SM, in
 * between which we possibly suspend execution. After every suspend, control
 * returns to this function, and we jump to the appropriate arm of the switch
 * statement based on the SM state. If we never suspend, control remains in
 * this function, passing through each state from top to bottom.
 *
 * When invoked by a callback, the `status` argument indicates whether the
 * async operation succeeded or failed, and the return value is ignored. TODO(marco6): remove return value.
 *
 * There are some backward-compatibility warts here. In particular, when
 * an error occurs, we sometimes signal it by returning an error code
 * and sometimes by just setting `req->status` (and returning one of the two
 * "success" codes). This is done to preserve exactly how each error was handled in
 * the previous exec code. FIXME(marco6): which is a terrible idea.
 */
void exec_tick(struct exec *req, int status)
{
	int err = 0;
	sqlite3_vfs *vfs = NULL;
	struct leader *l = req->leader;
	PRE(l != NULL);

	switch (sm_state(&req->sm)) {
	case EXEC_WAITING:
		PRE(status == 0);
		if (l->db->active_leader != NULL && l->db->active_leader != l) {
			/* supend as another leader is keeping the database busy,
			 * but also start a timer as this statement should not sit
			 * in the queue for too long. In the case the timer expires
			 * the statement will just fail with SQLITE_BUSY. */
			err = raft_timer_start(l->raft, &req->timer,
				l->db->config->busy_timeout, 0, exec_timeout_cb);
			if (err != RAFT_RESULT_OK) {
				/* given that it wasn't possible to wait for the other leader,
				 * the only way out is to report to the user that the db is busy.*/
				req->status = SQLITE_BUSY;
				return exec_done(req);
			}
			
			queue_insert_tail(&l->db->pending_queue, &req->queue);
			return exec_suspend(req);
		}
		sm_move(&req->sm, EXEC_START);
		__attribute__((fallthrough));
	case EXEC_START:
		PRE(l->db->active_leader == NULL || l->db->active_leader == l);
		err = raft_timer_stop(l->raft, &req->timer);
		assert(err == 0);

		l->db->active_leader = l;
		err = leader_barrier_v2(l, &req->barrier, exec_barrier_cb);
		if (err != 0) {
			req->status = err;
			return exec_done(req);
		}
		return exec_suspend(req);
	case EXEC_BARRIER:
		if (status != 0) {
			req->status = status;
			return exec_done(req);
		}
		err = exec_apply(req);
		if (err != 0) {
			req->status = err;
			return exec_done(req);
		}
		return exec_suspend(req);
	case EXEC_APPLIED:
		vfs = sqlite3_vfs_find(l->db->config->name);
		PRE(vfs != NULL);
		if (status == 0) {
			/* TODO(marco6): if defer_cb is true, it means that the code arrived here synchronously. If that's true, nothing has been added to the log.
			 * This was used as a filter to only run this checkpoint logic if the code applied something.
			 * This is an incredibly shit way of filtering a checkpoint which would still happen **after every fucking transaction**. */
			if (!req->defer_cb) {
				leaderMaybeCheckpointLegacy(l);
			}
		} else {
			req->status = exec_status(status);
			VfsAbort(vfs, l->db->path);
		}
		l->inflight = NULL;
		return exec_done(req);
	case EXEC_DONE:
	case EXEC_FAILED:
		return; // do nothing
	default:
		POST(false && "impossible!");
	}
}

static void exec_suspend(struct exec *req) {
	/* after suspending the pseudo-coroutine, the call stack
	 * is unwinded, so there is no need to unwind it again by
	 * deferring the final callback. */
	req->defer_cb = false;
}

int leader_exec_v2(struct leader *l,
		   struct exec *req,
		   sqlite3_stmt *stmt,
		   exec_cb cb)
{
	PRE(cb != NULL);
	PRE(l->exec == NULL);
	sm_init(&req->sm, exec_invariant, NULL, exec_states, "exec",
		EXEC_WAITING);
	req->leader = l;
	req->stmt = stmt;
	req->defer_cb = true;
	req->cb = cb;
	req->barrier.data = req;
	req->barrier.cb = NULL;
	req->work = (pool_work_t){};
	l->exec = req;
	exec_tick(req, 0);
	return 0;
}
