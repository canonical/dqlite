#include <stdint.h>
#include <stdio.h>

#include "../include/dqlite.h"

#include "./lib/assert.h"

#include "command.h"
#include "conn.h"
#include "gateway.h"
#include "leader.h"
#include "lib/sm.h"
#include "lib/threadpool.h"
#include "server.h"
#include "tracing.h"
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

/**
 * State machine for exec requests.
 */
enum {
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
	S(START,    A(BARRIER)|A(FAILED)|A(DONE), SM_INITIAL),
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

static void exec_done(struct exec *, int);

void leader__close(struct leader *l)
{
	tracef("leader close");
	int rc;
	/* TODO: there shouldn't be any ongoing exec request. */
	if (l->exec != NULL) {
		assert(l->inflight == NULL);
		l->exec->status = SQLITE_ERROR;
		exec_done(l->exec, 0);
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

	db->tx_id = 1;
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

	if (state == BARRIER_PASSED) {
		cb(barrier, status);
	}
}

static void barrier_raft_cb(struct raft_barrier *, int);

static int barrier_tick(struct barrier *barrier, int status)
{
	int rv;

	if (sm_state(&barrier->sm) == BARRIER_START) {
		PRE(status == 0);
		rv = raft_barrier(barrier->leader->raft, &barrier->req, barrier_raft_cb);
		if (rv != 0) {
			barrier_done(barrier, rv);
		}
		return rv;
	}

	PRE(sm_state(&barrier->sm) == BARRIER_PASSED);
	status = status == 0 ? 0 :
		 status == RAFT_LEADERSHIPLOST ? SQLITE_IOERR_LEADERSHIP_LOST :
		 SQLITE_ERROR;
	barrier_done(barrier, status);
	return 0;
}

static void barrier_raft_cb(struct raft_barrier *rb, int status)
{
	struct barrier *barrier = rb->data;
	PRE(barrier != NULL);
	/* TODO(cole) it seems that raft can invoke this callback more than
	 * once, investigate and fix that and then remove this workaround. */
	if (sm_state(&barrier->sm) > BARRIER_START) {
		return;
	}
	sm_move(&barrier->sm, BARRIER_PASSED);
	(void)barrier_tick(rb->data, status);
}

int leader_barrier_v2(struct leader *l,
		      struct barrier *barrier,
		      barrier_cb cb)
{
	int rv;

	if (!needsBarrier(l)) {
		return LEADER_NOT_ASYNC;
	}

	sm_init(&barrier->sm, barrier_invariant, NULL, barrier_states, "barrier",
		BARRIER_START);
	barrier->cb = cb;
	barrier->leader = l;
	barrier->req.data = barrier;
	rv = barrier_tick(barrier, 0);
	POST(rv != LEADER_NOT_ASYNC);
	return rv;
}

static void exec_done(struct exec *req, int asyncness)
{
	int status = req->status;
	status = status ? status : SQLITE_ERROR;
	if (status == SQLITE_DONE) {
		sm_move(&req->sm, EXEC_DONE);
	} else {
		sm_fail(&req->sm, EXEC_FAILED, status);
	}
	sm_fini(&req->sm);
	req->leader->exec = NULL;
	if (req->cb != NULL && asyncness == 0) {
		req->cb(req, status);
	}
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
		return LEADER_NOT_ASYNC;
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

static int exec_tick(struct exec *, int);

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
	exec_tick(req, status);
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
 * When invoked by leader_exec_v2, the return value indicates
 * whether we suspended (0), finished without suspending (LEADER_NOT_ASYNC),
 * or encountered an error (any other value). When invoked by a callback,
 * the `status` argument indicates whether the async operation succeeded
 * or failed, and the return value is ignored.
 *
 * There are some backward-compatibility warts here. In particular, when
 * an error occurs, we sometimes signal it by returning an error code
 * and sometimes by just setting `req->status` (and returning one of the two
 * "success" codes). This is done to preserve exactly how each error was handled in
 * the previous exec code.
 */
int exec_tick(struct exec *req, int status)
{
	struct leader *l;
	sqlite3_vfs *vfs;
	int barrier_rv = 0;
	int apply_rv = 0;
	/* Eventual return value of this function. Also tracks whether we
	 * previously suspended while processing this request (0) or not
	 * (LEADER_NOT_ASYNC). */
	int ret = 0;

	switch (sm_state(&req->sm)) {
	case EXEC_START:
		PRE(status == 0);
		l = req->leader;
		PRE(l != NULL);
		barrier_rv = leader_barrier_v2(l, &req->barrier, exec_barrier_cb);
		if (barrier_rv == 0) {
			/* suspended */
			ret = 0;
			break;
		} else if (barrier_rv != LEADER_NOT_ASYNC) {
			/* return error to caller, don't invoke callback,
			 * but set req->status so that that the SM will
			 * record the failure */
			req->status = barrier_rv;
			exec_done(req, LEADER_NOT_ASYNC);
			ret = barrier_rv;
			break;
		} /* else barrier_rv == LEADER_NOT_ASYNC => */
		ret = LEADER_NOT_ASYNC;
		sm_move(&req->sm, EXEC_BARRIER);
		POST(status == 0);
		/* fallthrough */
	case EXEC_BARRIER:
		if (status != 0) {
			/* error, we must have suspended, so invoke the callback */
			PRE(ret == 0);
			req->status = status;
			exec_done(req, 0);
			break;
		}
		apply_rv = exec_apply(req);
		if (apply_rv == 0) {
			/* suspended */
			ret = 0;
			break;
		} else if (apply_rv != LEADER_NOT_ASYNC) {
			/* error, record it in `req->status` and either invoke
			 * the callback (if we suspended) or tell the caller to
			 * invoke it (otherwise---it would be more consistent
			 * to return the error code in this case, but for the
			 * sake of compatibility we do it this way instead) */
			req->status = apply_rv;
			exec_done(req, ret);
			break;
		} /* else apply_rv == LEADER_NOT_ASYNC => */
		ret &= LEADER_NOT_ASYNC;
		sm_move(&req->sm, EXEC_APPLIED);
		POST(status == 0);
		/* fallthrough */
	case EXEC_APPLIED:
		l = req->leader;
		PRE(l != NULL);
		vfs = sqlite3_vfs_find(l->db->config->name);
		PRE(vfs != NULL);
		/* apply_rv == 0 if and only if we suspended at the previous step,
		 * if and only if the transaction generated frames---this logic is
		 * copied carefully from the previous version of the code */
		PRE(apply_rv == 0 || apply_rv == LEADER_NOT_ASYNC);
		if (apply_rv == 0) {
			if (status == 0) {
				leaderMaybeCheckpointLegacy(l);
			} else {
				req->status = exec_status(status);
				VfsAbort(vfs, l->db->path);
			}
			l->inflight = NULL;
			l->db->tx_id = 0;
		}
		/* finished successfully */
		exec_done(req, ret);
		break;
	default:
		POST(false && "impossible!");
	}

	return ret;
}

int leader_exec_v2(struct leader *l,
		   struct exec *req,
		   sqlite3_stmt *stmt,
		   exec_cb cb)
{
	if (l->exec != NULL) {
		return SQLITE_BUSY;
	}
	l->exec = req;

	sm_init(&req->sm, exec_invariant, NULL, exec_states, "exec",
		EXEC_START);
	req->leader = l;
	req->stmt = stmt;
	req->cb = cb;
	req->barrier.data = req;
	req->barrier.cb = NULL;
	req->work = (pool_work_t){};

	return exec_tick(req, 0);
}
