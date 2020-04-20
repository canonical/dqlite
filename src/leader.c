#include <stdio.h>

#include "../include/dqlite.h"

#include "./lib/assert.h"

#include "command.h"
#include "format.h"
#include "leader.h"

#define LOOP_CORO_STACK_SIZE 1024 * 1024 /* TODO: make this configurable? */

static void maybeExecDone(struct exec *req)
{
	if (!req->done) {
		return;
	}
	req->leader->exec = NULL;
	if (req->cb != NULL) {
		req->cb(req, req->status);
	}
}

static void checkpointApplyCb(struct raft_apply *req, int status, void *result)
{
	struct leader *l = req->data;
	(void)result;
	(void)status;       /* TODO: log a warning in case of errors. */
	co_switch(l->loop); /* Resume apply() */
	maybeExecDone(l->exec);
}

static int maybeCheckpoint(void *ctx,
			   sqlite3 *db,
			   const char *schema,
			   int pages)
{
	struct leader *l = ctx;
	struct sqlite3_file *file;
	struct raft_buffer buf;
	struct command_checkpoint command;
	volatile void *region;
	uint32_t mx_frame;
	uint32_t read_marks[FORMAT__WAL_NREADER];
	int i;
	int rv;
	(void)db;
	(void)schema;

	/* Check if the size of the WAL is beyond the threshold. */
	if ((unsigned)pages < l->db->config->checkpoint_threshold) {
		/* Nothing to do yet. */
		return SQLITE_OK;
	}

	/* Get the database file associated with this connection */
	rv = sqlite3_file_control(l->conn, "main", SQLITE_FCNTL_FILE_POINTER,
				  &file);
	assert(rv == SQLITE_OK); /* Should never fail */

	/* Get the first SHM region, which contains the WAL header. */
	rv = file->pMethods->xShmMap(file, 0, 0, 0, &region);
	assert(rv == SQLITE_OK); /* Should never fail */

	/* Get the current value of mxFrame. */
	format__get_mx_frame((const uint8_t *)region, &mx_frame);

	/* Get the content of the read marks. */
	format__get_read_marks((const uint8_t *)region, read_marks);

	/* Check each mark and associated lock. This logic is similar to the one
	 * in the walCheckpoint function of wal.c, in the SQLite code. */
	for (i = 0; i < SQLITE_SHM_NLOCK; i++) {
		int flags = SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE;

		rv = file->pMethods->xShmLock(file, i, 1, flags);
		if (rv == SQLITE_BUSY) {
			/* It's locked. Let's postpone the checkpoint
			 * for now. */
			return SQLITE_OK;
		}

		/* Not locked. Let's release the lock we just
		 * acquired. */
		flags = SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE;
		file->pMethods->xShmLock(file, i, 1, flags);
	}

	/* Attempt to perfom a checkpoint across all nodes.
	 *
	 * TODO: reason about if it's indeed fine to ignore all kind of
	 * errors. */
	command.filename = l->db->filename;
	rv = command__encode(COMMAND_CHECKPOINT, &command, &buf);
	if (rv != 0) {
		goto abort;
	}
	rv = raft_apply(l->raft, &l->apply, &buf, 1, checkpointApplyCb);
	if (rv != 0) {
		goto abort_after_command_encode;
	}
	co_switch(l->main);

	return SQLITE_OK;

abort_after_command_encode:
	raft_free(buf.base);
abort:
	assert(rv != 0);
	/* TODO: log a warning. */
	return SQLITE_OK;
}

/* Open a SQLite connection and set it to leader replication mode. */
static int openConnection(const char *filename,
			  const char *vfs,
			  const char *replication,
			  void *replication_arg,
			  unsigned page_size,
			  sqlite3 **conn)
{
	char pragma[255];
	int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	char *msg = NULL;
	int rc;

	rc = sqlite3_open_v2(filename, conn, flags, vfs);
	if (rc != SQLITE_OK) {
		goto err;
	}

	/* Enable extended result codes */
	rc = sqlite3_extended_result_codes(*conn, 1);
	if (rc != SQLITE_OK) {
		goto err_after_open;
	}

	/* Set the page size. */
	sprintf(pragma, "PRAGMA page_size=%d", page_size);
	rc = sqlite3_exec(*conn, pragma, NULL, NULL, &msg);
	if (rc != SQLITE_OK) {
		goto err_after_open;
	}

	/* Disable syncs. */
	rc = sqlite3_exec(*conn, "PRAGMA synchronous=OFF", NULL, NULL, &msg);
	if (rc != SQLITE_OK) {
		goto err_after_open;
	}

	/* Set WAL journaling. */
	rc = sqlite3_exec(*conn, "PRAGMA journal_mode=WAL", NULL, NULL, &msg);
	if (rc != SQLITE_OK) {
		goto err_after_open;
	}

	/* Set WAL replication. */
	rc = sqlite3_wal_replication_leader(*conn, "main", replication,
					    replication_arg);

	if (rc != SQLITE_OK) {
		goto err_after_open;
	}

	/* TODO: make setting foreign keys optional. */
	rc = sqlite3_exec(*conn, "PRAGMA foreign_keys=1", NULL, NULL, &msg);
	if (rc != SQLITE_OK) {
		goto err_after_open;
	}

	return 0;

err_after_open:
	sqlite3_close(*conn);
err:
	if (msg != NULL) {
		sqlite3_free(msg);
	}
	return rc;
}

static struct leader *loop_arg_leader; /* For initializing the loop coroutine */
static struct exec *loop_arg_exec;     /* Next exec request to execute */

static void loop()
{
	struct leader *l = loop_arg_leader;
	co_switch(l->main);
	while (1) {
		struct exec *req = loop_arg_exec;
		int rc;
		rc = sqlite3_step(req->stmt);
		req->done = true;
		req->status = rc;
		co_switch(l->main);
	};
}

static int initLoopCoroutine(struct leader *l)
{
	l->loop = co_create(LOOP_CORO_STACK_SIZE, loop);
	if (l->loop == NULL) {
		return DQLITE_NOMEM;
	}
	loop_arg_leader = l;
	co_switch(l->loop);
	return 0;
}

/* Whether we need to submit a barrier request because there is no transaction
 * in progress in the underlying database and the FSM is behind the last log
 * index. */
static bool needsBarrier(struct leader *l)
{
	return (l->db->tx == NULL || l->db->tx->is_zombie) &&
	       raft_last_applied(l->raft) < raft_last_index(l->raft);
}

int leader__init(struct leader *l, struct db *db, struct raft *raft)
{
	int rc;
	l->db = db;
	l->raft = raft;
	l->main = co_active();
	rc = initLoopCoroutine(l);
	if (rc != 0) {
		goto err;
	}
	rc = openConnection(db->filename, db->config->name, db->config->name, l,
			    db->config->page_size, &l->conn);
	if (rc != 0) {
		goto err_after_loop_create;
	}
	sqlite3_wal_hook(l->conn, maybeCheckpoint, l);

	l->exec = NULL;
	l->apply.data = l;
	l->inflight = NULL;
	QUEUE__PUSH(&db->leaders, &l->queue);
	return 0;

err_after_loop_create:
	co_delete(l->loop);
err:
	return rc;
}

void leader__close(struct leader *l)
{
	int rc;
	/* TODO: there shouldn't be any ongoing exec request. */
	if (l->exec != NULL) {
		if (l->inflight != NULL) {
			/* TODO: make leader_close async instead */
			l->inflight->leader = NULL;
		}
		l->exec->done = true;
		l->exec->status = SQLITE_ERROR;
		maybeExecDone(l->exec);
	}
	rc = sqlite3_close(l->conn);
	assert(rc == 0);

	/* TODO: untested: this is a temptative fix for the zombie tx assertion
	 * failure. */
	if (l->db->tx != NULL && l->db->tx->conn == l->conn) {
		db__delete_tx(l->db);
	}

	co_delete(l->loop);
	QUEUE__REMOVE(&l->queue);
}

static void execBarrierCb(struct barrier *barrier, int status)
{
	struct exec *req = barrier->data;
	struct leader *l = req->leader;
	if (status != 0) {
		l->exec->done = true;
		l->exec->status = status;
		maybeExecDone(l->exec);
		return;
	}
	loop_arg_exec = l->exec;
	co_switch(l->loop);
	maybeExecDone(l->exec);
}

int leader__exec(struct leader *l,
		 struct exec *req,
		 sqlite3_stmt *stmt,
		 exec_cb cb)
{
	int rv;
	if (l->exec != NULL) {
		return SQLITE_BUSY;
	}
	l->exec = req;

	req->leader = l;
	req->stmt = stmt;
	req->cb = cb;
	req->done = false;
	req->barrier.data = req;

	rv = leader__barrier(l, &req->barrier, execBarrierCb);
	if (rv != 0) {
		return rv;
	}
	return 0;
}

static void raftBarrierCb(struct raft_barrier *req, int status)
{
	struct barrier *barrier = req->data;
	int rv = 0;
	if (status != 0) {
		if (status == RAFT_LEADERSHIPLOST) {
			rv = SQLITE_IOERR_LEADERSHIP_LOST;
		} else {
			rv = SQLITE_ERROR;
		}
	}
	barrier->cb(barrier, rv);
}

int leader__barrier(struct leader *l, struct barrier *barrier, barrier_cb cb)
{
	int rv;
	if (!needsBarrier(l)) {
		cb(barrier, 0);
		return 0;
	}
	barrier->cb = cb;
	barrier->leader = l;
	barrier->req.data = barrier;
	rv = raft_barrier(l->raft, &barrier->req, raftBarrierCb);
	if (rv != 0) {
		return rv;
	}
	return 0;
}
