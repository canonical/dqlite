#include <stdio.h>

#include "../include/dqlite.h"

#include "./lib/assert.h"

#include "leader.h"

#define LOOP_CORO_STACK_SIZE 1024 * 1024 /* TODO: make this configurable? */

/* Open a SQLite connection and set it to leader replication mode. */
static int open_conn(const char *filename,
		     const char *vfs,
		     const char *replication,
		     void *replication_arg,
		     unsigned page_size,
		     sqlite3 **conn);
static int init_loop(struct leader *l);
static void maybe_exec_done(struct exec *req);

static void loop();
static struct leader *loop_arg_leader; /* For initializing the loop coroutine */
static struct exec *loop_arg_exec;     /* Next exec request to execute */

/* Whether we need to submit a barrier request because there is no transaction
 * in progress in the underlying database and the FSM is behind the last log
 * index. */
static bool needs_barrier(struct leader *l)
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
	rc = init_loop(l);
	if (rc != 0) {
		goto err;
	}
	rc = open_conn(db->filename, db->config->name, db->config->name, l,
		       db->config->page_size, &l->conn);
	if (rc != 0) {
		goto err_after_loop_create;
	}
	l->exec = NULL;
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
	assert(l->exec == NULL);
	rc = sqlite3_close(l->conn);
	assert(rc == 0);
	co_delete(l->loop);
	QUEUE__REMOVE(&l->queue);
}

static void exec_barrier_cb(struct barrier *barrier, int status)
{
	struct exec *req = barrier->data;
	struct leader *l = req->leader;
	if (status != 0) {
		l->exec->done = true;
		maybe_exec_done(l->exec);
		return;
	}
	loop_arg_exec = l->exec;
	co_switch(l->loop);
	maybe_exec_done(l->exec);
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

	rv = leader__barrier(l, &req->barrier, exec_barrier_cb);
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
	if (!needs_barrier(l)) {
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

static void maybe_exec_done(struct exec *req)
{
	if (!req->done) {
		return;
	}
	req->leader->exec = NULL;
	if (req->cb != NULL) {
		req->cb(req, req->status);
	}
}

static int open_conn(const char *filename,
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

static int init_loop(struct leader *l)
{
	l->loop = co_create(LOOP_CORO_STACK_SIZE, loop);
	if (l->loop == NULL) {
		return DQLITE_NOMEM;
	}
	loop_arg_leader = l;
	co_switch(l->loop);
	return 0;
}

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
