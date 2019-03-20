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

int leader__init(struct leader *l, struct db *db)
{
	int rc;
	l->db = db;
	l->main = co_active();
	rc = init_loop(l);
	if (rc != 0) {
		goto err;
	}
	rc = open_conn(db->filename, db->options->vfs, db->options->replication,
		       l, db->options->page_size, &l->conn);
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

int leader__exec(struct leader *l,
		 struct exec *req,
		 sqlite3_stmt *stmt,
		 exec_cb cb)
{
	if (l->exec != NULL) {
		return SQLITE_BUSY;
	}
	l->exec = req;
	req->leader = l;
	req->stmt = stmt;
	req->cb = cb;
	req->done = false;
	loop_arg_exec = req;
	co_switch(l->loop);
	maybe_exec_done(req);
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
