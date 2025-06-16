/**
 * Track the state of leader connection and execute statements asynchronously.
 */

#ifndef LEADER_H_
#define LEADER_H_

#include <sqlite3.h>
#include <stdbool.h>

#include "db.h"
#include "lib/queue.h"
#include "lib/sm.h" /* struct sm */
#include "lib/threadpool.h"
#include "raft.h"

struct exec;
struct leader;

typedef void (*exec_work_cb)(struct exec *req);
typedef void (*exec_done_cb)(struct exec *req);
typedef void (*leader_close_cb)(struct leader *l);

struct leader {
	void           *data;     /* User data. */
	struct db      *db;       /* Database for the connection. */
	sqlite3        *conn;     /* Underlying SQLite connection. */
	struct raft    *raft;     /* Raft instance. */
	struct exec    *exec;     /* Exec request in progress, if any. */
	queue           queue;    /* Prev/next leader, used by struct db. */
	int             pending;  /* Number of pending requests. */
	leader_close_cb close_cb; /* Close callback. When not NULL it means that
				     the leader is closing. */
};

/**
 * Asynchronous request to execute a statement.
 */
struct exec {
	/** User data. Not used by the tate machine */
	void *data;

	/** 
	 * Already prepared statement to execute. 
	 * Either this or sql must be set, but not both.
	 *
	 * If set, the statement will not be finalized.
	 * It will always be NULL at the end of the execution.
	 */
	sqlite3_stmt *stmt;

	/** 
	 * SQL statement to execute.
	 * Either this or stmt must be set, but not both.
	 *
	 * The prepared statement will be finalized at the end of the execution.
	 * It will always be NULL at the end of the execution.
	 */
	const char *sql;

	/**
	 * Tail of the statement. It will be set to the tail of the sql statement
	 * after a prepare, if set at the start of the execution.
	 *
	 * It might be NULL if there is no statement left to execute.
	 * 
	 * The lifetime of this pointer is tied to sql field as this will just be
	 * a pointer to that memory.
	 */
	const char *tail;

	/*
	 * Result code for the operation. RAFT_OK if the operation was successful.
	 */
	int status;

	/* Fields below should not be touched by the user. */

	/*
	 * Used to enqueue execs in the db queue.
	 */
	queue queue;
	struct sm sm;

	/*
	 * Leader on which this request is being executed.
	 */
	struct leader *leader;
	struct raft_barrier barrier;

	/*
	 * Timer to limit the time spent in the queue.
	 */
	struct raft_timer timer; 
	struct raft_apply apply;

	exec_work_cb work_cb;
	exec_done_cb done_cb;
};

/**
 * Initialize a new leader connection.
 *
 * This function will start the leader loop coroutine and pause it immediately,
 * transfering control back to main coroutine and then opening a new leader
 * connection against the given database.
 */
int leader__init(struct leader *l, struct db *db, struct raft *raft);

/**
 * Submit a request to step a SQLite statement.
 *
 * This is an asynchronous operation in general. 
 *
 * If set, the work callback will be called once the statement is prepared
 * and ready to be executed. Once the callback is done with the statement, it is
 * the it's responsibility to schedule a leader_exec_resume as the state
 * machine is suspended. If the callback is not set, the state machine will not
 * suspend. The work callback will not be called if the prepared statement
 * contains no SQL (e.g. it is just spaces or just a comment).
 *
 * In case the statement generates an update on the database, this routine will make
 * sure that it is replicated before calling the done callback. As such it is important
 * not to inform the client of a successful transaction until the done callback is
 * called. The done callback is always executed in the loop thread.
 *
 * The status passed to the done callback is always one of the `RAFT_*` error codes.
 * The `SQLITE_*` error code can be obtained by calling `sqlite3_errcode` on the 
 * connection as only one exec is ever allowed on the same connection concurrently.
 *
 * The prepared statement is never freed by this routine.
 */
void leader_exec(struct leader *leader,
	struct exec *req,
	exec_work_cb work,
	exec_done_cb done);

/**
 * Sets the result of the operation the state machine suspended on.
 * 
 * This should be called right before resuming after work is done.
 *
 * Results should always be part of the RAFT_* series of error codes.
 */
void leader_exec_result(struct exec *req, int result);

/**
 * Resumes the execution of the exec state machine.
 */
void leader_exec_resume(struct exec *req);

/**
 * Aborts the current leader exec request, if possible.
 *
 * If the query is already finished (e.g. the work callback was already executed 
 * and successfully scheduled the resume callback), the request cannot be aborted
 * and will continue the replication phase to the end. Otherwise, the work callback
 * will not be called and the query will fail with SQLITE_ABORT error code.
 *
 * If there is no request in progress, this function does nothing.
 *
 */
void leader_exec_abort(struct exec *req);

void leader__close(struct leader *l, leader_close_cb close_cb);

#endif /* LEADER_H_*/
