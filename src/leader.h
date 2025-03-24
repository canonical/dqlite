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

#define SQLITE_IOERR_NOT_LEADER (SQLITE_IOERR | (40 << 8))
#define SQLITE_IOERR_LEADERSHIP_LOST (SQLITE_IOERR | (41 << 8))

struct exec;
struct barrier;
struct leader;

typedef void (*exec_work_cb)(struct exec *req);
typedef void (*exec_done_cb)(struct exec *req);

/* Wrapper around raft_apply, saving context information. */
// TODO remove this.
struct apply {
	struct raft_apply req; /* Raft apply request */
	int status;            /* Raft apply result */
	struct leader *leader; /* Leader connection that triggered the hook */
	int type;              /* Command type */
	union {                /* Command-specific data */
		struct {
			bool is_commit;
		} frames;
	};
};

struct leader {
	struct db *db;            /* Database the connection. */
	sqlite3 *conn;            /* Underlying SQLite connection. */
	struct raft *raft;        /* Raft instance. */
	struct exec *exec;        /* Exec request in progress, if any. */
	queue queue;              /* Prev/next leader, used by struct db. */
	struct apply *inflight;   /* TODO: remove this. */
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

	/******* Internal state *******/
	int status;
	queue queue;
	struct sm sm;
	struct leader *leader;
	struct raft_barrier barrier;
	struct raft_timer timer;
	struct {
		dqlite_vfs_frame *ptr;
		unsigned          len;
	} frames;

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

void leader__close(struct leader *l);

/**
 * Submit a request to step a SQLite statement.
 *
 * This is an asynchronous operation in general. 
 *
 * If set, the work callback will be called for once the statement is prepared
 * and ready to be executed. Once the query is done with the statement, it is
 * the callback's responsibility to schedule a leader_exec_resume as the state
 * machine is suspended. If the callback is not set, the state machine will not
 * suspend. The work callback will not be called if the prepared statement
 * contains no SQL (e.g. it is just spaces or just a comment).
 *
 * In case the statement generates an update on the database, this routine will make
 * sure that it is replicated before calling the done callback. As such it is important
 * not to inform the client of a successful transaction until the done callback is
 * called. The done callback is always executed in the loop thread.
 *
 * The prepared statement is never freed by this routine.
 */
void leader_exec(struct exec *req,
		exec_work_cb work,
		exec_done_cb done);

void leader_exec_resume(struct exec *req, int status);

inline void leader_exec_sql(struct exec *req,
		const char *sql,
		exec_work_cb work,
		exec_done_cb done)
{
	req->stmt = NULL;
	req->sql  = sql;

	return leader_exec(req, work, done);
}

inline void leader_exec_stmt(struct exec *req,
		sqlite3_stmt *stmt,
		exec_work_cb  work,
		exec_done_cb  done)
{
	req->stmt = stmt;
	req->sql  = NULL;

	return leader_exec(req, work, done);
}


#endif /* LEADER_H_*/
