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

#define LEADER_NOT_ASYNC INT_MAX

struct exec;
struct barrier;
struct leader;

typedef void (*exec_cb)(struct exec *req, int status);
typedef void (*barrier_cb)(struct barrier *req, int status);

/* Wrapper around raft_apply, saving context information. */
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
	struct db *db;          /* Database the connection. */
	sqlite3 *conn;          /* Underlying SQLite connection. */
	struct raft *raft;      /* Raft instance. */
	struct exec *exec;      /* Exec request in progress, if any. */
	queue queue;            /* Prev/next leader, used by struct db. */
	struct apply *inflight; /* TODO: make leader__close async */
};

struct barrier {
	void *data;
	struct sm sm;
	struct leader *leader;
	struct raft_barrier req;
	barrier_cb cb;
};

/**
 * Asynchronous request to execute a statement.
 */
struct exec {
	void *data;
	struct sm sm;
	struct leader *leader;
	struct barrier barrier;
	sqlite3_stmt *stmt;
	int status;
	queue queue;
	exec_cb cb;
	pool_work_t work;
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
 * Submit a raft barrier request if there is no transaction in progress on the
 * underlying database and the FSM is behind the last log index.
 *
 * The callback will only be invoked asynchronously: if no barrier is needed,
 * this function will return without invoking the callback.
 *
 * Returns 0 if the callback was scheduled successfully or LEADER_NOT_ASYNC
 * if no barrier is needed. Any other value indicates an error.
 */
int leader_barrier_v2(struct leader *l,
		      struct barrier *barrier,
		      barrier_cb cb);

/**
 * Submit a request to step a SQLite statement.
 *
 * This is an asynchronous operation in general. It can yield to the event
 * loop at two points:
 *
 * - When running the preliminary barrier (see leader_barrier_v2). This
 *   is skipped if no barrier is necessary.
 * - When replicating the transaction in raft. This is skipped if the
 *   statement doesn't generate any changed pages.
 *
 * This function returns 0 if it successfully suspended for one of these
 * async operations. It returns LEADER_NOT_ASYNC to indicate that it
 * did not suspend, and in this case `req->status` shows whether an error
 * occurred. Any other return value indicates an error.
 */
int leader_exec_v2(struct leader *l,
		   struct exec *req,
		   sqlite3_stmt *stmt,
		   exec_cb cb);

#endif /* LEADER_H_*/
