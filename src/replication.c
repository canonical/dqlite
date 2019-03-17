#include <stddef.h>

#include <libco.h>
#include <sqlite3.h>

#include "./lib/assert.h"
#include "./lib/logger.h"

#include "command.h"
#include "leader.h"
#include "queue.h"
#include "replication.h"

/* Set to 1 to enable tracing. */
#if 1
#define tracef(MSG, ...) debugf(r->logger, MSG, ##__VA_ARGS__)
#else
#define tracef(MSG, ...)
#endif

/* Implementation of the sqlite3_wal_replication interface */
struct replication
{
	struct dqlite_logger *logger;
	struct raft *raft;
	queue apply_reqs;
};

static void apply_cb(struct raft_apply *req, int status)
{
	struct leader *leader;
	struct exec *r;
	leader = req->data;
	raft_free(req);
	if (status != 0) {
		assert(0); /* TODO */
	}
	co_switch(leader->loop);
	r = leader->exec;
	if (r != NULL && r->done) {
		leader->exec = NULL;
		if (r->cb != NULL) {
			r->cb(r, r->status);
		}
	}
}

static int apply(struct replication *r,
		 struct leader *leader,
		 int type,
		 const void *command)
{
	struct raft_apply *req;
	struct raft_buffer buf;
	int rc;

	req = raft_malloc(sizeof *req);
	if (req == NULL) {
		rc = DQLITE_NOMEM;
		goto err;
	}
	req->data = leader;

	rc = command__encode(type, command, &buf);
	if (rc != 0) {
		goto err_after_req_alloc;
	}

	rc = raft_apply(r->raft, req, &buf, 1, apply_cb);
	if (rc != 0) {
		goto err_after_command_encode;
	}

	co_switch(leader->main);
	return 0;

err_after_command_encode:
	raft_free(buf.base);
err_after_req_alloc:
	raft_free(req);
err:
	return rc;
}

/* Check if a follower connection is already open for the leader's database, if
 * not open one with the open command. */
static int maybe_add_follower(struct replication *r, struct leader *leader)
{
	struct command_open c;
	int rc;
	if (leader->db->follower != NULL) {
		return 0;
	}
	c.filename = leader->db->filename;
	rc = apply(r, leader, COMMAND_OPEN, &c);
	if (rc != 0) {
		return rc;
	}
	return 0;
}

static int maybe_handle_in_progress_tx(struct replication *r,
				       struct leader *leader)
{
	struct tx *tx = leader->db->tx;
	struct command_undo c;
	int rc;

	if (tx == NULL) {
		return 0;
	}
	assert(tx->id != 0);

	/* Check if the in-progress transaction is a leader. */
	if (tx__is_leader(tx)) {
		/* We queue up and serialize concurrent leader transactions, and
		 * SQLite prevents the same connection from entering a write
		 * transaction twice, so this must be a zombie of ourselves,
		 * meaning that a Frames command failed because we were not
		 * leaders anymore at that time and that was a commit frames
		 * command following one or more non-commit frames commands that
		 * were successfully applied. */
		assert(tx->conn == leader->conn);
		assert(tx->is_zombie);
		assert(tx->state == TX__WRITING);
		/* Create a surrogate follower. We'll undo the transaction
		 * below. */
		/* TODO: actually implement this */
		/* m.surrogateWriting(tracer, txn) */
	}

	c.tx_id = tx->id;
	rc = apply(r, leader, COMMAND_UNDO, &c);
	if (rc != 0) {
		return rc;
	}
	return 0;
}

int replication__begin(sqlite3_wal_replication *replication, void *arg)
{
	struct replication *r = replication->pAppData;
	struct leader *leader = arg;
	unsigned long long tx_id;
	int rc;

	if (raft_state(r->raft) != RAFT_LEADER) {
		return SQLITE_IOERR_NOT_LEADER;
	}
	assert(leader->exec != NULL);

	rc = maybe_add_follower(r, leader);
	if (rc != 0) {
		return rc;
	}

	rc = maybe_handle_in_progress_tx(r, leader);
	if (rc != 0) {
		return rc;
	}

	/* Use the last applied index as transaction ID.
	 *
	 * If this server is still the leader, this number is guaranteed to be
	 * strictly higher than any previous transaction ID, since after a
	 * leadership change we always call raft_barrier() to advance the FSM up
	 * to the latest committed log, and raft_barrier() itself will increment
	 * the applied index by one.
	 *
	 * If this server is not the leader anymore, it does not matter which ID
	 * we pick because any coming frames or undo hook will fail with
	 * SQLITE_IOERR_NOT_LEADER.
	 */
	tx_id = raft_last_applied(r->raft);

	rc = db__create_tx(leader->db, tx_id, leader->conn);
	if (rc != 0) {
		return rc;
	}

	return SQLITE_OK;
}

int replication__abort(sqlite3_wal_replication *r, void *arg)
{
	(void)r;
	(void)arg;

	return SQLITE_OK;
}

int replication__frames(sqlite3_wal_replication *replication,
			void *arg,
			int page_size,
			int n_frames,
			sqlite3_wal_replication_frame *frames,
			unsigned truncate,
			int is_commit)
{
	struct replication *r = replication->pAppData;
	struct leader *leader = arg;
	struct tx *tx = leader->db->tx;
	struct command_frames c;
	int rc;

	assert(tx != NULL);
	assert(tx->conn == leader->conn);
	assert(tx->state == TX__PENDING || tx->state == TX__WRITING);

	/* We checked that we were leader in the begin hook. If we had lost
	 * leadership in the meantime, the relevant apply callback would have
	 * fired with RAFT_ELEADERSHIPLOST and SQLite wouldn't have invoked the
	 * frames hook. */
	assert(raft_state(r->raft) == RAFT_LEADER);

	c.filename = leader->db->filename;
	c.tx_id = tx->id;
	c.page_size = page_size;
	c.truncate = truncate;
	c.is_commit = is_commit;
	c.n_pages = n_frames;
	c.data = frames;

	rc = apply(r, leader, COMMAND_FRAMES, &c);
	if (rc != 0) {
		return rc;
	}

	return SQLITE_OK;
}

int replication__undo(sqlite3_wal_replication *r, void *arg)
{
	(void)r;
	(void)arg;

	return SQLITE_OK;
}

int replication__end(sqlite3_wal_replication *replication, void *arg)
{
	struct leader *leader = arg;
	struct tx *tx = leader->db->tx;

	(void)replication;

	if (tx == NULL) {
		/* TODO */
	} else {
		assert(tx->conn == leader->conn);
	}

	db__delete_tx(leader->db);

	return SQLITE_OK;
}

int replication__init(struct sqlite3_wal_replication *replication,
		      struct dqlite_logger *logger,
		      struct raft *raft)
{
	struct replication *r = sqlite3_malloc(sizeof *r);

	if (r == NULL) {
		return DQLITE_NOMEM;
	}

	r->logger = logger;
	r->raft = raft;
	QUEUE__INIT(&r->apply_reqs);

	replication->iVersion = 1;
	replication->pAppData = r;
	replication->xBegin = replication__begin;
	replication->xAbort = replication__abort;
	replication->xFrames = replication__frames;
	replication->xUndo = replication__undo;
	replication->xEnd = replication__end;

	return 0;
}

void replication__close(struct sqlite3_wal_replication *replication)
{
	struct replication *r = replication->pAppData;
	sqlite3_free(r);
}
