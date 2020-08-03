#include "replication.h"

#include <libco.h>
#include <sqlite3.h>
#include <stddef.h>

#include "command.h"
#include "leader.h"
#include "lib/assert.h"

/* Set to 1 to enable tracing. */
#if 0
#define tracef(MSG, ...) debugf(r->logger, MSG, ##__VA_ARGS__)
#else
#define tracef(MSG, ...)
#endif

/* Implementation of the sqlite3_wal_replication interface */
struct replication
{
	struct logger *logger;
	struct raft *raft;
	queue apply_reqs;
};

static void framesAbortBecauseLeadershipLost(struct leader *leader,
					     int is_commit)
{
	if (is_commit) {
		/* Mark the transaction as zombie. Possible scenarios:
		 *
		 * 1. This server gets re-elected right away as leader.
		 *
		 *    In this case we'll try to apply this lost command log
		 *    again. If we succeed, our FSM will transition this zombie
		 *    transaction into to a surrogate follower and our next
		 *    begin hook invokation will issue an Undo command, which
		 *    (if successfull) will be a no-op on our FSM and an actual
		 *    rollback on the followers (regardless of whether this was
		 *    the first non-commit frames command or a further one). If
		 *    we fail to re-apply the command there will be a new
		 *    election, and we'll end up again in either this case (1)
		 *    or the next one (2). Same if the Undo command fails.
		 *
		 * 2. Another server gets elected as leader.
		 *
		 *    In this case there are two possible scenarios.
		 *
		 *    2.1. No quorum was reached for the lost commit
		 *         command. This means that no FSM (including ours) will
		 *         ever try to apply it. If this lost non-commit frames
		 *         command was the first one of a transaction, the new
		 *         leader will see no dangling follower and will just
		 *         start a new transaction with a new ID, sending a
		 *         Frames command to our FSM. Our FSM will detect the
		 *         zombie transaction and simply purge it from the
		 *         registry.
		 *
		 *    2.2 A quorum was reached for the lost commit command. This
		 *        means that the new leader will replicate it to every
		 *        server that didn't apply it yet, which includes us,
		 *        and then issue an Undo command to abort the
		 *        transaction. In this case our FSM will behave like in
		 *        case 1.*/
		tx__zombie(leader->db->tx);
	} else {
		/* Mark the transaction as zombie. Possible scenarios:
		 *
		 * 1. This server gets re-elected right away as leader.
		 *
		 *    In this case we'll try to apply this lost command log
		 *    again. If we succeed, our FSM will transition this zombie
		 *    transaction into to a surrogate follower and our next
		 *    Begin hook invokation will issue an Undo command, which
		 *    (if successfull) will be a no-op on our FSM and an actual
		 *    rollback on the followers (regardless of whether this was
		 *    the first non-commit frames command or a further one). If
		 *    we fail to re-apply the command there will be a new
		 *    election, and we'll end up again in either this case (1)
		 *    or the next one (2). Same if the Undo command fails.
		 *
		 * 2. Another server gets elected as leader.
		 *
		 *    In this case there are two possible scenarios.
		 *
		 *    2.1. No quorum was reached for the lost commit
		 *         command. This means that no FSM (including ours) will
		 *         ever try to apply it. If this lost non-commit frames
		 *         command was the first one of a transaction, the new
		 *         leader will see no dangling follower transaction and
		 *         will just start a new transaction with a new ID,
		 *         sending a Frames command to our FSM. Our FSM will
		 *         detect the zombie transaction and simply purge it
		 *         from the registry.
		 *
		 *    2.2 A quorum was reached for the lost commit command. This
		 *        means that the new leader will replicate it to every
		 *        server that didn't apply it yet, which includes us,
		 *        and then issue an Undo command to abort the
		 *        transaction. In this case our FSM will behave like in
		 *        case 1.
		 */
		tx__zombie(leader->db->tx);
	}
}

static void applyCb(struct raft_apply *req, int status, void *result)
{
	struct apply *apply;
	struct leader *leader;
	struct exec *r;
	(void)result;
	apply = req->data;
	leader = apply->leader;
	if (leader == NULL) {
		raft_free(apply);
		return;
	}
	r = leader->exec;
	apply->status = status;

	co_switch(leader->loop); /* Resume apply() */

	if (r != NULL && r->done) {
		leader->exec = NULL;
		if (r->cb != NULL) {
			r->cb(r, r->status);
		}
	}
}

/* Handle xFrames failures due to this server not not being the leader. */
static int framesAbortBecauseNotLeader(struct leader *leader, int is_commit)
{
	struct tx *tx = leader->db->tx;
	if (tx->state == TX__PENDING) {
		/* No Frames command was applied, so followers don't know about
		 * this transaction. If this is a commit frame, we don't need to
		 * do anything special, the xUndo hook will just remove it. If
		 * it's not a commit frame, the undo hook won't be fired and we
		 * need to remove the transaction here. */
		if (!is_commit) {
			db__delete_tx(leader->db);
		}
	} else {
		/* At least one Frames command was applied, so the transaction
		 * exists on the followers. We mark the transaction as zombie,
		 * the begin hook of next leader (either us or somebody else)
		 * will detect a dangling transaction and issue an Undo command
		 * to roll it back. In its apply Undo command logic our FSM will
		 * detect that the rollback is for a zombie and just no-op
		 * it. */
		tx__zombie(tx);
	}
	return SQLITE_IOERR_NOT_LEADER;
}

static int apply(struct replication *r,
		 struct apply *apply,
		 struct leader *leader,
		 int type,
		 const void *command)
{
	struct raft_buffer buf;
	int rc;

	apply->leader = leader;
	apply->req.data = apply;
	apply->type = type;

	rc = command__encode(type, command, &buf);
	if (rc != 0) {
		goto err;
	}

	rc = raft_apply(r->raft, &apply->req, &buf, 1, applyCb);
	if (rc != 0) {
		switch (rc) {
			case RAFT_TOOBIG:
				rc = SQLITE_TOOBIG;
				break;
			case RAFT_TOOMANY:
				/* The only case where raft returns RAFT_TOOMANY
				 * is when the AIO events system limit gets
				 * reached. */
				rc = SQLITE_IOERR_WRITE;
				break;
			default:
				rc = SQLITE_ERROR;
				break;
		}
		goto err_after_command_encode;
	}
	leader->inflight = apply;

	co_switch(leader->main);

	leader->inflight = NULL;

	if (apply->status != 0) {
		switch (apply->status) {
			case RAFT_LEADERSHIPLOST:
				rc = SQLITE_IOERR_LEADERSHIP_LOST;
				break;
			case RAFT_NOSPACE:
				rc = SQLITE_IOERR_WRITE;
				break;
			case RAFT_SHUTDOWN:
				/* If we got here it means we have manually
				 * fired the apply callback from
				 * gateway__close(). In this case we don't
				 * free() the apply object, since it will be
				 * freed when the callback is fired again by
				 * raft.
				 *
				 * TODO: we should instead make gatewa__close()
				 * itself asynchronous. */
				apply->leader = NULL;
				return SQLITE_ABORT;
			default:
				rc = SQLITE_IOERR;
				break;
		}
		switch (apply->type) {
			case COMMAND_FRAMES:
				if (apply->status == RAFT_LEADERSHIPLOST) {
					framesAbortBecauseLeadershipLost(
					    leader, apply->frames.is_commit);
				} else {
					/* TODO: are all errors equivalent to
					 * not leader? */
					framesAbortBecauseNotLeader(
					    leader, apply->frames.is_commit);
				}
				break;
			default:
				printf(
				    "unexpected apply failure for command type "
				    "%d\n",
				    apply->type);
				assert(0);
				break;
		};
	} else {
		rc = SQLITE_OK;
	}

	raft_free(apply);

	return rc;

err_after_command_encode:
	raft_free(buf.base);
err:
	raft_free(apply);
	return rc;
}

/* Check if a follower connection is already open for the leader's database, if
 * not open one with the open command. */
static int maybeAddFollower(struct replication *r, struct leader *leader)
{
	struct command_open c;
	struct apply *req;
	int rc;
	if (leader->db->follower != NULL) {
		return 0;
	}
	if (leader->db->opening) {
		return SQLITE_BUSY;
	}
	req = raft_malloc(sizeof *req);
	if (req == NULL) {
		return DQLITE_NOMEM;
	}

	c.filename = leader->db->filename;
	leader->db->opening = true;
	rc = apply(r, req, leader, COMMAND_OPEN, &c);
	leader->db->opening = false;
	if (rc != 0) {
		return rc;
	}
	return 0;
}

static int maybeHandleInProgressTx(struct replication *r, struct leader *leader)
{
	struct tx *tx = leader->db->tx;
	struct command_undo c;
	struct apply *req;
	int rc;

	if (tx == NULL) {
		return 0;
	}
	assert(tx->id != 0);

	/* Check if the in-progress transaction is a leader. */
	if (tx__is_leader(tx)) {
		/* Check if the transaction was started by another connection.
		 *
		 * In that case it's not worth proceeding further, since most
		 * probably the current in-progress transaction will complete
		 * successfully and modify the database, so a further write
		 * attempt from this other connection would fail with
		 * SQLITE_BUSY_SNAPSHOT.
		 *
		 * No dqlite state has been modified, and the WAL write lock has
		 * of course not been acquired.
		 *
		 * We just return SQLITE_BUSY, which has the same effect as the
		 * call to sqlite3WalBeginWriteTransaction (invoked in pager.c
		 * after a successful xBegin) would have. */
		if (tx->conn != leader->conn) {
			return SQLITE_BUSY;
		}

		/* SQLite prevents the same connection from entering a write
		 * transaction twice, so this must be a zombie of ourselves,
		 * meaning that a Frames command failed because we were not
		 * leaders anymore at that time and that frames command was
		 * following one or more non-commit frames commands that were
		 * successfully applied. */
		if (!tx->is_zombie) {
			/* TODO: if there's a pending leader tx for this
			 * connection, let's just remove it, although it's not
			 * clear how this could happen. */
			if (tx->state == TX__PENDING && tx->dry_run) {
				db__delete_tx(leader->db);
				return 0;
			}
			printf("non-zombie tx id=%ld state=%d dry-run=%d\n",
			       tx->id, tx->state, tx->dry_run);
		}
		assert(tx->is_zombie);
		assert(tx->state == TX__WRITING);
		assert(leader->db->follower != NULL);

		/* Create a surrogate follower. We'll undo the transaction
		 * below. */
		tx__surrogate(tx, leader->db->follower);
	}

	c.tx_id = tx->id;

	req = raft_malloc(sizeof *req);
	if (req == NULL) {
		return DQLITE_NOMEM;
	}

	rc = apply(r, req, leader, COMMAND_UNDO, &c);
	if (rc != 0) {
		return rc;
	}
	return 0;
}

/* The main tasks of the begin hook are to check that no other write transaction
 * is in progress and to cleanup any dangling follower transactions that might
 * have been left open after a leadership change.
 *
 * Concurrent calls can happen because the xBegin hook is fired by SQLite before
 * acquiring the WAL write lock (i.e. before calling WalBeginWriteTransaction),
 * so different connections can enter the xBegin hook at any time.
 *
 * The errors that can be returned are:
 *
 *  - SQLITE_BUSY:  If we detect that a write transaction is in progress on
 *                  another connection, or an Open request to create a follower
 *                  connection has been submitted by and is in progress. The
 *                  SQLite's call to sqlite3PagerBegin that triggered the xBegin
 *                  hook will propagate the error to sqlite3BtreeBeginTrans and
 *                  bubble up further, eventually failing the statement that
 *                  triggered the write attempt. The client should then execute
 *                  a ROLLBACK and then decide what to do.
 *
 *  - SQLITE_IOERR: This is returned if we are not the leader when the hook
 *                  fires or if we fail to apply the Open follower command log,
 *                  in case no follower for this database is open yet. We
 *                  include the relevant extended code, either
 *                  SQLITE_IOERR_NOT_LEADER if this server is not the leader
 *                  anymore or it is being shutdown, or
 *                  SQLITE_IOERR_LEADERSHIP_LOST if leadership was lost while
 *                  applying the Open command. The SQLite's call to
 *                  sqlite3PagerBegin that triggered the xBegin hook will
 *                  propagate the error to sqlite3BtreeBeginTrans, which will in
 *                  turn propagate it to the OP_Transaction case of vdbe.c,
 *                  which will goto abort_due_to_error and finally call
 *                  sqlite3VdbeHalt, automatically rolling back the
 *                  transaction. Since no write transaction was actually started
 *                  the xEnd hook is not fired.
 */
static int methodBegin(sqlite3_wal_replication *replication, void *arg)
{
	struct replication *r = replication->pAppData;
	struct leader *leader = arg;
	unsigned long long tx_id;
	int rc;

	if (raft_state(r->raft) != RAFT_LEADER) {
		return SQLITE_IOERR_NOT_LEADER;
	}

	/* We are always invoked in the context of an exec request */
	assert(leader->exec != NULL);

	rc = maybeAddFollower(r, leader);
	if (rc != 0) {
		return rc;
	}

	rc = maybeHandleInProgressTx(r, leader);
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

static int methodAbort(sqlite3_wal_replication *r, void *arg)
{
	(void)r;
	(void)arg;

	return SQLITE_OK;
}

static int methodFrames(sqlite3_wal_replication *replication,
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
	struct apply *req;
	int rc;

	assert(tx != NULL);
	assert(tx->conn == leader->conn);
	assert(tx->state == TX__PENDING || tx->state == TX__WRITING);

	if (raft_state(r->raft) != RAFT_LEADER) {
		return framesAbortBecauseNotLeader(leader, is_commit);
	}

	c.filename = leader->db->filename;
	c.tx_id = tx->id;
	c.truncate = truncate;
	c.is_commit = (uint8_t)is_commit;
	c.frames.n_pages = (uint32_t)n_frames;
	c.frames.page_size = (uint16_t)page_size;
	c.frames.data = frames;

	req = raft_malloc(sizeof *req);
	if (req == NULL) {
		return DQLITE_NOMEM;
	}

	req->frames.is_commit = is_commit;

	rc = apply(r, req, leader, COMMAND_FRAMES, &c);
	if (rc != 0) {
		return rc;
	}

	return SQLITE_OK;
}

static int methodUndo(sqlite3_wal_replication *replication, void *arg)
{
	struct replication *r = replication->pAppData;
	struct leader *leader = arg;
	struct tx *tx = leader->db->tx;

	assert(tx != NULL);
	assert(tx->conn == leader->conn);

	if (tx->is_zombie) {
		/* This zombie originated from the Frames hook. There are two
		 * scenarios:
		 *
		 * 1. Leadership was lost while applying the Frames command.
		 *
		 *    We can't simply remove the transaction since the Frames
		 *    command might eventually get committed. We just ignore it,
		 *    and let it handle by our FSM in that case (i.e. if we are
		 *    re-elected or a quorum was reached and another leader
		 *    tries to apply it).
		 *
		 * 2. This server was not the leader anymore when the Frames
		 *    hook fired for a commit frames batch which was the last of
		 *    a sequence of non-commit ones.
		 *
		 *    In this case we're being invoked by SQLite which is trying
		 *    to rollback the transaction. We can't simply remove the
		 *    transaction since the next leader will detect a dangling
		 *    transaction and try to issue an Undo command. We just
		 *    ignore the zombie and let our FSM handle it when the Undo
		 *    command will be applied. */
		return SQLITE_OK;
	}

	if (tx->state == TX__PENDING) {
		/* This means that the Undo hook fired because this node was not
		 * the leader when trying to apply the first Frames command, so
		 * no follower knows about it. We can just return, the
		 * transaction will be removed by the End hook. */
		return SQLITE_OK;
	}

	// Check if we're the leader.
	if (raft_state(r->raft) != RAFT_LEADER) {
		/* If we have lost leadership we're in a state where the
		 * transaction began on this node and a quorum of followers. We
		 * return an error, and SQLite will ignore it, however we need
		 * to mark the transaction as zombie, so the next leader will
		 * try to undo it across all nodes. */
		tx__zombie(tx);
		return SQLITE_IOERR_NOT_LEADER;
	}

	/* We don't really care whether the Undo command applied just below here
	 * will be committed or not.If the command fails, we'll create a
	 * surrogate follower: if the command still gets committed, then the
	 * rollback succeeds and the next leader will start fresh, if if the
	 * command does not get committed, the next leader will find a stale
	 * follower and re-try to roll it back. */
	/* if txn.State() != transaction.Pending { */
	/* 	command := protocol.NewUndo(txn.ID()) */
	/* 	if err := m.apply(tracer, conn, command); err != nil { */
	/* 		m.surrogateWriting(tracer, txn) */
	/* 		return errno(err) */
	/* 	} */
	/* } */

	return SQLITE_OK;
}

static int methodEnd(sqlite3_wal_replication *replication, void *arg)
{
	struct leader *leader = arg;
	struct tx *tx = leader->db->tx;

	(void)replication;

	if (tx == NULL) {
		/* TODO */
		assert(0);
	} else {
		assert(tx->conn == leader->conn);
	}

	if (tx->is_zombie) {
		/* Ignore zombie transactions as we don't know what will happen
		 * to them (either committed or not). */
		return 0;
	}

	db__delete_tx(leader->db);

	return SQLITE_OK;
}

int replication__init(struct sqlite3_wal_replication *replication,
		      struct config *config,
		      struct raft *raft)
{
	struct replication *r = sqlite3_malloc(sizeof *r);

	if (r == NULL) {
		return DQLITE_NOMEM;
	}

	r->logger = &config->logger;
	r->raft = raft;
	QUEUE__INIT(&r->apply_reqs);

	replication->iVersion = 1;
	replication->pAppData = r;
	replication->xBegin = methodBegin;
	replication->xAbort = methodAbort;
	replication->xFrames = methodFrames;
	replication->xUndo = methodUndo;
	replication->xEnd = methodEnd;
	replication->zName = config->name;

	sqlite3_wal_replication_register(replication, 0);

	return 0;
}

void replication__close(struct sqlite3_wal_replication *replication)
{
	struct replication *r = replication->pAppData;
	sqlite3_wal_replication_unregister(replication);
	sqlite3_free(r);
}
