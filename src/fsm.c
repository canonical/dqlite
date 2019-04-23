#include <raft.h>

#include "lib/assert.h"

#include "command.h"
#include "fsm.h"

struct fsm
{
	struct logger *logger;
	struct registry *registry;
};

static int apply_open(struct fsm *f, const struct command_open *c)
{
	struct db *db;
	int rc;

	rc = registry__db_get(f->registry, c->filename, &db);
	if (rc != 0) {
		return rc;
	}
	rc = db__open_follower(db);
	if (rc != 0) {
		return rc;
	}

	return 0;
}

static int apply_frames(struct fsm *f, const struct command_frames *c)
{
	struct db *db;
	struct tx *tx;
	unsigned *page_numbers;
	void *pages;
	bool is_begin = true;
	int rc;

	rc = registry__db_get(f->registry, c->filename, &db);
	assert(rc == 0); /* We have registered this filename before */

	assert(db->follower != NULL); /* We have issued an open command */

	tx = db->tx;

	if (tx != NULL) {
		/* TODO: handle leftover leader zombie transactions with lower
		 * ID */
		assert(tx->id == c->tx_id);

		if (tx__is_leader(tx)) {
			if (tx->is_zombie) {
				/* TODO */
			} else {
				/* We're executing this FSM command in during
				 * the execution of the replication->frames()
				 * hook. */
			}
		} else {
			/* We're executing the Frames command as followers. The
			 * transaction must be in the Writing state. */
			assert(tx->state == TX__WRITING);
			is_begin = false;
		}
	} else {
		/* We don't know about this transaction, it must be a new
		 * follower transaction. */
		rc = db__create_tx(db, c->tx_id, db->follower);
		if (rc != 0) {
			return rc;
		}
		tx = db->tx;
	}

	rc = command_frames__page_numbers(c, &page_numbers);
	if (rc != 0) {
		return rc;
	}

	command_frames__pages(c, &pages);

	rc = tx__frames(tx, is_begin, c->frames.page_size, c->frames.n_pages,
			page_numbers, pages, c->truncate, c->is_commit);

	sqlite3_free(page_numbers);

	if (rc != 0) {
		return rc;
	}

	/* If the commit flag is on, this is the final write of a transaction,
	 */
	if (c->is_commit) {
		/* Save the ID of this transaction in the buffer of recently
		 * committed transactions. */
		/* TODO: f.registry.TxnCommittedAdd(txn) */

		/* If it's a follower, we also unregister it. */
		if (!tx__is_leader(tx)) {
			db__delete_tx(db);
		}
	}

	return 0;
}

static int apply_undo(struct fsm *f, const struct command_undo *c)
{
	struct db *db;
	struct tx *tx;
	int rc;

	registry__db_by_tx_id(f->registry, c->tx_id, &db);
	tx = db->tx;
	assert(tx != NULL);

	rc = tx__undo(tx);
	if (rc != 0) {
		return rc;
	}

	/* Let's decide whether to remove the transaction from the registry or
	 * not. The following scenarios are possible:
	 *
	 * 1. This is a non-zombie leader transaction. We can assume that this
	 *    command is being applied in the context of an undo hook execution,
	 *    which will wait for the command to succeed and then remove the
	 *    transaction by itself in the end hook, so no need to remove it
	 *    here.
	 *
	 * 2. This is a follower transaction. We're done here, since undone is
	 *    a final state, so let's remove the transaction.
	 *
	 * 3. This is a zombie leader transaction. This can happen if the leader
	 *    lost leadership when applying the a non-commit frames, but the
	 *    command was still committed (either by us is we were re-elected,
	 *    or by another server if the command still reached a quorum). In
	 *    that case we're handling an Undo command to rollback a dangling
	 *    transaction, and we have to remove the zombie ourselves, because
	 *    nobody else would do it otherwise. */
	if (!tx__is_leader(tx) || tx->is_zombie) {
		db__delete_tx(db);
	}

	return 0;
}

static int fsm__apply(struct raft_fsm *fsm,
		      const struct raft_buffer *buf,
		      void **result)
{
	struct fsm *f = fsm->data;
	int type;
	void *command;
	int rc;
	rc = command__decode(buf, &type, &command);
	if (rc != 0) {
		// errorf(f->logger, "fsm: decode command: %d", rc);
		goto err;
	}
	switch (type) {
		case COMMAND_OPEN:
			rc = apply_open(f, command);
			break;
		case COMMAND_FRAMES:
			rc = apply_frames(f, command);
			break;
		case COMMAND_UNDO:
			rc = apply_undo(f, command);
			break;
		default:
			rc = RAFT_MALFORMED;
			goto err_after_command_decode;
	}
	raft_free(command);

	*result = NULL;

	return 0;

err_after_command_decode:
	raft_free(command);
err:
	return rc;
}

static int fsm__snapshot(struct raft_fsm *fsm,
			 struct raft_buffer *bufs[],
			 unsigned *n_bufs)
{
	struct fsm *f = fsm->data;
	queue *head;
	QUEUE__FOREACH(head, &f->registry->dbs)
	{
		struct db *db;
		db = QUEUE__DATA(head, struct db, queue);
	}
	(void)bufs;
	(void)n_bufs;
	return 0;
}

static int fsm__restore(struct raft_fsm *fsm, struct raft_buffer *buf)
{
	struct fsm *f = fsm->data;
	(void)buf;
	(void)f;
	return 0;
}

int fsm__init(struct raft_fsm *fsm,
	      struct config *config,
	      struct registry *registry)
{
	struct fsm *f = raft_malloc(sizeof *fsm);

	if (f == NULL) {
		return DQLITE_NOMEM;
	}

	f->logger = &config->logger;
	f->registry = registry;

	fsm->version = 1;
	fsm->data = f;
	fsm->apply = fsm__apply;
	fsm->snapshot = fsm__snapshot;
	fsm->restore = fsm__restore;

	return 0;
}

void fsm__close(struct raft_fsm *fsm)
{
	struct fsm *f = fsm->data;
	raft_free(f);
}
