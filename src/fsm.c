#include <raft.h>

#include "lib/assert.h"
#include "lib/serialize.h"

#include "command.h"
#include "fsm.h"
#include "vfs.h"

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

	rc = tx__frames(tx, is_begin, c->frames.page_size,
			(int)c->frames.n_pages, page_numbers, pages,
			c->truncate, c->is_commit);

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

static int apply_checkpoint(struct fsm *f, const struct command_checkpoint *c)
{
	struct db *db;
	int size;
	int ckpt;
	int rv;

	rv = registry__db_get(f->registry, c->filename, &db);
	assert(rv == 0);        /* We have registered this filename before. */
	assert(db->tx == NULL); /* No transaction is in progress. */

	rv = sqlite3_wal_checkpoint_v2(
	    db->follower, "main", SQLITE_CHECKPOINT_TRUNCATE, &size, &ckpt);
	if (rv != 0) {
		return rv;
	}

	/* Since no reader transaction is in progress, we must be able to
	 * checkpoint the entire WAL */
	assert(size == 0);
	assert(ckpt == 0);

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
		case COMMAND_CHECKPOINT:
			rc = apply_checkpoint(f, command);
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

#define SNAPSHOT_FORMAT 1

#define SNAPSHOT_HEADER(X, ...)          \
	X(uint64, format, ##__VA_ARGS__) \
	X(uint64, n, ##__VA_ARGS__)
SERIALIZE__DEFINE(snapshotHeader, SNAPSHOT_HEADER);
SERIALIZE__IMPLEMENT(snapshotHeader, SNAPSHOT_HEADER);

#define SNAPSHOT_DATABASE(X, ...)           \
	X(text, filename, ##__VA_ARGS__)    \
	X(uint64, main_size, ##__VA_ARGS__) \
	X(uint64, wal_size, ##__VA_ARGS__)
SERIALIZE__DEFINE(snapshotDatabase, SNAPSHOT_DATABASE);
SERIALIZE__IMPLEMENT(snapshotDatabase, SNAPSHOT_DATABASE);

/* Encode the global snapshot header. */
static int encodeSnapshotHeader(unsigned n, struct raft_buffer *buf)
{
	struct snapshotHeader header;
	void *cursor;
	header.format = SNAPSHOT_FORMAT;
	header.n = n;
	buf->len = snapshotHeader__sizeof(&header);
	buf->base = raft_malloc(buf->len);
	if (buf->base == NULL) {
		return RAFT_NOMEM;
	}
	cursor = buf->base;
	snapshotHeader__encode(&header, &cursor);
	return 0;
}

/* Generate the WAL filename associated with the given main database
 * filename. */
static char *generateWalFilename(const char *filename)
{
	char *out;
	out = sqlite3_malloc((int)(strlen(filename) + strlen("-wal") + 1));
	if (out == NULL) {
		return out;
	}
	sprintf(out, "%s-wal", filename);
	return out;
}

/* Encode the given database. */
static int encodeDatabase(struct db *db, struct raft_buffer bufs[3])
{
	struct snapshotDatabase header;
	char *walFilename;
	void *cursor;
	int rv;

	walFilename = generateWalFilename(db->filename);
	if (walFilename == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}

	header.filename = db->filename;

	/* Main database file. */
	rv = VfsFileRead(db->config->name, db->filename, &bufs[1].base,
			 &bufs[1].len);
	if (rv != 0) {
		goto err_after_wal_filename_alloc;
	}
	header.main_size = bufs[1].len;

	/* WAL file. */
	rv = VfsFileRead(db->config->name, walFilename, &bufs[2].base,
			 &bufs[2].len);
	if (rv != 0) {
		goto err_after_main_file_read;
	}
	header.wal_size = bufs[2].len;

	/* Database header. */
	bufs[0].len = snapshotDatabase__sizeof(&header);
	bufs[0].base = raft_malloc(bufs[0].len);
	if (bufs[0].base == NULL) {
		rv = RAFT_NOMEM;
		goto err_after_wal_file_read;
	}
	cursor = bufs[0].base;
	snapshotDatabase__encode(&header, &cursor);

	sqlite3_free(walFilename);

	return 0;

err_after_wal_file_read:
	sqlite3_free(bufs[2].base);
err_after_main_file_read:
	sqlite3_free(bufs[1].base);
err_after_wal_filename_alloc:
	sqlite3_free(walFilename);
err:
	assert(rv != 0);
	return rv;
}

/* Decode the database contained in a snapshot. */
static int decodeDatabase(struct fsm *f, struct cursor *cursor)
{
	struct snapshotDatabase header;
	struct db *db;
	char *walFilename;
	int rv;

	rv = snapshotDatabase__decode(cursor, &header);
	if (rv != 0) {
		return rv;
	}
	rv = registry__db_get(f->registry, header.filename, &db);
	if (rv != 0) {
		return rv;
	}
	rv = VfsFileWrite(db->config->name, db->filename, cursor->p,
			  header.main_size);
	if (rv != 0) {
		return rv;
	}
	walFilename = generateWalFilename(db->filename);
	if (walFilename == NULL) {
		return RAFT_NOMEM;
	}
	cursor->p += header.main_size;
	if (header.wal_size > 0) {
		rv = VfsFileWrite(db->config->name, walFilename, cursor->p,
				  header.wal_size);
		if (rv != 0) {
			sqlite3_free(walFilename);
			return rv;
		}
	}
	cursor->p += header.wal_size;
	sqlite3_free(walFilename);

	rv = db__open_follower(db);
	if (rv != 0) {
		return rv;
	}

	return 0;
}

static int fsm__snapshot(struct raft_fsm *fsm,
			 struct raft_buffer *bufs[],
			 unsigned *n_bufs)
{
	struct fsm *f = fsm->data;
	queue *head;
	struct db *db;
	unsigned n = 0;
	unsigned i;
	int rv;

	/* First count how many databases we have and check that no transaction
	 * is in progress. */
	QUEUE__FOREACH(head, &f->registry->dbs)
	{
		db = QUEUE__DATA(head, struct db, queue);
		if (db->tx != NULL) {
			return RAFT_BUSY;
		}
		n++;
	}

	*n_bufs = 1;      /* Snapshot header */
	*n_bufs += n * 3; /* Database header, main and wal */
	*bufs = raft_malloc(*n_bufs * sizeof **bufs);
	if (*bufs == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}

	rv = encodeSnapshotHeader(n, &(*bufs)[0]);
	if (rv != 0) {
		goto err_after_bufs_alloc;
	}

	/* Encode individual databases. */
	i = 1;
	QUEUE__FOREACH(head, &f->registry->dbs)
	{
		db = QUEUE__DATA(head, struct db, queue);
		rv = encodeDatabase(db, &(*bufs)[i]);
		if (rv != 0) {
			goto err_after_encode_header;
		}
		i += 3;
	}

	return 0;

err_after_encode_header:
	do {
		raft_free((*bufs)[i].base);
		i--;
	} while (i > 0);
err_after_bufs_alloc:
	raft_free(*bufs);
err:
	assert(rv != 0);
	return rv;
}

static int fsm__restore(struct raft_fsm *fsm, struct raft_buffer *buf)
{
	struct fsm *f = fsm->data;
	struct cursor cursor = {buf->base, buf->len};
	struct snapshotHeader header;
	unsigned i;
	int rv;

	rv = snapshotHeader__decode(&cursor, &header);
	if (rv != 0) {
		return rv;
	}
	if (header.format != SNAPSHOT_FORMAT) {
		return RAFT_MALFORMED;
	}

	for (i = 0; i < header.n; i++) {
		rv = decodeDatabase(f, &cursor);
		if (rv != 0) {
			return rv;
		}
	}

	raft_free(buf->base);

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
