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
	struct
	{
		unsigned n_pages;
		unsigned long *page_numbers;
		uint8_t *pages;
	} pending; /* For upgrades from V1 */
};

static int apply_open(struct fsm *f, const struct command_open *c)
{
	(void)f;
	(void)c;
	return 0;
}

static int add_pending_pages(struct fsm *f,
			     unsigned long *page_numbers,
			     uint8_t *pages,
			     unsigned n_pages,
			     unsigned page_size)
{
	unsigned n = f->pending.n_pages + n_pages;
	unsigned i;

	f->pending.page_numbers = sqlite3_realloc64(
	    f->pending.page_numbers, n * sizeof *f->pending.page_numbers);

	if (f->pending.page_numbers == NULL) {
		return DQLITE_NOMEM;
	}

	f->pending.pages = sqlite3_realloc64(f->pending.pages, n * page_size);

	if (f->pending.pages == NULL) {
		return DQLITE_NOMEM;
	}

	for (i = 0; i < n_pages; i++) {
		unsigned j = f->pending.n_pages + i;
		f->pending.page_numbers[j] = page_numbers[i];
		memcpy(f->pending.pages + j * page_size,
		       (uint8_t *)pages + i * page_size, page_size);
	}
	f->pending.n_pages = n;

	return 0;
}

static int apply_frames(struct fsm *f, const struct command_frames *c)
{
	struct db *db;
	sqlite3_vfs *vfs;
	unsigned long *page_numbers;
	void *pages;
	int exists;
	int rv;

	rv = registry__db_get(f->registry, c->filename, &db);
	if (rv != 0) {
		return rv;
	}

	vfs = sqlite3_vfs_find(db->config->name);

	/* Check if the database file exists, and create it by opening a
	 * connection if it doesn't. */
	rv = vfs->xAccess(vfs, c->filename, 0, &exists);
	assert(rv == 0);

	if (!exists) {
		rv = db__open_follower(db);
		if (rv != 0) {
			return rv;
		}
		sqlite3_close(db->follower);
		db->follower = NULL;
	}

	rv = command_frames__page_numbers(c, &page_numbers);
	if (rv != 0) {
		return rv;
	}

	command_frames__pages(c, &pages);

	/* If the commit marker is set, we apply the changes directly to the
	 * VFS. Otherwise, if the commit marker is not set, this must be an
	 * upgrade from V1, we accumulate uncommitted frames in memory until the
	 * final commit or a rollback. */
	if (c->is_commit) {
		if (f->pending.n_pages > 0) {
			rv = add_pending_pages(f, page_numbers, pages,
					       c->frames.n_pages,
					       db->config->page_size);
			if (rv != 0) {
				return DQLITE_NOMEM;
			}
			rv =
			    VfsApply(vfs, db->filename, f->pending.n_pages,
				     f->pending.page_numbers, f->pending.pages);
			if (rv != 0) {
				return rv;
			}
			sqlite3_free(f->pending.page_numbers);
			sqlite3_free(f->pending.pages);
			f->pending.n_pages = 0;
			f->pending.page_numbers = NULL;
			f->pending.pages = NULL;
		} else {
			rv = VfsApply(vfs, db->filename, c->frames.n_pages,
				      page_numbers, pages);
			if (rv != 0) {
				return rv;
			}
		}
	} else {
		rv =
		    add_pending_pages(f, page_numbers, pages, c->frames.n_pages,
				      db->config->page_size);
		if (rv != 0) {
			return DQLITE_NOMEM;
		}
	}

	sqlite3_free(page_numbers);

	return 0;
}

static int apply_undo(struct fsm *f, const struct command_undo *c)
{
	(void)c;

	if (f->pending.n_pages == 0) {
		return 0;
	}

	sqlite3_free(f->pending.page_numbers);
	sqlite3_free(f->pending.pages);
	f->pending.n_pages = 0;
	f->pending.page_numbers = NULL;
	f->pending.pages = NULL;

	return 0;
}

static int apply_checkpoint(struct fsm *f, const struct command_checkpoint *c)
{
	struct db *db;
	struct sqlite3_file *file;
	int size;
	int ckpt;
	int rv;

	rv = registry__db_get(f->registry, c->filename, &db);
	assert(rv == 0); /* We have registered this filename before. */

	/* Use a new connection to force re-opening the WAL. */
	rv = db__open_follower(db);
	if (rv != 0) {
		return rv;
	}

	rv = sqlite3_file_control(db->follower, "main",
				  SQLITE_FCNTL_FILE_POINTER, &file);
	assert(rv == SQLITE_OK); /* Should never fail */

	/* If there's a checkpoint lock in place, we must be the node that
	 * originated the checkpoint command in the first place, let's release
	 * it. */
	file->pMethods->xShmLock(file, 1 /* checkpoint lock */, 1,
				 SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE);

	rv = sqlite3_wal_checkpoint_v2(
	    db->follower, "main", SQLITE_CHECKPOINT_TRUNCATE, &size, &ckpt);
	if (rv != 0) {
		return rv;
	}

	sqlite3_close(db->follower);
	db->follower = NULL;

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

/* Encode the given database. */
static int encodeDatabase(struct db *db, struct raft_buffer bufs[2])
{
	struct snapshotDatabase header;
	sqlite3_vfs *vfs;
	uint32_t database_size = 0;
	uint8_t *page;
	void *cursor;
	int rv;

	header.filename = db->filename;

	vfs = sqlite3_vfs_find(db->config->name);
	rv = VfsSnapshot(vfs, db->filename, &bufs[1].base, &bufs[1].len);
	if (rv != 0) {
		goto err;
	}

	/* Extract the database size from the first page. */
	page = bufs[1].base;
	database_size += (uint32_t)(page[28] << 24);
	database_size += (uint32_t)(page[29] << 16);
	database_size += (uint32_t)(page[30] << 8);
	database_size += (uint32_t)(page[31]);

	header.main_size = database_size * db->config->page_size;
	header.wal_size = bufs[1].len - header.main_size;

	/* Database header. */
	bufs[0].len = snapshotDatabase__sizeof(&header);
	bufs[0].base = raft_malloc(bufs[0].len);
	if (bufs[0].base == NULL) {
		rv = RAFT_NOMEM;
		goto err_after_snapshot;
	}
	cursor = bufs[0].base;
	snapshotDatabase__encode(&header, &cursor);

	return 0;

err_after_snapshot:
	raft_free(bufs[1].base);
err:
	assert(rv != 0);
	return rv;
}

/* Decode the database contained in a snapshot. */
static int decodeDatabase(struct fsm *f, struct cursor *cursor)
{
	struct snapshotDatabase header;
	struct db *db;
	sqlite3_vfs *vfs;
	size_t n;
	int exists;
	int rv;

	rv = snapshotDatabase__decode(cursor, &header);
	if (rv != 0) {
		return rv;
	}
	rv = registry__db_get(f->registry, header.filename, &db);
	if (rv != 0) {
		return rv;
	}

	vfs = sqlite3_vfs_find(db->config->name);

	/* Check if the database file exists, and create it by opening a
	 * connection if it doesn't. */
	rv = vfs->xAccess(vfs, header.filename, 0, &exists);
	assert(rv == 0);

	if (!exists) {
		rv = db__open_follower(db);
		if (rv != 0) {
			return rv;
		}
		sqlite3_close(db->follower);
		db->follower = NULL;
	}

	n = header.main_size + header.wal_size;
	rv = VfsRestore(vfs, db->filename, cursor->p, n);
	if (rv != 0) {
		return rv;
	}
	cursor->p += n;

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
		if (db->tx_id != 0) {
			return RAFT_BUSY;
		}
		n++;
	}

	*n_bufs = 1;      /* Snapshot header */
	*n_bufs += n * 2; /* Database header an content */
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
		i += 2;
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
	struct fsm *f = raft_malloc(sizeof *f);

	if (f == NULL) {
		return DQLITE_NOMEM;
	}

	f->logger = &config->logger;
	f->registry = registry;
	f->pending.n_pages = 0;
	f->pending.page_numbers = NULL;
	f->pending.pages = NULL;

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
