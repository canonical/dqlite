#include "lib/serialize.h"

#include "command.h"
#include "fsm.h"
#include "leader.h"
#include "protocol.h"
#include "raft.h"
#include "tracing.h"
#include "vfs.h"

#include <assert.h>
#include <sys/mman.h>

struct fsm
{
	struct logger *logger;
	struct registry *registry;
};

/* Not used */
static int apply_open(struct fsm *f, const struct command_open *c)
{
	tracef("fsm apply open");
	(void)f;
	(void)c;
	return 0;
}

static int databaseReadLock(struct db *db)
{
	if (!db->read_lock) {
		db->read_lock = 1;
		return 0;
	} else {
		return -1;
	}
}

static int databaseReadUnlock(struct db *db)
{
	if (db->read_lock) {
		db->read_lock = 0;
		return 0;
	} else {
		return -1;
	}
}

static void maybeCheckpoint(struct db *db, sqlite3 *conn)
{
	tracef("maybe checkpoint");
	int rv;

	/* Don't run when a snapshot is busy. Running a checkpoint while a
	 * snapshot is busy will result in illegal memory accesses by the
	 * routines that try to access database page pointers contained in the
	 * snapshot. */
	rv = databaseReadLock(db);
	if (rv != 0) {
		tracef("busy snapshot %d", rv);
		return;
	}

	rv = VfsCheckpoint(conn, db->config->checkpoint_threshold);
	if (rv == SQLITE_BUSY) {
		tracef("checkpoint: busy reader or writer");
	} else if (rv != SQLITE_OK) {
		tracef("checkpoint failed: %d", rv);
	}

	rv = databaseReadUnlock(db);
	assert(rv == 0);
}

static int apply_frames(struct fsm *f, const struct command_frames *c)
{
	tracef("fsm apply frames");
	struct db *db;
	int rv;

	rv = registry__db_get(f->registry, c->filename, &db);
	if (rv != 0) {
		tracef("db get failed %d", rv);
		return rv;
	}

	sqlite3 *conn = NULL;
	if (db->active_leader != NULL) {
		/* Leader transaction */
		conn = db->active_leader->conn;
	} else {
		/* Follower transaction */
		rv = db__open(db, &conn);
		if (rv != 0) {
			tracef("open follower failed %d", rv);
			return rv;
		}
	}

	/* The commit marker must be set as otherwise this must be an
	 * upgrade from V1, which is not supported anymore. */
	if (!c->is_commit) {
		rv = DQLITE_PROTO;
		goto error;
	}

	struct vfsTransaction transaction = {
		.n_pages      = c->frames.n_pages,
		.page_numbers = c->frames.page_numbers,
		.pages   	  = c->frames.pages,
	};
	rv = VfsApply(conn, &transaction);
	if (rv != 0) {
		tracef("VfsApply failed %d", rv);
		rv = rv == SQLITE_BUSY ? RAFT_BUSY : RAFT_IOERR;
		goto error;
	}

	maybeCheckpoint(db, conn);
error:
	if (db->active_leader == NULL) {
		sqlite3_close(conn);
	}
	sqlite3_free(c->frames.page_numbers);
	sqlite3_free(c->frames.pages);
	return rv;
}

/* Not used */
static int apply_undo(struct fsm *f, const struct command_undo *c)
{
	(void)f;
	(void)c;
	tracef("apply undo %" PRIu64, c->tx_id);
	return 0;
}

/* Checkpoints used to be coordinated cluster-wide, these days a node
 * checkpoints independently in `apply_frames`, the checkpoint command becomes a
 * no-op for modern nodes. */
static int apply_checkpoint(struct fsm *f, const struct command_checkpoint *c)
{
	(void)f;
	(void)c;
	tracef("apply no-op checkpoint");
	return 0;
}

static int fsm__apply(struct raft_fsm *fsm,
		      const struct raft_buffer *buf,
		      void **result)
{
	tracef("fsm apply");
	struct fsm *f = fsm->data;
	int type;
	void *command;
	int rc;
	rc = command__decode(buf, &type, &command);
	if (rc != 0) {
		tracef("fsm: decode command: %d", rc);
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
			break;
	}

	raft_free(command);
err:
	*result = NULL;
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
	char *cursor;
	header.format = SNAPSHOT_FORMAT;
	header.n = n;
	buf->len = snapshotHeader__sizeof(&header);
	buf->base = sqlite3_malloc64(buf->len);
	if (buf->base == NULL) {
		return RAFT_NOMEM;
	}
	cursor = buf->base;
	snapshotHeader__encode(&header, &cursor);
	return 0;
}

/* Encode the given database. */
static int encodeDatabase(struct db *db,
			  struct raft_buffer r_bufs[],
			  uint32_t n)
{
	struct snapshotDatabase header;
	char *cursor;
	struct dqlite_buffer *bufs = (struct dqlite_buffer *)r_bufs;
	int rv;

	header.filename = db->filename;
	header.main_size = (n - 1) * (uint64_t)db->config->page_size;
	/* The database is checkpointed before writing it to disk.  As such,
	 * wal_size is always 0. */
	header.wal_size = 0;

	rv = VfsShallowSnapshot(db->vfs, db->filename, &bufs[1], n - 1);
	if (rv != 0) {
		goto err;
	}

	/* Database header. */
	bufs[0].len = snapshotDatabase__sizeof(&header);
	bufs[0].base = sqlite3_malloc64(bufs[0].len);
	if (bufs[0].base == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}
	cursor = bufs[0].base;
	snapshotDatabase__encode(&header, &cursor);

	return 0;

err:
	assert(rv != 0);
	return rv;
}

/* Decode the database contained in a snapshot. */
static int decodeDatabase(struct fsm *f, struct cursor *cursor)
{
	struct snapshotDatabase header;
	struct db *db;
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

	if (db->leaders > 0) {
		return RAFT_BUSY;
	}

	/* Check if the database file exists, and create it by opening a
	 * connection if it doesn't. */
	rv = db->vfs->xAccess(db->vfs, header.filename, 0, &exists);
	assert(rv == 0);

	if (!exists) {
		sqlite3 *conn;
		rv = db__open(db, &conn);
		if (rv != 0) {
			return rv;
		}
		sqlite3_close(conn);
	}

	tracef("main_size:%" PRIu64 " wal_size:%" PRIu64, header.main_size,
	       header.wal_size);
	if (header.main_size + header.wal_size > SIZE_MAX) {
		tracef("main_size + wal_size would overflow max DB size");
		return -1;
	}

	/* Due to the check above, this cast is safe. */
	n = (size_t)(header.main_size + header.wal_size);
	rv = VfsRestore(db->vfs, db->filename, cursor->p, n);
	if (rv != 0) {
		return rv;
	}
	cursor->p += n;

	return 0;
}

static unsigned dbNumPages(struct db *db)
{
	int rv;
	uint32_t n;

	rv = VfsDatabaseNumPages(db->vfs, db->filename, true, &n);
	assert(rv == 0);
	return n;
}

/* Determine the total number of raft buffers needed for a snapshot */
static unsigned snapshotNumBufs(struct fsm *f)
{
	struct db *db;
	queue *head;
	unsigned n = 1; /* snapshot header */

	QUEUE_FOREACH(head, &f->registry->dbs)
	{
		n += 1; /* database header */
		db = QUEUE_DATA(head, struct db, queue);
		n += dbNumPages(db); /* 1 buffer per page (zero copy) */
	}

	return n;
}

/* An example array of snapshot buffers looks like this:
 *
 * bufs:  SH DH1 P1 P2 P3 DH2 P1 P2
 * index:  0   1  2  3  4   6  7  8
 *
 * SH:   Snapshot Header
 * DHx:  Database Header
 * Px:   Database Page (not to be freed)
 * */
static void freeSnapshotBufs(struct fsm *f,
			     struct raft_buffer bufs[],
			     unsigned n_bufs)
{
	(void)f;
	unsigned i;
	uint64_t skip_size;
	struct cursor cursor;
	struct snapshotDatabase header;

	if (bufs == NULL || n_bufs == 0) {
		return;
	}

	/* Free snapshot header */
	sqlite3_free(bufs[0].base);

	/* Free all database headers */
	
	i = 1;
	skip_size = 0;
	for (i = 1, skip_size = 0; i < n_bufs; i++) {
		if (skip_size >= bufs[i].len) {
			skip_size -= bufs[i].len;
		} else {
			assert(skip_size == 0);
			cursor.p = bufs[i].base;
			cursor.cap = bufs[i].len;
			int rv = snapshotDatabase__decode(&cursor, &header);
			assert(rv == 0);
			sqlite3_free(bufs[i].base);

			skip_size = header.main_size;
		}
	}
	assert(skip_size == 0);
}

static int fsm__snapshot(struct raft_fsm *fsm,
			 struct raft_buffer *bufs[],
			 unsigned *n_bufs)
{
	struct fsm *f = fsm->data;
	queue *head;
	struct db *db;
	unsigned n_db = 0;
	unsigned i;
	int rv;

	/* First count how many databases we have and check that no checkpoint 
	 * nor other snapshot is in progress. */
	QUEUE_FOREACH(head, &f->registry->dbs)
	{
		db = QUEUE_DATA(head, struct db, queue);
		if (db->read_lock) {
			return RAFT_BUSY;
		}
		n_db++;
	}

	/* Lock all databases, preventing the checkpoint from running */
	QUEUE_FOREACH(head, &f->registry->dbs)
	{
		db = QUEUE_DATA(head, struct db, queue);
		rv = databaseReadLock(db);
		assert(rv == 0);
	}

	*n_bufs = snapshotNumBufs(f);
	*bufs = sqlite3_malloc64(*n_bufs * sizeof **bufs);
	if (*bufs == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}

	rv = encodeSnapshotHeader(n_db, &(*bufs)[0]);
	if (rv != 0) {
		goto err_after_bufs_alloc;
	}

	/* Encode individual databases. */
	i = 1;
	QUEUE_FOREACH(head, &f->registry->dbs)
	{
		db = QUEUE_DATA(head, struct db, queue);
		/* database_header + num_pages */
		unsigned n = 1 + dbNumPages(db);
		rv = encodeDatabase(db, &(*bufs)[i], n);
		if (rv != 0) {
			goto err_after_encode_header;
		}
		i += n;
	}

	assert(i == *n_bufs);
	return 0;

err_after_encode_header:
	freeSnapshotBufs(f, *bufs, i);
err_after_bufs_alloc:
	sqlite3_free(*bufs);
err:
	QUEUE_FOREACH(head, &f->registry->dbs)
	{
		db = QUEUE_DATA(head, struct db, queue);
		databaseReadUnlock(db);
	}
	assert(rv != 0);
	return rv;
}

static int fsm__snapshot_finalize(struct raft_fsm *fsm,
				  struct raft_buffer *bufs[],
				  unsigned *n_bufs)
{
	struct fsm *f = fsm->data;
	queue *head;
	struct db *db;
	unsigned n_db;
	struct snapshotHeader header;
	int rv;

	if (bufs == NULL) {
		return 0;
	}

	/* Decode the header to determine the number of databases. */
	struct cursor cursor = {(*bufs)[0].base, (*bufs)[0].len};
	rv = snapshotHeader__decode(&cursor, &header);
	if (rv != 0) {
		tracef("decode failed %d", rv);
		return -1;
	}
	if (header.format != SNAPSHOT_FORMAT) {
		tracef("bad format");
		return -1;
	}

	/* Free allocated buffers */
	freeSnapshotBufs(f, *bufs, *n_bufs);
	sqlite3_free(*bufs);
	*bufs = NULL;
	*n_bufs = 0;

	/* Unlock all databases that were locked for the snapshot, this is safe
	 * because DB's are only ever added at the back of the queue. */
	n_db = 0;
	QUEUE_FOREACH(head, &f->registry->dbs)
	{
		if (n_db == header.n) {
			break;
		}
		db = QUEUE_DATA(head, struct db, queue);
		rv = databaseReadUnlock(db);
		assert(rv == 0);
		n_db++;
	}

	return 0;
}

static int fsm__restore(struct raft_fsm *fsm, struct raft_buffer *buf)
{
	tracef("fsm restore");
	struct fsm *f = fsm->data;
	struct cursor cursor = {buf->base, buf->len};
	struct snapshotHeader header;
	unsigned i;
	int rv;

	rv = snapshotHeader__decode(&cursor, &header);
	if (rv != 0) {
		tracef("decode failed %d", rv);
		return rv;
	}
	if (header.format != SNAPSHOT_FORMAT) {
		tracef("bad format");
		return RAFT_MALFORMED;
	}

	for (i = 0; i < header.n; i++) {
		rv = decodeDatabase(f, &cursor);
		if (rv != 0) {
			tracef("decode failed");
			return rv;
		}
	}

	/* Don't use sqlite3_free as this buffer is allocated by raft. */
	raft_free(buf->base);

	return 0;
}

int fsm__init(struct raft_fsm *fsm,
	      struct config *config,
	      struct registry *registry)
{
	tracef("fsm init");
	struct fsm *f = raft_malloc(sizeof *f);

	if (f == NULL) {
		return DQLITE_NOMEM;
	}

	f->logger = &config->logger;
	f->registry = registry;

	fsm->version = 2;
	fsm->data = f;
	fsm->apply = fsm__apply;
	fsm->snapshot = fsm__snapshot;
	fsm->snapshot_finalize = fsm__snapshot_finalize;
	fsm->restore = fsm__restore;

	return 0;
}

void fsm__close(struct raft_fsm *fsm)
{
	tracef("fsm close");
	struct fsm *f = fsm->data;
	raft_free(f);
}
