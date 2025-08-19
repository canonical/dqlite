#include "lib/serialize.h"

#include "command.h"
#include "fsm.h"
#include "leader.h"
#include "protocol.h"
#include "raft.h"
#include "registry.h"
#include "tracing.h"
#include "vfs.h"

#include <assert.h>
#include <sys/mman.h>

struct fsmDatabaseSnapshot {
	sqlite3 *conn;
	struct raft_buffer header;
	struct vfsSnapshot files;
};

struct fsmSnapshot {
	struct raft_buffer header;
	struct fsmDatabaseSnapshot *databases;
	size_t database_count;
};

struct fsm
{
	struct logger *logger;
	struct registry *registry;
	struct fsmSnapshot snapshot;
};

/* Not used */
static int apply_open(struct fsm *f, const struct command_open *c)
{
	tracef("fsm apply open");
	(void)f;
	(void)c;
	return 0;
}

static int apply_frames(struct fsm *f, const struct command_frames *c)
{
	tracef("fsm apply frames");
	struct db *db;
	int rv;

	rv = registry__create(f->registry, c->filename, &db);
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

	int ckpt_rv = VfsCheckpoint(conn, db->config->checkpoint_threshold);
	if (ckpt_rv == SQLITE_BUSY) {
		tracef("checkpoint: busy reader or writer");
	} else if (ckpt_rv != SQLITE_OK) {
		tracef("checkpoint failed: %d", ckpt_rv);
	}

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
static int encodeSnapshotHeader(size_t n, struct raft_buffer *buf)
{
	struct snapshotHeader header;
	char *cursor;
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

/* Decode the database contained in a snapshot. */
static int decodeDatabase(struct fsm *f, struct cursor *cursor)
{
	struct snapshotDatabase header;
	int rv = snapshotDatabase__decode(cursor, &header);
	if (rv != 0) {
		return rv;
	}

	struct db *db;
	rv = registry__create(f->registry, header.filename, &db);
	if (rv != DQLITE_OK) {
		if (rv == DQLITE_NOMEM) {
			return RAFT_NOMEM;
		}
		return RAFT_ERROR;
	}

	sqlite3 *conn;
	rv = db__open(db, &conn);
	if (rv != SQLITE_OK) {
		if (rv == SQLITE_NOMEM) {
			return RAFT_NOMEM;
		}
		return RAFT_ERROR;
	}

	tracef("main_size:%" PRIu64 " wal_size:%" PRIu64, header.main_size,
	       header.wal_size);
	if (header.main_size > SIZE_MAX - header.wal_size) {
		tracef("main_size + wal_size would overflow max DB size");
		return -1;
	}

	const size_t page_size = f->registry->config->page_size;
	assert((header.main_size % page_size) == 0);
	assert(header.wal_size == 0);

	const size_t page_count = header.main_size / page_size;

	void **pages = raft_malloc(sizeof(void*) * page_count);
	if (pages == NULL) {
		sqlite3_close(conn);
		return RAFT_NOMEM;
	}
	for (size_t i = 0; i < page_count; i++) {
		pages[i] = (void*)(cursor->p + i * page_size);
	}

	const struct vfsSnapshot snapshot = {
		.main = {
			.page_count = header.main_size / page_size,
			.page_size = page_size,
			.pages = pages,
		},
		// FIXME this breaks compatibility with very old dqlite 1.17.x (the one before shallow snapshot got in)
		.wal = {
			.page_count = 0,
			.page_size = page_size,
			.pages = NULL,
		}
	};

	/* Due to the check above, this cast is safe. */
	rv = VfsRestore(conn, &snapshot);
	/* TODO: we could run PRAGMA integrity_check here or PRAGMA quick_check... */
	raft_free(pages);
	sqlite3_close(conn);
	if (rv != SQLITE_OK) {
		if (rv == SQLITE_CORRUPT) {
			return RAFT_CORRUPT;
		} else if (rv == SQLITE_NOMEM) {
			return RAFT_NOMEM;
		} else if (rv == SQLITE_BUSY) {
			return RAFT_BUSY;
		}
		return RAFT_ERROR;
	}
	cursor->p += header.main_size + header.wal_size;

	return RAFT_OK;
}

static int snapshotDatabase(struct db *db, struct fsmDatabaseSnapshot *snapshot)
{
	int rv = db__open(db, &snapshot->conn);
	if (rv == SQLITE_NOMEM) {
		return RAFT_NOMEM;
	}
	if (rv != SQLITE_OK) {
		return RAFT_ERROR;
	}

	rv = VfsCheckpoint(snapshot->conn, 0);
	if (rv == SQLITE_BUSY) {
		tracef("checkpoint: busy reader or writer");
	} else if (rv != SQLITE_OK) {
		tracef("checkpoint failed: %d", rv);
	}

	rv = VfsAcquireSnapshot(snapshot->conn, &snapshot->files);
	/* I think this can be an assert. */
	if (rv != SQLITE_OK) {
		sqlite3_close(snapshot->conn);
		if (rv == SQLITE_NOMEM) {
			return RAFT_NOMEM;
		}
		return RAFT_ERROR;
	}

	const struct snapshotDatabase header = {
		.filename = db->filename,
		.main_size = snapshot->files.main.page_count * snapshot->files.main.page_size,
		.wal_size = snapshot->files.wal.page_count * snapshot->files.wal.page_size,
	};

	const size_t header_size = snapshotDatabase__sizeof(&header);
	void *header_buffer = raft_malloc(header_size);
	if (header_buffer == NULL) {
		VfsReleaseSnapshot(snapshot->conn, &snapshot->files);
		sqlite3_close(snapshot->conn);
		return RAFT_NOMEM;
	}
	char *cursor = header_buffer;
	snapshotDatabase__encode(&header, &cursor);

	snapshot->header = (struct raft_buffer) {
		.base = header_buffer,
		.len = header_size,
	};

	return RAFT_OK;
}

static int fsm__snapshot(struct raft_fsm *fsm,
			 struct raft_buffer *bufs[],
			 unsigned *n_bufs)
{
	struct fsm *f = fsm->data;

	/* Only one running snapshot can be active per fsm */
	if (f->snapshot.header.len != 0) {
		return RAFT_BUSY;
	}

	PRE(f->snapshot.header.len == 0 && f->snapshot.header.base == NULL);
	PRE(f->snapshot.database_count == 0 && f->snapshot.databases == NULL);

	const size_t database_count = registry__size(f->registry);

	struct raft_buffer header;
	int rv = encodeSnapshotHeader(database_count, &header);
	if (rv != 0) {
		return rv;
	}

	struct fsmDatabaseSnapshot *databases =
	    raft_calloc(database_count, sizeof(*databases));
	if (databases == NULL) {
		raft_free(header.base);
		return RAFT_NOMEM;
	}

	/* First count how many databases we have and check that no checkpoint
	 * nor other snapshot is in progress. */
	size_t buffer_count = 1; /* For the snapshot header. */
	unsigned i = 0;
	queue *head;
	QUEUE_FOREACH(head, &f->registry->dbs)
	{
		struct db *db = QUEUE_DATA(head, struct db, queue);
		rv = snapshotDatabase(db, &databases[i]);
		if (rv != RAFT_OK) {
			goto err;
		}

		buffer_count += 1 /* For the database header */ +
				databases[i].files.main.page_count +
				databases[i].files.wal.page_count;
		i++;
	}

	struct raft_buffer *buffers = raft_malloc(buffer_count * sizeof(struct raft_buffer));
	if (buffers == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}
	*bufs = buffers;
	*n_bufs = (unsigned)buffer_count;

	/* Just copy all buffers in the right order. */
	buffers[0] = header;
	unsigned buff_i = 1;
	for (i = 0; i < database_count; i++) {
		assert(buff_i < buffer_count);
		buffers[buff_i] = databases[i].header;
		buff_i++;

		assert((buff_i + databases[i].files.main.page_count) <=
		       buffer_count);
		for (unsigned j = 0; j < databases[i].files.main.page_count;
		     j++) {
			buffers[buff_i] = (struct raft_buffer){
				.base = databases[i].files.main.pages[j],
				.len = databases[i].files.main.page_size,
			};
			buff_i++;
		}

		assert((buff_i + databases[i].files.wal.page_count) <=
		       buffer_count);
		for (unsigned j = 0; j < databases[i].files.wal.page_count;
		     j++) {
			buffers[buff_i] = (struct raft_buffer){
				.base = databases[i].files.wal.pages[j],
				.len = databases[i].files.wal.page_size,
			};
			buff_i++;
		}
	}
	assert(buff_i == buffer_count);

	f->snapshot = (struct fsmSnapshot){
		.header = header,
		.databases = databases,
		.database_count = database_count,
	};
	return RAFT_OK;

err:
	for (i = 0; i < database_count; i++) {
		if (databases[i].conn != NULL) {
			raft_free(databases[i].header.base);
			VfsReleaseSnapshot(databases[i].conn,
					   &databases[i].files);
			sqlite3_close(databases[i].conn);
		}
	}
	raft_free(databases);
	raft_free(header.base);
	return rv;
}

static int fsm__snapshot_finalize(struct raft_fsm *fsm,
				  struct raft_buffer *bufs[],
				  unsigned *n_bufs)
{
	(void)n_bufs;

	struct fsm *f = fsm->data;

	PRE(f->snapshot.header.len != 0 && f->snapshot.header.base != NULL);
	PRE(f->snapshot.database_count == 0 || f->snapshot.databases != NULL);

	raft_free(*bufs);
	*bufs = NULL;
	*n_bufs = 0;

	for (unsigned int i = 0; i < f->snapshot.database_count; i++) {
		if (f->snapshot.databases[i].conn != NULL) {
			raft_free(f->snapshot.databases[i].header.base);
			VfsReleaseSnapshot(f->snapshot.databases[i].conn,
					   &f->snapshot.databases[i].files);
			sqlite3_close(f->snapshot.databases[i].conn);
		}
	}
	raft_free(f->snapshot.databases);
	raft_free(f->snapshot.header.base);

	f->snapshot = (struct fsmSnapshot){};
	return RAFT_OK;
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
	*f = (struct fsm) {
		.logger = &config->logger,
		.registry = registry,
	};

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
