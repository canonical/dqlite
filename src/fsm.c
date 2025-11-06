#include <assert.h>
#include <stdint.h>
#include <sys/mman.h>

#include "command.h"
#include "fsm.h"
#include "leader.h"
#include "lib/assert.h"
#include "lib/serialize.h"
#include "protocol.h"
#include "raft.h"
#include "registry.h"
#include "tracing.h"
#include "vfs.h"

struct fsmDatabaseSnapshot {
	sqlite3 *conn;
	struct raft_buffer header;
	struct vfsSnapshot content;
};

struct fsmSnapshot {
	struct raft_buffer header;
	struct fsmDatabaseSnapshot *databases;
	size_t database_count;
};

struct fsm {
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

	rv = registry__get_or_create(f->registry, c->filename, &db);
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
		.n_pages = c->frames.n_pages,
		.page_numbers = c->frames.page_numbers,
		.pages = c->frames.pages,
	};
	rv = VfsApply(conn, &transaction);
	if (rv != 0) {
		tracef("VfsApply failed %d", rv);
		rv = rv == SQLITE_BUSY ? RAFT_BUSY : RAFT_IOERR;
		goto error;
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

static int fsm__apply(struct raft_fsm *fsm, const struct raft_buffer *buf)
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
int encodeSnapshotHeader(size_t n, struct raft_buffer *buf)
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

// static int vfsWalRestore(struct vfsWal *w,
// 			 const uint8_t *data,
// 			 size_t n,
// 			 uint32_t page_size)
// {
// 	struct vfsFrame **frames;
// 	unsigned n_frames;
// 	unsigned i;
// 	size_t offset;
// 	int rv;

// 	if (n == 0) {
// 		return 0;
// 	}

// 	assert(w->n_tx == 0);

// 	assert(n > VFS__WAL_HEADER_SIZE);
// 	assert(((n - (size_t)VFS__WAL_HEADER_SIZE) %
// 		((size_t)vfsFrameSize(page_size))) == 0);

// 	n_frames = (unsigned)((n - (size_t)VFS__WAL_HEADER_SIZE) /
// 			      ((size_t)vfsFrameSize(page_size)));

// 	frames = sqlite3_malloc64(sizeof *frames * n_frames);
// 	if (frames == NULL) {
// 		goto oom;
// 	}

// 	for (i = 0; i < n_frames; i++) {
// 		struct vfsFrame *frame = vfsFrameCreate(page_size);
// 		const uint8_t *p;

// 		if (frame == NULL) {
// 			unsigned j;
// 			for (j = 0; j < i; j++) {
// 				vfsFrameDestroy(frames[j]);
// 			}
// 			goto oom_after_frames_alloc;
// 		}
// 		frames[i] = frame;

// 		offset = (size_t)VFS__WAL_HEADER_SIZE +
// 			 ((size_t)i * (size_t)vfsFrameSize(page_size));
// 		p = &data[offset];
// 		memcpy(frame->header, p, VFS__FRAME_HEADER_SIZE);
// 		memcpy(frame->page, p + VFS__FRAME_HEADER_SIZE, page_size);
// 	}

// 	memcpy(w->hdr, data, VFS__WAL_HEADER_SIZE);

// 	rv = vfsWalTruncate(w, 0);
// 	assert(rv == 0);

// 	w->frames = frames;
// 	w->n_frames = n_frames;

// 	return 0;

// oom_after_frames_alloc:
// 	sqlite3_free(frames);
// oom:
// 	return DQLITE_NOMEM;
// }

static int decodeDatabase(const struct registry *r,
			  struct cursor *cursor,
			  struct vfsSnapshot *snapshot,
			  const char **filename)
{
	struct snapshotDatabase header;
	int rv = snapshotDatabase__decode(cursor, &header);
	if (rv != 0) {
		return RAFT_INVALID;
	}

	tracef("main_size:%" PRIu64 " wal_size:%" PRIu64, header.main_size,
	       header.wal_size);
	if (header.main_size > SIZE_MAX - header.wal_size) {
		tracef("main_size + wal_size would overflow max DB size");
		return RAFT_INVALID;
	}

	const size_t page_size = r->config->vfs.page_size;
	dqlite_assert((header.main_size % page_size) == 0);

	size_t page_count = (size_t)header.main_size / page_size;
	void **pages = raft_malloc(sizeof(void *) * page_count);
	if (pages == NULL) {
		return RAFT_NOMEM;
	}
	for (size_t i = 0; i < page_count; i++) {
		pages[i] = (void *)(cursor->p + i * page_size);
	}
	cursor->p += header.main_size;

	const size_t wal_header_size = 32;
	if (header.wal_size > wal_header_size) {
		tracef("pre 1.17 snapshot loading");
		const size_t wal_frame_header_size = 24;
		const size_t wal_frame_size = page_size + wal_frame_header_size;

		dqlite_assert(header.wal_size > wal_header_size);
		dqlite_assert(((header.wal_size - (size_t)wal_header_size) %
			       wal_frame_size) == 0);

		const unsigned n_frames =
		    (unsigned)((header.wal_size - (size_t)wal_header_size) /
			       wal_frame_size);
		const char *wal = cursor->p;
		const void *last_frame =
		    (const uint8_t *)wal + wal_header_size +
		    n_frames * wal_frame_size - wal_frame_size;

		const size_t wal_page_count = ByteGetBe32(last_frame + 4);
		if (wal_page_count > page_count) {
			void *wal_pages = raft_realloc(
			    pages, sizeof(void *) * wal_page_count);
			if (wal_pages == NULL) {
				raft_free(pages);
				return RAFT_NOMEM;
			}
			pages = wal_pages;
			page_count = wal_page_count;
		}

		/* Read pages in the WAL order */
		for (const char *frame = wal + wal_header_size;
		     frame < wal + header.wal_size; frame += wal_frame_size) {
			uint32_t page_number =
			    ByteGetBe32((const uint8_t *)frame);
			if (page_number > page_count) {
				continue;
			}
			pages[page_number - 1] =
			    (void *)(frame + wal_frame_header_size);
		}
	}
	cursor->p += header.wal_size;

	*snapshot = (struct vfsSnapshot){
		.page_count = page_count,
		.page_size = page_size,
		.pages = pages,
	};
	*filename = header.filename;

	return RAFT_OK;
}

static int integrityCheckCb(void *pArg, int n, char **values, char **names)
{
	bool *check_passed = pArg;

	PRE(check_passed != NULL);
	PRE(n == 1);
	PRE(sqlite3_stricmp(names[0], "quick_check") == 0);

	if (sqlite3_stricmp(values[0], "ok") != 0) {
		tracef("PRAGMA quick_check: %s", values[0]);
		*check_passed = false;
	}

	return SQLITE_OK;
}

static int restoreDatabase(struct registry *r,
			   const char *filename,
			   const struct vfsSnapshot *snapshot)
{
	struct db *db;
	int rv = registry__get_or_create(r, filename, &db);
	if (rv != DQLITE_OK) {
		return rv == DQLITE_NOMEM ? RAFT_NOMEM : RAFT_ERROR;
	}

	sqlite3 *conn;
	rv = db__open(db, &conn);
	if (rv != SQLITE_OK) {
		return rv == SQLITE_NOMEM ? RAFT_NOMEM : RAFT_ERROR;
	}
	rv = VfsRestore(conn, snapshot);

	if (rv == SQLITE_OK) {
		bool check_passed = true;
		char *errmsg;
		rv = sqlite3_exec(conn, "PRAGMA quick_check", integrityCheckCb,
				  &check_passed, &errmsg);
		if (rv != SQLITE_OK) {
			tracef("PRAGMA quick_check failed: %s (%d)", errmsg,
			       rv);
		} else if (!check_passed) {
			rv = SQLITE_CORRUPT;
		}
	}

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

	rv = VfsCheckpoint(snapshot->conn);
	if (rv == SQLITE_BUSY) {
		tracef("checkpoint: busy reader or writer");
	} else if (rv != SQLITE_OK) {
		tracef("checkpoint failed: %d", rv);
	}

	rv = VfsAcquireSnapshot(snapshot->conn, &snapshot->content);
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
		.main_size =
		    snapshot->content.page_count * snapshot->content.page_size,
		.wal_size = 0,
	};

	const size_t header_size = snapshotDatabase__sizeof(&header);
	void *header_buffer = raft_malloc(header_size);
	if (header_buffer == NULL) {
		VfsReleaseSnapshot(snapshot->conn, &snapshot->content);
		sqlite3_close(snapshot->conn);
		return RAFT_NOMEM;
	}
	char *cursor = header_buffer;
	snapshotDatabase__encode(&header, &cursor);

	snapshot->header = (struct raft_buffer){
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
				databases[i].content.page_count;
		i++;
	}

	struct raft_buffer *buffers =
	    raft_malloc(buffer_count * sizeof(struct raft_buffer));
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
		dqlite_assert(buff_i < buffer_count);
		buffers[buff_i] = databases[i].header;
		buff_i++;

		dqlite_assert((buff_i + databases[i].content.page_count) <=
			      buffer_count);
		for (unsigned j = 0; j < databases[i].content.page_count; j++) {
			buffers[buff_i] = (struct raft_buffer){
				.base = databases[i].content.pages[j],
				.len = databases[i].content.page_size,
			};
			buff_i++;
		}
	}
	dqlite_assert(buff_i == buffer_count);

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
					   &databases[i].content);
			sqlite3_close(databases[i].conn);
		}
	}
	raft_free(databases);
	raft_free(header.base);
	return rv;
}

static int fsm__snapshot_finalize(struct raft_fsm *fsm,
				  struct raft_buffer bufs[],
				  unsigned n_bufs)
{
	(void)n_bufs;

	struct fsm *f = fsm->data;

	PRE(f->snapshot.header.len != 0 && f->snapshot.header.base != NULL);
	PRE(f->snapshot.database_count == 0 || f->snapshot.databases != NULL);

	raft_free(bufs);

	for (unsigned int i = 0; i < f->snapshot.database_count; i++) {
		if (f->snapshot.databases[i].conn != NULL) {
			raft_free(f->snapshot.databases[i].header.base);
			VfsReleaseSnapshot(f->snapshot.databases[i].conn,
					   &f->snapshot.databases[i].content);
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
	struct cursor cursor = { buf->base, buf->len };
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
		struct vfsSnapshot snapshot;
		const char *filename;
		rv = decodeDatabase(f->registry, &cursor, &snapshot, &filename);
		if (rv != RAFT_OK) {
			tracef("decode failed");
			return rv;
		}
		rv = restoreDatabase(f->registry, filename, &snapshot);
		raft_free(snapshot.pages);
		if (rv != RAFT_OK) {
			tracef("restore failed");
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
	*f = (struct fsm){
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
