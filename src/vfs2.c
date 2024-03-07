#include "vfs2.h"

#include "lib/byte.h"
#include "lib/queue.h"
#include "lib/sm.h"
#include "tracing.h"
#include "utils.h"

#include <pthread.h>
#include <sqlite3.h>

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define VFS2_WAL_FIXED_SUFFIX1 "-xwal1"
#define VFS2_WAL_FIXED_SUFFIX2 "-xwal2"

#define VFS2_WAL_INDEX_REGION_SIZE (1 << 15)
#define VFS2_WAL_FRAME_HDR_SIZE 24

#define VFS2_EXCLUSIVE UINT_MAX

#define VFS2_SHM_WRITE_LOCK 0

static const uint32_t invalid_magic = 0x17171717;

/*

                                                                +--------------------NOT_OPEN
                                                                |                    |
                                                                | xOpen("foo-wal")   | xOpen("foo-wal")
                                                                |                    |
                                                                |                    V
                                                                |                    EMPTY
                                                                |      xWrite(hdr)   |
                                                                |  +-----------------+
                                                                |  |
                                           xWrite(frames)       V  V
                                     +--------------------------BASE-----------------------------+
                                     |                          ^  ^     vfs2_apply_uncommitted     |
                                     |                          |  |                                |
                                     V     vfs2_abort           |  |     vfs2_commit, vfs2_abort    V
                                ACTIVE--------------------------+  +--------------------------------FOLLOWING
                                     |                          |
                     COMMIT_PHASETWO |                          |
                                     V     vfs2_abort           |
                                HIDDEN--------------------------+
                                     |                          |
            vf2_{poll, shallow_poll} |                          |
                                     V     vfs2_{abort,commit}  |
                                POLLED--------------------------+

*/

enum {
	WTX_NOT_OPEN, /* WAL not yet opened */
	WTX_EMPTY, /* WAL opened but nothing in either of the backing files */
	WTX_BASE,  /* WAL opened and WAL-cur has at least the header written,
		      with nothing pending */
	WTX_ACTIVE,
	WTX_HIDDEN,
	WTX_POLLED,
	WTX_FOLLOWING,
};

static const struct sm_conf wtx_states[SM_STATES_MAX] = {
        [WTX_NOT_OPEN] = {
                .flags = SM_INITIAL|SM_FINAL,
                .name = "initial",
                .allowed = BITS(WTX_NOT_OPEN)|BITS(WTX_EMPTY)|BITS(WTX_BASE),
        },
	[WTX_EMPTY] = {
		.flags = 0,
		.name = "empty",
		.allowed = BITS(WTX_BASE)|BITS(WTX_NOT_OPEN),
	},
	[WTX_BASE] = {
		.flags = 0,
		.name = "base",
		.allowed = BITS(WTX_BASE)|BITS(WTX_ACTIVE)|BITS(WTX_NOT_OPEN),
	},
        [WTX_ACTIVE] = {
                .flags = 0,
                .name = "active",
                .allowed = BITS(WTX_BASE)|BITS(WTX_ACTIVE)|BITS(WTX_HIDDEN),
        },
        [WTX_HIDDEN] = {
                .flags = 0,
                .name = "hidden",
                .allowed = BITS(WTX_BASE)|BITS(WTX_POLLED)|BITS(WTX_NOT_OPEN),
        },
        [WTX_POLLED] = {
                .flags = 0,
                .name = "polled",
                .allowed = BITS(WTX_BASE),
        },
};

/**
 * Userdata owned by the VFS.
 */
struct vfs2_data {
	sqlite3_vfs *orig;       /* underlying VFS */
	pthread_rwlock_t rwlock; /* protects the queue */
	atomic_uint page_size;
	queue queue; /* queue of vfs2_db_entry */
};

/**
 * Linked list element representing a single database/WAL pair.
 */
struct vfs2_db_entry {
	struct vfs2_file *db;
	struct vfs2_file *wal;

	struct sm wtx_sm;

	char *db_name;
	struct vfs2_wal_slice wal_limit;

	queue link;
};

/**
 * Layout-compatible with the first part of the WAL index header.
 */
struct vfs2_wal_index_basic_hdr {
	uint32_t iVersion;
	uint8_t unused[4];
	uint32_t iChange;
	uint8_t isInit;
	uint8_t bigEndCksum;
	uint16_t szPage;
	uint32_t mxFrame;
	uint32_t nPage;
	uint32_t aFrameCksum[2];
	struct vfs2_salts salts;
	uint32_t aCksum[2];
};

struct vfs2_wal_hdr {
	uint8_t magic[4];
	uint8_t version[4];
	uint8_t page_size[4];
	uint8_t ckpoint_seqno[4];
	struct vfs2_salts salts;
	uint8_t cksum1[4];
	uint8_t cksum2[4];
};

struct vfs2_wal_index_full_hdr {
	struct vfs2_wal_index_basic_hdr basic[2];
	uint32_t nBackfill;
	uint32_t marks[5];
	uint8_t locks[SQLITE_SHM_NLOCK];
	uint32_t nBackfillAttempted;
	uint8_t unused[4];
};

/**
 * View of the zeroth shm region, which contains the WAL index header.
 */
union vfs2_shm_region0 {
	struct vfs2_wal_index_full_hdr hdr;
	char bytes[VFS2_WAL_INDEX_REGION_SIZE];
};

struct vfs2_wal {
	const char *moving_name;   /* e.g. /path/to/my.db-wal */
	char *wal_cur_fixed_name;  /* e.g. /path/to/my.db-xwal1 */
	sqlite3_file *wal_prev;    /* underlying file object for WAL-prev */
	char *wal_prev_fixed_name; /* e.g. /path/to/my.db-xwal2 */

	uint32_t commit_end; /* frame index, zero-based, should be in sync with mxFrame */

	/* All pending_txn fields pertain to a transaction that has at least one
	 * frame in the WAL and is the last transaction represented in the WAL.
	 * Writing a frame either updates the pending transaction or starts a
	 * new transaction. A frame starts a new transaction if it is written at
	 * the end of the WAL and the physically preceding frame has a nonzero
	 * commit marker. */
	dqlite_vfs_frame *pending_txn_frames; /* for vfs2_poll */
	uint32_t pending_txn_len;
	uint32_t pending_txn_last_frame_commit; /* commit marker for the
						   physical last frame */
};

struct vfs2_db {
	const char *name; /* e.g. /path/to/my.db */

	/* Copy of the WAL index header that reflects the last really-committed
	 * (i.e. in Raft too) transaction, or the initial state of the WAL if
	 * no transactions have been committed yet. */
	struct vfs2_wal_index_basic_hdr prev_txn_hdr;
	/* Copy of the WAL index header that reflects a sorta-committed
	 * transaction that has not yet been through Raft, or all zeros
	 * if no transaction fits this description. */
	struct vfs2_wal_index_basic_hdr pending_txn_hdr;
	/* When the WAL is restarted (or started for the first time), we capture
	 * the initial WAL index header in prev_txn_hdr.
	 *
	 * When we get SQLITE_FCNTL_COMMIT_PHASETWO, we copy the WAL index
	 * header from shm into pending_txn_hdr, then overwrite the shm with
	 * prev_txn_hdr to hide the transaction.
	 *
	 * When we get vfs2_apply, we overwrite both prev_txn_hdr and the shm
	 * with pending_txn_hdr. */

	void **regions;
	int regions_len;
	unsigned refcount;

	unsigned locks[SQLITE_SHM_NLOCK];
};

/**
 * VFS-specific file object, upcastable to sqlite3_file.
 */
struct vfs2_file {
	struct sqlite3_file base; /* vtable, must be first */
	sqlite3_file *orig;       /* underlying file object */
	struct vfs2_data *vfs_data;
	struct vfs2_db_entry *entry;
	int flags; /* from xOpen */
	union {
		/* if this file object is a WAL */
		struct vfs2_wal wal;
		/* if this file object is a main file */
		struct vfs2_db db_shm;
	};
};

static uint32_t get_salt1(struct vfs2_salts s)
{
	return ByteGetBe32(s.salt1);
}

static uint32_t get_salt2(struct vfs2_salts s)
{
	return ByteGetBe32(s.salt2);
}

static bool salts_equal(struct vfs2_salts a, struct vfs2_salts b)
{
	return get_salt1(a) == get_salt1(b) && get_salt2(a) == get_salt2(b);
}

static struct vfs2_wal_index_full_hdr *get_full_hdr(struct vfs2_db *db)
{
	PRE(db->regions_len > 0);
	PRE(db->regions != NULL);
	return db->regions[0];
}

static bool region0_mapped(struct vfs2_db *db)
{
	return db->regions_len > 0 && db->regions != NULL &&
	       db->regions[0] != NULL;
}

static bool no_pending_txn(struct vfs2_wal *wal)
{
	return wal->pending_txn_len == 0 && wal->pending_txn_frames == NULL && wal->pending_txn_last_frame_commit == 0;
}

static bool have_pending_txn(struct vfs2_wal *wal)
{
	return wal->pending_txn_len > 0 && wal->pending_txn_frames != NULL;
}

static bool write_lock_held(struct vfs2_db *db)
{
	return db->locks[VFS2_SHM_WRITE_LOCK] == VFS2_EXCLUSIVE;
}

static bool wal_index_hdr_fresh(const struct vfs2_wal_index_full_hdr *hdr)
{
	/* TODO check other things here? */
	return hdr->basic[0].mxFrame == 0;
}

static bool wal_index_basic_hdr_equal(struct vfs2_wal_index_basic_hdr a,
				      struct vfs2_wal_index_basic_hdr b)
{
	return memcmp(&a, &b, sizeof(struct vfs2_wal_index_basic_hdr)) == 0;
}

static bool wal_index_basic_hdr_advanced(struct vfs2_wal_index_basic_hdr new,
					 struct vfs2_wal_index_basic_hdr old)
{
	return new.iChange == old.iChange + 1 &&
	       new.nPage >= old.nPage /* no vacuums here */
	       && ((get_salt1(new.salts) == get_salt1(old.salts) &&
		    get_salt2(new.salts) == get_salt2(old.salts)) ||
		/* note the weirdness with zero salts */
		   (get_salt1(old.salts) == 0 &&
		    get_salt2(old.salts) == 0))
	       && new.mxFrame > old.mxFrame;
}

static bool wtx_invariant(const struct sm *sm, int prev)
{
	struct vfs2_db_entry *entry =
	    CONTAINER_OF(sm, struct vfs2_db_entry, wtx_sm);
	struct vfs2_wal *wal = (entry->wal == NULL) ? NULL : &entry->wal->wal;
	struct vfs2_db *db_shm =
	    (entry->db == NULL) ? NULL : &entry->db->db_shm;

	/* TODO go over these checks again and strengthen them */
	/* TODO rewrite in expression-oriented style? */

	if (sm_state(sm) == WTX_NOT_OPEN) {
		CHECK(wal == NULL);
	}

	if (sm_state(sm) == WTX_EMPTY) {
		CHECK(wal != NULL);
	}

	if (sm_state(sm) == WTX_BASE) {
		CHECK(db_shm != NULL);
		CHECK(wal != NULL);
		CHECK(no_pending_txn(wal));
		CHECK(wal_index_basic_hdr_equal(db_shm->pending_txn_hdr, (struct vfs2_wal_index_basic_hdr){}));

		if (prev == WTX_BASE) {
			/* just after a WAL swap */
			CHECK(write_lock_held(db_shm));
			CHECK(region0_mapped(db_shm));
			CHECK(wal_index_hdr_fresh(get_full_hdr(db_shm)));
		}
	}

	if (sm_state(sm) == WTX_ACTIVE) {
		CHECK(db_shm != NULL);
		CHECK(wal != NULL);
		CHECK(have_pending_txn(wal));
		CHECK(region0_mapped(db_shm));
		CHECK(write_lock_held(db_shm));

		struct vfs2_wal_index_full_hdr *hdr = get_full_hdr(db_shm);
		CHECK(wal_index_basic_hdr_equal(hdr->basic[0],
						db_shm->prev_txn_hdr) ||
		      wal_index_basic_hdr_advanced(hdr->basic[0],
						   db_shm->prev_txn_hdr));
		CHECK(wal_index_basic_hdr_equal(db_shm->pending_txn_hdr, (struct vfs2_wal_index_basic_hdr){}));

		if (prev == WTX_BASE) {
			/* first frame in a txn */
			CHECK(wal->pending_txn_len == 1);
		}
	}

	if (sm_state(sm) == WTX_HIDDEN) {
		CHECK(db_shm != NULL);
		CHECK(wal != NULL);
		CHECK(have_pending_txn(wal));
		CHECK(region0_mapped(db_shm));
		CHECK(!write_lock_held(db_shm));

		struct vfs2_wal_index_full_hdr *hdr = get_full_hdr(db_shm);
		CHECK(wal_index_basic_hdr_equal(hdr->basic[0],
						db_shm->prev_txn_hdr));
		CHECK(wal_index_basic_hdr_advanced(db_shm->pending_txn_hdr,
						   hdr->basic[0]));
	}

	if (sm_state(sm) == WTX_POLLED) {
		CHECK(db_shm != NULL);
		CHECK(wal != NULL);
		CHECK(!have_pending_txn(wal));
		CHECK(region0_mapped(db_shm));
		CHECK(write_lock_held(db_shm));

		struct vfs2_wal_index_full_hdr *hdr = get_full_hdr(db_shm);
		CHECK(wal_index_basic_hdr_equal(hdr->basic[0],
						db_shm->prev_txn_hdr));
		CHECK(wal_index_basic_hdr_advanced(db_shm->pending_txn_hdr,
						   hdr->basic[0]));
	}

	return true;
}

static bool is_valid_page_size(unsigned long n)
{
	return n >= 1 << 9 && n <= 1 << 16 && (n & (n - 1)) == 0;
}

static int check_wal_integrity(sqlite3_file *f)
{
	/* TODO */
	(void)f;
	return SQLITE_OK;
}

static void unregister_file(struct vfs2_file *file)
{
	pthread_rwlock_wrlock(&file->vfs_data->rwlock);
	queue *q = queue_head(&file->vfs_data->queue);
	while (q != &file->vfs_data->queue) {
		queue *next = q->next;
		struct vfs2_db_entry *entry =
		    QUEUE_DATA(q, struct vfs2_db_entry, link);
		if (entry->db == file) {
			entry->db = NULL;
		} else if (entry->wal == file) {
			entry->wal = NULL;
		}
		if (entry->wal == NULL) {
			sm_move(&entry->wtx_sm, WTX_NOT_OPEN);
		}
		if (entry->db == NULL && entry->wal == NULL) {
			q->prev->next = q->next;
			q->next->prev = q->prev;
			sm_fini(&entry->wtx_sm);
			sqlite3_free(entry->db_name);
			sqlite3_free(entry);
		}
		q = next;
	}
	pthread_rwlock_unlock(&file->vfs_data->rwlock);
}

/* sqlite3_io_methods implementations begin here */

static int vfs2_close(sqlite3_file *file)
{
	int rv, rvprev;
	struct vfs2_file *xfile = (struct vfs2_file *)file;

	unregister_file(xfile);

	rvprev = SQLITE_OK;
	if (xfile->flags & SQLITE_OPEN_WAL) {
		sqlite3_free(xfile->wal.wal_cur_fixed_name);
		sqlite3_free(xfile->wal.wal_prev_fixed_name);
		if (xfile->wal.wal_prev->pMethods != NULL) {
			rvprev = xfile->wal.wal_prev->pMethods->xClose(
			    xfile->wal.wal_prev);
		}
		if (xfile->wal.pending_txn_frames != NULL) {
			for (uint32_t i = 0; i < xfile->wal.pending_txn_len;
			     i++) {
				sqlite3_free(
				    xfile->wal.pending_txn_frames[i].data);
			}
		}
		sqlite3_free(xfile->wal.pending_txn_frames);
		sqlite3_free(xfile->wal.wal_prev);
	} else if (xfile->flags & SQLITE_OPEN_MAIN_DB) {
		for (int i = 0; i < xfile->db_shm.regions_len; i++) {
			sqlite3_free(xfile->db_shm.regions[i]);
		}
		sqlite3_free(xfile->db_shm.regions);
	}
	rv = SQLITE_OK;
	if (xfile->orig->pMethods != NULL) {
		rv = xfile->orig->pMethods->xClose(xfile->orig);
	}
	sqlite3_free(xfile->orig);
	if (rv != SQLITE_OK) {
		return rv;
	}
	return rvprev;
}

static int vfs2_read(sqlite3_file *file, void *buf, int amt, sqlite3_int64 ofst)
{
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	return xfile->orig->pMethods->xRead(xfile->orig, buf, amt, ofst);
}

static int vfs2_wal_swap(struct vfs2_file *wal,
			 const struct vfs2_wal_hdr *wal_hdr)
{
	PRE(wal->wal.pending_txn_len == 0);
	PRE(wal->wal.pending_txn_frames == NULL);
	int rv;

	atomic_uint *p = &wal->vfs_data->page_size;
	unsigned expected = 0;
	uint32_t z = ByteGetBe32(wal_hdr->page_size);
	if (!atomic_compare_exchange_strong(p, &expected, z)) {
		assert(expected == z);
	}

	sqlite3_file *phys_outgoing = wal->orig;
	char *name_outgoing = wal->wal.wal_cur_fixed_name;
	sqlite3_file *phys_incoming = wal->wal.wal_prev;
	char *name_incoming = wal->wal.wal_prev_fixed_name;

	tracef("wal swap outgoing=%s incoming=%s", name_outgoing, name_incoming);

	/* Write the new header of the incoming WAL. */
	rv = phys_incoming->pMethods->xWrite(phys_incoming, wal_hdr,
					     sizeof(struct vfs2_wal_hdr), 0);
	if (rv != SQLITE_OK) {
		return rv;
	}

	/* In-memory WAL swap. */
	struct vfs2_file *db = wal->entry->db;
	assert(db != NULL);
	wal->orig = phys_incoming;
	wal->wal.wal_cur_fixed_name = name_incoming;
	wal->wal.wal_prev = phys_outgoing;
	wal->wal.wal_prev_fixed_name = name_outgoing;
	wal->wal.commit_end = 0;
	sm_move(&wal->entry->wtx_sm, WTX_BASE);

	/* Move the moving name. */
	rv = unlink(wal->wal.moving_name);
	if (rv != 0) {
		return SQLITE_IOERR;
	}
	rv = link(name_incoming, wal->wal.moving_name);
	if (rv != 0) {
		return SQLITE_IOERR;
	}

	/* TODO do we need an fsync here? */

	/* Best-effort: invalidate the outgoing physical WAL so that nobody gets
	 * confused. */
	(void)phys_outgoing->pMethods->xWrite(phys_outgoing, &invalid_magic,
					      sizeof(invalid_magic), 0);
	return SQLITE_OK;
}

static int vfs2_wal_write_frame_hdr(struct vfs2_file *wal,
				    const void *buf,
				    uint32_t x)
{
	x -= wal->wal.commit_end;

	assert(x <= wal->wal.pending_txn_len);
	uint32_t n = wal->wal.pending_txn_len;
	dqlite_vfs_frame *frames = wal->wal.pending_txn_frames;
	if (wal->wal.pending_txn_len == 0 && x == 0) {
		/* check that the WAL-index hdr makes sense and save it */
		struct vfs2_db *db_shm = &wal->entry->db->db_shm;
		struct vfs2_wal_index_basic_hdr hdr =
		    get_full_hdr(db_shm)->basic[0];
		assert(hdr.isInit != 0);
		assert(hdr.mxFrame == wal->wal.commit_end);
		db_shm->prev_txn_hdr = hdr;
	}
	if (x == n) {
		/* FIXME reallocating every time seems bad */
		wal->wal.pending_txn_frames =
		    sqlite3_realloc64(frames, (sqlite_uint64)sizeof(*frames) *
						  (sqlite3_uint64)(n + 1));
		if (wal->wal.pending_txn_frames == NULL) {
			return SQLITE_NOMEM;
		}
		dqlite_vfs_frame *frame = &wal->wal.pending_txn_frames[n];
		frame->page_number = ByteGetBe32(buf);
		frame->data = NULL;
		wal->wal.pending_txn_last_frame_commit =
		    ByteGetBe32((const uint8_t *)buf + 4);
		wal->wal.pending_txn_len++;
	} else {
		/* Overwriting a previously-written frame in the current
		 * transaction. */
		dqlite_vfs_frame *frame = &wal->wal.pending_txn_frames[x];
		frame->page_number = ByteGetBe32(buf);
		sqlite3_free(frame->data);
		frame->data = NULL;
	}
	sm_move(&wal->entry->wtx_sm, WTX_ACTIVE);
	return SQLITE_OK;
}

static int vfs2_wal_post_write(struct vfs2_file *wal,
			       const void *buf,
			       int amt,
			       sqlite3_int64 ofst)
{

	unsigned page_size = atomic_load(&wal->vfs_data->page_size);
	uint32_t frame_size = VFS2_WAL_FRAME_HDR_SIZE + page_size;
	if (amt == VFS2_WAL_FRAME_HDR_SIZE) {
		sqlite3_int64 x =
		    ofst - (sqlite3_int64)sizeof(struct vfs2_wal_hdr);
		assert(x % frame_size == 0);
		x /= frame_size;
		return vfs2_wal_write_frame_hdr(wal, buf, (uint32_t)x);
	} else if (amt == (int)page_size) {
		sqlite3_int64 x = ofst - VFS2_WAL_FRAME_HDR_SIZE -
				  (sqlite3_int64)sizeof(struct vfs2_wal_hdr);
		assert(x % frame_size == 0);
		x /= frame_size;
		x -= wal->wal.commit_end;
		assert(0 <= x && x < wal->wal.pending_txn_len);
		dqlite_vfs_frame *frame = &wal->wal.pending_txn_frames[x];
		assert(frame->data == NULL);
		frame->data = sqlite3_malloc(amt);
		if (frame->data == NULL) {
			return SQLITE_NOMEM;
		}
		memcpy(frame->data, buf, (size_t)amt);

		sm_move(&wal->entry->wtx_sm, WTX_ACTIVE);
		return SQLITE_OK;
	} else {
		assert(0);
	}
}

static int vfs2_write(sqlite3_file *file,
		      const void *buf,
		      int amt,
		      sqlite3_int64 ofst)
{
	int rv;
	struct vfs2_file *xfile = (struct vfs2_file *)file;

	if ((xfile->flags & SQLITE_OPEN_WAL) && ofst == 0) {
		assert(amt == sizeof(struct vfs2_wal_hdr));
		return vfs2_wal_swap(xfile, buf);
	}

	rv = xfile->orig->pMethods->xWrite(xfile->orig, buf, amt, ofst);
	if (rv != SQLITE_OK) {
		return rv;
	}

	if (xfile->flags & SQLITE_OPEN_WAL) {
		tracef("wrote to WAL name=%s amt=%d ofst=%lld", xfile->wal.wal_cur_fixed_name, amt, ofst);
		return vfs2_wal_post_write(xfile, buf, amt, ofst);
	}

	return SQLITE_OK;
}

static int vfs2_truncate(sqlite3_file *file, sqlite3_int64 size)
{
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	return xfile->orig->pMethods->xTruncate(xfile->orig, size);
}

static int vfs2_sync(sqlite3_file *file, int flags)
{
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	return xfile->orig->pMethods->xSync(xfile->orig, flags);
}

static int vfs2_file_size(sqlite3_file *file, sqlite3_int64 *size)
{
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	return xfile->orig->pMethods->xFileSize(xfile->orig, size);
}

static int vfs2_lock(sqlite3_file *file, int mode)
{
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	return xfile->orig->pMethods->xLock(xfile->orig, mode);
}

static int vfs2_unlock(sqlite3_file *file, int mode)
{
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	return xfile->orig->pMethods->xUnlock(xfile->orig, mode);
}

static int vfs2_check_reserved_lock(sqlite3_file *file, int *out)
{
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	return xfile->orig->pMethods->xCheckReservedLock(xfile->orig, out);
}

static int interpret_pragma(struct vfs2_file *f, char **args)
{
	char **e = &args[0];
	char *left = args[1];
	PRE(left != NULL);
	char *right = args[2];

	if (strcmp(left, "page_size") == 0 && right != NULL) {
		char *end;
		unsigned long z = strtoul(right, &end, 10);
		if (*end == '\0' && is_valid_page_size(z)) {
			unsigned expected = 0;
			atomic_uint *page_size = &f->vfs_data->page_size;
			if (!atomic_compare_exchange_strong(page_size, &expected, (unsigned)z) && expected != z) {
				*e = sqlite3_mprintf("can't modify page size once set");
				return SQLITE_ERROR;
			}
		}
	} else if (strcmp(left, "journal_mode") == 0 && right != NULL && strcasecmp(right, "wal") != 0) {
		*e = sqlite3_mprintf("dqlite requires WAL mode");
		return SQLITE_ERROR;
	}

	return SQLITE_NOTFOUND;
}

static int vfs2_file_control(sqlite3_file *file, int op, void *arg)
{
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	PRE(xfile->flags & SQLITE_OPEN_MAIN_DB);
	int rv;

	if (op == SQLITE_FCNTL_COMMIT_PHASETWO && xfile->entry->wal != NULL && xfile->entry->wal->wal.pending_txn_len != 0) {
		/* Hide the transaction that was just written by resetting
		 * the WAL index header. */
		struct vfs2_wal_index_full_hdr *hdr =
		    get_full_hdr(&xfile->db_shm);
		xfile->db_shm.pending_txn_hdr = hdr->basic[0];
		hdr->basic[0] = xfile->db_shm.prev_txn_hdr;
		hdr->basic[1] = hdr->basic[0];
		sm_move(&xfile->entry->wtx_sm, WTX_HIDDEN);
	} else if (op == SQLITE_FCNTL_PRAGMA) {
		rv = interpret_pragma(xfile, arg);
		if (rv != SQLITE_NOTFOUND) {
			return rv;
		}
	} else if (op == SQLITE_FCNTL_PERSIST_WAL) {
		// XXX handle setting as well as getting?
		int *out = arg;
		*out = 1;
		return SQLITE_OK;
	}

	rv = xfile->orig->pMethods->xFileControl(xfile->orig, op, arg);
	assert(ERGO(op == SQLITE_FCNTL_PRAGMA, rv == SQLITE_NOTFOUND));
	return rv;
}

static int vfs2_sector_size(sqlite3_file *file)
{
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	return xfile->orig->pMethods->xSectorSize(xfile->orig);
}

static int vfs2_device_characteristics(sqlite3_file *file)
{
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	return xfile->orig->pMethods->xDeviceCharacteristics(xfile->orig);
}

static int vfs2_fetch(sqlite3_file *file,
		      sqlite3_int64 ofst,
		      int amt,
		      void **out)
{
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	return xfile->orig->pMethods->xFetch(xfile->orig, ofst, amt, out);
}

static int vfs2_unfetch(sqlite3_file *file, sqlite3_int64 ofst, void *buf)
{
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	return xfile->orig->pMethods->xUnfetch(xfile->orig, ofst, buf);
}

static int vfs2_shm_map(sqlite3_file *file,
			int pgno,
			int pgsz,
			int extend,
			void volatile **out)
{
	tracef("vfs2_shm_map(%p, %d, %d, %d, %p)", file, pgno, pgsz, extend,
	       out);
	int rv;
	void *region;
	struct vfs2_file *xfile = (struct vfs2_file *)file;

	if (xfile->db_shm.regions != NULL && pgno < xfile->db_shm.regions_len) {
		region = xfile->db_shm.regions[pgno];
		assert(region != NULL);
	} else if (extend) {
		void **regions;

		assert(pgsz == VFS2_WAL_INDEX_REGION_SIZE);
		assert(pgno == xfile->db_shm.regions_len);
		region = sqlite3_malloc(pgsz);
		if (region == NULL) {
			rv = SQLITE_NOMEM;
			goto err;
		}

		memset(region, 0, (size_t)pgsz);

		/* FIXME reallocating every time seems bad */
		regions = sqlite3_realloc64(
		    xfile->db_shm.regions,
		    (sqlite3_uint64)sizeof(*xfile->db_shm.regions) *
			(sqlite3_uint64)(xfile->db_shm.regions_len + 1));
		if (regions == NULL) {
			rv = SQLITE_NOMEM;
			goto err_after_region_malloc;
		}

		xfile->db_shm.regions = regions;
		xfile->db_shm.regions[pgno] = region;
		xfile->db_shm.regions_len++;
	} else {
		region = NULL;
	}

	*out = region;

	if (pgno == 0 && region != NULL) {
		xfile->db_shm.refcount++;
	}

	return SQLITE_OK;

err_after_region_malloc:
	sqlite3_free(region);
err:
	assert(rv != SQLITE_OK);
	*out = NULL;
	return rv;
}

static int vfs2_shm_lock(sqlite3_file *file, int ofst, int n, int flags)
{
	struct vfs2_file *xfile = (struct vfs2_file *)file;

	assert(file != NULL);
	assert(ofst >= 0);
	assert(n >= 0);

	assert(ofst >= 0 && ofst + n <= SQLITE_SHM_NLOCK);
	assert(n >= 1);
	assert(n == 1 || (flags & SQLITE_SHM_EXCLUSIVE) != 0);

	assert(flags == (SQLITE_SHM_LOCK | SQLITE_SHM_SHARED) ||
	       flags == (SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE) ||
	       flags == (SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED) ||
	       flags == (SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE));

	assert(xfile->flags & SQLITE_OPEN_MAIN_DB);

	unsigned *locks = xfile->db_shm.locks;
	if (flags == (SQLITE_SHM_LOCK | SQLITE_SHM_SHARED)) {
		for (int i = ofst; i < ofst + n; i++) {
			if (locks[i] == VFS2_EXCLUSIVE) {
				return SQLITE_BUSY;
			}
		}

		for (int i = ofst; i < ofst + n; i++) {
			locks[i]++;
		}
	} else if (flags == (SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)) {
		for (int i = ofst; i < ofst + n; i++) {
			if (locks[i] > 0) {
				return SQLITE_BUSY;
			}
		}

		for (int i = ofst; i < ofst + n; i++) {
			locks[i] = VFS2_EXCLUSIVE;
		}

		/* XXX maybe this shouldn't be an assertion */
		if (ofst == VFS2_SHM_WRITE_LOCK) {
			assert(n == 1);
			struct vfs2_file *wal = xfile->entry->wal;
			assert(wal->wal.pending_txn_len == 0);
		}
	} else if (flags == (SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED)) {
		for (int i = ofst; i < ofst + n; i++) {
			assert(locks[i] > 0);
			locks[i]--;
		}
	} else if (flags == (SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE)) {
		for (int i = ofst; i < ofst + n; i++) {
			assert(locks[i] == VFS2_EXCLUSIVE);
			locks[i] = 0;
		}

		/* Unlocking the write lock: roll back any uncommitted
		 * transaction. */
		if (ofst == VFS2_SHM_WRITE_LOCK) {
			assert(n == 1);
			struct vfs2_file *wal = xfile->entry->wal;

			if (wal->wal.pending_txn_len > 0 &&
			    wal->wal.pending_txn_last_frame_commit == 0) {
				for (uint32_t i = 0;
				     i < wal->wal.pending_txn_len; i++) {
					sqlite3_free(
					    wal->wal.pending_txn_frames[i]
						.data);
				}
				sqlite3_free(wal->wal.pending_txn_frames);
				wal->wal.pending_txn_frames = NULL;
				wal->wal.pending_txn_len = 0;
				wal->wal.pending_txn_last_frame_commit = 0;
				sm_move(&wal->entry->wtx_sm, WTX_BASE);
			}
		}
	} else {
		assert(0);
	}

	return SQLITE_OK;
}

static void vfs2_shm_barrier(sqlite3_file *file)
{
	(void)file;
}

static int vfs2_shm_unmap(sqlite3_file *file, int delete)
{
	(void)delete;
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	xfile->db_shm.refcount--;
	if (xfile->db_shm.refcount == 0) {
		for (int i = 0; i < xfile->db_shm.regions_len; i++) {
			void *region = xfile->db_shm.regions[i];
			assert(region != NULL);
			sqlite3_free(region);
		}
		sqlite3_free(xfile->db_shm.regions);

		xfile->db_shm.regions = NULL;
		xfile->db_shm.regions_len = 0;
		memset(xfile->db_shm.locks, 0, sizeof(xfile->db_shm.locks));
	}
	return SQLITE_OK;
}

/* sqlite3_io_methods implementations end here */

static struct sqlite3_io_methods vfs2_io_methods = {
	.iVersion = 3,
	.xClose = vfs2_close,
	.xRead = vfs2_read,
	.xWrite = vfs2_write,
	.xTruncate = vfs2_truncate,
	.xSync = vfs2_sync,
	.xFileSize = vfs2_file_size,
	.xLock = vfs2_lock,
	.xUnlock = vfs2_unlock,
	.xCheckReservedLock = vfs2_check_reserved_lock,
	.xFileControl = vfs2_file_control,
	.xSectorSize = vfs2_sector_size,
	.xDeviceCharacteristics = vfs2_device_characteristics,
	.xShmMap = vfs2_shm_map,
	.xShmLock = vfs2_shm_lock,
	.xShmBarrier = vfs2_shm_barrier,
	.xShmUnmap = vfs2_shm_unmap,
	.xFetch = vfs2_fetch,
	.xUnfetch = vfs2_unfetch
};

/* sqlite3_vfs implementations begin here */

static int compare_wal_headers(struct vfs2_wal_hdr a,
			       struct vfs2_wal_hdr b,
			       bool *ordered)
{
	if (get_salt1(a.salts) == get_salt1(b.salts) + 1) {
		*ordered = true;
	} else if (get_salt1(b.salts) == get_salt1(a.salts) + 1) {
		*ordered = false;
	} else {
		return SQLITE_ERROR;
	}
	return SQLITE_OK;
}

static int vfs2_open_wal(sqlite3_vfs *vfs,
			 const char *name,
			 struct vfs2_file *xout,
			 int flags,
			 int *out_flags)
{
	int rv;
	struct vfs2_data *data = vfs->pAppData;

	const char *dash = strrchr(name, '-');
	assert(dash != NULL);
	static_assert(
	    sizeof(VFS2_WAL_FIXED_SUFFIX1) == sizeof(VFS2_WAL_FIXED_SUFFIX2),
	    "uneven WAL suffixes");
	if ((size_t)(dash - name) + strlen(VFS2_WAL_FIXED_SUFFIX1) >
	    (size_t)data->orig->mxPathname) {
		return SQLITE_ERROR;
	}

	/* Collect memory allocations in one place to simplify the control
	 * flow. A small amount of memory will be leaked if one of the later
	 * allocations fails. */
	char *fixed1 = sqlite3_malloc(data->orig->mxPathname + 1);
	char *fixed2 = sqlite3_malloc(data->orig->mxPathname + 1);
	sqlite3_file *phys1 = sqlite3_malloc(data->orig->szOsFile);
	sqlite3_file *phys2 = sqlite3_malloc(data->orig->szOsFile);
	if (fixed1 == NULL || fixed2 == NULL || phys1 == NULL ||
	    phys2 == NULL) {
		return SQLITE_NOMEM;
	}
	xout->wal.wal_cur_fixed_name = fixed1;
	xout->wal.wal_prev_fixed_name = fixed2;
	memset(phys1, 0, (size_t)data->orig->szOsFile);
	xout->orig = phys1;
	memset(phys2, 0, (size_t)data->orig->szOsFile);
	xout->wal.wal_prev = phys2;
	xout->wal.moving_name = name;

	/* Open the two physical WALs. */
	strncpy(fixed1, name, (size_t)(dash - name));
	fixed1[dash - name] = '\0';
	strcat(fixed1, VFS2_WAL_FIXED_SUFFIX1);
	int out_flags1;
	rv = data->orig->xOpen(data->orig, fixed1, phys1, flags, &out_flags1);
	if (rv != SQLITE_OK) {
		return rv;
	}
	strncpy(fixed2, name, (size_t)(dash - name));
	fixed2[dash - name] = '\0';
	strcat(fixed2, VFS2_WAL_FIXED_SUFFIX2);
	int out_flags2;
	rv = data->orig->xOpen(data->orig, fixed2, phys2, flags, &out_flags2);
	if (rv != SQLITE_OK) {
		return rv;
	}

	/* Determine the relationship between the two physical WALs. */
	sqlite3_int64 size1;
	rv = phys1->pMethods->xFileSize(phys1, &size1);
	if (rv != SQLITE_OK) {
		return rv;
	}
	if (size1 < (sqlite3_int64)sizeof(struct vfs2_wal_hdr)) {
		size1 = 0;
	}
	sqlite3_int64 size2;
	rv = phys2->pMethods->xFileSize(phys2, &size2);
	if (rv != SQLITE_OK) {
		return rv;
	}
	if (size2 < (sqlite3_int64)sizeof(struct vfs2_wal_hdr)) {
		size2 = 0;
	}

	struct vfs2_wal_hdr hdr1 = { 0 };
	if (size1 > 0) {
		rv = phys1->pMethods->xRead(phys1, &hdr1, sizeof(hdr1), 0);
		if (rv != SQLITE_OK) {
			return rv;
		}
	}
	struct vfs2_wal_hdr hdr2 = { 0 };
	if (size2 > 0) {
		rv = phys2->pMethods->xRead(phys2, &hdr2, sizeof(hdr2), 0);
		if (rv != SQLITE_OK) {
			return rv;
		}
	}

	struct vfs2_wal_hdr hdr_cur = { 0 };
	struct vfs2_wal_hdr hdr_prev = { 0 };
	sqlite3_int64 size_cur = 0;
	sqlite3_int64 size_prev = 0;

	bool wal1_is_fresh;
	if (size2 == 0) {
		wal1_is_fresh = true;
	} else if (size1 == 0) {
		wal1_is_fresh = false;
	} else {
		rv = compare_wal_headers(hdr1, hdr2, &wal1_is_fresh);
		if (rv != SQLITE_OK) {
			return rv;
		}
	}
	if (wal1_is_fresh) {
		rv = unlink(name);
		(void)rv;
		rv = link(fixed1, name);
		(void)rv;

		hdr_prev = hdr2;
		size_prev = size2;
		hdr_cur = hdr1;
		size_cur = size1;

		if (out_flags != NULL) {
			*out_flags = out_flags1;
		}
	} else {
		rv = unlink(name);
		(void)rv;
		rv = link(fixed2, name);
		(void)rv;

		xout->orig = phys2;
		xout->wal.moving_name = name;
		xout->wal.wal_cur_fixed_name = fixed2;
		xout->wal.wal_prev = phys1;
		xout->wal.wal_prev_fixed_name = fixed1;

		hdr_prev = hdr1;
		size_prev = size1;
		hdr_cur = hdr2;
		size_cur = size2;

		if (out_flags != NULL) {
			*out_flags = out_flags2;
		}
	}

	rv = check_wal_integrity(xout->wal.wal_prev);
	if (rv != SQLITE_OK) {
		return rv;
	}

	struct vfs2_wal_slice limit = xout->entry->wal_limit;
	if (size1 > 0 && size2 > 0 && limit.len > 0) {
		uint32_t page_size = ByteGetBe32(hdr_cur.page_size);
		assert(ByteGetBe32(hdr_prev.page_size) == page_size);
		sqlite3_int64 implied_size =
		    (sqlite3_int64)sizeof(struct vfs2_wal_hdr) +
		    (sqlite3_int64)(VFS2_WAL_FRAME_HDR_SIZE + page_size) *
			(sqlite3_int64)(limit.start + limit.len);

		if (salts_equal(limit.salts, hdr_prev.salts)) {
			if (size_prev != implied_size) {
				return SQLITE_ERROR;
			}
			rv = xout->orig->pMethods->xTruncate(
			    xout->orig, sizeof(struct vfs2_wal_hdr));
			if (rv != SQLITE_OK) {
				return rv;
			}
			xout->wal.commit_end = 0;
		} else if (salts_equal(limit.salts, hdr_cur.salts)) {
			if (size_cur < implied_size) {
				return SQLITE_ERROR;
			}
			rv = xout->orig->pMethods->xTruncate(xout->orig,
							     implied_size);
			if (rv != SQLITE_OK) {
				return rv;
			}
			xout->wal.commit_end = limit.start + limit.len;
		} else {
			return SQLITE_ERROR;
		}
	} else {
		xout->wal.commit_end = 0;
	}

	xout->entry->wal = xout;
	if (size_cur > 0) {
		uint32_t z = ByteGetBe32(hdr_cur.page_size);
		assert(z > 0);
		atomic_store(&data->page_size, z);
		sm_move(&xout->entry->wtx_sm, WTX_BASE);
	} else {
		sm_move(&xout->entry->wtx_sm, WTX_EMPTY);
	}
	tracef("opened WAL cur=%s prev=%s", xout->wal.wal_cur_fixed_name, xout->wal.wal_prev_fixed_name);
	return SQLITE_OK;
}

static int vfs2_open_db(sqlite3_vfs *vfs,
			const char *name,
			struct vfs2_file *xout,
			int flags,
			int *out_flags)
{
	int rv;
	struct vfs2_data *data = vfs->pAppData;

	xout->orig = sqlite3_malloc(data->orig->szOsFile);
	if (xout->orig == NULL) {
		return SQLITE_NOMEM;
	}
	memset(xout->orig, 0, (size_t)data->orig->szOsFile);
	rv = data->orig->xOpen(data->orig, name, xout->orig, flags, out_flags);
	if (rv != SQLITE_OK) {
		return rv;
	}

	xout->db_shm.name = name;
	xout->db_shm.regions = NULL;
	xout->db_shm.regions_len = 0;
	xout->db_shm.refcount = 0;
	memset(xout->db_shm.locks, 0, sizeof(xout->db_shm.locks));
	xout->entry->db = xout;
	return SQLITE_OK;
}

static struct vfs2_db_entry *get_or_create_entry(struct vfs2_data *data,
						 const char *name,
						 int flags)
{
	bool name_is_db = (flags & SQLITE_OPEN_MAIN_DB) != 0;
	bool name_is_wal = (flags & SQLITE_OPEN_WAL) != 0;
	assert(name_is_db ^ name_is_wal);
	const char *dash = strrchr(name, '-');
	assert(ERGO(name_is_wal, dash != NULL));

	struct vfs2_db_entry *res = NULL;
	pthread_rwlock_rdlock(&data->rwlock);
	queue *q;
	QUEUE_FOREACH(q, &data->queue)
	{
		struct vfs2_db_entry *cur =
		    QUEUE_DATA(q, struct vfs2_db_entry, link);
		if ((name_is_db && strcmp(cur->db_name, name) == 0) ||
		    (name_is_wal && strncmp(cur->db_name, name, (size_t)(dash - name)) == 0)) {
			res = cur;
			break;
		}
	}
	pthread_rwlock_unlock(&data->rwlock);
	if (res != NULL) {
		return res;
	}

	res = sqlite3_malloc(sizeof(*res));
	if (res == NULL) {
		return NULL;
	}
	memset(res, 0, sizeof(*res));
	size_t len = name_is_db ? strlen(name) : (size_t)(dash - name);
	res->db_name = sqlite3_malloc((int)len + 1);
	if (res->db_name == NULL) {
		sqlite3_free(res);
		return NULL;
	}
	memcpy(res->db_name, name, len);
	res->db_name[len] = '\0';

	sm_init(&res->wtx_sm, wtx_invariant, NULL, wtx_states, WTX_NOT_OPEN);

	pthread_rwlock_wrlock(&data->rwlock);
	queue_insert_tail(&data->queue, &res->link);
	pthread_rwlock_unlock(&data->rwlock);
	return res;
}

static int vfs2_open(sqlite3_vfs *vfs,
		     const char *name,
		     sqlite3_file *out,
		     int flags,
		     int *out_flags)
{
	struct vfs2_file *xout = (struct vfs2_file *)out;
	struct vfs2_data *data = vfs->pAppData;
	memset(xout, 0, sizeof(*xout));
	xout->base.pMethods = &vfs2_io_methods;
	xout->flags = flags;
	xout->vfs_data = data;

	if (flags & SQLITE_OPEN_WAL) {
		struct vfs2_db_entry *entry =
		    get_or_create_entry(data, name, flags);
		if (entry == NULL) {
			return SQLITE_NOMEM;
		}
		assert(entry->wal == NULL);
		xout->entry = entry;
		int rv = vfs2_open_wal(vfs, name, xout, flags, out_flags);
		return rv;
	} else if (flags & SQLITE_OPEN_MAIN_DB) {
		struct vfs2_db_entry *entry =
		    get_or_create_entry(data, name, flags);
		if (entry == NULL) {
			return SQLITE_NOMEM;
		}
		assert(entry->db == NULL);
		xout->entry = entry;
		return vfs2_open_db(vfs, name, xout, flags, out_flags);
	} else {
		xout->orig = sqlite3_malloc(data->orig->szOsFile);
		if (xout->orig == NULL) {
			return SQLITE_NOMEM;
		}
		return data->orig->xOpen(data->orig, name, xout->orig, flags,
					 out_flags);
	}
}

/* TODO does this need to be customized? */
static int vfs2_delete(sqlite3_vfs *vfs, const char *name, int sync_dir)
{
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xDelete(data->orig, name, sync_dir);
}

/* TODO does this need to be customized? */
static int vfs2_access(sqlite3_vfs *vfs, const char *name, int flags, int *out)
{
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xAccess(data->orig, name, flags, out);
}

static int vfs2_full_pathname(sqlite3_vfs *vfs,
			      const char *name,
			      int n,
			      char *out)
{
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xFullPathname(data->orig, name, n, out);
}

static void *vfs2_dl_open(sqlite3_vfs *vfs, const char *filename)
{
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xDlOpen(data->orig, filename);
}

static void vfs2_dl_error(sqlite3_vfs *vfs, int n, char *msg)
{
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xDlError(data->orig, n, msg);
}

typedef void (*vfs2_sym)(void);
static vfs2_sym vfs2_dl_sym(sqlite3_vfs *vfs, void *dl, const char *symbol)
{
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xDlSym(data->orig, dl, symbol);
}

static void vfs2_dl_close(sqlite3_vfs *vfs, void *dl)
{
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xDlClose(data->orig, dl);
}

static int vfs2_randomness(sqlite3_vfs *vfs, int n, char *out)
{
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xRandomness(data->orig, n, out);
}

static int vfs2_sleep(sqlite3_vfs *vfs, int microseconds)
{
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xSleep(data->orig, microseconds);
}

static int vfs2_current_time(sqlite3_vfs *vfs, double *out)
{
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xCurrentTime(data->orig, out);
}

/* TODO update this to reflect syscalls that we make ourselves (not through the
 * base VFS) */
static int vfs2_get_last_error(sqlite3_vfs *vfs, int n, char *out)
{
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xGetLastError(data->orig, n, out);
}

static int vfs2_current_time_int64(sqlite3_vfs *vfs, sqlite3_int64 *out)
{
	struct vfs2_data *data = vfs->pAppData;
	if (data->orig->iVersion < 2) {
		return SQLITE_ERROR;
	}
	return data->orig->xCurrentTimeInt64(data->orig, out);
}

/* sqlite3_vfs implementations end here */

sqlite3_vfs *vfs2_make(sqlite3_vfs *orig, const char *name, unsigned page_size)
{
	if (page_size != 0 && !is_valid_page_size(page_size)) {
		return NULL;
	}
	struct vfs2_data *data = sqlite3_malloc(sizeof(*data));
	struct sqlite3_vfs *vfs = sqlite3_malloc(sizeof(*vfs));
	if (data == NULL || vfs == NULL) {
		return NULL;
	}
	data->orig = orig;
	pthread_rwlock_init(&data->rwlock, NULL);
	atomic_init(&data->page_size, page_size);
	queue_init(&data->queue);
	vfs->iVersion = 2;
	vfs->szOsFile = sizeof(struct vfs2_file);
	vfs->mxPathname = orig->mxPathname;
	vfs->zName = name;
	vfs->pAppData = data;
	vfs->xOpen = vfs2_open;
	vfs->xDelete = vfs2_delete;
	vfs->xAccess = vfs2_access;
	vfs->xFullPathname = vfs2_full_pathname;
	vfs->xDlOpen = vfs2_dl_open;
	vfs->xDlError = vfs2_dl_error;
	vfs->xDlSym = vfs2_dl_sym;
	vfs->xDlClose = vfs2_dl_close;
	vfs->xRandomness = vfs2_randomness;
	vfs->xSleep = vfs2_sleep;
	vfs->xCurrentTime = vfs2_current_time;
	vfs->xGetLastError = vfs2_get_last_error;
	vfs->xCurrentTimeInt64 = vfs2_current_time_int64;
	return vfs;
}

/* XXX return codes */
int vfs2_set_wal_limit(sqlite3_vfs *vfs,
		       const char *name,
		       struct vfs2_wal_slice sl)
{
	struct vfs2_data *data = vfs->pAppData;
	struct vfs2_db_entry *entry =
	    get_or_create_entry(data, name, SQLITE_OPEN_MAIN_DB);
	if (entry == NULL) {
		return 1;
	}
	/* If the WAL is already open then all is lost */
	if (entry->wal != NULL) {
		return 1;
	}
	entry->wal_limit = sl;
	return 0;
}

/* FIXME what return codes should this use? */
int vfs2_commit(sqlite3_file *file, struct vfs2_wal_slice sl)
{
	(void)sl; /* XXX */

	struct vfs2_file *xfile = (struct vfs2_file *)file;
	if (!(xfile->flags & SQLITE_OPEN_MAIN_DB)) {
		return 1;
	}
	struct vfs2_file *wal = xfile->entry->wal;
	if (wal == NULL) {
		return 1;
	}
	if (xfile->db_shm.regions_len == 0) {
		return 1;
	}
	unsigned *locks = xfile->db_shm.locks;
	if (locks[VFS2_SHM_WRITE_LOCK] != VFS2_EXCLUSIVE) {
		return 1;
	}
	locks[VFS2_SHM_WRITE_LOCK] = 0;

	union vfs2_shm_region0 *region0 = xfile->db_shm.regions[0];
	region0->hdr.basic[0] = xfile->db_shm.pending_txn_hdr;
	region0->hdr.basic[1] = xfile->db_shm.pending_txn_hdr;
	xfile->db_shm.prev_txn_hdr = xfile->db_shm.pending_txn_hdr;
	xfile->db_shm.pending_txn_hdr = (struct vfs2_wal_index_basic_hdr){};
	wal->wal.commit_end += wal->wal.pending_txn_len;
	wal->wal.pending_txn_len = 0;
	wal->wal.pending_txn_last_frame_commit = 0;

	sm_move(&xfile->entry->wtx_sm, WTX_BASE);

	return 0;
}

int vfs2_poll(sqlite3_file *file, dqlite_vfs_frame **frames, unsigned *n, struct vfs2_wal_slice *sl)
{
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	if (!(xfile->flags & SQLITE_OPEN_MAIN_DB)) {
		return 1;
	}
	struct vfs2_file *wal = xfile->entry->wal;
	if (wal == NULL) {
		return 1;
	}

	uint32_t len = wal->wal.pending_txn_len;
	if (len > 0) {
		/* Don't go through vfs2_shm_lock here since that has additional
		 * checks that assume the context of being called from inside
		 * SQLite. */
		unsigned *locks = xfile->db_shm.locks;
		if (locks[VFS2_SHM_WRITE_LOCK] > 0) {
			return 1;
		}
		locks[VFS2_SHM_WRITE_LOCK] = VFS2_EXCLUSIVE;
	}

	if (n != NULL && frames != NULL) {
		*n = len;
		*frames = wal->wal.pending_txn_frames;
	} else if (wal->wal.pending_txn_frames != NULL) {
		for (uint32_t i = 0; i < len; i++) {
			sqlite3_free(wal->wal.pending_txn_frames[i].data);
		}
		sqlite3_free(wal->wal.pending_txn_frames);
	}
	wal->wal.pending_txn_frames = NULL;

	if (sl != NULL) {
		sl->len = len;
		sl->salts = xfile->db_shm.pending_txn_hdr.salts;
		sl->start = xfile->db_shm.prev_txn_hdr.mxFrame;
		sl->len = len;
	}

	sm_move(&xfile->entry->wtx_sm, WTX_POLLED);

	return 0;
}

void vfs2_destroy(sqlite3_vfs *vfs)
{
	struct vfs2_data *data = vfs->pAppData;
	pthread_rwlock_destroy(&data->rwlock);
	sqlite3_free(data);
	sqlite3_free(vfs);
}

/* TODO maybe this should prophylactically truncate the WAL? */
int vfs2_abort(sqlite3_file *file)
{
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	if (!(xfile->flags & SQLITE_OPEN_MAIN_DB)) {
		return 1;
	}
	struct vfs2_file *wal = xfile->entry->wal;
	if (wal == NULL) {
		return 1;
	}

	unsigned *locks = xfile->db_shm.locks;
	locks[VFS2_SHM_WRITE_LOCK] = 0;

	struct vfs2_wal_index_full_hdr *hdr = get_full_hdr(&xfile->db_shm);
	hdr->basic[0] = xfile->db_shm.prev_txn_hdr;
	hdr->basic[1] = xfile->db_shm.prev_txn_hdr;
	xfile->db_shm.pending_txn_hdr = (struct vfs2_wal_index_basic_hdr){};

	dqlite_vfs_frame *frames = wal->wal.pending_txn_frames;
	if (frames != NULL) {
		uint32_t n = wal->wal.pending_txn_len;
		for (uint32_t i = 0; i < n; i++) {
			sqlite3_free(frames[i].data);
		}
	}
	sqlite3_free(frames);
	wal->wal.pending_txn_frames = NULL;
	wal->wal.pending_txn_len = 0;
	wal->wal.pending_txn_last_frame_commit = 0;

	sm_move(&xfile->entry->wtx_sm, WTX_BASE);
	return 0;
}

int vfs2_read_wal(sqlite3_file *file,
		  struct vfs2_wal_txn *txns,
		  size_t txns_len)
{
	/* TODO */
	(void)file;
	(void)txns;
	(void)txns_len;
	return 0;
}

int vfs2_apply_uncommitted(sqlite3_file *file, const dqlite_vfs_frame *frames, unsigned len, struct vfs2_wal_slice *out)
{
	(void)file;
	(void)frames;
	(void)len;
	(void)out;
	/*
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	if (!(xfile->flags & SQLITE_OPEN_MAIN_DB)) {
		return 1;
	}
	int rv;

	unsigned *locks = xfile->db_shm.locks;
	if (locks[VFS2_SHM_WRITE_LOCK] > 0) {
		return 1;
	}
	locks[VFS2_SHM_WRITE_LOCK] = VFS2_EXCLUSIVE;

	struct vfs2_salts salts;
	rv = maybe_setup_or_swap_wal(xfile, len, &salts);
	if (rv != SQLITE_OK) {
		return 1;
	}
	struct vfs2_file *wal = xfile->entry->wal;

	// TODO assert that nothing is mapped => don't need to invalidate the WAL index

	uint32_t page_size = atomic_load(&xfile->vfs_data->page_size);
	assert(page_size > 0);
	sqlite3_int64 ofst;
	sqlite3_file *wal_cur = xfile->wal.wal_cur;
	rv = wal_cur->xFileSize(wal_cur, &ofst);
	if (rv != SQLITE_OK) {
		return 1;
	}
	for (unsigned i = 0; i < len; i++) {
		struct vfs2_wal_frame_hdr frame_hdr;
		rv = wal_cur->xWrite(wal_cur, &frame_hdr, sizeof(frame_hdr), ofst);
		if (rv != SQLITE_OK) {
			return 1;
		}
		ofst += sizeof(frame_hdr);
		rv = wal_cur->xWrite(wal_cur, frames[i].data, page_size, ofst);
		if (rv != SQLITE_OK) {
			return 1;
		}
		ofst += page_size;
	}

	sm_move(&entry->wtx_sm, WTX_FOLLOWING);
	*/
	return 0;

}
