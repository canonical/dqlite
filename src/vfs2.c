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
 * States:
 *
 * - INITIAL
 * - IN_WRITE_TXN
 * - WRITE_TXN_HIDDEN
 * - WRITE_TXN_POLLED
 *
 *
 *                                      +--                 >---------->--------->INITIAL <<< xWrite WAL header (swap)
 *                                      |                   |          |          |
 *                                      |                   |          |          |
 *                                      |                   |          |          | xWrite(size = WAL_FRAME_HDR_SIZE)
 *                                      |                   | rollback |          |
 *                                      |                   |          |          |
 *                                      |                   |          |          v
 *                                      |        vfs2_abort |          +----------IN_WRITE_TXN <<< xWrite further frames
 *                                      |           or      |                     |
 *                                      |        xWrite(wal)|                     |
 *             vfs2_apply or vfs2_abort |                   |                     | SQLITE_FCNTL_COMMIT_PHASETWO
 *                or xWrite(wal)        |                   |                     |
 *                                      |                   |                     |
 *                                      |                   |                     v
 *                                      |                   +---------------------WRITE_TXN_HIDDEN
 *                                      |                                         |
 *                                      |                                         |
 *                                      |                                         | vfs2_poll or vfs2_shallow_poll
 *                                      |                                         |
 *                                      |                                         |
 *                                      |                                         v
 *                                      +--                                    ---WRITE_TXN_POLLED
 *
 *
 *
 *
 *                                      UNRECOVERABLE_ERROR?
 *
 */

enum {
	WTX_INITIAL,
	WTX_ESTABL,
	WTX_ACTIVE,
	WTX_HIDDEN,
	WTX_POLLED
};

static const struct sm_conf wtx_states[SM_STATES_MAX] = {
        [WTX_INITIAL] = {
                .flags = SM_INITIAL|SM_FINAL,
                .name = "initial",
                .allowed = BITS(WTX_ESTABL),
        },
	[WTX_ESTABL] = {
		.flags = SM_FINAL,
		.name = "establ",
		.allowed = BITS(WTX_ESTABL)|BITS(WTX_ACTIVE),
	},
        [WTX_ACTIVE] = {
                .flags = SM_FINAL,
                .name = "active",
                .allowed = BITS(WTX_ESTABL)|BITS(WTX_ACTIVE)|BITS(WTX_HIDDEN),
        },
        [WTX_HIDDEN] = {
                .flags = SM_FINAL,
                .name = "hidden",
                .allowed = BITS(WTX_ESTABL)|BITS(WTX_HIDDEN)|BITS(WTX_POLLED),
        },
        [WTX_POLLED] = {
                .flags = SM_FINAL,
                .name = "polled",
                .allowed = BITS(WTX_ESTABL)|BITS(WTX_POLLED),
        },
};

/**
 * Userdata owned by the VFS.
 */
struct vfs2_data
{
	sqlite3_vfs *orig;       /* underlying VFS */
	pthread_rwlock_t rwlock; /* protects the queue */
	atomic_uint page_size;
	queue queue; /* queue of vfs2_db_entry */
};

/**
 * Linked list element representing a single database/WAL pair.
 */
struct vfs2_db_entry
{
	struct vfs2_file *db;
	struct vfs2_file *wal;

	struct sm wtx_sm;

	queue link;
};

/**
 * Layout-compatible with the first part of the WAL index header.
 */
struct vfs2_wal_index_basic_hdr
{
	uint32_t iVersion;
	uint8_t unused[4];
	uint32_t iChange;
	uint8_t isInit;
	uint8_t bigEndCksum;
	uint16_t szPage;
	uint32_t mxFrame;
	uint32_t nPage;
	uint32_t aFrameCksum[2];
	uint32_t aSalt[2];
	uint32_t aCksum[2];
};

static const struct vfs2_wal_index_basic_hdr zeroed_basic_hdr = {0};

struct vfs2_wal_hdr
{
	uint32_t magic;
	uint32_t version;
	uint32_t page_size;
	uint32_t ckpoint_seqno;
	uint32_t salt[2];
	uint32_t cksum[2];
};

struct vfs2_wal_index_full_hdr
{
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

struct vfs2_wal
{
	const char *moving_name;   /* e.g. /path/to/my.db-wal */
	char *wal_cur_fixed_name;  /* e.g. /path/to/my.db-xwal1 */
	sqlite3_file *wal_prev;    /* underlying file object for WAL-prev */
	char *wal_prev_fixed_name; /* e.g. /path/to/my.db-xwal2 */

	uint32_t commit_end; /* frame index, zero-based */

	/* All pending_txn fields pertain to a transaction that has at least one
	 * frame in the WAL and is the last transaction represented in the WAL.
	 * Writing a frame either updates the pending transaction or starts a
	 * new transaction. A frame starts a new transaction if it is written at
	 * the end of the WAL and the physically preceding frame has a nonzero
	 * commit marker. */
	dqlite_vfs_frame *pending_txn_frames;   /* for vfs2_poll */
	uint32_t pending_txn_len;
	uint32_t pending_txn_last_frame_commit; /* commit marker for the
						   physical last frame */

	/* Used for bookkeeping related to the WAL swap. XXX can we get rid
	 * of this somehow? */
	bool is_empty;
};

struct vfs2_db
{
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
struct vfs2_file
{
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

static struct vfs2_wal_index_full_hdr *get_full_hdr(struct vfs2_db *db)
{
	PRE(db->regions_len > 0);
	PRE(db->regions != NULL);
	return db->regions[0];
}

static bool region0_mapped(struct vfs2_db *db)
{
	return db->regions_len > 0
		&& db->regions != NULL
		&& db->regions[0] != NULL;
}

static bool no_pending_txn(struct vfs2_wal *wal)
{
	return wal->pending_txn_len == 0 && wal->pending_txn_frames == NULL;
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

static bool wal_index_basic_hdr_equal(struct vfs2_wal_index_basic_hdr a, struct vfs2_wal_index_basic_hdr b)
{
	return memcmp(&a, &b, sizeof(struct vfs2_wal_index_basic_hdr)) == 0;
}

static bool wal_index_basic_hdr_advanced(struct vfs2_wal_index_basic_hdr new, struct vfs2_wal_index_basic_hdr old)
{
	return new.iChange == old.iChange + 1
		&& new.nPage >= old.nPage /* no vacuums here */
		// XXX this is the zero salts thing again
		/* && memcmp(new.aSalt, old.aSalt, sizeof(new.aSalt)) == 0 */
		&& new.mxFrame > old.mxFrame;
}

static bool wtx_invariant(const struct sm *sm, int prev)
{
	struct vfs2_db_entry *entry = CONTAINER_OF(sm, struct vfs2_db_entry, wtx_sm);
	struct vfs2_wal *wal = (entry->wal == NULL) ? NULL : &entry->wal->wal;
	struct vfs2_db *db_shm = (entry->db == NULL) ? NULL : &entry->db->db_shm;

	if (db_shm != NULL && db_shm->regions_len > 0) {
		struct vfs2_wal_index_full_hdr *hdr = db_shm->regions[0];
		tracef("region0 salts are %u %u", ByteGetBe32((void *)&hdr->basic[0].aSalt[0]), ByteGetBe32((void *)&hdr->basic[0].aSalt[1]));
	}

	if (sm_state(sm) == WTX_INITIAL) {
		CHECK(wal == NULL);
		CHECK(db_shm != NULL);
		CHECK(!write_lock_held(db_shm));
	}

	if (sm_state(sm) == WTX_ESTABL) {
		CHECK(db_shm != NULL);
		CHECK(wal != NULL);
		CHECK(no_pending_txn(wal));

		struct vfs2_wal_index_full_hdr *hdr = get_full_hdr(db_shm);
		CHECK(wal_index_basic_hdr_equal(db_shm->prev_txn_hdr, hdr->basic[0]));
		CHECK(wal_index_basic_hdr_equal(db_shm->pending_txn_hdr, zeroed_basic_hdr));

		if (prev == WTX_ESTABL) {
			/* just after a WAL swap */
			CHECK(write_lock_held(db_shm));
			CHECK(region0_mapped(db_shm));
			CHECK(wal_index_hdr_fresh(hdr));
		}
	}

	if (sm_state(sm) == WTX_ACTIVE) {
		CHECK(db_shm != NULL);
		CHECK(wal != NULL);
		CHECK(have_pending_txn(wal));
		CHECK(region0_mapped(db_shm));
		CHECK(write_lock_held(db_shm));

		struct vfs2_wal_index_full_hdr *hdr = get_full_hdr(db_shm);
		CHECK(wal_index_basic_hdr_equal(hdr->basic[0], db_shm->prev_txn_hdr) || wal_index_basic_hdr_advanced(hdr->basic[0], db_shm->prev_txn_hdr));
		CHECK(wal_index_basic_hdr_equal(db_shm->pending_txn_hdr, zeroed_basic_hdr));

		if (prev == WTX_INITIAL) {
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
		CHECK(wal_index_basic_hdr_equal(hdr->basic[0], db_shm->prev_txn_hdr));
		CHECK(wal_index_basic_hdr_advanced(db_shm->pending_txn_hdr, hdr->basic[0]));
	}

	if (sm_state(sm) == WTX_POLLED) {
		CHECK(db_shm != NULL);
		CHECK(wal != NULL);
		CHECK(!have_pending_txn(wal));
		CHECK(region0_mapped(db_shm));
		CHECK(write_lock_held(db_shm));

		struct vfs2_wal_index_full_hdr *hdr = get_full_hdr(db_shm);
		CHECK(wal_index_basic_hdr_equal(hdr->basic[0], db_shm->prev_txn_hdr));
		CHECK(wal_index_basic_hdr_advanced(db_shm->pending_txn_hdr, hdr->basic[0]));
	}

	return true;
}

static bool is_valid_page_size(unsigned long n)
{
	return n >= 1 << 9 && n <= 1 << 16 && (n & (n - 1)) == 0;
}

static void unregister_file(struct vfs2_file *file)
{
	pthread_rwlock_wrlock(&file->vfs_data->rwlock);
	/* Not using QUEUE__FOREACH here, we want to be careful and explicit
	 * since we're modifying the queue while iterating over it. */
	queue *q = QUEUE__NEXT(&file->vfs_data->queue);
	while (q != &file->vfs_data->queue) {
		queue *next = QUEUE__NEXT(q);
		struct vfs2_db_entry *entry =
		    QUEUE__DATA(q, struct vfs2_db_entry, link);
		if (entry->db == file) {
			entry->db = NULL;
		} else if (entry->wal == file) {
			entry->wal = NULL;
		}
		if (entry->db == NULL && entry->wal == NULL) {
			QUEUE__PREV_NEXT(q) = QUEUE__NEXT(q);
			QUEUE__NEXT_PREV(q) = QUEUE__PREV(q);
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
			for (uint32_t i = 0; i < xfile->wal.pending_txn_len; i++) {
				sqlite3_free(xfile->wal.pending_txn_frames[i].data);
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

static int vfs2_wal_swap(struct vfs2_file *wal, const struct vfs2_wal_hdr *hdr)
{
	tracef("WAL SWAP TIME B)");
	PRE(wal->wal.pending_txn_len == 0);
	PRE(wal->wal.pending_txn_frames == NULL);
	int rv;

	sqlite3_file *phys_outgoing = wal->orig;
	char *name_outgoing = wal->wal.wal_cur_fixed_name;
	sqlite3_file *phys_incoming = wal->wal.wal_prev;
	char *name_incoming = wal->wal.wal_prev_fixed_name;

	/* Write the new header of the incoming WAL. */
	rv = phys_incoming->pMethods->xWrite(phys_incoming, hdr,
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

	/* Copy the WAL index header that SQLite has written so
	 * that we can restore it later. This relies on SQLite
	 * writing the WAL index header before restarting the
	 * WAL -- assert that this is the case by comparing salts. */
	assert(db->db_shm.regions_len > 0);
	union vfs2_shm_region0 *region0 = db->db_shm.regions[0];
	assert(hdr->salt[0] == region0->hdr.basic[0].aSalt[0]);
	assert(hdr->salt[1] == region0->hdr.basic[0].aSalt[1]);
	db->db_shm.prev_txn_hdr = region0->hdr.basic[0];

	sm_move(&wal->entry->wtx_sm, WTX_ESTABL);

	/* Move the moving name. */
	rv = unlink(wal->wal.moving_name);
	if (rv != 0) {
		return SQLITE_IOERR;
	}
	rv = link(name_incoming, wal->wal.moving_name);
	if (rv != 0) {
		return SQLITE_IOERR;
	}

	/* Best-effort: invalidate the outgoing physical WAL so that nobody gets confused. */
	(void)phys_outgoing->pMethods->xWrite(phys_outgoing, &invalid_magic,
					      sizeof(invalid_magic), 0);
	return SQLITE_OK;
}

static int vfs2_wal_write_frame_hdr(struct vfs2_file *wal,
				    const void *buf,
				    sqlite3_int64 x)
{
	x -= wal->wal.commit_end;
	assert(0 <= x && x <= wal->wal.pending_txn_len);
	uint32_t n = wal->wal.pending_txn_len;
	dqlite_vfs_frame *frames = wal->wal.pending_txn_frames;
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
	uint32_t page_size = atomic_load(&wal->vfs_data->page_size);
	assert(is_valid_page_size(page_size));
	uint32_t frame_size = VFS2_WAL_FRAME_HDR_SIZE + page_size;

	if (ofst == 0) {
		assert(amt == sizeof(struct vfs2_wal_hdr));

		struct vfs2_file *db = wal->entry->db;
		assert(db->db_shm.regions_len > 0);
		union vfs2_shm_region0 *region0 = db->db_shm.regions[0];
		// XXX investigate why region0 has zero salts at this point (before the first WAL swap)
		assert(region0->hdr.basic[0].isInit != 0);
		db->db_shm.prev_txn_hdr = region0->hdr.basic[0];

		sm_move(&wal->entry->wtx_sm, WTX_ESTABL);
	} else if (amt == VFS2_WAL_FRAME_HDR_SIZE) {
		tracef("ofst=%lld pgsz=%u", ofst, wal->vfs_data->page_size);
		sqlite3_int64 x =
		    ofst - (sqlite3_int64)sizeof(struct vfs2_wal_hdr);
		assert(x % frame_size == 0);
		x /= frame_size;
		return vfs2_wal_write_frame_hdr(wal, buf, x);
	} else if (amt == (int)wal->vfs_data->page_size) {
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
	}

	return SQLITE_OK;
}

static int vfs2_write(sqlite3_file *file,
		      const void *buf,
		      int amt,
		      sqlite3_int64 ofst)
{
	int rv;
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	bool wal_was_empty;

	if ((xfile->flags & SQLITE_OPEN_WAL) && ofst == 0) {
		assert(amt == sizeof(struct vfs2_wal_hdr));
		wal_was_empty = xfile->wal.is_empty;
		xfile->wal.is_empty = false;
		if (!wal_was_empty) {
			return vfs2_wal_swap(xfile, buf);
		}
		// sm_move(&xfile->entry->wtx_sm, WTX_ESTABL);
	}

	rv = xfile->orig->pMethods->xWrite(xfile->orig, buf, amt, ofst);
	if (rv != SQLITE_OK) {
		return rv;
	}

	if (xfile->flags & SQLITE_OPEN_WAL) {
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

static int vfs2_file_control(sqlite3_file *file, int op, void *arg)
{
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	assert(xfile->flags & SQLITE_OPEN_MAIN_DB);

	if (op == SQLITE_FCNTL_COMMIT_PHASETWO) {
		/* If no frames have been written to the WAL, this is a no-op. */
		if (xfile->entry->wal == NULL || xfile->entry->wal->wal.pending_txn_len == 0) {
			goto forward;
		}
		/* Else hide the transaction that was just written by resetting
		 * the WAL index header. */
		struct vfs2_wal_index_full_hdr *hdr = get_full_hdr(&xfile->db_shm);
		xfile->db_shm.pending_txn_hdr = hdr->basic[0];
		hdr->basic[0] = xfile->db_shm.prev_txn_hdr;
		hdr->basic[1] = hdr->basic[0];
		sm_move(&xfile->entry->wtx_sm, WTX_HIDDEN);
	} else if (op == SQLITE_FCNTL_PRAGMA) {
		char **args = arg;
		char **e = &args[0];
		char *left = args[1];
		char *right = args[2];
		char *end;
		if (strcmp(left, "page_size") == 0 && right != NULL) {
			/* TODO detect and set default page size??? */
			unsigned long pgsz = strtoul(right, &end, 10);
			if (*end != '\0' || !is_valid_page_size(pgsz)) {
				/* Let SQLite return whatever error code is
				 * appropriate for this case */
				goto forward;
			}
			unsigned expected = 0;
			if (!atomic_compare_exchange_strong(
				&xfile->vfs_data->page_size, &expected,
				(unsigned)pgsz) &&
			    expected != pgsz) {
				*e = sqlite3_mprintf(
				    "can't modify page size once set");
				return SQLITE_ERROR;
			}
		} else if (strcmp(left, "journal_mode") == 0 && right != NULL) {
			if (strcasecmp(right, "wal") != 0) {
				*e =
				    sqlite3_mprintf("dqlite requires WAL mode");
				return SQLITE_ERROR;
			}
		}
	} else if (op == SQLITE_FCNTL_PERSIST_WAL) {
		int *out = arg;
		*out = 1;
		return SQLITE_OK;
	}

forward:
	return xfile->orig->pMethods->xFileControl(xfile->orig, op, arg);
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
	tracef("vfs2_shm_map(%p, %d, %d, %d, %p)", file, pgno, pgsz, extend, out);
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

		/* Unlocking the write lock: roll back any uncommitted transaction. */
		if (ofst == VFS2_SHM_WRITE_LOCK) {
			assert(n == 1);
			struct vfs2_file *wal = xfile->entry->wal;

			if (wal->wal.pending_txn_len > 0 && wal->wal.pending_txn_last_frame_commit == 0) {
				for (uint32_t i = 0; i < wal->wal.pending_txn_len; i++) {
					sqlite3_free(wal->wal.pending_txn_frames[i].data);
				}
				sqlite3_free(wal->wal.pending_txn_frames);
				wal->wal.pending_txn_frames = NULL;
				wal->wal.pending_txn_len = 0;
				sm_move(&wal->entry->wtx_sm, WTX_ESTABL);
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
    .xUnfetch = vfs2_unfetch};

/* sqlite3_vfs implementations begin here */

static int compare_wals(sqlite3_file *xwal1,
			sqlite3_file *xwal2,
			bool *invert,
			bool *empty)
{
	int rv;
	sqlite3_int64 size1;
	rv = xwal1->pMethods->xFileSize(xwal1, &size1);
	if (rv != SQLITE_OK) {
		return rv;
	}
	sqlite3_int64 size2;
	rv = xwal2->pMethods->xFileSize(xwal2, &size2);
	if (rv != SQLITE_OK) {
		return rv;
	}

	if (size1 == 0 && size2 == 0) {
		*empty = true;
	}

	if (size2 == 0) {
		*invert = false;
	} else if (size1 == 0) {
		*invert = true;
	} else {
		struct vfs2_wal_hdr hdr1;
		rv = xwal1->pMethods->xRead(xwal1, &hdr1, sizeof(hdr1), 0);
		if (rv != SQLITE_OK) {
			return rv;
		}
		struct vfs2_wal_hdr hdr2;
		rv = xwal2->pMethods->xRead(xwal2, &hdr2, sizeof(hdr2), 0);
		if (rv != SQLITE_OK) {
			return rv;
		}
		uint32_t counter1 = ByteGetBe32((const void *)&hdr1.cksum[0]);
		uint32_t counter2 = ByteGetBe32((const void *)&hdr2.cksum[0]);
		if (counter1 == counter2 + 1) {
			*invert = false;
		} else if (counter2 == counter1 + 1) {
			*invert = true;
		} else {
			return SQLITE_ERROR;
		}
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
	if ((size_t)(dash - name) + strlen(VFS2_WAL_FIXED_SUFFIX1) >
	    (size_t)data->orig->mxPathname) {
		rv = SQLITE_ERROR;
		goto err;
	}

	/* Collect memory allocations in one place to simplify the control flow
	 */
	char *fixed1 = sqlite3_malloc(data->orig->mxPathname + 1);
	char *fixed2 = sqlite3_malloc(data->orig->mxPathname + 1);
	sqlite3_file *phys1 = sqlite3_malloc(data->orig->szOsFile);
	sqlite3_file *phys2 = sqlite3_malloc(data->orig->szOsFile);
	if (fixed1 == NULL || fixed2 == NULL || phys1 == NULL || phys2 == NULL) {
		rv = SQLITE_NOMEM;
		goto err;
	}

	/* Open the two physical WALs */
	strncpy(fixed1, name, (size_t)(dash - name));
	fixed1[dash - name] = '\0';
	strcat(fixed1, VFS2_WAL_FIXED_SUFFIX1);
	strncpy(fixed2, name, (size_t)(dash - name));
	fixed2[dash - name] = '\0';
	strcat(fixed2, VFS2_WAL_FIXED_SUFFIX2);
	int out_flags1, out_flags2;
	memset(phys1, 0, (size_t)data->orig->szOsFile);
	rv = data->orig->xOpen(data->orig, fixed1, phys1, flags, &out_flags1);
	if (rv != SQLITE_OK) {
		goto err_after_open_phys1;
	}
	memset(phys2, 0, (size_t)data->orig->szOsFile);
	rv = data->orig->xOpen(data->orig, fixed2, phys2, flags, &out_flags2);
	if (rv != SQLITE_OK) {
		goto err_after_open_phys2;
	}

	bool invert;
	rv = compare_wals(phys1, phys2, &invert, &xout->wal.is_empty);
	if (invert) {
		rv = unlink(name);
		(void)rv;
		rv = link(fixed2, name);
		(void)rv;

		xout->orig = phys2;
		xout->wal.moving_name = name;
		xout->wal.wal_cur_fixed_name = fixed2;
		xout->wal.wal_prev = phys1;
		xout->wal.wal_prev_fixed_name = fixed1;

		if (out_flags != NULL) {
			*out_flags = out_flags2;
		}
	} else {
		rv = unlink(name);
		(void)rv;
		rv = link(fixed1, name);
		(void)rv;

		xout->orig = phys1;
		xout->wal.moving_name = name;
		xout->wal.wal_cur_fixed_name = fixed1;
		xout->wal.wal_prev = phys2;
		xout->wal.wal_prev_fixed_name = fixed2;

		if (out_flags != NULL) {
			*out_flags = out_flags1;
		}
	}

	xout->wal.is_empty = true;

	return SQLITE_OK;

err_after_open_phys2:
	if (phys2->pMethods != NULL) {
		phys2->pMethods->xClose(phys2);
	}
err_after_open_phys1:
	if (phys1->pMethods != NULL) {
		phys1->pMethods->xClose(phys1);
	}
	sqlite3_free(phys2);
	sqlite3_free(phys1);
	sqlite3_free(fixed2);
	sqlite3_free(fixed1);
err:
	return rv;
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
	return SQLITE_OK;
}

static struct vfs2_db_entry *get_or_create_entry(struct vfs2_data *data, int flags, const char *name, struct vfs2_file *f)
{
	queue *q;
	struct vfs2_db_entry *res = NULL;

	pthread_rwlock_rdlock(&data->rwlock);
	QUEUE__FOREACH(q, &data->queue)
	{
		struct vfs2_db_entry *cur = QUEUE__DATA(q, struct vfs2_db_entry, link);
		if ((flags & SQLITE_OPEN_MAIN_DB) && cur->wal != NULL) {
			const char *walname = cur->wal->wal.moving_name;
			const char *dash = strrchr(walname, '-');
			assert(dash != NULL);
			if (strncmp(walname, name, (size_t)(dash - walname)) != 0) {
				continue;
			}
			res = cur;
			cur->db = f;
			break;
		} else if ((f->flags & SQLITE_OPEN_WAL) && cur->db != NULL) {
			const char *walname = name;
			const char *dash = strrchr(walname, '-');
			assert(dash != NULL);
			if (strncmp(walname, cur->db->db_shm.name,
				    (size_t)(dash - walname)) != 0) {
				continue;
			}
			res = cur;
			cur->wal = f;
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
	if (f->flags & SQLITE_OPEN_MAIN_DB) {
		res->db = f;
		res->wal = NULL;
	} else if (f->flags & SQLITE_OPEN_WAL) {
		res->db = NULL;
		res->wal = f;
	}
	tracef("sm_init %p", res);
	sm_init(&res->wtx_sm, wtx_invariant, NULL, wtx_states, WTX_INITIAL);
	pthread_rwlock_wrlock(&data->rwlock);
	QUEUE__PUSH(&data->queue, &res->link);
	pthread_rwlock_unlock(&data->rwlock);
	return res;
}

static int vfs2_open(sqlite3_vfs *vfs,
		     const char *name,
		     sqlite3_file *out,
		     int flags,
		     int *out_flags)
{
	tracef("open %s", name);
	struct vfs2_file *xout = (struct vfs2_file *)out;
	struct vfs2_data *data = vfs->pAppData;
	/* We unconditionally set pMethods in the output, so SQLite will always
	 * call xClose. */
	xout->base.pMethods = &vfs2_io_methods;
	xout->flags = flags;
	xout->vfs_data = data;

	if (flags & SQLITE_OPEN_WAL) {
		xout->entry = get_or_create_entry(data, flags, name, xout);
		if (xout->entry == NULL) {
			return SQLITE_NOMEM;
		}
		return vfs2_open_wal(vfs, name, xout, flags, out_flags);
	} else if (flags & SQLITE_OPEN_MAIN_DB) {
		xout->entry = get_or_create_entry(data, flags, name, xout);
		if (xout->entry == NULL) {
			return SQLITE_NOMEM;
		}
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

/* TODO update this */
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
	QUEUE__INIT(&data->queue);
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

/* FIXME what return codes should this use? */
int vfs2_apply(sqlite3_file *file)
{
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
	xfile->db_shm.pending_txn_hdr = zeroed_basic_hdr;
	wal->wal.commit_end += wal->wal.pending_txn_len;
	wal->wal.pending_txn_len = 0;

	sm_move(&xfile->entry->wtx_sm, WTX_ESTABL);

	return 0;
}

int vfs2_shallow_poll(sqlite3_file *file, struct vfs2_wal_slice *out)
{
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	if (!(xfile->flags & SQLITE_OPEN_MAIN_DB)) {
		return 1;
	}

	uint32_t len = xfile->db_shm.pending_txn_hdr.mxFrame - xfile->db_shm.prev_txn_hdr.mxFrame;
	if (len > 0) {
		/* Don't go through vfs2_shm_lock here since that has additional checks
		 * that assume the context of being called from inside SQLite. */
		unsigned *locks = xfile->db_shm.locks;
		if (locks[VFS2_SHM_WRITE_LOCK] > 0) {
			return 1;
		}
		locks[VFS2_SHM_WRITE_LOCK] = VFS2_EXCLUSIVE;

		struct vfs2_wal *wal = &xfile->entry->wal->wal;
		dqlite_vfs_frame *frames = wal->pending_txn_frames;
		uint32_t n = wal->pending_txn_len;
		for (uint32_t i = 0; i < n; i++) {
			sqlite3_free(frames[i].data);
		}
		sqlite3_free(frames);
		wal->pending_txn_frames = NULL;
	}

	out->salt[0] = xfile->db_shm.pending_txn_hdr.aSalt[0];
	out->salt[1] = xfile->db_shm.pending_txn_hdr.aSalt[1];
	out->start = xfile->db_shm.prev_txn_hdr.mxFrame;
	out->len = len;

	sm_move(&xfile->entry->wtx_sm, WTX_POLLED);

	return 0;
}

int vfs2_poll(sqlite3_file *file, dqlite_vfs_frame **frames, unsigned *n)
{
	struct vfs2_file *xfile = (struct vfs2_file *)file;
	if (!(xfile->flags & SQLITE_OPEN_MAIN_DB)) {
		return 1;
	}
	struct vfs2_file *wal = xfile->entry->wal;
	if (wal == NULL) {
		return 1;
	}

	*n = wal->wal.pending_txn_len;
	*frames = wal->wal.pending_txn_frames;
	wal->wal.pending_txn_frames = NULL;

	if (*n > 0) {
		/* Don't go through vfs2_shm_lock here since that has additional checks
		 * that assume the context of being called from inside SQLite. */
		unsigned *locks = xfile->db_shm.locks;
		if (locks[VFS2_SHM_WRITE_LOCK] > 0) {
			return 1;
		}
		locks[VFS2_SHM_WRITE_LOCK] = VFS2_EXCLUSIVE;
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

	union vfs2_shm_region0 *region0 = xfile->db_shm.regions[0];
	region0->hdr.basic[0] = xfile->db_shm.prev_txn_hdr;
	region0->hdr.basic[1] = xfile->db_shm.prev_txn_hdr;
	xfile->db_shm.pending_txn_hdr = zeroed_basic_hdr;

	dqlite_vfs_frame *frames = wal->wal.pending_txn_frames;
	uint32_t n = wal->wal.pending_txn_len;
	for (uint32_t i = 0; i < n; i++) {
		sqlite3_free(frames[i].data);
	}
	sqlite3_free(frames);
	wal->wal.pending_txn_frames = NULL;
	wal->wal.pending_txn_len = 0;

	sm_move(&xfile->entry->wtx_sm, WTX_ESTABL);
	return 0;
}
