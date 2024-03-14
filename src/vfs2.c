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
struct common {
	sqlite3_vfs *orig;       /* underlying VFS */
	pthread_rwlock_t rwlock; /* protects the queue */
	queue queue; /* queue of entry */
};

/**
 * Layout-compatible with the first part of the WAL index header.
 */
struct wal_index_basic_hdr {
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

struct wal_hdr {
	uint8_t magic[4];
	uint8_t version[4];
	uint8_t page_size[4];
	uint8_t ckpoint_seqno[4];
	struct vfs2_salts salts;
	uint8_t cksum1[4];
	uint8_t cksum2[4];
};

struct wal_index_full_hdr {
	struct wal_index_basic_hdr basic[2];
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
	struct wal_index_full_hdr hdr;
	char bytes[VFS2_WAL_INDEX_REGION_SIZE];
};

struct entry {
	struct common *common;

	uint32_t page_size;

	char *main_db_name;

	char *wal_moving_name;
	char *wal_cur_fixed_name;
	sqlite3_file *wal_cur;
	char *wal_prev_fixed_name;
	sqlite3_file *wal_prev;

	unsigned refcount_main_db;
	unsigned refcount_wal;

	struct wal_index_basic_hdr prev_txn_hdr;
	struct wal_index_basic_hdr pending_txn_hdr;

	void **shm_regions;
	int shm_regions_len;
	unsigned shm_refcount;
	unsigned shm_locks[SQLITE_SHM_NLOCK];

	dqlite_vfs_frame *pending_txn_frames;
	uint32_t pending_txn_start;
	uint32_t pending_txn_len;
	uint32_t pending_txn_last_frame_commit;

	uint32_t wal_cursor;

	struct sm wtx_sm;

	queue link;
};

/**
 * VFS-specific file object, upcastable to sqlite3_file.
 */
struct file {
	struct sqlite3_file base;
	int flags;
	sqlite3_file *orig; /* NULL for WAL */
	struct entry *entry; /* NULL for non-main non-WAL */
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

static struct wal_index_full_hdr *get_full_hdr(struct entry *e)
{
	PRE(e->shm_regions_len > 0);
	PRE(e->shm_regions != NULL);
	return e->shm_regions[0];
}

static bool region0_mapped(struct entry *e)
{
	return e->shm_regions_len > 0 && e->shm_regions != NULL &&
	       e->shm_regions[0] != NULL;
}

static bool no_pending_txn(struct entry *e)
{
	return e->pending_txn_len == 0 && e->pending_txn_frames == NULL && e->pending_txn_last_frame_commit == 0;
}

static bool have_pending_txn(struct entry *e)
{
	return e->pending_txn_len > 0 && e->pending_txn_frames != NULL;
}

static bool write_lock_held(struct entry *e)
{
	return e->shm_locks[VFS2_SHM_WRITE_LOCK] == VFS2_EXCLUSIVE;
}

static bool wal_index_hdr_fresh(const struct wal_index_full_hdr *hdr)
{
	/* TODO check other things here? */
	return hdr->basic[0].mxFrame == 0;
}

static bool wal_index_basic_hdr_equal(struct wal_index_basic_hdr a,
				      struct wal_index_basic_hdr b)
{
	return memcmp(&a, &b, sizeof(struct wal_index_basic_hdr)) == 0;
}

static bool wal_index_basic_hdr_advanced(struct wal_index_basic_hdr new,
					 struct wal_index_basic_hdr old)
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

static bool is_valid_page_size(unsigned long n)
{
	return n >= 1 << 9 && n <= 1 << 16 && (n & (n - 1)) == 0;
}

static bool wtx_invariant(const struct sm *sm, int prev)
{
	(void)sm;
	(void)prev;
	(void)wal_index_basic_hdr_advanced;
	(void)is_valid_page_size;
	(void)wal_index_basic_hdr_equal;
	(void)wal_index_hdr_fresh;
	(void)write_lock_held;
	(void)have_pending_txn;
	(void)no_pending_txn;
	(void)region0_mapped;
	// struct entry *e = CONTAINER_OF(sm, struct entry, wtx_sm);

	/* TODO go over these checks again and strengthen them */
	/* TODO rewrite in expression-oriented style? */

	// if (sm_state(sm) == WTX_NOT_OPEN) {
	// 	CHECK(wal == NULL);
	// }

	// if (sm_state(sm) == WTX_EMPTY) {
	// 	CHECK(wal != NULL);
	// }

	// if (sm_state(sm) == WTX_BASE) {
	// 	CHECK(db_shm != NULL);
	// 	CHECK(wal != NULL);
	// 	CHECK(no_pending_txn(wal));
	// 	CHECK(wal_index_basic_hdr_equal(db_shm->pending_txn_hdr, (struct wal_index_basic_hdr){}));

	// 	if (prev == WTX_BASE) {
	// 		/* just after a WAL swap */
	// 		CHECK(write_lock_held(db_shm));
	// 		CHECK(region0_mapped(db_shm));
	// 		CHECK(wal_index_hdr_fresh(get_full_hdr(db_shm)));
	// 	}
	// }

	// if (sm_state(sm) == WTX_ACTIVE) {
	// 	CHECK(db_shm != NULL);
	// 	CHECK(wal != NULL);
	// 	CHECK(have_pending_txn(wal));
	// 	CHECK(region0_mapped(db_shm));
	// 	CHECK(write_lock_held(db_shm));

	// 	struct wal_index_full_hdr *hdr = get_full_hdr(db_shm);
	// 	CHECK(wal_index_basic_hdr_equal(hdr->basic[0],
	// 					db_shm->prev_txn_hdr) ||
	// 	      wal_index_basic_hdr_advanced(hdr->basic[0],
	// 					   db_shm->prev_txn_hdr));
	// 	CHECK(wal_index_basic_hdr_equal(db_shm->pending_txn_hdr, (struct wal_index_basic_hdr){}));

	// 	if (prev == WTX_BASE) {
	// 		/* first frame in a txn */
	// 		CHECK(wal->pending_txn_len == 1);
	// 	}
	// }

	// if (sm_state(sm) == WTX_HIDDEN) {
	// 	CHECK(db_shm != NULL);
	// 	CHECK(wal != NULL);
	// 	CHECK(have_pending_txn(wal));
	// 	CHECK(region0_mapped(db_shm));
	// 	CHECK(!write_lock_held(db_shm));

	// 	struct wal_index_full_hdr *hdr = get_full_hdr(db_shm);
	// 	CHECK(wal_index_basic_hdr_equal(hdr->basic[0],
	// 					db_shm->prev_txn_hdr));
	// 	CHECK(wal_index_basic_hdr_advanced(db_shm->pending_txn_hdr,
	// 					   hdr->basic[0]));
	// }

	// if (sm_state(sm) == WTX_POLLED) {
	// 	CHECK(db_shm != NULL);
	// 	CHECK(wal != NULL);
	// 	CHECK(!have_pending_txn(wal));
	// 	CHECK(region0_mapped(db_shm));
	// 	CHECK(write_lock_held(db_shm));

	// 	struct wal_index_full_hdr *hdr = get_full_hdr(db_shm);
	// 	CHECK(wal_index_basic_hdr_equal(hdr->basic[0],
	// 					db_shm->prev_txn_hdr));
	// 	CHECK(wal_index_basic_hdr_advanced(db_shm->pending_txn_hdr,
	// 					   hdr->basic[0]));
	// }

	return true;
}

static int check_wal_integrity(sqlite3_file *f)
{
	/* TODO */
	(void)f;
	return SQLITE_OK;
}

/* sqlite3_io_methods implementations begin here */

static sqlite3_file *get_orig(struct file *f)
{
	if (f->flags & SQLITE_OPEN_WAL) {
		return f->entry->wal_cur;
	} else {
		return f->orig;
	}
}

static int vfs2_close(sqlite3_file *file)
{
	struct file *xfile = (struct file *)file;
	struct entry *e = xfile->entry;
	int rv;

	/* TODO unregister */

	int rvprev = SQLITE_OK;
	if (xfile->flags & SQLITE_OPEN_WAL) {
		sqlite3_free(e->wal_cur_fixed_name);
		sqlite3_free(e->wal_prev_fixed_name);
		if (e->wal_prev->pMethods != NULL) {
			rvprev = e->wal_prev->pMethods->xClose(e->wal_prev);
		}
		dqlite_vfs_frame *frames = e->pending_txn_frames;
		uint32_t len = e->pending_txn_len;
		if (frames != NULL) {
			for (uint32_t i = 0; i < len; i++) {
				sqlite3_free(frames[i].data);
			}
		}
		sqlite3_free(frames);
		sqlite3_free(e->wal_prev);
	} else if (xfile->flags & SQLITE_OPEN_MAIN_DB) {
		for (int i = 0; i < e->shm_regions_len; i++) {
			sqlite3_free(e->shm_regions[i]);
		}
		sqlite3_free(e->shm_regions);
	}
	rv = SQLITE_OK;
	sqlite3_file *orig = get_orig(xfile);
	if (orig->pMethods != NULL) {
		rv = orig->pMethods->xClose(orig);
	}
	sqlite3_free(orig);
	if (rv != SQLITE_OK) {
		return rv;
	}
	return rvprev;
}

static int vfs2_read(sqlite3_file *file, void *buf, int amt, sqlite3_int64 ofst)
{
	struct file *xfile = (struct file *)file;
	sqlite3_file *orig = get_orig(xfile);
	return orig->pMethods->xRead(orig, buf, amt, ofst);
}

static int vfs2_wal_swap(struct entry *e,
			 const struct wal_hdr *wal_hdr)
{
	PRE(e->pending_txn_len == 0);
	PRE(e->pending_txn_frames == NULL);
	int rv;

	sqlite3_file *phys_outgoing = e->wal_cur;
	char *name_outgoing = e->wal_cur_fixed_name;
	sqlite3_file *phys_incoming = e->wal_prev;
	char *name_incoming = e->wal_prev_fixed_name;

	tracef("wal swap outgoing=%s incoming=%s", name_outgoing, name_incoming);

	/* Write the new header of the incoming WAL. */
	rv = phys_incoming->pMethods->xWrite(phys_incoming, wal_hdr,
					     sizeof(struct wal_hdr), 0);
	if (rv != SQLITE_OK) {
		return rv;
	}

	/* In-memory WAL swap. */
	e->wal_cur = phys_incoming;
	e->wal_cur_fixed_name = name_incoming;
	e->wal_prev = phys_outgoing;
	e->wal_prev_fixed_name = name_outgoing;
	e->wal_cursor = 0;
	sm_move(&e->wtx_sm, WTX_BASE);

	/* Move the moving name. */
	rv = unlink(e->wal_moving_name);
	if (rv != 0) {
		return SQLITE_IOERR;
	}
	rv = link(name_incoming, e->wal_moving_name);
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

static int vfs2_wal_write_frame_hdr(struct entry *e,
				    const void *buf,
				    uint32_t x)
{
	dqlite_vfs_frame *frames = e->pending_txn_frames;
	uint32_t n = e->pending_txn_len;
	x -= e->pending_txn_start;
	assert(x <= n);
	if (e->pending_txn_len == 0 && x == 0) {
		/* check that the WAL-index hdr makes sense and save it */
		struct wal_index_basic_hdr hdr = get_full_hdr(e)->basic[0];
		assert(hdr.isInit != 0);
		assert(hdr.mxFrame == e->pending_txn_start);
		e->prev_txn_hdr = hdr;
	}
	if (x == n) {
		/* FIXME reallocating every time seems bad */
		sqlite3_uint64 z = (sqlite3_uint64)sizeof(*frames) * (sqlite3_uint64)(n + 1);
		e->pending_txn_frames = sqlite3_realloc64(frames, z);
		if (e->pending_txn_frames == NULL) {
			return SQLITE_NOMEM;
		}
		dqlite_vfs_frame *frame = &e->pending_txn_frames[n];
		frame->page_number = ByteGetBe32(buf);
		frame->data = NULL;
		e->pending_txn_last_frame_commit =
		    ByteGetBe32((const uint8_t *)buf + 4);
		e->pending_txn_len++;
	} else {
		/* Overwriting a previously-written frame in the current
		 * transaction. */
		dqlite_vfs_frame *frame = &e->pending_txn_frames[x];
		frame->page_number = ByteGetBe32(buf);
		sqlite3_free(frame->data);
		frame->data = NULL;
	}
	sm_move(&e->wtx_sm, WTX_ACTIVE);
	return SQLITE_OK;
}

static int vfs2_wal_post_write(struct entry *e,
			       const void *buf,
			       int amt,
			       sqlite3_int64 ofst)
{
	uint32_t frame_size = VFS2_WAL_FRAME_HDR_SIZE + e->page_size;
	if (amt == VFS2_WAL_FRAME_HDR_SIZE) {
		sqlite3_int64 x =
		    ofst - (sqlite3_int64)sizeof(struct wal_hdr);
		assert(x % frame_size == 0);
		x /= frame_size;
		return vfs2_wal_write_frame_hdr(e, buf, (uint32_t)x);
	} else if (amt == (int)e->page_size) {
		sqlite3_int64 x = ofst - VFS2_WAL_FRAME_HDR_SIZE -
				  (sqlite3_int64)sizeof(struct wal_hdr);
		assert(x % frame_size == 0);
		x /= frame_size;
		x -= e->pending_txn_start;
		assert(0 <= x && x < e->pending_txn_len);
		dqlite_vfs_frame *frame = &e->pending_txn_frames[x];
		assert(frame->data == NULL);
		frame->data = sqlite3_malloc(amt);
		if (frame->data == NULL) {
			return SQLITE_NOMEM;
		}
		memcpy(frame->data, buf, (size_t)amt);
		sm_move(&e->wtx_sm, WTX_ACTIVE);
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
	struct file *xfile = (struct file *)file;
	int rv;

	if ((xfile->flags & SQLITE_OPEN_WAL) && ofst == 0) {
		assert(amt == sizeof(struct wal_hdr));
		const struct wal_hdr *hdr = buf;
		struct entry *e = xfile->entry;
		e->page_size = ByteGetBe32(hdr->page_size);
		return vfs2_wal_swap(e, hdr);
	}

	sqlite3_file *orig = get_orig(xfile);
	rv = orig->pMethods->xWrite(orig, buf, amt, ofst);
	if (rv != SQLITE_OK) {
		return rv;
	}

	if (xfile->flags & SQLITE_OPEN_WAL) {
		struct entry *e = xfile->entry;
		tracef("wrote to WAL name=%s amt=%d ofst=%lld", e->wal_cur_fixed_name, amt, ofst);
		return vfs2_wal_post_write(e, buf, amt, ofst);
	}

	return SQLITE_OK;
}

static int vfs2_truncate(sqlite3_file *file, sqlite3_int64 size)
{
	struct file *xfile = (struct file *)file;
	sqlite3_file *orig = get_orig(xfile);
	return orig->pMethods->xTruncate(orig, size);
}

static int vfs2_sync(sqlite3_file *file, int flags)
{
	struct file *xfile = (struct file *)file;
	sqlite3_file *orig = get_orig(xfile);
	return orig->pMethods->xSync(orig, flags);
}

static int vfs2_file_size(sqlite3_file *file, sqlite3_int64 *size)
{
	struct file *xfile = (struct file *)file;
	sqlite3_file *orig = get_orig(xfile);
	return orig->pMethods->xFileSize(orig, size);
}

static int vfs2_lock(sqlite3_file *file, int mode)
{
	struct file *xfile = (struct file *)file;
	sqlite3_file *orig = get_orig(xfile);
	return orig->pMethods->xLock(orig, mode);
}

static int vfs2_unlock(sqlite3_file *file, int mode)
{
	struct file *xfile = (struct file *)file;
	sqlite3_file *orig = get_orig(xfile);
	return orig->pMethods->xUnlock(orig, mode);
}

static int vfs2_check_reserved_lock(sqlite3_file *file, int *out)
{
	struct file *xfile = (struct file *)file;
	sqlite3_file *orig = get_orig(xfile);
	return orig->pMethods->xCheckReservedLock(orig, out);
}

static int interpret_pragma(char **args)
{
	char **e = &args[0];
	char *left = args[1];
	PRE(left != NULL);
	char *right = args[2];

	if (strcmp(left, "journal_mode") == 0 && right != NULL && strcasecmp(right, "wal") != 0) {
		*e = sqlite3_mprintf("dqlite requires WAL mode");
		return SQLITE_ERROR;
	}

	return SQLITE_NOTFOUND;
}

static int vfs2_file_control(sqlite3_file *file, int op, void *arg)
{
	struct file *xfile = (struct file *)file;
	PRE(xfile->flags & SQLITE_OPEN_MAIN_DB);
	struct entry *e = xfile->entry;
	int rv;

	if (op == SQLITE_FCNTL_COMMIT_PHASETWO && e->pending_txn_len != 0) {
		/* Hide the transaction that was just written by resetting
		 * the WAL index header. */
		struct wal_index_full_hdr *hdr = get_full_hdr(e);
		e->pending_txn_hdr = hdr->basic[0];
		hdr->basic[0] = e->prev_txn_hdr;
		hdr->basic[1] = hdr->basic[0];
		sm_move(&xfile->entry->wtx_sm, WTX_HIDDEN);
	} else if (op == SQLITE_FCNTL_PRAGMA) {
		rv = interpret_pragma(arg);
		if (rv != SQLITE_NOTFOUND) {
			return rv;
		}
	} else if (op == SQLITE_FCNTL_PERSIST_WAL) {
		/* TODO handle setting as well as getting (?) */
		int *out = arg;
		*out = 1;
		return SQLITE_OK;
	}

	sqlite3_file *orig = get_orig(xfile);
	rv = orig->pMethods->xFileControl(orig, op, arg);
	assert(ERGO(op == SQLITE_FCNTL_PRAGMA, rv == SQLITE_NOTFOUND));
	return rv;
}

static int vfs2_sector_size(sqlite3_file *file)
{
	struct file *xfile = (struct file *)file;
	sqlite3_file *orig = get_orig(xfile);
	return orig->pMethods->xSectorSize(orig);
}

static int vfs2_device_characteristics(sqlite3_file *file)
{
	struct file *xfile = (struct file *)file;
	sqlite3_file *orig = get_orig(xfile);
	return orig->pMethods->xDeviceCharacteristics(orig);
}

static int vfs2_fetch(sqlite3_file *file,
		      sqlite3_int64 ofst,
		      int amt,
		      void **out)
{
	struct file *xfile = (struct file *)file;
	sqlite3_file *orig = get_orig(xfile);
	return orig->pMethods->xFetch(orig, ofst, amt, out);
}

static int vfs2_unfetch(sqlite3_file *file, sqlite3_int64 ofst, void *buf)
{
	struct file *xfile = (struct file *)file;
	sqlite3_file *orig = get_orig(xfile);
	return orig->pMethods->xUnfetch(orig, ofst, buf);
}

static int vfs2_shm_map(sqlite3_file *file,
			int regno,
			int regsz,
			int extend,
			void volatile **out)
{
	struct file *xfile = (struct file *)file;
	struct entry *e = xfile->entry;
	void *region;
	int rv;

	if (e->shm_regions != NULL && regno < e->shm_regions_len) {
		region = e->shm_regions[regno];
		assert(region != NULL);
	} else if (extend != 0) {
		assert(regno == e->shm_regions_len);
		region = sqlite3_malloc(regsz);
		if (region == NULL) {
			rv = SQLITE_NOMEM;
			goto err;
		}
		memset(region, 0, (size_t)regsz);
		/* FIXME reallocating every time seems bad */
		sqlite3_uint64 z = (sqlite3_uint64)sizeof(*e->shm_regions) * (sqlite3_uint64)(e->shm_regions_len + 1);
		void **regions = sqlite3_realloc64(e->shm_regions, z);
		if (regions == NULL) {
			rv = SQLITE_NOMEM;
			goto err_after_region_malloc;
		}
		e->shm_regions = regions;
		e->shm_regions[regno] = region;
		e->shm_regions_len++;
	} else {
		region = NULL;
	}

	*out = region;
	if (regno == 0 && region != NULL) {
		e->shm_refcount++;
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
	struct file *xfile = (struct file *)file;
	struct entry *e = xfile->entry;

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

	if (flags == (SQLITE_SHM_LOCK | SQLITE_SHM_SHARED)) {
		for (int i = ofst; i < ofst + n; i++) {
			if (e->shm_locks[i] == VFS2_EXCLUSIVE) {
				return SQLITE_BUSY;
			}
		}

		for (int i = ofst; i < ofst + n; i++) {
			e->shm_locks[i]++;
		}
	} else if (flags == (SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)) {
		for (int i = ofst; i < ofst + n; i++) {
			if (e->shm_locks[i] > 0) {
				return SQLITE_BUSY;
			}
		}

		for (int i = ofst; i < ofst + n; i++) {
			e->shm_locks[i] = VFS2_EXCLUSIVE;
		}

		/* XXX maybe this shouldn't be an assertion */
		if (ofst == VFS2_SHM_WRITE_LOCK) {
			assert(n == 1);
			assert(e->pending_txn_len == 0);
		}
	} else if (flags == (SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED)) {
		for (int i = ofst; i < ofst + n; i++) {
			assert(e->shm_locks[i] > 0);
			e->shm_locks[i]--;
		}
	} else if (flags == (SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE)) {
		for (int i = ofst; i < ofst + n; i++) {
			assert(e->shm_locks[i] == VFS2_EXCLUSIVE);
			e->shm_locks[i] = 0;
		}

		/* Unlocking the write lock: roll back any uncommitted
		 * transaction. */
		if (ofst == VFS2_SHM_WRITE_LOCK) {
			assert(n == 1);
			dqlite_vfs_frame *frames = e->pending_txn_frames;
			uint32_t len = e->pending_txn_len;
			if (len > 0 && e->pending_txn_last_frame_commit == 0) {
				for (uint32_t i = 0; i < len; i++) {
					sqlite3_free(frames[i].data);
				}
				sqlite3_free(frames);
				e->pending_txn_frames = NULL;
				e->pending_txn_len = 0;
				e->pending_txn_last_frame_commit = 0;
				sm_move(&e->wtx_sm, WTX_BASE);
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
	struct file *xfile = (struct file *)file;
	struct entry *e = xfile->entry;
	e->shm_refcount--;
	if (e->shm_refcount == 0) {
		for (int i = 0; i < e->shm_regions_len; i++) {
			void *region = e->shm_regions[i];
			assert(region != NULL);
			sqlite3_free(region);
		}
		sqlite3_free(e->shm_regions);

		e->shm_regions = NULL;
		e->shm_regions_len = 0;
		memset(e->shm_locks, 0, sizeof(e->shm_locks));
	}
	return SQLITE_OK;
}

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

static int compare_wal_headers(struct wal_hdr a,
			       struct wal_hdr b,
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

static int read_wal_hdr(sqlite3_file *wal, sqlite3_int64 *size, struct wal_hdr *hdr)
{
	int rv;

	rv = wal->pMethods->xFileSize(wal, size);
	if (rv != SQLITE_OK) {
		return rv;
	}
	if (*size >= (sqlite3_int64)sizeof(struct wal_hdr)) {
		rv = wal->pMethods->xRead(wal, hdr, sizeof(*hdr), 0);
		if (rv != SQLITE_OK) {
			return rv;
		}
	}
	return SQLITE_OK;
}

static int open_entry(struct common *common, const char *name, struct entry *e)
{
	sqlite3_vfs *v = common->orig;
	int path_cap = v->mxPathname + 1;
	int file_cap = v->szOsFile;
	int rv;

	*e = (struct entry){};
	e->common = common;
	e->main_db_name = sqlite3_malloc(path_cap);
	if (e->main_db_name == NULL) {
		return SQLITE_NOMEM;
	}
	strcpy(e->main_db_name, name);

	e->wal_moving_name = sqlite3_malloc(path_cap);
	if (e->wal_moving_name == NULL) {
		return SQLITE_NOMEM;
	}
	strcpy(e->wal_moving_name, name);
	strcat(e->wal_moving_name, "-wal");

	e->wal_cur_fixed_name = sqlite3_malloc(path_cap);
	if (e->wal_cur_fixed_name == NULL) {
		return SQLITE_NOMEM;
	}
	strcpy(e->wal_cur_fixed_name, name);
	strcat(e->wal_cur_fixed_name, "-xwal1");

	e->wal_prev_fixed_name = sqlite3_malloc(path_cap);
	if (e->wal_prev_fixed_name == NULL) {
		return SQLITE_NOMEM;
	}
	strcpy(e->wal_prev_fixed_name, name);
	strcat(e->wal_prev_fixed_name, "-xwal2");

	/* TODO more flags here? */
	int phys_wal_flags = SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_WAL|SQLITE_OPEN_NOFOLLOW|SQLITE_OPEN_EXRESCODE;

	e->wal_cur = sqlite3_malloc(file_cap);
	if (e->wal_cur == NULL) {
		return SQLITE_NOMEM;
	}
	rv = v->xOpen(v, e->wal_cur_fixed_name, e->wal_cur, phys_wal_flags, NULL);
	if (rv != SQLITE_OK) {
		return rv;
	}

	e->wal_prev = sqlite3_malloc(file_cap);
	if (e->wal_prev == NULL) {
		return SQLITE_NOMEM;
	}
	rv = v->xOpen(v, e->wal_prev_fixed_name, e->wal_prev, phys_wal_flags, NULL);
	if (rv != SQLITE_OK) {
		return rv;
	}

	sqlite3_int64 size1;
	struct wal_hdr hdr1;
	rv = read_wal_hdr(e->wal_cur, &size1, &hdr1);
	if (rv != SQLITE_OK) {
		return rv;
	}
	sqlite3_int64 size2;
	struct wal_hdr hdr2;
	rv = read_wal_hdr(e->wal_prev, &size2, &hdr2);
	if (rv != SQLITE_OK) {
		return rv;
	}

	struct wal_hdr hdr_cur = hdr1;
	sqlite3_int64 size_cur = size1;

	bool wal1_is_fresh;
	if (size2 < (sqlite3_int64)sizeof(struct wal_hdr)) {
		wal1_is_fresh = true;
	} else if (size1 < (sqlite3_int64)sizeof(struct wal_hdr)) {
		wal1_is_fresh = false;
	} else {
		rv = compare_wal_headers(hdr1, hdr2, &wal1_is_fresh);
		if (rv != SQLITE_OK) {
			return rv;
		}
	}
	if (!wal1_is_fresh) {
		void *temp;

		temp = e->wal_cur;
		e->wal_cur = e->wal_prev;
		e->wal_prev = temp;

		temp = e->wal_cur_fixed_name;
		e->wal_cur_fixed_name = e->wal_prev_fixed_name;
		e->wal_prev_fixed_name = temp;

		hdr_cur = hdr2;
		size_cur = size2;
	}

	rv = unlink(e->wal_moving_name);
	(void)rv;
	rv = link(e->wal_cur_fixed_name, e->wal_moving_name);
	(void)rv;

	if (size_cur >= (sqlite3_int64)sizeof(struct wal_hdr)) {
		e->page_size = ByteGetBe32(hdr_cur.page_size);
	}
	
	/* TODO sm_init with the appropriate state */
	(void)wtx_states;
	(void)wtx_invariant;

	return SQLITE_OK;
}

static int set_up_entry(struct common *common, const char *name, int flags, int *out_flags, struct entry **e)
{
	bool name_is_db = (flags & SQLITE_OPEN_MAIN_DB) != 0;
	bool name_is_wal = (flags & SQLITE_OPEN_WAL) != 0;
	assert(name_is_db ^ name_is_wal);
	int rv;

	struct entry *res = NULL;
	pthread_rwlock_rdlock(&common->rwlock);
	queue *q;
	QUEUE_FOREACH(q, &common->queue)
	{
		struct entry *cur = QUEUE_DATA(q, struct entry, link);
		if ((name_is_db && strcmp(cur->main_db_name, name) == 0) ||
		    (name_is_wal && strcmp(cur->wal_moving_name, name) == 0)) {
			res = cur;
			break;
		}
	}
	pthread_rwlock_unlock(&common->rwlock);
	if (res != NULL) {
		unsigned *count = name_is_db ? &res->refcount_main_db : &res->refcount_wal;
		*count += 1;
		sqlite3_free(*e);
		*e = res;
		/* TODO set out_flags if this is a WAL */
		(void)out_flags;
		return SQLITE_OK;
	}

	assert(name_is_db);
	res = *e;
	rv = open_entry(common, name, res);
	if (rv != SQLITE_OK) {
		sqlite3_free(res);
		*e = NULL;
		return rv;
	}
	res->refcount_main_db = 1;
	pthread_rwlock_wrlock(&common->rwlock);
	queue_insert_tail(&common->queue, &res->link);
	pthread_rwlock_unlock(&common->rwlock);
	return SQLITE_OK;
}

static int vfs2_open(sqlite3_vfs *vfs,
		     const char *name,
		     sqlite3_file *out,
		     int flags,
		     int *out_flags)
{
	struct file *xout = (struct file *)out;
	struct common *common = vfs->pAppData;
	int rv;

	*xout = (struct file){};
	xout->base.pMethods = &vfs2_io_methods;
	xout->flags = flags;

	if ((flags & SQLITE_OPEN_WAL) == 0) {
		sqlite3_vfs *v = common->orig;
		xout->orig = sqlite3_malloc(v->szOsFile);
		if (xout->orig == NULL) {
			return SQLITE_NOMEM;
		}
		rv = v->xOpen(v, name, xout->orig, flags, out_flags);
		if (rv != SQLITE_OK) {
			return rv;
		}
	}

	if (flags & (SQLITE_OPEN_MAIN_DB|SQLITE_OPEN_WAL)) {
		xout->entry = sqlite3_malloc(sizeof(*xout->entry));
		if (xout->entry == NULL) {
			return SQLITE_NOMEM;
		}
		rv = set_up_entry(common, name, flags, out_flags, &xout->entry);
		if (rv != SQLITE_OK) {
			return rv;
		}
	}

	return SQLITE_OK;
}

/* TODO does this need to be customized? */
static int vfs2_delete(sqlite3_vfs *vfs, const char *name, int sync_dir)
{
	struct common *data = vfs->pAppData;
	return data->orig->xDelete(data->orig, name, sync_dir);
}

/* TODO does this need to be customized? */
static int vfs2_access(sqlite3_vfs *vfs, const char *name, int flags, int *out)
{
	struct common *data = vfs->pAppData;
	return data->orig->xAccess(data->orig, name, flags, out);
}

static int vfs2_full_pathname(sqlite3_vfs *vfs,
			      const char *name,
			      int n,
			      char *out)
{
	struct common *data = vfs->pAppData;
	return data->orig->xFullPathname(data->orig, name, n, out);
}

static void *vfs2_dl_open(sqlite3_vfs *vfs, const char *filename)
{
	struct common *data = vfs->pAppData;
	return data->orig->xDlOpen(data->orig, filename);
}

static void vfs2_dl_error(sqlite3_vfs *vfs, int n, char *msg)
{
	struct common *data = vfs->pAppData;
	return data->orig->xDlError(data->orig, n, msg);
}

typedef void (*vfs2_sym)(void);
static vfs2_sym vfs2_dl_sym(sqlite3_vfs *vfs, void *dl, const char *symbol)
{
	struct common *data = vfs->pAppData;
	return data->orig->xDlSym(data->orig, dl, symbol);
}

static void vfs2_dl_close(sqlite3_vfs *vfs, void *dl)
{
	struct common *data = vfs->pAppData;
	return data->orig->xDlClose(data->orig, dl);
}

static int vfs2_randomness(sqlite3_vfs *vfs, int n, char *out)
{
	struct common *data = vfs->pAppData;
	return data->orig->xRandomness(data->orig, n, out);
}

static int vfs2_sleep(sqlite3_vfs *vfs, int microseconds)
{
	struct common *data = vfs->pAppData;
	return data->orig->xSleep(data->orig, microseconds);
}

static int vfs2_current_time(sqlite3_vfs *vfs, double *out)
{
	struct common *data = vfs->pAppData;
	return data->orig->xCurrentTime(data->orig, out);
}

/* TODO update this to reflect syscalls that we make ourselves (not through the
 * base VFS) */
static int vfs2_get_last_error(sqlite3_vfs *vfs, int n, char *out)
{
	struct common *data = vfs->pAppData;
	return data->orig->xGetLastError(data->orig, n, out);
}

static int vfs2_current_time_int64(sqlite3_vfs *vfs, sqlite3_int64 *out)
{
	struct common *data = vfs->pAppData;
	if (data->orig->iVersion < 2) {
		return SQLITE_ERROR;
	}
	return data->orig->xCurrentTimeInt64(data->orig, out);
}

/* sqlite3_vfs implementations end here */

sqlite3_vfs *vfs2_make(sqlite3_vfs *orig, const char *name)
{
	struct common *common = sqlite3_malloc(sizeof(*common));
	struct sqlite3_vfs *vfs = sqlite3_malloc(sizeof(*vfs));
	if (common == NULL || vfs == NULL) {
		return NULL;
	}
	common->orig = orig;
	pthread_rwlock_init(&common->rwlock, NULL);
	queue_init(&common->queue);
	vfs->iVersion = 2;
	vfs->szOsFile = sizeof(struct file);
	vfs->mxPathname = orig->mxPathname;
	vfs->zName = name;
	vfs->pAppData = common;
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

int vfs2_unapply_after(sqlite3_file *file, struct vfs2_wal_slice stop)
{
	/* TODO write lock stuff */
	/* TODO ensure we're in the right state? */
	/* TODO keep the wal-index header up to date (don't worry about the rest of the wal-index) */
	PRE(stop.len > 0);
	struct file *xfile = (struct file *)file;
	PRE(xfile->flags & SQLITE_OPEN_MAIN_DB);
	struct entry *e = xfile->entry;
	int rv;

	sqlite3_int64 size_cur;
	struct wal_hdr hdr_cur;
	rv = read_wal_hdr(e->wal_cur, &size_cur, &hdr_cur);
	if (rv != SQLITE_OK) {
		return 1;
	}
	if (size_cur < (sqlite3_int64)sizeof(struct wal_hdr)) {
		return 1;
	}

	sqlite3_int64 size_prev;
	struct wal_hdr hdr_prev;
	rv = read_wal_hdr(e->wal_prev, &size_prev, &hdr_prev);
	if (rv != SQLITE_OK) {
		return 1;
	}

	sqlite3_int64 implied_size =
	    (sqlite3_int64)sizeof(struct wal_hdr) +
	    (sqlite3_int64)(VFS2_WAL_FRAME_HDR_SIZE + e->page_size) *
	    (sqlite3_int64)(stop.start + stop.len);
	if (salts_equal(stop.salts, hdr_cur.salts)) {
		if (size_cur < implied_size) {
			return 1;
		}
		e->wal_cursor = stop.start + stop.len;
	} else if (size_prev < (sqlite3_int64)sizeof(struct wal_hdr)) {
		return 1;
	} else if (salts_equal(stop.salts, hdr_prev.salts)) {
		if (size_prev != implied_size) {
			return 1;
		}
	} else {
		return 1;
	}

	return 0;
}

int vfs2_commit(sqlite3_file *file, struct vfs2_wal_slice stop)
{
	(void)stop; // XXX
	/* TODO get this to support the follower side as well */
	/* TODO (follower) if everything has been committed then release the write lock */
	/* TODO (follower) keep the wal-index header up to date (don't worry about the rest of the wal-index) */
	struct file *xfile = (struct file *)file;
	PRE(xfile->flags & SQLITE_OPEN_MAIN_DB);
	struct entry *e = xfile->entry;
	if (e->shm_regions_len == 0) {
		return 1;
	}
	if (e->shm_locks[VFS2_SHM_WRITE_LOCK] != VFS2_EXCLUSIVE) {
		return 1;
	}
	e->shm_locks[VFS2_SHM_WRITE_LOCK] = 0;

	struct wal_index_full_hdr *hdr = get_full_hdr(e);
	hdr->basic[0] = e->pending_txn_hdr;
	hdr->basic[1] = e->pending_txn_hdr;
	e->prev_txn_hdr = e->pending_txn_hdr;
	e->pending_txn_hdr = (struct wal_index_basic_hdr){};
	/* XXX */
	// commit_end += pending_txn_len;
	e->pending_txn_len = 0;
	e->pending_txn_last_frame_commit = 0;

	sm_move(&xfile->entry->wtx_sm, WTX_BASE);

	return 0;
}

int vfs2_commit_barrier(sqlite3_file *file)
{
	(void)file;
	return 0;
	/* TODO implement */
	/* TODO release write lock */
	/* TODO invalidate wal-index header to trigger a rebuild */
}

int vfs2_poll(sqlite3_file *file, dqlite_vfs_frame **frames, unsigned *n, struct vfs2_wal_slice *sl)
{
	struct file *xfile = (struct file *)file;
	PRE(xfile->flags & SQLITE_OPEN_WAL);
	struct entry *e = xfile->entry;

	uint32_t len = e->pending_txn_len;
	if (len > 0) {
		/* Don't go through vfs2_shm_lock here since that has additional
		 * checks that assume the context of being called from inside
		 * SQLite. */
		if (e->shm_locks[VFS2_SHM_WRITE_LOCK] > 0) {
			return 1;
		}
		e->shm_locks[VFS2_SHM_WRITE_LOCK] = VFS2_EXCLUSIVE;
	}

	if (n != NULL && frames != NULL) {
		*n = len;
		*frames = e->pending_txn_frames;
	} else if (e->pending_txn_frames != NULL) {
		for (uint32_t i = 0; i < len; i++) {
			sqlite3_free(e->pending_txn_frames[i].data);
		}
		sqlite3_free(e->pending_txn_frames);
	}
	e->pending_txn_frames = NULL;

	if (sl != NULL) {
		sl->len = len;
		sl->salts = e->pending_txn_hdr.salts;
		sl->start = e->prev_txn_hdr.mxFrame;
		sl->len = len;
	}

	sm_move(&xfile->entry->wtx_sm, WTX_POLLED);

	return 0;
}

void vfs2_destroy(sqlite3_vfs *vfs)
{
	struct common *data = vfs->pAppData;
	pthread_rwlock_destroy(&data->rwlock);
	sqlite3_free(data);
	sqlite3_free(vfs);
}

int vfs2_abort(sqlite3_file *file)
{
	/* TODO maybe can "followerize" this and get rid of vfs2_unapply_after? */
	struct file *xfile = (struct file *)file;
	PRE(xfile->flags & SQLITE_OPEN_MAIN_DB);
	struct entry *e = xfile->entry;

	e->shm_locks[VFS2_SHM_WRITE_LOCK] = 0;

	struct wal_index_full_hdr *hdr = get_full_hdr(e);
	hdr->basic[0] = e->prev_txn_hdr;
	hdr->basic[1] = e->prev_txn_hdr;
	e->pending_txn_hdr = (struct wal_index_basic_hdr){};

	dqlite_vfs_frame *frames = e->pending_txn_frames;
	if (frames != NULL) {
		uint32_t n = e->pending_txn_len;
		for (uint32_t i = 0; i < n; i++) {
			sqlite3_free(frames[i].data);
		}
	}
	sqlite3_free(frames);
	e->pending_txn_frames = NULL;
	e->pending_txn_len = 0;
	e->pending_txn_last_frame_commit = 0;

	sm_move(&xfile->entry->wtx_sm, WTX_BASE);
	return 0;
}

int vfs2_read_wal(sqlite3_file *file,
		  struct vfs2_wal_txn *txns,
		  size_t txns_len)
{
	/* TODO implement this */
	/* TODO check_wal_integrity if this is the first time we're reading from
	 * this (physical) WAL */
	(void)check_wal_integrity;
	(void)file;
	(void)txns;
	(void)txns_len;
	return 0;
}

int vfs2_apply_uncommitted(sqlite3_file *file, const dqlite_vfs_frame *frames, unsigned len, struct vfs2_wal_slice *out)
{
	/* TODO implement this */
	/* TODO acquire write lock if not held */
	/* TODO keep the wal-index header up to date (don't worry about the rest of the wal-index) */
	(void)file;
	(void)frames;
	(void)len;
	(void)out;
	/*
	struct file *xfile = (struct file *)file;
	if (!(xfile->flags & SQLITE_OPEN_MAIN_DB)) {
		return 1;
	}
	int rv;

	unsigned *shm_locks = xfile->db_shm.shm_locks;
	if (shm_locks[VFS2_SHM_WRITE_LOCK] > 0) {
		return 1;
	}
	shm_locks[VFS2_SHM_WRITE_LOCK] = VFS2_EXCLUSIVE;

	struct vfs2_salts salts;
	rv = maybe_setup_or_swap_wal(xfile, len, &salts);
	if (rv != SQLITE_OK) {
		return 1;
	}
	struct file *wal = xfile->entry->wal;

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
