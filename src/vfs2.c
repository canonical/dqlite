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
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define VFS2_WAL_FIXED_SUFFIX1 "-xwal1"
#define VFS2_WAL_FIXED_SUFFIX2 "-xwal2"

#define VFS2_WAL_INDEX_REGION_SIZE (1 << 15)
#define VFS2_WAL_FRAME_HDR_SIZE 24

#define VFS2_EXCLUSIVE UINT_MAX

#define WAL_WRITE_LOCK 0
#define WAL_CKPT_LOCK 1
#define WAL_RECOVER_LOCK 2

#define LE_MAGIC 0x377f0682
#define BE_MAGIC 0x377f0683

#define WAL_NREADER (SQLITE_SHM_NLOCK - 3)

#define READ_MARK_UNUSED 0xffffffff

static const uint32_t invalid_magic = 0x17171717;

enum {
	/* Entry is not yet open. */
	WTX_CLOSED,
	/* Next WAL write will be a header write, causing a WAL swap (WAL-cur is empty or fully checkpointed). */
	WTX_EMPTY,
	/* Non-leader, at least one transaction in WAL-cur is not committed. */
	WTX_FOLLOWING,
	/* Non-leader, all transactions in WAL-cur are committed (but at least one is not checkpointed). */
	WTX_FLUSH,
	WTX_INVALIDATED,
	/* Leader, all transactions in WAL-cur are committed (but at least one is not checkpointed). */
	WTX_BASE,
	/* Leader, transaction in progress. */
	WTX_ACTIVE,
	/* Leader, transaction committed by SQLite and hidden. */
	WTX_HIDDEN,
	/* Leader, transation committed by SQLite, hidden, and polled. */
	WTX_POLLED
};

/*

Diagram of the state machine (some transitions omitted when they would crowd the diagram even more):

+----------------------+  sqlite3_open             +------------------------------------------------------------------------------+
|        CLOSED        | ------------------------> |                                  FOLLOWING                                   | <+
+----------------------+                           +------------------------------------------------------------------------------+  |
  |                                                  ^                          ^                          |                         |
  | sqlite3_open                                     | vfs2_apply_uncommitted   | vfs2_apply_uncommitted   | vfs2_{commit,unapply}   | vfs2_apply_uncommitted
  v                                                  |                          |                          v                         |
+----------------------------------------------------------------------------+  |                        +------------------------+  |
|                                                                            |  +----------------------- |                        |  |
|                                   EMPTY                                    |                           |         FLUSH          |  |
|                                                                            | <------------------------ |                        |  |
+----------------------------------------------------------------------------+   sqlite3_wal_checkpoint  +------------------------+  |
  |                       ^                                                                                |                         |
  | vfs2_commit_barrier   | sqlite3_wal_checkpoint                                                         |                         |
  v                       |                                                                                |                         |
+----------------------------------------------------------------------------+  vfs2_commit_barrier        |                         |
|                                    BASE                                    | <---------------------------+                         |
+----------------------------------------------------------------------------+                                                       |
  |                       ^                          |                                                                               |
  | sqlite3_step          | vfs2_unhide              +-------------------------------------------------------------------------------+
  v                       |
+----------------------+  |
|        ACTIVE        |  |
+----------------------+  |
  |                       |
  | COMMIT_PHASETWO       |
  v                       |
+----------------------+  |
|        HIDDEN        |  |
+----------------------+  |
  |                       |
  | vfs2_poll             |
  v                       |
+----------------------+  |
|        POLLED        | -+
+----------------------+

*/

static const struct sm_conf wtx_states[SM_STATES_MAX] = {
	[WTX_CLOSED] = {
		.flags = SM_INITIAL|SM_FINAL,
		.name = "closed",
		.allowed = BITS(WTX_EMPTY)|BITS(WTX_FOLLOWING)|BITS(WTX_FLUSH),
	},
	[WTX_EMPTY] = {
		.flags = 0,
		.name = "empty",
		.allowed = BITS(WTX_FOLLOWING)|BITS(WTX_FLUSH)|BITS(WTX_ACTIVE)|BITS(WTX_CLOSED),
	},
	[WTX_FOLLOWING] = {
		.flags = 0,
		.name = "following",
		.allowed = BITS(WTX_FOLLOWING)|BITS(WTX_FLUSH)|BITS(WTX_INVALIDATED)|BITS(WTX_CLOSED),
	},
	[WTX_FLUSH] = {
		.flags = 0,
		.name = "flush",
		.allowed = BITS(WTX_FOLLOWING)|BITS(WTX_FLUSH)|BITS(WTX_INVALIDATED)|BITS(WTX_CLOSED),
	},
	[WTX_INVALIDATED] = {
		.flags = 0,
		.name = "invalidated",
		.allowed = BITS(WTX_ACTIVE)|BITS(WTX_CLOSED),
	},
	[WTX_BASE] = {
		.flags = 0,
		.name = "base",
		.allowed = BITS(WTX_FOLLOWING)|BITS(WTX_BASE)|BITS(WTX_ACTIVE)|BITS(WTX_EMPTY)|BITS(WTX_CLOSED),
	},
	[WTX_ACTIVE] = {
		.flags = 0,
		.name = "active",
		.allowed = BITS(WTX_BASE)|BITS(WTX_ACTIVE)|BITS(WTX_HIDDEN)|BITS(WTX_CLOSED),
	},
	[WTX_HIDDEN] = {
		.flags = 0,
		.name = "hidden",
		.allowed = BITS(WTX_BASE)|BITS(WTX_POLLED)|BITS(WTX_CLOSED),
	},
	[WTX_POLLED] = {
		.flags = 0,
		.name = "polled",
		.allowed = BITS(WTX_BASE)|BITS(WTX_CLOSED),
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

struct cksums {
	uint32_t cksum1;
	uint32_t cksum2;
};

static bool is_bigendian(void)
{
	int x = 1;
	return *(char *)(&x) == 0;
}

static uint32_t native_magic(void)
{
	return is_bigendian() ? BE_MAGIC : LE_MAGIC;
}

static void update_cksums(uint32_t magic, const uint8_t *p, size_t len, struct cksums *sums)
{
	PRE(magic == BE_MAGIC || magic == LE_MAGIC);
	PRE(len % 8 == 0);
	const uint8_t *end = p + len;
	for (; p != end; p += 8) {
		if (magic == BE_MAGIC) {
			sums->cksum1 += ByteGetBe32(p) + sums->cksum2;
			sums->cksum2 += ByteGetBe32(p + 4) + sums->cksum1;
		} else {
			sums->cksum1 += ByteGetLe32(p) + sums->cksum2;
			sums->cksum2 += ByteGetLe32(p + 4) + sums->cksum1;
		}
	}
}

static bool cksums_equal(struct cksums a, struct cksums b)
{
	return a.cksum1 == b.cksum1 && a.cksum2 == b.cksum2;
}

/**
 * Layout-compatible with the first part of the WAL index header.
 *
 * Note: everything is native-endian except the salts, hence the use of
 * native integer types here.
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
	struct cksums frame_cksums;
	struct vfs2_salts salts;
	struct cksums cksums;
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

struct wal_frame_hdr {
	uint8_t page_number[4];
	uint8_t commit[4];
	struct vfs2_salts salts;
	uint8_t cksum1[4];
	uint8_t cksum2[4];
};

struct wal_index_full_hdr {
	struct wal_index_basic_hdr basic[2];
	uint32_t nBackfill;
	uint32_t marks[WAL_NREADER];
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
	/* Next/prev entries for this VFS. */
	queue link;

	/* e.g. /path/to/some.db */
	char *main_db_name;

	/* The WALs are represented by two physical files (inodes)
	 * and three filenames. For each of the two physical files
	 * there is a "fixed name" that always points to that file.
	 * The "moving name" always points to one of the two physical
	 * files, but switches between them on every WAL swap. */

	/* e.g. /path/to/some.db-wal */
	char *wal_moving_name;
	/* e.g. /path/to/some.db-xwal1 */
	char *wal_cur_fixed_name;
	/* Base VFS file object for WAL-cur */
	sqlite3_file *wal_cur;
	/* e.g. /path/to/some.db-xwal2 */
	char *wal_prev_fixed_name;
	/* Base VFS file object for WAL-prev */
	sqlite3_file *wal_prev;

	/* Number of `struct file` with SQLITE_OPEN_MAIN_DB that point to this entry */
	unsigned refcount_main_db;
	/* Number of `struct file` with SQLITE_OPEN_WAL that point to this entry */
	unsigned refcount_wal;

	/* if WAL-cur is nonempty at startup, we read its header, verify the checkum,
	 * and use it to initialize the page size. otherwise, we wait until the first
	 * write to the WAL, which should be the header */
	uint32_t page_size;

	/* For ACTIVE, HIDDEN, POLLED: the header that hides the pending txn */
	struct wal_index_basic_hdr prev_txn_hdr;
	/* For ACTIVE, HIDDEN, POLLED: the header that shows the pending txn */
	struct wal_index_basic_hdr pending_txn_hdr;

	/* shm implementation; holds the WAL index */
	void **shm_regions;
	int shm_regions_len;
	unsigned shm_refcount;
	/* Zero for unlocked, positive for read-locked, UINT_MAX for write-locked */
	unsigned shm_locks[SQLITE_SHM_NLOCK];

	/* For ACTIVE, HIDDEN: the pending txn. start and len
	 * are in units of frames. */
	dqlite_vfs_frame *pending_txn_frames;
	uint32_t pending_txn_start;
	uint32_t pending_txn_len;
	uint32_t pending_txn_last_frame_commit;

	/* Frame index, points to the physical end of WAL-cur */
	uint32_t wal_cursor;
	/* Cached header of WAL-cur */
	struct wal_hdr wal_cur_hdr;
	/* Cached header of WAL-prev */
	struct wal_hdr wal_prev_hdr;

	struct sm wtx_sm;
	/* VFS-wide data (immutable) */
	struct common *common;
};

/**
 * VFS-specific file object, upcastable to sqlite3_file.
 */
struct file {
	/* Our custom sqlite3_io_methods vtable; must come first. Always
	 * present. */
	struct sqlite3_file base;
	/* Flags passed to the xOpen that created this file. */
	int flags;
	/* File object created by the base (unix) VFS. Not used for WAL. */
	sqlite3_file *orig;
	/* Common data between main DB and WAL. Not used for other kinds of
	 * file. */
	struct entry *entry;
};

static void free_pending_txn(struct entry *e)
{
	if (e->pending_txn_frames != NULL) {
		for (uint32_t i = 0; i < e->pending_txn_len; i++) {
			sqlite3_free(e->pending_txn_frames[i].data);
		}
		sqlite3_free(e->pending_txn_frames);
	}
	e->pending_txn_frames = 0;
	e->pending_txn_len = 0;
	e->pending_txn_last_frame_commit = 0;
}

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

static bool no_pending_txn(const struct entry *e)
{
	return e->pending_txn_len == 0 && e->pending_txn_frames == NULL && e->pending_txn_last_frame_commit == 0;
}

static bool write_lock_held(const struct entry *e)
{
	return e->shm_locks[WAL_WRITE_LOCK] == VFS2_EXCLUSIVE;
}

static bool wal_index_basic_hdr_equal(struct wal_index_basic_hdr a,
				      struct wal_index_basic_hdr b)
{
	/* memcmp is okay here since struct wal_index_basic_hdr has no
	 * padding. */
	return memcmp(&a, &b, sizeof(struct wal_index_basic_hdr)) == 0;
}

static bool wal_index_basic_hdr_zeroed(struct wal_index_basic_hdr h)
{
	return wal_index_basic_hdr_equal(h, (struct wal_index_basic_hdr){});
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

/* Check that the hash tables in the WAL index have been initialized
 * by looking for nonzero bytes after the WAL index header. (TODO:
 * actually parse the hash tables?) */
static bool wal_index_recovered(const struct entry *e)
{
	PRE(e->shm_regions_len > 0);
	char *p = e->shm_regions[0];
	for (size_t i = sizeof(struct wal_index_full_hdr); i < VFS2_WAL_INDEX_REGION_SIZE; i++) {
		if (p[i] != 0) {
			return true;
		}
	}
	return false;
}

static bool is_valid_page_size(unsigned long n)
{
	return n >= 1 << 9 && n <= 1 << 16 && is_po2(n);
}

static bool is_open(const struct entry *e)
{
	return e->main_db_name != NULL
		&& e->wal_moving_name != NULL
		&& e->wal_cur_fixed_name != NULL
		&& e->wal_cur != NULL
		&& e->wal_prev_fixed_name != NULL
		&& e->wal_prev != NULL
		&& (e->refcount_main_db > 0 || e->refcount_wal > 0)
		&& e->shm_regions != NULL
		&& e->shm_regions_len > 0
		&& e->shm_regions[0] != NULL
		&& e->common != NULL;
}

static bool basic_hdr_valid(struct wal_index_basic_hdr bhdr)
{

	struct cksums sums = {};
	uint32_t magic = bhdr.bigEndCksum ? BE_MAGIC : LE_MAGIC;
	size_t n = offsetof(struct wal_index_basic_hdr, cksums);
	update_cksums(magic, (uint8_t *)&bhdr, n, &sums);
	return bhdr.iVersion == 3007000
		&& bhdr.isInit == 1
		&& cksums_equal(sums, bhdr.cksums);
}

static bool full_hdr_valid(const struct wal_index_full_hdr *ihdr)
{
	return basic_hdr_valid(ihdr->basic[0]) &&
		wal_index_basic_hdr_equal(ihdr->basic[0], ihdr->basic[1]);
}

static bool wtx_invariant(const struct sm *sm, int prev)
{
	/* TODO make use of this */
	(void)prev;

	struct entry *e = CONTAINER_OF(sm, struct entry, wtx_sm);

	if (sm_state(sm) == WTX_CLOSED) {
		char *region = (char *)e;
		char zeroed[offsetof(struct entry, wtx_sm)] = {};

		return CHECK(memcmp(region, zeroed, sizeof(zeroed)) == 0) &&
			CHECK(e->common != NULL);
	}

	if (!CHECK(is_open(e))) {
		return false;
	}

	struct wal_index_full_hdr *ihdr = get_full_hdr(e);
	/* TODO(cole) fill this out */
	if (sm_state(sm) == WTX_INVALIDATED) {
		if (!CHECK(!full_hdr_valid(ihdr))) {
			return false;
		}
		return true;
	}
	if (!CHECK(full_hdr_valid(ihdr))) {
		return false;
	}
	uint32_t mx = ihdr->basic[0].mxFrame;
	uint32_t backfill = ihdr->nBackfill;
	uint32_t cursor = e->wal_cursor;
	if (!CHECK(backfill <= mx) || !CHECK(mx <= cursor)) {
		return false;
	}

	/* TODO any checks applicable to the read marks and read locks? */

	if (sm_state(sm) == WTX_EMPTY) {
		return CHECK(mx == backfill) &&
			CHECK(mx == cursor) &&
			CHECK(no_pending_txn(e)) &&
			CHECK(!write_lock_held(e));
	}

	if (!CHECK(is_valid_page_size(e->page_size))) {
		return false;
	}

	if (sm_state(sm) == WTX_FOLLOWING) {
		return CHECK(no_pending_txn(e)) &&
			CHECK(write_lock_held(e)) &&
			CHECK(mx < cursor);
	}

	if (sm_state(sm) == WTX_FLUSH) {
		return CHECK(no_pending_txn(e)) &&
			CHECK(!write_lock_held(e)) &&
			CHECK(ERGO(mx > 0, backfill < mx)) &&
			CHECK(mx == cursor);
	}

	if (sm_state(sm) == WTX_BASE) {
		return CHECK(no_pending_txn(e)) &&
			CHECK(!write_lock_held(e)) &&
			CHECK(ERGO(mx > 0, backfill < mx)) &&
			CHECK(mx == cursor) &&
			CHECK(ERGO(mx > 0, wal_index_recovered(e)));
	}

	if (sm_state(sm) == WTX_ACTIVE) {
		return CHECK(wal_index_basic_hdr_equal(get_full_hdr(e)->basic[0], e->prev_txn_hdr)) &&
			CHECK(wal_index_basic_hdr_zeroed(e->pending_txn_hdr)) &&
			CHECK(write_lock_held(e));
	}

	if (!CHECK(mx < cursor) ||
			!CHECK(e->pending_txn_len > 0) ||
			!CHECK(e->pending_txn_start + e->pending_txn_len == e->wal_cursor)) {
		return false;
	}

	if (sm_state(sm) == WTX_HIDDEN) {
		bool res = CHECK(wal_index_basic_hdr_equal(get_full_hdr(e)->basic[0], e->prev_txn_hdr)) &&
			CHECK(wal_index_basic_hdr_advanced(e->pending_txn_hdr, e->prev_txn_hdr)) &&
			CHECK(!write_lock_held(e)) &&
			CHECK(e->pending_txn_frames != NULL);
		if (!res) {
			return false;
		}
		for (uint32_t i = 0; i < e->pending_txn_len; i++) {
			res &= CHECK(e->pending_txn_frames[i].data != NULL);
		}
		return res;
	}

	if (sm_state(sm) == WTX_POLLED) {
		return CHECK(wal_index_basic_hdr_equal(get_full_hdr(e)->basic[0], e->prev_txn_hdr)) &&
			CHECK(wal_index_basic_hdr_advanced(e->pending_txn_hdr, e->prev_txn_hdr)) &&
			CHECK(write_lock_held(e)) &&
			CHECK(e->pending_txn_frames == NULL);
	}

	assert(0);
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
	return (f->flags & SQLITE_OPEN_WAL) ? f->entry->wal_cur : f->orig;
}

static int vfs2_close(sqlite3_file *file)
{
	struct file *xfile = (struct file *)file;
	int rv;

	rv = SQLITE_OK;
	if (xfile->flags & SQLITE_OPEN_MAIN_DB) {
		if (xfile->orig->pMethods != NULL) {
			rv = xfile->orig->pMethods->xClose(xfile->orig);
		}
		sqlite3_free(xfile->orig);
		if (xfile->entry != NULL) {
			PRE(xfile->entry->refcount_main_db > 0);
			xfile->entry->refcount_main_db -= 1;
		}
	} else if (xfile->flags & SQLITE_OPEN_WAL) {
		PRE(xfile->entry != NULL);
		PRE(xfile->entry->refcount_wal > 0);
		xfile->entry->refcount_wal -= 1;
	} else if (xfile->orig->pMethods != NULL) {
		rv = xfile->orig->pMethods->xClose(xfile->orig);
		sqlite3_free(xfile->orig);
	}
	return rv;
}

static int vfs2_read(sqlite3_file *file, void *buf, int amt, sqlite3_int64 ofst)
{
	struct file *xfile = (struct file *)file;
	sqlite3_file *orig = get_orig(xfile);
	return orig->pMethods->xRead(orig, buf, amt, ofst);
}

static int wal_swap(struct entry *e,
		    const struct wal_hdr *wal_hdr)
{
	PRE(e->pending_txn_len == 0);
	PRE(e->pending_txn_frames == NULL);
	int rv;

	e->page_size = ByteGetBe32(wal_hdr->page_size);

	/* Terminology: the outgoing WAL is the one that's moving
	 * from cur to prev. The incoming WAL is the one that's moving
	 * from prev to cur. */
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
	e->wal_prev_hdr = e->wal_cur_hdr;
	e->wal_cur_hdr = *wal_hdr;

	/* Move the moving name. */
	rv = unlink(e->wal_moving_name);
	if (rv != 0 && errno != ENOENT) {
		tracef("unlink = IOERR");
		return SQLITE_IOERR;
	}
	rv = link(name_incoming, e->wal_moving_name);
	if (rv != 0) {
		tracef("link = IOERR");
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
				    const struct wal_frame_hdr *fhdr,
				    uint32_t x)
{
	dqlite_vfs_frame *frames = e->pending_txn_frames;
	if (no_pending_txn(e)) {
		assert(x == e->wal_cursor);
		e->pending_txn_start = x;
	}
	uint32_t n = e->pending_txn_len;
	tracef("orig=%u start=%u n=%u", x, e->pending_txn_start, n);
	x -= e->pending_txn_start;
	assert(x <= n);
	if (e->pending_txn_len == 0 && x == 0) {
		/* check that the WAL-index hdr makes sense and save it */
		struct wal_index_basic_hdr hdr = get_full_hdr(e)->basic[0];
		assert(hdr.isInit == 1);
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
		uint32_t commit = ByteGetBe32(fhdr->commit);
		frame->page_number = ByteGetBe32(fhdr->page_number);
		frame->data = NULL;
		e->pending_txn_last_frame_commit = commit;
		e->pending_txn_len++;
	} else {
		/* Overwriting a previously-written frame in the current
		 * transaction. */
		dqlite_vfs_frame *frame = &e->pending_txn_frames[x];
		frame->page_number = ByteGetBe32(fhdr->page_number);
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
		ofst -= (sqlite3_int64)sizeof(struct wal_hdr);
		assert(ofst % frame_size == 0);
		sqlite3_int64 frame_ofst = ofst / (sqlite3_int64)frame_size;
		return vfs2_wal_write_frame_hdr(e, buf, (uint32_t)frame_ofst);
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
		tracef("WAL SWAP LEADER");
		rv = wal_swap(e, hdr);
		if (rv != SQLITE_OK) {
			return rv;
		}
		/* check that the WAL-index hdr makes sense and save it */
		struct wal_index_basic_hdr ihdr = get_full_hdr(e)->basic[0];
		assert(ihdr.isInit == 1);
		assert(ihdr.mxFrame == 0);
		e->prev_txn_hdr = ihdr;
		sm_move(&e->wtx_sm, WTX_ACTIVE);
		return SQLITE_OK;
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
		e->wal_cursor += e->pending_txn_len;
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

static __attribute__((noinline)) int busy(void)
{
	return SQLITE_BUSY;
}

static int vfs2_shm_lock(sqlite3_file *file, int ofst, int n, int flags)
{
	struct file *xfile = (struct file *)file;
	PRE(xfile != NULL);
	struct entry *e = xfile->entry;

	assert(ofst >= 0 && ofst + n <= SQLITE_SHM_NLOCK);
	assert(n >= 1);
	assert(n == 1 || (flags & SQLITE_SHM_EXCLUSIVE) != 0);

	assert(flags == (SQLITE_SHM_LOCK | SQLITE_SHM_SHARED) ||
	       flags == (SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE) ||
	       flags == (SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED) ||
	       flags == (SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE));

	assert(xfile->flags & SQLITE_OPEN_MAIN_DB);

	if (flags == (SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE) &&
		ofst <= WAL_RECOVER_LOCK && WAL_RECOVER_LOCK < ofst + n)
	{
		struct wal_index_basic_hdr bh = get_full_hdr(e)->basic[0];
		tracef("RECOVERY isInit=%u mxFrame=%u", bh.isInit, bh.mxFrame);
	}

	if (flags == (SQLITE_SHM_LOCK | SQLITE_SHM_SHARED)) {
		for (int i = ofst; i < ofst + n; i++) {
			if (e->shm_locks[i] == VFS2_EXCLUSIVE) {
				return busy();
			}
		}

		for (int i = ofst; i < ofst + n; i++) {
			e->shm_locks[i]++;
		}
	} else if (flags == (SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)) {
		for (int i = ofst; i < ofst + n; i++) {
			if (e->shm_locks[i] > 0) {
				return busy();
			}
		}

		for (int i = ofst; i < ofst + n; i++) {
			e->shm_locks[i] = VFS2_EXCLUSIVE;
		}

		/* XXX maybe this shouldn't be an assertion */
		if (ofst == WAL_WRITE_LOCK) {
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

		if (ofst <= WAL_RECOVER_LOCK && WAL_RECOVER_LOCK < ofst + n) {
			tracef("unlocking the recovery lock!");
			struct wal_index_full_hdr *fh = get_full_hdr(e);
			tracef("mxFrame after recovery is %u, cursor is %u, isInit is %u", fh->basic[0].mxFrame, e->wal_cursor, fh->basic[0].isInit);
		}

		if (ofst == WAL_WRITE_LOCK) {
			/* Unlocking the write lock: roll back any uncommitted
			 * transaction. */
			assert(n == 1);
			tracef("unlocking write lock");
			/* FIXME(cole) iron this out */
			if (sm_state(&e->wtx_sm) >= WTX_BASE && e->pending_txn_last_frame_commit == 0) {
				free_pending_txn(e);
				sm_move(&e->wtx_sm, WTX_BASE);
			}
		} else if (ofst == WAL_CKPT_LOCK && n == 1) {
			/* End of a checkpoint: if all frames have been backfilled,
			 * move to EMPTY. */
			assert(n == 1);
			struct wal_index_full_hdr *ihdr = get_full_hdr(e);
			if (ihdr->nBackfill == ihdr->basic[0].mxFrame) {
				sm_move(&e->wtx_sm, WTX_EMPTY);
			}
		} /* else if (ofst <= WAL_RECOVER_LOCK && WAL_RECOVER_LOCK < ofst + n) {
			sm_move(&e->wtx_sm, WTX_BASE);
		} */
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
	struct file *xfile = (struct file *)file;
	struct entry *e = xfile->entry;
	tracef("UNMAP %p", e);
	e->shm_refcount--;
	if (e->shm_refcount == 0 && delete) {
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
	} else {
		*hdr = (struct wal_hdr){};
	}
	return SQLITE_OK;
}

static struct wal_index_full_hdr initial_full_hdr(struct wal_hdr whdr)
{
	struct wal_index_full_hdr ihdr = {};
	ihdr.basic[0].iVersion = 3007000;
	ihdr.basic[0].isInit = 1;
	ihdr.basic[0].bigEndCksum = is_bigendian();
	ihdr.basic[0].szPage = (uint16_t)ByteGetBe32(whdr.page_size);
	struct cksums sums = {};
	update_cksums(native_magic(), (const void *)&ihdr.basic[0], offsetof(struct wal_index_basic_hdr, cksums), &sums);
	ihdr.basic[0].cksums = sums;
	ihdr.basic[1] = ihdr.basic[0];
	ihdr.marks[0] = 0;
	ihdr.marks[1] = 0;
	ihdr.marks[2] = READ_MARK_UNUSED;
	ihdr.marks[3] = READ_MARK_UNUSED;
	ihdr.marks[4] = READ_MARK_UNUSED;
	return ihdr;
}

static void set_mx_frame(struct wal_index_full_hdr *ihdr, uint32_t mx, struct wal_frame_hdr fhdr)
{
	uint32_t num_pages = ByteGetBe32(fhdr.commit);
	PRE(num_pages > 0);
	ihdr->basic[0].iChange += 1;
	ihdr->basic[0].mxFrame = mx;
	ihdr->basic[0].nPage = num_pages;
	/* XXX byte order */
	ihdr->basic[0].frame_cksums.cksum1 = ByteGetBe32(fhdr.cksum1);
	ihdr->basic[0].frame_cksums.cksum2 = ByteGetBe32(fhdr.cksum2);
	struct cksums sums = {};
	update_cksums(native_magic(), (const void *)&ihdr->basic[0], 40, &sums);
	ihdr->basic[0].cksums = sums;
	ihdr->basic[1] = ihdr->basic[0];
}

static void restart_full_hdr(struct wal_index_full_hdr *ihdr, struct wal_hdr new_whdr)
{
	/* cf. walRestartHdr */
	ihdr->basic[0].mxFrame = 0;
	ihdr->basic[0].salts = new_whdr.salts;
	struct cksums sums = {};
	update_cksums(native_magic(), (const void *)&ihdr->basic[0], 40, &sums);
	ihdr->basic[0].cksums = sums;
	ihdr->basic[1] = ihdr->basic[0];
	ihdr->nBackfill = 0;
	ihdr->nBackfillAttempted = 0;
}

static uint32_t wal_cursor_from_size(uint32_t page_size, sqlite3_int64 size)
{
	sqlite3_int64 whdr_size = (sqlite3_int64)sizeof(struct wal_hdr);
	if (size < whdr_size) {
		return 0;
	}
	sqlite3_int64 x = (size - whdr_size) / ((sqlite3_int64)sizeof(struct wal_frame_hdr) + (sqlite3_int64)page_size);
	return (uint32_t)x;
}

static sqlite3_int64 wal_offset_from_cursor(uint32_t page_size, uint32_t cursor)
{
	return (sqlite3_int64)sizeof(struct wal_hdr) + (sqlite3_int64)cursor * ((sqlite3_int64)sizeof(struct wal_frame_hdr) + (sqlite3_int64)page_size);
}

static int open_entry(struct common *common, const char *name, struct entry *e)
{
	sqlite3_vfs *v = common->orig;
	int path_cap = v->mxPathname + 1;
	int file_cap = v->szOsFile;
	int rv;

	*e = (struct entry){};
	e->common = common;
	sm_init(&e->wtx_sm, wtx_invariant, NULL, wtx_states, WTX_CLOSED);

	e->refcount_main_db = 1;

	e->main_db_name = sqlite3_malloc(path_cap);
	e->wal_moving_name = sqlite3_malloc(path_cap);
	e->wal_cur_fixed_name = sqlite3_malloc(path_cap);
	e->wal_prev_fixed_name = sqlite3_malloc(path_cap);
	if (e->main_db_name == NULL ||
			e->wal_moving_name == NULL ||
			e->wal_cur_fixed_name == NULL ||
			e->wal_prev_fixed_name == NULL) {
		return SQLITE_NOMEM;
	}

	strcpy(e->main_db_name, name);

	size_t pc = (size_t)path_cap;
	snprintf(e->wal_moving_name, pc, "%s-wal", name);
	snprintf(e->wal_cur_fixed_name, pc, "%s-xwal1", name);
	snprintf(e->wal_prev_fixed_name, pc, "%s-xwal2", name);

	/* TODO EXRESCODE? */
	int phys_wal_flags = SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|
		SQLITE_OPEN_WAL|SQLITE_OPEN_NOFOLLOW;

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
	struct wal_hdr hdr_prev = hdr2;

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
		hdr_prev = hdr1;
	}

	e->wal_cur_hdr = hdr_cur;
	e->wal_prev_hdr = hdr_prev;

	rv = unlink(e->wal_moving_name);
	(void)rv;
	rv = link(e->wal_cur_fixed_name, e->wal_moving_name);
	(void)rv;

	e->shm_regions = sqlite3_malloc(sizeof(void *[1]));
	if (e->shm_regions == NULL) {
		return SQLITE_NOMEM;
	}
	e->shm_regions[0] = sqlite3_malloc(VFS2_WAL_INDEX_REGION_SIZE);
	if (e->shm_regions[0] == NULL) {
		return SQLITE_NOMEM;
	}
	memset(e->shm_regions[0], 0, VFS2_WAL_INDEX_REGION_SIZE);
	e->shm_regions_len = 1;
	tracef("REGIONS %p", e);

	*get_full_hdr(e) = initial_full_hdr(hdr_cur);

	/* TODO(cole) implement deferred initialization of page size when
	 * WAL-cur is empty at startup. */
	int next = WTX_EMPTY;
	/* TODO verify the header here */
	if (size_cur >= wal_offset_from_cursor(0 /* this doesn't matter */, 0)) {
		/* This is guaranteed to be correct since we don't
		 * support changing the page size dynamically. */
		e->page_size = ByteGetBe32(hdr_cur.page_size);
		e->wal_cursor = wal_cursor_from_size(e->page_size, size_cur);
		next = WTX_FLUSH;
	}
	if (size_cur >= wal_offset_from_cursor(e->page_size, 1)) {
		e->shm_locks[WAL_WRITE_LOCK] = VFS2_EXCLUSIVE;
		next = WTX_FOLLOWING;
	}
	sm_move(&e->wtx_sm, next);

	return SQLITE_OK;
}

static int set_up_entry(struct common *common, const char *name, int flags, struct entry **e)
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
		sqlite3_free(*e);
		*e = res;
		unsigned *refcount = name_is_db ? &res->refcount_main_db : &res->refcount_wal;
		*refcount += 1;
		return SQLITE_OK;
	}

	assert(name_is_db);
	res = *e;
	/* If open_entry fails we still want to link in the entry. Since we unconditionally
	 * set pMethods in our file vtable, SQLite will xClose the file and vfs2_close
	 * will run to clean up the partial work of open_entry. */
	rv = open_entry(common, name, res);
	pthread_rwlock_wrlock(&common->rwlock);
	queue_insert_tail(&common->queue, &res->link);
	pthread_rwlock_unlock(&common->rwlock);
	return rv;
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
		rv = set_up_entry(common, name, flags, &xout->entry);
		if (rv != SQLITE_OK) {
			return rv;
		}
	}

	if ((flags & SQLITE_OPEN_WAL) && out_flags != NULL) {
		*out_flags = flags;
	}

	return SQLITE_OK;
}

/* TODO does this need to be customized? should it ever be called on one of our files? */
static int vfs2_delete(sqlite3_vfs *vfs, const char *name, int sync_dir)
{
	struct common *data = vfs->pAppData;
	return data->orig->xDelete(data->orig, name, sync_dir);
}

static int vfs2_access(sqlite3_vfs *vfs, const char *name, int flags, int *out)
{
	/* TODO always report that the WAL exists (?) */
	/* TODO other customizations? */
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
 * base VFS -- store last error in vfs [thread-local?]) */
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

int vfs2_unapply(sqlite3_file *file, struct vfs2_wal_slice first_to_unapply)
{
	struct file *xfile = (struct file *)file;
	PRE(xfile->flags & SQLITE_OPEN_MAIN_DB);
	struct entry *e = xfile->entry;
	PRE(salts_equal(first_to_unapply.salts, e->wal_cur_hdr.salts));
	PRE(first_to_unapply.start + first_to_unapply.len <= e->wal_cursor);
	struct wal_index_full_hdr *ihdr = get_full_hdr(e);
	PRE(first_to_unapply.start >= ihdr->basic[0].mxFrame);
	PRE(e->shm_locks[WAL_WRITE_LOCK] == VFS2_EXCLUSIVE);

	e->wal_cursor = first_to_unapply.start;

	if (e->wal_cursor == ihdr->basic[0].mxFrame) {
		e->shm_locks[WAL_WRITE_LOCK] = 0;
		sm_move(&e->wtx_sm, WTX_FLUSH);
	} else {
		sm_move(&e->wtx_sm, WTX_FOLLOWING);
	}
	return SQLITE_OK;
}

int vfs2_unhide(sqlite3_file *file)
{
	struct file *xfile = (struct file *)file;
	PRE(xfile->flags & SQLITE_OPEN_MAIN_DB);
	struct entry *e = xfile->entry;
	PRE(e->shm_locks[WAL_WRITE_LOCK] == VFS2_EXCLUSIVE);
	e->shm_locks[WAL_WRITE_LOCK] = 0;

	struct wal_index_full_hdr *hdr = get_full_hdr(e);
	hdr->basic[0] = e->pending_txn_hdr;
	hdr->basic[1] = e->pending_txn_hdr;
	e->prev_txn_hdr = e->pending_txn_hdr;
	e->pending_txn_hdr = (struct wal_index_basic_hdr){};
	e->pending_txn_len = 0;
	e->pending_txn_last_frame_commit = 0;

	sm_move(&xfile->entry->wtx_sm, WTX_BASE);

	return 0;
}

int vfs2_commit(sqlite3_file *file, struct vfs2_wal_slice stop)
{
	struct file *xfile = (struct file *)file;
	PRE(xfile->flags & SQLITE_OPEN_MAIN_DB);
	struct entry *e = xfile->entry;
	uint32_t commit = stop.start + stop.len;
	PRE(e->wal_cursor >= commit);
	PRE(salts_equal(stop.salts, e->wal_cur_hdr.salts));
	PRE(e->shm_locks[WAL_WRITE_LOCK] == VFS2_EXCLUSIVE);
	sqlite3_file *wal_cur = e->wal_cur;
	struct wal_frame_hdr fhdr;
	int rv = wal_cur->pMethods->xRead(wal_cur, &fhdr, sizeof(fhdr), wal_offset_from_cursor(e->page_size, stop.start + stop.len - 1));
	if (rv == SQLITE_OK) {
		return rv;
	}
	set_mx_frame(get_full_hdr(e), commit, fhdr);
	if (commit == e->wal_cursor) {
		e->shm_locks[WAL_WRITE_LOCK] = 0;
		sm_move(&e->wtx_sm, WTX_FLUSH);
	} else {
		sm_move(&e->wtx_sm, WTX_FOLLOWING);
	}
	return 0;
}

int vfs2_commit_barrier(sqlite3_file *file)
{
	struct file *xfile = (struct file *)file;
	PRE(xfile->flags & SQLITE_OPEN_MAIN_DB);
	struct entry *e = xfile->entry;
	tracef("BARRIER %p", e);
	PRE(e->shm_regions_len > 0);
	if (e->wal_cursor > 0) {
		sqlite3_file *wal_cur = e->wal_cur;
		struct wal_frame_hdr fhdr;
		/* FIXME(cole) */
		PRE(e->wal_cursor > 0);
		sqlite3_int64 off = wal_offset_from_cursor(e->page_size, e->wal_cursor - 1);
		int rv = wal_cur->pMethods->xRead(wal_cur, &fhdr, sizeof(fhdr), off);
		if (rv != SQLITE_OK) {
			return rv;
		}
		set_mx_frame(get_full_hdr(e), e->wal_cursor, fhdr);
		/* It's okay if the write lock isn't held. */
		e->shm_locks[WAL_WRITE_LOCK] = 0;
		get_full_hdr(e)->basic[0].isInit = 0;
		/* The next transaction will cause SQLite to run recovery. */
		sm_move(&e->wtx_sm, WTX_INVALIDATED);
	}
	return 0;
}

int vfs2_poll(sqlite3_file *file, dqlite_vfs_frame **frames, unsigned *n, struct vfs2_wal_slice *sl)
{
	struct file *xfile = (struct file *)file;
	PRE(xfile->flags & SQLITE_OPEN_MAIN_DB);
	struct entry *e = xfile->entry;

	uint32_t len = e->pending_txn_len;
	if (len > 0) {
		/* Don't go through vfs2_shm_lock here since that has additional
		 * checks that assume the context of being called from inside
		 * SQLite. */
		if (e->shm_locks[WAL_WRITE_LOCK] > 0) {
			return 1;
		}
		e->shm_locks[WAL_WRITE_LOCK] = VFS2_EXCLUSIVE;
	}

	/* Note, not resetting pending_txn_{start,len} because they are used by later states */
	if (n != NULL && frames != NULL) {
		*n = len;
		*frames = e->pending_txn_frames;
	} else {
		for (uint32_t i = 0; i < e->pending_txn_len; i++) {
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

	/* FIXME(cole) need some additional tracking for ongoing
	 * txn after standalone BEGIN? */
	if (len > 0) {
		sm_move(&xfile->entry->wtx_sm, WTX_POLLED);
	}

	return 0;
}

void vfs2_destroy(sqlite3_vfs *vfs)
{
	struct common *data = vfs->pAppData;
	queue *head = &data->queue;
	queue *cur = queue_next(head);
	while (cur != head) {
		queue *next = queue_next(cur);
		struct entry *e = QUEUE_DATA(cur, struct entry, link);
		PRE(e->refcount_main_db == 0);
		PRE(e->refcount_wal == 0);

		sqlite3_free(e->main_db_name);
		sqlite3_free(e->wal_moving_name);
		sqlite3_free(e->wal_cur_fixed_name);
		if (e->wal_cur->pMethods != NULL) {
			e->wal_cur->pMethods->xClose(e->wal_cur);
		}
		sqlite3_free(e->wal_cur);
		sqlite3_free(e->wal_prev_fixed_name);
		if (e->wal_prev->pMethods != NULL) {
			e->wal_prev->pMethods->xClose(e->wal_prev);
		}
		sqlite3_free(e->wal_prev);

		free_pending_txn(e);

		PRE(e->shm_refcount == 0);
		for (int i = 0; i < e->shm_regions_len; i++) {
			sqlite3_free(e->shm_regions[i]);
		}
		sqlite3_free(e->shm_regions);

		sqlite3_free(e);
		cur = next;
	}
	pthread_rwlock_destroy(&data->rwlock);
	sqlite3_free(data);
	sqlite3_free(vfs);
}

int vfs2_abort(sqlite3_file *file)
{
	struct file *xfile = (struct file *)file;
	PRE(xfile->flags & SQLITE_OPEN_MAIN_DB);
	struct entry *e = xfile->entry;

	e->shm_locks[WAL_WRITE_LOCK] = 0;

	struct wal_index_full_hdr *hdr = get_full_hdr(e);
	hdr->basic[0] = e->prev_txn_hdr;
	hdr->basic[1] = e->prev_txn_hdr;
	e->pending_txn_hdr = (struct wal_index_basic_hdr){};

	e->wal_cursor = e->pending_txn_start;
	free_pending_txn(e);

	sm_move(&xfile->entry->wtx_sm, WTX_BASE);
	return 0;
}

int vfs2_read_wal(sqlite3_file *file,
		  struct vfs2_wal_txn *txns,
		  size_t txns_len)
{
	struct file *xfile = (struct file *)file;
	PRE(xfile->flags & SQLITE_OPEN_MAIN_DB);
	struct entry *e = xfile->entry;
	int rv;

	/* TODO check wal integrity before reading it */
	(void)check_wal_integrity;

	int page_size = (int)e->page_size;
	for (size_t i = 0; i < txns_len; i++) {
		dqlite_vfs_frame *f = sqlite3_malloc64(txns[i].meta.len * sizeof(*f));
		if (f == NULL) {
			goto oom;
		}
		txns[i].frames = f;
		for (size_t j = 0; j < txns[i].meta.len; j++) {
			void *p = sqlite3_malloc(page_size);
			if (p == NULL) {
				goto oom;
			}
			txns[i].frames[j].data = p;
		}
	}

	for (size_t i = 0; i < txns_len; i++) {
		sqlite3_file *wal;
		unsigned read_lock;
		bool from_wal_cur = salts_equal(txns[i].meta.salts, e->wal_cur_hdr.salts);
		bool from_wal_prev = salts_equal(txns[i].meta.salts, e->wal_prev_hdr.salts);
		assert(from_wal_cur ^ from_wal_prev);
		if (from_wal_cur) {
			rv = vfs2_pseudo_read_begin(file, e->wal_cursor, &read_lock);
			if (rv != SQLITE_OK) {
				return 1;
			}
			wal = e->wal_cur;
		} else {
			wal = e->wal_prev;
		}

		uint32_t start = txns[i].meta.start;
		uint32_t len = txns[i].meta.len;
		for (uint32_t j = 0; j < len; j++) {
			sqlite3_int64 off = wal_offset_from_cursor(e->page_size, start + j);
			struct wal_frame_hdr fhdr;
			rv = wal->pMethods->xRead(wal, &fhdr, sizeof(fhdr), off);
			if (rv != SQLITE_OK) {
				return 1;
			}
			off += (sqlite3_int64)sizeof(fhdr);
			rv = wal->pMethods->xRead(wal, txns[i].frames[j].data, page_size, off);
			if (rv != SQLITE_OK) {
				return 1;
			}
			txns[i].frames[j].page_number = ByteGetBe32(fhdr.page_number);
		}
		if (from_wal_cur) {
			vfs2_pseudo_read_end(file, read_lock);
		}
	}

	return 0;

oom:
	for (uint32_t i = 0; i < txns_len; i++) {
		for (uint32_t j = 0; j < txns[i].meta.len; j++) {
			sqlite3_free(txns[i].frames[j].data);
		}
		sqlite3_free(txns[i].frames);
		txns[i].frames = NULL;
	}
	return 1;
}

static int write_one_frame(struct entry *e, struct wal_frame_hdr hdr, void *data)
{
	int rv;

	sqlite3_int64 off = wal_offset_from_cursor(e->page_size, e->wal_cursor);
	rv = e->wal_cur->pMethods->xWrite(e->wal_cur, &hdr, sizeof(hdr), off);
	if (rv != SQLITE_OK) {
		return rv;
	}
	off += (sqlite3_int64)sizeof(hdr);
	rv = e->wal_cur->pMethods->xWrite(e->wal_cur, data, (int)e->page_size, off);
	if (rv != SQLITE_OK) {
		return rv;
	}
	e->wal_cursor += 1;
	tracef("wal_cursor=%u", e->wal_cursor);
	return SQLITE_OK;
}

static struct wal_hdr next_wal_hdr(const struct entry *e)
{
	struct wal_hdr ret;
	struct wal_hdr old = e->wal_cur_hdr;
	BytePutBe32(native_magic(), ret.magic);
	BytePutBe32(3007000, ret.version);
	BytePutBe32(e->page_size, ret.page_size);
	uint32_t ckpoint_seqno = ByteGetBe32(old.ckpoint_seqno);
	BytePutBe32(ckpoint_seqno + 1, ret.ckpoint_seqno);
	uint32_t salt1;
	/* TODO(cole) explain sqlite3_randomness here */
       	if (ckpoint_seqno == 0) {
		salt1 = get_salt1(old.salts) + 1;
	} else {
		sqlite3_randomness(sizeof(salt1), (void *)&salt1);
	}
	BytePutBe32(salt1, ret.salts.salt1);
	sqlite3_randomness(sizeof(ret.salts.salt1), (void *)&ret.salts.salt2);
	struct cksums sums = {};
	update_cksums(native_magic(), (void *)&ret, 24, &sums);
	BytePutBe32(sums.cksum1, ret.cksum1);
	BytePutBe32(sums.cksum2, ret.cksum2);
	return ret;
}

static struct wal_frame_hdr txn_frame_hdr(struct entry *e, struct cksums sums, uint32_t commit, dqlite_vfs_frame frame)
{
	struct wal_frame_hdr fhdr;

	PRE(frame.page_number < (unsigned long)UINT32_MAX);
	BytePutBe32((uint32_t)frame.page_number, fhdr.page_number);
	BytePutBe32(commit, fhdr.commit);
	update_cksums(ByteGetBe32(e->wal_cur_hdr.magic), (const void *)(&fhdr), 8, &sums);
	update_cksums(ByteGetBe32(e->wal_cur_hdr.magic), frame.data, e->page_size, &sums);
	fhdr.salts = e->wal_cur_hdr.salts;
	BytePutBe32(sums.cksum1, fhdr.cksum1);
	BytePutBe32(sums.cksum2, fhdr.cksum2);
	return fhdr;
}

int vfs2_apply_uncommitted(sqlite3_file *file, uint32_t page_size,
		const dqlite_vfs_frame *frames, unsigned len,
		struct vfs2_wal_slice *out)
{
	PRE(len > 0);
	PRE(is_valid_page_size(page_size));
	for (unsigned i = 0; i < len; i++) {
		PRE(frames[i].page_number < UINT32_MAX);
	}
	struct file *xfile = (struct file *)file;
	PRE(xfile->flags & SQLITE_OPEN_MAIN_DB);
	struct entry *e = xfile->entry;
	/* XXX(cole) */
	if (e->page_size == 0) {
		e->page_size = page_size;
	}
	PRE(page_size == e->page_size);
	int rv;

	/* The write lock is always held if there is at least one
	 * uncommitted frame in WAL-cur. In FOLLOWING state, we allow
	 * adding more frames to WAL-cur even if there are already
	 * some uncommitted frames. Hence we don't check the write
	 * lock here before "acquiring" it, we just make sure that
	 * it's held before returning.
	 *
	 * The write lock will be released when a call to vfs2_commit
	 * or vfs2_unapply causes the number of committed frames in
	 * WAL-cur (mxFrame) to equal the number of applied frames
	 * (wal_cursor). */
	e->shm_locks[WAL_WRITE_LOCK] = VFS2_EXCLUSIVE;
	
	struct wal_index_full_hdr *ihdr = get_full_hdr(e);
	uint32_t mx = ihdr->basic[0].mxFrame;
	/* TODO(cole) understand the behavior when mx == 0 */
	if (/* mx > 0 && */ ihdr->nBackfill == mx && mx == e->wal_cursor) {
		struct wal_hdr new_whdr = next_wal_hdr(e);
		restart_full_hdr(ihdr, new_whdr);
		tracef("WAL SWAP mx=%u cksums=%u %u", mx, ByteGetBe32(new_whdr.cksum1), ByteGetBe32(new_whdr.cksum2));
		rv = wal_swap(e, &new_whdr);
		if (rv != SQLITE_OK) {
			return 1;
		}
		/* sm_move(&e->wtx_sm, WTX_FLUSH); */
	}

	uint32_t start = e->wal_cursor;

	struct cksums sums;
	uint32_t db_size;
	if (start > 0) {
		/* TODO cache this in the entry? */
		struct wal_frame_hdr prev_fhdr;
		sqlite3_int64 off = wal_offset_from_cursor(e->page_size, e->wal_cursor - 1);
		rv = e->wal_cur->pMethods->xRead(e->wal_cur, &prev_fhdr, sizeof(prev_fhdr), off);
		if (rv != SQLITE_OK) {
			return 1;
		}

		sums.cksum1 = ByteGetBe32(prev_fhdr.cksum1);
		sums.cksum2 = ByteGetBe32(prev_fhdr.cksum2);
		db_size = ByteGetBe32(prev_fhdr.commit);
		assert(db_size > 0);
	} else {

		/* TODO cache this in the entry? */
		uint8_t b[100];
		rv = xfile->orig->pMethods->xRead(xfile->orig, &b, sizeof(b), 0);
		if (rv != SQLITE_OK) {
			return 1;
		}

		sums.cksum1 = ByteGetBe32(e->wal_cur_hdr.cksum1);
		sums.cksum2 = ByteGetBe32(e->wal_cur_hdr.cksum2);
		db_size = ByteGetBe32(b + 28);
		/* TODO cross-check with xFileSize */
	}

	struct wal_frame_hdr fhdr = txn_frame_hdr(e, sums, 0 == len - 1 ? db_size : 0, frames[0]);
	rv = write_one_frame(e, fhdr, frames[0].data);
	if (rv != SQLITE_OK) {
		return 1;
	}

	for (unsigned i = 1; i < len; i++) {
		sums.cksum1 = ByteGetBe32(fhdr.cksum1);
		sums.cksum2 = ByteGetBe32(fhdr.cksum2);
		uint32_t pn = (uint32_t)frames[i].page_number;
		db_size = pn > db_size ? pn : db_size;
		fhdr = txn_frame_hdr(e, sums, i == len - 1 ? db_size : 0, frames[i]);
		rv = write_one_frame(e, fhdr, frames[i].data);
		if (rv != SQLITE_OK) {
			return 1;
		}
	}	

	sm_move(&e->wtx_sm, WTX_FOLLOWING);
	out->salts = e->wal_cur_hdr.salts;
	out->start = start;
	out->len = len;
	return 0;
}

/* Get the index of the `i`th read lock in the array of
 * shm locks. There are five read locks, and three non-read
 * locks that come before the read locks.
 * See https://sqlite.org/walformat.html#wal_locks. */
static unsigned read_lock(unsigned i)
{
	PRE(i < 5);
	return 3 + i;
}

int vfs2_pseudo_read_begin(sqlite3_file *file, uint32_t target, unsigned *out)
{
	struct file *xfile = (struct file *)file;
	PRE(xfile->flags & SQLITE_OPEN_MAIN_DB);
	struct entry *e = xfile->entry;
	struct wal_index_full_hdr *ihdr = get_full_hdr(e);

	/* adapted from walTryBeginRead */
	uint32_t max_mark = 0;
	unsigned max_index = 0;
	for (unsigned i = 1; i < WAL_NREADER; i++){
		uint32_t cur = ihdr->marks[i];
		if (max_mark <= cur && cur <= target) {
			assert(cur != READ_MARK_UNUSED);
			max_mark = cur;
			max_index = i;
		}
	}
	if (max_mark < target || max_index == 0) {
		for (unsigned i = 1; i < WAL_NREADER; i++) {
			if (e->shm_locks[read_lock(i)] > 0) {
				continue;
			}
			ihdr->marks[i] = target;
			max_mark = target;
			max_index = i;
			break;
		}
	}
	if (max_index == 0) {
		return 1;
	}
	*out = max_index;
	return 0;
}

int vfs2_pseudo_read_end(sqlite3_file *file, unsigned i)
{
	struct file *xfile = (struct file *)file;
	PRE(xfile->flags & SQLITE_OPEN_MAIN_DB);
	struct entry *e = xfile->entry;
	PRE(e->shm_locks[i] > 0);
	e->shm_locks[i] -= 1;
	return 0;
}
