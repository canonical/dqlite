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

#define DB_FILE_HEADER_SIZE 100
#define DB_FILE_HEADER_NPAGES_OFFSET 28

static const uint32_t invalid_magic = 0x17171717;

/* clang-format off */

enum {
	/* Entry is not yet open. */
	WTX_CLOSED,
	/* Next WAL write will be a header write, causing a WAL swap (WAL-cur is
	   empty or fully checkpointed). */
	WTX_EMPTY,
	/* Non-leader, at least one transaction in WAL-cur is not committed. */
	WTX_FOLLOWING,
	/* Leader, all transactions in WAL-cur are committed (but at least one
	   is not checkpointed). */
	WTX_BASE,
	/* Leader, transaction in progress. */
	WTX_ACTIVE,
	/* Leader, transaction committed by SQLite and hidden. */
	WTX_HIDDEN,
	/* Leader, transation committed by SQLite, hidden, and polled. */
	WTX_POLLED,
	WTX_NR
};

/**
 * Major transitions for this state machine:
 *
 *     +-----------CLOSED----------+
 *     |                           |
 * xOpen && no tx in WAL-cur   xOpen && tx in WAL-cur
 *     |                           |
 *     |                 (FB)      |
 *     v      (BE)     <------     v  v-+ vfs2_add_uncommitted
 *     EMPTY<------BASE------>FOLLOWING-+
 *                 ^  |  (BF)
 *                 |  |
 *                 |  | sqlite3_step && xWrite(WAL)
 *                 |  |
 *                 |  v    v-+ xWrite(WAL)
 *                 |--ACTIVE-+
 *                 |  |
 *      vfs2_abort |  | COMMIT_PHASETWO
 *                 |  |
 *                 |  v
 *                 |--HIDDEN
 *                 |  |
 *                 |  | vfs2_poll
 *                 |  |
 *                 |  v
 *                 +--POLLED
 *
 * Abbreviations and omissions:
 * - BE occurs when we run a full checkpoint.
 * - FB occurs when we call vfs2_apply or vfs2_unadd and no uncommitted
 *   transactions remain afterward.
 * - BF occurs via vfs2_add_uncommitted.
 * - EMPTY may move to ACTIVE via sqlite3_step.
 * - All states may move back to CLOSED.
 */
static const struct sm_conf wtx_states[WTX_NR] = {
	[WTX_CLOSED] = {
		.flags = SM_INITIAL|SM_FINAL,
		.name = "closed",
		.allowed = BITS(WTX_EMPTY)
		          |BITS(WTX_BASE)
			  |BITS(WTX_FOLLOWING),
	},
        [WTX_EMPTY] = {
                .name = "empty",
                .allowed = BITS(WTX_FOLLOWING)
                          |BITS(WTX_ACTIVE)
                          |BITS(WTX_CLOSED),
        },
        [WTX_FOLLOWING] = {
                .name = "following",
                .allowed = BITS(WTX_BASE)
                          |BITS(WTX_FOLLOWING)
                          |BITS(WTX_CLOSED),
        },
        [WTX_BASE] = {
                .name = "base",
                .allowed = BITS(WTX_ACTIVE)
                          |BITS(WTX_FOLLOWING)
                          |BITS(WTX_EMPTY)
                          |BITS(WTX_CLOSED),
        },
        [WTX_ACTIVE] = {
                .name = "active",
                .allowed = BITS(WTX_BASE)
                          |BITS(WTX_ACTIVE)
                          |BITS(WTX_HIDDEN)
                          |BITS(WTX_CLOSED),
        },
        [WTX_HIDDEN] = {
                .name = "hidden",
                .allowed = BITS(WTX_BASE)
                          |BITS(WTX_POLLED)
                          |BITS(WTX_CLOSED),
        },
        [WTX_POLLED] = {
                .name = "polled",
                .allowed = BITS(WTX_BASE)
                          |BITS(WTX_CLOSED),
        },
};

/**
 * State machine that just tracks which WAL is WAL-cur, for observability.
 */
enum {
	WAL_1,
	WAL_2,
	WAL_NR
};

static const struct sm_conf wal_states[WAL_NR] = {
	[WAL_1] = {
		.name = "wal1",
		.allowed = BITS(WAL_2),
		.flags = SM_INITIAL|SM_FINAL
	},
	[WAL_2] = {
		.name = "wal2",
		.allowed = BITS(WAL_1),
		.flags = SM_INITIAL|SM_FINAL
	}

};

/**
 * State machine that just tracks the state of the WAL write lock, for
 * observability.
 */
enum {
	WLK_UNLOCKED,
	WLK_LOCKED,
	WLK_NR
};

static const struct sm_conf wlk_states[WLK_NR] = {
	[WLK_UNLOCKED] = {
		.name = "unlocked",
		.allowed = BITS(WLK_LOCKED),
		.flags = SM_INITIAL|SM_FINAL
	},
	[WLK_LOCKED] = {
		.name = "locked",
		.allowed = BITS(WLK_UNLOCKED),
		.flags = SM_INITIAL|SM_FINAL
	}
};

/**
 * State machine that tracks who has been working on the shm, for
 * observability.
 *
 * Other than observability, the point of this state machine is to check that
 * SQLite does not run recovery on the shm after vfs2 has modified it.
 */
enum {
	/* Nothing in the shm yet. */
	SHM_EMPTY,
	/* SQLite is holding the recovery lock, building up the shm. */
	SHM_RECOVERING,
	/* SQLite has finished building up the shm, vfs2 has not touched it. */
	SHM_RECOVERED,
	/* vfs2 made the last modification to the shm. */
	SHM_MANAGED,
	SHM_NR
};

static const struct sm_conf shm_states[SHM_NR] = {
	[SHM_EMPTY] = {
		.name = "empty",
		.allowed = BITS(SHM_RECOVERING)|BITS(SHM_MANAGED),
		.flags = SM_INITIAL
	},
	[SHM_RECOVERING] = {
		.name = "recovering",
		.allowed = BITS(SHM_RECOVERED),
	},
	[SHM_RECOVERED] = {
		.name = "recovered",
		.allowed = BITS(SHM_MANAGED)
	},
	[SHM_MANAGED] = {
		.name = "managed",
		.allowed = BITS(SHM_MANAGED)
	},
};

/**
 * State machine that just tracks whether a checkpoint is in progress or not,
 * for observability.
 */
enum {
	CKPT_QUIESCENT,
	CKPT_CHECKPOINTING,
	CKPT_NR
};

static const struct sm_conf ckpt_states[CKPT_NR] = {
	[CKPT_QUIESCENT] = {
		.name = "quiescent",
		.allowed = BITS(CKPT_CHECKPOINTING),
		.flags = SM_INITIAL
	},
	[CKPT_CHECKPOINTING] = {
		.name = "checkpointing",
		.allowed = BITS(CKPT_QUIESCENT)
	}
};

/* clang-format on */

/**
 * A dummy invariant, for when you just don't care.
 */
static bool no_invariant(const struct sm *sm, int prev)
{
	(void)sm;
	(void)prev;
	return true;
}

/**
 * Userdata owned by the VFS.
 */
struct common {
	sqlite3_vfs *orig;       /* underlying VFS */
	pthread_rwlock_t rwlock; /* protects the queue */
	queue queue;             /* queue of entry */
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

static void update_cksums(const uint8_t *p, size_t len, struct cksums *sums)
{
	PRE(len % 8 == 0);
	const uint8_t *end = p + len;
	uint32_t n;
	while (p != end) {
		memcpy(&n, p, 4);
		sums->cksum1 += n + sums->cksum2;
		p += 4;
		memcpy(&n, p, 4);
		sums->cksum2 += n + sums->cksum1;
		p += 4;
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

#define SHM_SHORT_PGNOS_LEN 4062
#define SHM_LONG_PGNOS_LEN 4096
#define SHM_HT_LEN 8192

/**
 * View of a shm region.
 *
 * The zeroth region looks like this (not to scale):
 *
 * | header | page numbers | hash table |
 *
 * The first and later regions look like this (also not to scale):
 *
 * |      page numbers     | hash table |
 */
struct shm_region {
	union {
		/* region 0 */
		struct {
			struct wal_index_full_hdr hdr;
			uint32_t pgnos_short[SHM_SHORT_PGNOS_LEN];
		};
		/* region 1 and later */
		uint32_t pgnos_long[SHM_LONG_PGNOS_LEN];
	};
	uint16_t ht[SHM_HT_LEN];
};

/**
 * "Shared" memory implementation for storing the WAL-index.
 *
 * All the regions are stored in a single allocation, whose size is a multiple
 * of VFS2_WAL_INDEX_REGION_SIZE. We realloc to create additional regions as
 * they are demanded.
 */
struct shm {
	struct shm_region *regions;
	int num_regions;
	/* Counts the net number of times that SQLite has mapped the zeroth
	 * region. As a sanity check, we assert that this value is zero before
	 * we free the shm. */
	unsigned refcount;
	struct sm sm;
};

static_assert(sizeof(struct shm_region) == VFS2_WAL_INDEX_REGION_SIZE,
	      "shm regions have the expected size");

static void write_basic_hdr_cksums(struct wal_index_basic_hdr *bhdr)
{
	struct cksums sums = {};
	const uint8_t *p = (const uint8_t *)bhdr;
	size_t len = offsetof(struct wal_index_basic_hdr, cksums);
	update_cksums(p, len, &sums);
	bhdr->cksums = sums;
}

/**
 * Perform initial setup of the WAL-index.
 *
 * This allocates the zeroth region and fills in the WAL-index header
 * based on the provided header of WAL-cur.
 */
static int shm_init(struct shm *shm, struct wal_hdr whdr)
{
	shm->regions = sqlite3_malloc64(VFS2_WAL_INDEX_REGION_SIZE);
	if (shm->regions == NULL) {
		return SQLITE_NOMEM;
	}
	shm->regions[0] = (struct shm_region){};
	shm->num_regions = 1;
	shm->refcount = 0;

	struct shm_region *r0 = &shm->regions[0];
	struct wal_index_full_hdr *ihdr = &r0->hdr;
	ihdr->basic[0].iVersion = 3007000;
	ihdr->basic[0].isInit = 1;
	ihdr->basic[0].bigEndCksum = is_bigendian();
	ihdr->basic[0].szPage = (uint16_t)ByteGetBe32(whdr.page_size);
	write_basic_hdr_cksums(&ihdr->basic[0]);
	ihdr->basic[1] = ihdr->basic[0];
	ihdr->marks[0] = 0;
	ihdr->marks[1] = READ_MARK_UNUSED;
	ihdr->marks[2] = READ_MARK_UNUSED;
	ihdr->marks[3] = READ_MARK_UNUSED;
	ihdr->marks[4] = READ_MARK_UNUSED;
	sm_move(&shm->sm, SHM_MANAGED);
	return SQLITE_OK;
}

/**
 * Clear out all data in the WAL-index after a WAL swap, and re-initialize the
 * header.
 */
static void shm_restart(struct shm *shm, struct wal_hdr whdr)
{
	for (int i = 0; i < shm->num_regions; i++) {
		shm->regions[i] = (struct shm_region){};
	}

	/* TODO(cole) eliminate redundancy with shm_init */
	struct shm_region *r0 = &shm->regions[0];
	struct wal_index_full_hdr *ihdr = &r0->hdr;
	ihdr->basic[0].iVersion = 3007000;
	ihdr->basic[0].isInit = 1;
	ihdr->basic[0].bigEndCksum = is_bigendian();
	ihdr->basic[0].szPage = (uint16_t)ByteGetBe32(whdr.page_size);
	write_basic_hdr_cksums(&ihdr->basic[0]);
	ihdr->basic[1] = ihdr->basic[0];
	ihdr->marks[0] = 0;
	ihdr->marks[1] = READ_MARK_UNUSED;
	ihdr->marks[2] = READ_MARK_UNUSED;
	ihdr->marks[3] = READ_MARK_UNUSED;
	ihdr->marks[4] = READ_MARK_UNUSED;
	sm_move(&shm->sm, SHM_MANAGED);
}

/**
 * Add the page number for a frame to the appropriate page number array
 * in the WAL-index.
 *
 * This allocates a new shm region if necessary, and hence can fail with
 * SQLITE_NOMEM.
 */
static int shm_add_pgno(struct shm *shm, uint32_t frame, uint32_t pgno)
{
	PRE(shm->num_regions > 0);
	if (frame < SHM_SHORT_PGNOS_LEN) {
		struct shm_region *r0 = &shm->regions[0];
		r0->pgnos_short[frame] = pgno;
		return SQLITE_OK;
	}

	uint32_t regno = (frame - SHM_SHORT_PGNOS_LEN) / SHM_LONG_PGNOS_LEN;
	uint32_t index = (frame - SHM_SHORT_PGNOS_LEN) % SHM_LONG_PGNOS_LEN;
	PRE(regno <= (uint32_t)shm->num_regions + 1);
	if (regno == (uint32_t)shm->num_regions + 1) {
		sqlite3_uint64 sz =
		    (sqlite3_uint64)regno * VFS2_WAL_INDEX_REGION_SIZE;
		struct shm_region *p = sqlite3_realloc64(shm->regions, sz);
		if (p == NULL) {
			return SQLITE_NOMEM;
		}
		shm->regions = p;
		shm->num_regions++;
	}
	shm->regions[regno].pgnos_long[index] = pgno;
	sm_move(&shm->sm, SHM_MANAGED);
	return SQLITE_OK;
}

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

	/* Number of `struct file` with SQLITE_OPEN_MAIN_DB that point to this
	 * entry */
	unsigned refcount_main_db;
	/* Number of `struct file` with SQLITE_OPEN_WAL that point to this entry
	 */
	unsigned refcount_wal;

	/* if WAL-cur is nonempty at startup, we read its header, verify the
	 * checkum, and use it to initialize the page size. otherwise, we wait
	 * until the first write to the WAL, which should be the header */
	uint32_t page_size;

	/* For ACTIVE, HIDDEN, POLLED: the header that hides the pending txn */
	struct wal_index_basic_hdr prev_txn_hdr;
	/* For ACTIVE, HIDDEN, POLLED: the header that shows the pending txn */
	struct wal_index_basic_hdr pending_txn_hdr;

	/* Shared memory implementation: holds the WAL-index. */
	struct shm shm;
	/* Zero for unlocked, positive for read-locked, UINT_MAX for
	 * write-locked */
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
	struct sm wal_sm;
	struct sm wlk_sm;
	struct sm ckpt_sm;
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

static struct vfs2_salts make_salts(uint32_t salt1, uint32_t salt2)
{
	struct vfs2_salts ret;
	BytePutBe32(salt1, ret.salt1);
	BytePutBe32(salt2, ret.salt2);
	return ret;
}

static struct wal_index_full_hdr *get_full_hdr(struct entry *e)
{
	PRE(e->shm.num_regions > 0);
	PRE(e->shm.regions != NULL);
	return &e->shm.regions[0].hdr;
}

static bool no_pending_txn(const struct entry *e)
{
	return e->pending_txn_len == 0 && e->pending_txn_frames == NULL &&
	       e->pending_txn_last_frame_commit == 0;
}

static bool wal_index_basic_hdr_equal(struct wal_index_basic_hdr a,
				      struct wal_index_basic_hdr b)
{
	return memcmp(&a, &b, sizeof(struct wal_index_basic_hdr)) == 0;
}

static bool is_valid_page_size(unsigned long n)
{
	return n >= 1 << 9 && n <= 1 << 16 && is_po2(n);
}

static bool basic_hdr_valid(const struct wal_index_basic_hdr *bhdr)
{
	struct cksums sums = {};
	const uint8_t *p = (const uint8_t *)bhdr;
	size_t len = offsetof(struct wal_index_basic_hdr, cksums);
	update_cksums(p, len, &sums);
	return bhdr->iVersion == 3007000 && bhdr->isInit == 1 &&
	       cksums_equal(sums, bhdr->cksums);
}

static bool full_hdr_valid(const struct wal_index_full_hdr *ihdr)
{
	return basic_hdr_valid(&ihdr->basic[0]) &&
	       wal_index_basic_hdr_equal(ihdr->basic[0], ihdr->basic[1]);
}

static bool wtx_invariant(const struct sm *sm, int prev)
{
	(void)sm;
	(void)prev;
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
	return (f->flags & SQLITE_OPEN_WAL) ? f->entry->wal_cur : f->orig;
}

static void maybe_close_entry(struct entry *e)
{
	if (e->refcount_main_db > 0 || e->refcount_wal > 0) {
		return;
	}

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

	assert(e->shm.refcount == 0);
	sqlite3_free(e->shm.regions);

	pthread_rwlock_wrlock(&e->common->rwlock);
	queue_remove(&e->link);
	pthread_rwlock_unlock(&e->common->rwlock);
	sqlite3_free(e);
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
		xfile->entry->refcount_main_db -= 1;
		maybe_close_entry(xfile->entry);
	} else if (xfile->flags & SQLITE_OPEN_WAL) {
		xfile->entry->refcount_wal -= 1;
		maybe_close_entry(xfile->entry);
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

/**
 * Update the volatile state by exchanging WAL-cur and WAL-prev.
 */
static void wal_swap(struct entry *e, const struct wal_hdr *hdr)
{
	int rv;

	/* Terminology: the outgoing WAL is the one that's moving
	 * from cur to prev. The incoming WAL is the one that's moving
	 * from prev to cur. */
	sqlite3_file *phys_outgoing = e->wal_cur;
	char *name_outgoing = e->wal_cur_fixed_name;
	sqlite3_file *phys_incoming = e->wal_prev;
	char *name_incoming = e->wal_prev_fixed_name;

	e->wal_cur = phys_incoming;
	e->wal_cur_fixed_name = name_incoming;
	e->wal_prev = phys_outgoing;
	e->wal_prev_fixed_name = name_outgoing;
	e->wal_cursor = 0;
	e->wal_prev_hdr = e->wal_cur_hdr;
	e->wal_cur_hdr = *hdr;

	sm_move(&e->wal_sm, !sm_state(&e->wal_sm));

	/* Best-effort: flip the moving name.
	 *
	 * If these syscalls fail, we can end up with no moving name, or a
	 * moving name that points to the wrong WAL. We don't use the moving
	 * name as the source of truth, so this can't lead to dqlite operating
	 * incorrectly. At worst, it's inconvenient for users who want to
	 * inspect their database with SQLite (readonly! when dqlite is not
	 * running!). */
	rv = unlink(e->wal_moving_name);
	(void)rv;
	rv = link(name_incoming, e->wal_moving_name);
	(void)rv;

	/* Best-effort: invalidate the header of the outgoing physical WAL, so
	* that it can't be mistakenly applied to the database.

	* This provides some protection against users manipulating the
	* database with SQLite, or a bug in dqlite. But we don't rely on it
	* for correctness. */
	(void)phys_outgoing->pMethods->xWrite(phys_outgoing, &invalid_magic,
					      sizeof(invalid_magic), 0);

	/* TODO do we need an fsync here? */
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
		sqlite3_uint64 z =
		    (sqlite3_uint64)sizeof(*frames) * (sqlite3_uint64)(n + 1);
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
	if (amt == (int)sizeof(struct wal_hdr)) {
		return SQLITE_OK;
	} else if (amt == VFS2_WAL_FRAME_HDR_SIZE) {
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

	/* A write to the WAL at offset 0 must be a header write, and indicates
	 * that SQLite has reset the WAL. We react by doing a WAL swap.
	 */
	if ((xfile->flags & SQLITE_OPEN_WAL) && ofst == 0) {
		assert(amt == sizeof(struct wal_hdr));
		const struct wal_hdr *hdr = buf;
		struct entry *e = xfile->entry;
		wal_swap(e, hdr);
		/* Save the WAL-index header so that we can roll back to it in
		 * the future. The assertions check that the header we're
		 * saving has been updated to match the new, empty WAL. */
		struct wal_index_basic_hdr ihdr = get_full_hdr(e)->basic[0];
		assert(ihdr.isInit == 1);
		assert(ihdr.mxFrame == 0);
		e->prev_txn_hdr = ihdr;
		sm_move(&e->wtx_sm, WTX_ACTIVE);
	}

	sqlite3_file *orig = get_orig(xfile);
	rv = orig->pMethods->xWrite(orig, buf, amt, ofst);
	if (rv != SQLITE_OK) {
		return rv;
	}

	if (xfile->flags & SQLITE_OPEN_WAL) {
		struct entry *e = xfile->entry;
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

static int interpret_pragma(struct entry *e, char **args)
{
	char **err = &args[0];
	char *left = args[1];
	PRE(left != NULL);
	char *right = args[2];

	if (strcmp(left, "journal_mode") == 0 && right != NULL &&
	    strcasecmp(right, "wal") != 0) {
		*err = sqlite3_mprintf("dqlite requires WAL mode");
		return SQLITE_ERROR;
	} else if (strcmp(left, "page_size") == 0 && right != NULL) {
		char *end = right + strlen(right);
		unsigned long val = strtoul(right, &end, 10);
		if (right != end && *end == '\0' && is_valid_page_size(val)) {
			if (e->page_size != 0 && val != e->page_size) {
				*err = sqlite3_mprintf(
				    "page size cannot be changed");
				return SQLITE_ERROR;
			}
			e->page_size = (uint32_t)val;
		}
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
		rv = interpret_pragma(e, arg);
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
	PRE(regsz == VFS2_WAL_INDEX_REGION_SIZE);
	struct file *xfile = (struct file *)file;
	PRE(xfile->flags & SQLITE_OPEN_MAIN_DB);
	struct entry *e = xfile->entry;
	struct shm *shm = &e->shm;
	struct shm_region *region;
	int rv;

	if (shm->regions != NULL && regno < shm->num_regions) {
		region = &shm->regions[regno];
	} else if (extend != 0) {
		assert(regno == shm->num_regions);
		sqlite3_uint64 sz =
		    ((sqlite3_uint64)regno + 1) * (sqlite3_uint64)regsz;
		struct shm_region *p = sqlite3_realloc64(shm->regions, sz);
		if (p == NULL) {
			rv = SQLITE_NOMEM;
			goto err;
		}
		shm->regions = p;
		region = &shm->regions[regno];
		memset(region, 0, (size_t)regsz);
		shm->num_regions++;
	} else {
		region = NULL;
	}

	*out = region;
	if (regno == 0 && region != NULL) {
		e->shm.refcount++;
	}
	return SQLITE_OK;

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
			if (i == WAL_RECOVER_LOCK) {
				sm_move(&e->shm.sm, SHM_RECOVERING);
			}
		}

		/* XXX maybe this shouldn't be an assertion */
		if (ofst == WAL_WRITE_LOCK) {
			assert(n == 1);
			assert(e->pending_txn_len == 0);
			sm_move(&e->wlk_sm, WLK_LOCKED);
		}

		if (ofst == WAL_CKPT_LOCK && n == 1) {
			sm_move(&e->ckpt_sm, CKPT_CHECKPOINTING);
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
			sm_move(&e->shm.sm, SHM_RECOVERED);
		}

		if (ofst == WAL_WRITE_LOCK) {
			sm_move(&e->wlk_sm, WLK_UNLOCKED);
			/* If the last frame of the pending transaction has no
			 * commit marker when SQLite releases the write lock, it
			 * means that the transaction rolled back before it
			 * committed. We respond by throwing away our stored
			 * frames and resetting the state machine. */
			assert(n == 1);
			if (sm_state(&e->wtx_sm) > WTX_BASE &&
			    e->pending_txn_last_frame_commit == 0) {
				free_pending_txn(e);
				sm_move(&e->wtx_sm, WTX_BASE);
			}
		} else if (ofst == WAL_CKPT_LOCK && n == 1) {
			/* End of a checkpoint: if all frames have been
			 * backfilled, move to EMPTY. */
			struct wal_index_full_hdr *ihdr = get_full_hdr(e);
			if (ihdr->nBackfill == ihdr->basic[0].mxFrame) {
				sm_move(&e->wtx_sm, WTX_EMPTY);
			}
			sm_move(&e->ckpt_sm, CKPT_QUIESCENT);
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
	e->shm.refcount--;
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

static bool wal_hdr_is_valid(const struct wal_hdr *hdr)
{
	/* TODO(cole) Add other validity constraints. */
	struct cksums sums_found = {};
	const uint8_t *p = (const uint8_t *)hdr;
	size_t len = offsetof(struct wal_hdr, cksum1);
	update_cksums(p, len, &sums_found);
	struct cksums sums_expected = { ByteGetBe32(hdr->cksum1),
					ByteGetBe32(hdr->cksum2) };
	return cksums_equal(sums_expected, sums_found);
}

/**
 * Read the header of the given WAL file and detect corruption.
 *
 * If the file contains a valid header, returns this header in `hdr` and the
 * size of the file in `size`. If the file is too short to contain a header,
 * returns the size only. If the file can't be read, or it contains an invalid
 * header, returns an error.
 */
static int try_read_wal_hdr(sqlite3_file *wal,
			    sqlite3_int64 *size,
			    struct wal_hdr *hdr)
{
	int rv;

	rv = wal->pMethods->xFileSize(wal, size);
	if (rv != SQLITE_OK) {
		return rv;
	}
	if (*size < (sqlite3_int64)sizeof(*hdr)) {
		return SQLITE_OK;
	}
	rv = wal->pMethods->xRead(wal, hdr, sizeof(*hdr), 0);
	if (rv != SQLITE_OK) {
		return rv;
	}
	if (!wal_hdr_is_valid(hdr)) {
		return SQLITE_CORRUPT;
	}
	return SQLITE_OK;
}

static void pgno_ht_insert(uint16_t *ht, uint16_t fx, uint32_t pgno)
{
	uint32_t hash = pgno * 383;
	while (ht[hash % SHM_HT_LEN] != 0) {
		hash++;
	}
	/* SQLite uses 1-based frame indices in this context, reserving
	 * 0 for a sentinel value. */
	fx++;
	ht[hash % SHM_HT_LEN] = fx;
}

/**
 * Grab the page number for the given frame from the appropriate array
 * in the WAL-index and add it to the corresponding hash table.
 */
static void shm_update_ht(struct shm *shm, uint32_t frame)
{
	PRE(shm->num_regions > 0);
	sm_move(&shm->sm, SHM_MANAGED);
	if (frame < SHM_SHORT_PGNOS_LEN) {
		struct shm_region *r0 = &shm->regions[0];
		uint32_t pgno = r0->pgnos_short[frame];
		pgno_ht_insert(r0->ht, (uint16_t)frame, pgno);
		return;
	}

	uint32_t regno = (frame - SHM_SHORT_PGNOS_LEN) / SHM_LONG_PGNOS_LEN;
	uint32_t index = (frame - SHM_SHORT_PGNOS_LEN) % SHM_LONG_PGNOS_LEN;
	PRE(regno <= (uint32_t)shm->num_regions);
	struct shm_region *region = &shm->regions[regno];
	uint32_t pgno = region->pgnos_long[index];
	pgno_ht_insert(region->ht, (uint16_t)index, pgno);
}

static void set_mx_frame(struct entry *e,
			 uint32_t mx,
			 struct wal_frame_hdr fhdr)
{
	struct wal_index_full_hdr *ihdr = get_full_hdr(e);
	uint32_t num_pages = ByteGetBe32(fhdr.commit);
	PRE(num_pages > 0);
	uint32_t old_mx = ihdr->basic[0].mxFrame;
	ihdr->basic[0].iChange += 1;
	ihdr->basic[0].mxFrame = mx;
	ihdr->basic[0].nPage = num_pages;
	ihdr->basic[0].frame_cksums.cksum1 = ByteGetBe32(fhdr.cksum1);
	ihdr->basic[0].frame_cksums.cksum2 = ByteGetBe32(fhdr.cksum2);
	write_basic_hdr_cksums(&ihdr->basic[0]);
	ihdr->basic[1] = ihdr->basic[0];
	POST(full_hdr_valid(ihdr));

	struct shm *shm = &e->shm;
	for (uint32_t i = old_mx; i < mx; i++) {
		/* The page numbers array was already updated during the call
		 * to add_uncommitted, so we just need to update the hash array.
		 */
		shm_update_ht(shm, i);
	}
}

static sqlite3_int64 wal_offset_from_cursor(uint32_t page_size, uint32_t cursor)
{
	return (sqlite3_int64)sizeof(struct wal_hdr) +
	       (sqlite3_int64)cursor *
		   ((sqlite3_int64)sizeof(struct wal_frame_hdr) +
		    (sqlite3_int64)page_size);
}

/**
 * Read the given WAL file from beginning to end, initializing the WAL-index in
 * the process.
 *
 * The process stops when we reach a frame whose checksums are invalid, or
 * after the last valid commit frame, or on the first unsuccessful read, or at
 * the end of the transaction designated by `stop`, if that argument is
 * non-NULL.
 *
 * On return, the WAL-index header is initialized with mxFrame = 0 and other
 * data matching the given WAL, and the page numbers for all frames up to the
 * stopping point are recorded in the WAL-index; the hash tables are not
 * initialized.
 *
 * Returns the stopping point in units of frames, which becomes the wal_cursor.
 */
static int walk_wal(sqlite3_file *wal,
		    sqlite3_int64 size,
		    struct wal_hdr hdr,
		    const struct vfs2_wal_slice *stop,
		    uint32_t *wal_cursor,
		    struct shm *shm)
{
	uint32_t page_size = ByteGetBe32(hdr.page_size);
	struct cksums sums = { ByteGetBe32(hdr.cksum1),
			       ByteGetBe32(hdr.cksum2) };
	int rv;

	/* Check whether we have been provided a stopping point that corresponds
	 * to a transaction in the current WAL. (It's possible that the stopping
	 * point corresponds to a transaction that's in WAL-prev instead.) */
	bool have_stop = stop != NULL && salts_equal(stop->salts, hdr.salts);

	uint8_t *page_buf = sqlite3_malloc64(page_size);
	if (page_buf == NULL) {
		return SQLITE_NOMEM;
	}

	sqlite3_int64 off = sizeof(struct wal_hdr);
	while (off < size) {
		if (have_stop && *wal_cursor == stop->start + stop->len) {
			break;
		}

		struct wal_frame_hdr fhdr;
		rv = wal->pMethods->xRead(wal, &fhdr, sizeof(fhdr), off);
		if (rv != SQLITE_OK) {
			goto err;
		}
		if (!salts_equal(fhdr.salts, hdr.salts)) {
			break;
		}
		off += (sqlite3_int64)sizeof(fhdr);
		const uint8_t *p = (const uint8_t *)&fhdr;
		size_t len = offsetof(struct wal_frame_hdr, salts);
		update_cksums(p, len, &sums);

		rv = wal->pMethods->xRead(wal, page_buf, (int)page_size, off);
		if (rv != SQLITE_OK) {
			goto err;
		}
		off += page_size;
		update_cksums(page_buf, page_size, &sums);
		struct cksums frame_sums = { ByteGetBe32(fhdr.cksum1),
					     ByteGetBe32(fhdr.cksum2) };
		if (!cksums_equal(frame_sums, sums)) {
			break;
		}

		rv = shm_add_pgno(shm, *wal_cursor,
				  ByteGetBe32(fhdr.page_number));
		if (rv != SQLITE_OK) {
			goto err;
		}
		*wal_cursor += 1;

		/* We expect the last valid frame to have the commit marker.
		 * That's because if the last transaction wasn't fully written
		 * to the WAL, we should have been passed a `stop` argument
		 * corresponding to some preceding transaction. */
		if (off >= size) {
			assert(ByteGetBe32(fhdr.commit) > 0);
		}
	}

	sqlite3_free(page_buf);
	return SQLITE_OK;

err:
	sqlite3_free(page_buf);
	POST(rv != SQLITE_OK);
	return rv;
}

static int open_entry(struct common *common, const char *name, struct entry *e)
{
	sqlite3_vfs *v = common->orig;
	int path_cap = v->mxPathname + 1;
	int file_cap = v->szOsFile;
	int rv;

	*e = (struct entry){};
	e->common = common;
	sm_init(&e->wtx_sm, wtx_invariant, NULL, wtx_states, "wtx", WTX_CLOSED);

	e->refcount_main_db = 1;

	e->main_db_name = sqlite3_malloc(path_cap);
	e->wal_moving_name = sqlite3_malloc(path_cap);
	e->wal_cur_fixed_name = sqlite3_malloc(path_cap);
	e->wal_prev_fixed_name = sqlite3_malloc(path_cap);
	if (e->main_db_name == NULL || e->wal_moving_name == NULL ||
	    e->wal_cur_fixed_name == NULL || e->wal_prev_fixed_name == NULL) {
		return SQLITE_NOMEM;
	}

	strcpy(e->main_db_name, name);

	strcpy(e->wal_moving_name, name);
	strcat(e->wal_moving_name, "-wal");

	strcpy(e->wal_cur_fixed_name, name);
	strcat(e->wal_cur_fixed_name, "-xwal1");

	strcpy(e->wal_prev_fixed_name, name);
	strcat(e->wal_prev_fixed_name, "-xwal2");

	/* TODO EXRESCODE? */
	int phys_wal_flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
			     SQLITE_OPEN_WAL | SQLITE_OPEN_NOFOLLOW;

	e->wal_cur = sqlite3_malloc(file_cap);
	if (e->wal_cur == NULL) {
		return SQLITE_NOMEM;
	}
	rv = v->xOpen(v, e->wal_cur_fixed_name, e->wal_cur, phys_wal_flags,
		      NULL);
	if (rv != SQLITE_OK) {
		return rv;
	}

	e->wal_prev = sqlite3_malloc(file_cap);
	if (e->wal_prev == NULL) {
		return SQLITE_NOMEM;
	}
	rv = v->xOpen(v, e->wal_prev_fixed_name, e->wal_prev, phys_wal_flags,
		      NULL);
	if (rv != SQLITE_OK) {
		return rv;
	}

	sqlite3_int64 size1;
	struct wal_hdr hdr1;
	rv = try_read_wal_hdr(e->wal_cur, &size1, &hdr1);
	if (rv != SQLITE_OK) {
		return rv;
	}
	sqlite3_int64 size2;
	struct wal_hdr hdr2;
	rv = try_read_wal_hdr(e->wal_prev, &size2, &hdr2);
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
	sm_init(&e->wal_sm, no_invariant, NULL, wal_states, "wal",
		wal1_is_fresh ? WAL_1 : WAL_2);
	sm_relate(&e->wtx_sm, &e->wal_sm);

	e->wal_cur_hdr = hdr_cur;
	e->wal_prev_hdr = hdr_prev;

	rv = unlink(e->wal_moving_name);
	(void)rv;
	rv = link(e->wal_cur_fixed_name, e->wal_moving_name);
	(void)rv;

	sm_init(&e->shm.sm, no_invariant, NULL, shm_states, "shm", SHM_EMPTY);
	sm_relate(&e->wtx_sm, &e->shm.sm);

	/* If WAL-cur contains a valid header, walk it to initialize wal_cursor
	 * and the WAL-index. Also, set the page size.
	 *
	 * If WAL-cur is empty, then we are starting up for the first time, and
	 * we don't initialize the WAL-index. It will be initialized either by
	 * SQLite running recovery or by add_uncommitted, whichever happens
	 * first. (In general, we want to avoid letting SQLite run recovery, but
	 * in this case it's harmless, since if the WAL is empty then it can't
	 * read any uncommitted data.)
	 */
	if (size_cur > 0) {
		e->page_size = ByteGetBe32(hdr_cur.page_size);
		rv = shm_init(&e->shm, hdr_cur);
		if (rv != SQLITE_OK) {
			return rv;
		}
		rv = walk_wal(e->wal_cur, size_cur, hdr_cur, NULL,
			      &e->wal_cursor, &e->shm);
		if (rv != SQLITE_OK) {
			return rv;
		}
	}
	/* If we found at least one valid transaction in the WAL, take the write
	 * lock. */
	if (e->wal_cursor > 0) {
		e->shm_locks[WAL_WRITE_LOCK] = VFS2_EXCLUSIVE;
	}
	sm_init(&e->wlk_sm, no_invariant, NULL, wlk_states, "wlk",
		e->wal_cursor > 0 ? WLK_LOCKED : WLK_UNLOCKED);
	sm_relate(&e->wtx_sm, &e->wlk_sm);

	sm_init(&e->ckpt_sm, no_invariant, NULL, ckpt_states, "ckpt", CKPT_QUIESCENT);
	sm_relate(&e->wtx_sm, &e->ckpt_sm);

	sm_move(&e->wtx_sm, e->wal_cursor > 0 ? WTX_FOLLOWING
			    : size_cur > 0    ? WTX_BASE
					      : WTX_EMPTY);

	return SQLITE_OK;
}

static int set_up_entry(struct common *common,
			const char *name,
			int flags,
			struct entry **e)
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
		unsigned *refcount =
		    name_is_db ? &res->refcount_main_db : &res->refcount_wal;
		*refcount += 1;
		return SQLITE_OK;
	}

	assert(name_is_db);
	res = *e;
	/* If open_entry fails we still want to link in the entry. Since we
	 * unconditionally set pMethods in our file vtable, SQLite will xClose
	 * the file and vfs2_close will run to clean up the partial work of
	 * open_entry. */
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

	if (flags & (SQLITE_OPEN_MAIN_DB | SQLITE_OPEN_WAL)) {
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

/* TODO does this need to be customized? should it ever be called on one of our
 * files? */
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

int vfs2_unadd(sqlite3_file *file, struct vfs2_wal_slice first_to_unadd)
{
	struct file *xfile = (struct file *)file;
	PRE(xfile->flags & SQLITE_OPEN_MAIN_DB);
	struct entry *e = xfile->entry;
	PRE(salts_equal(first_to_unadd.salts, e->wal_cur_hdr.salts));
	PRE(first_to_unadd.start + first_to_unadd.len <= e->wal_cursor);
	struct wal_index_full_hdr *ihdr = get_full_hdr(e);
	PRE(first_to_unadd.start >= ihdr->basic[0].mxFrame);
	PRE(e->shm_locks[WAL_WRITE_LOCK] == VFS2_EXCLUSIVE);

	e->wal_cursor = first_to_unadd.start;

	if (e->wal_cursor == ihdr->basic[0].mxFrame) {
		e->shm_locks[WAL_WRITE_LOCK] = 0;
		sm_move(&e->wtx_sm, WTX_BASE);
		sm_move(&e->wlk_sm, WLK_UNLOCKED);
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
	sm_move(&e->wlk_sm, WLK_UNLOCKED);
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

int vfs2_apply(sqlite3_file *file, struct vfs2_wal_slice stop)
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
	int rv = wal_cur->pMethods->xRead(
	    wal_cur, &fhdr, sizeof(fhdr),
	    wal_offset_from_cursor(e->page_size, stop.start + stop.len - 1));
	if (rv != SQLITE_OK) {
		return rv;
	}
	set_mx_frame(e, commit, fhdr);
	if (commit == e->wal_cursor) {
		e->shm_locks[WAL_WRITE_LOCK] = 0;
		sm_move(&e->wlk_sm, WLK_UNLOCKED);
		sm_move(&e->wtx_sm, WTX_BASE);
	} else {
		sm_move(&e->wtx_sm, WTX_FOLLOWING);
	}
	return 0;
}

int vfs2_poll(sqlite3_file *file,
	      dqlite_vfs_frame **frames_out,
	      struct vfs2_wal_slice *sl_out)
{
	struct file *xfile = (struct file *)file;
	PRE(xfile->flags & SQLITE_OPEN_MAIN_DB);
	struct entry *e = xfile->entry;
	PRE(e->shm_locks[WAL_WRITE_LOCK] == 0);

	uint32_t len = e->pending_txn_len;
	dqlite_vfs_frame *frames = NULL;
	struct vfs2_wal_slice sl = {};
	/* If some frames were produced, take the write lock. */
	if (len > 0) {
		e->shm_locks[WAL_WRITE_LOCK] = VFS2_EXCLUSIVE;
		sm_move(&e->wlk_sm, WLK_LOCKED);
		frames = e->pending_txn_frames;
		e->pending_txn_frames = NULL;
		sl = (struct vfs2_wal_slice){ .salts = e->pending_txn_hdr.salts,
					      .start = e->prev_txn_hdr.mxFrame,
					      .len = len };
		/* We don't clear e->pending_txn_hdr here because it's used by
		 * vfs2_unhide. (By contrast, pending_txn_frames only exists
		 * to be returned by this function if requested.) */
		sm_move(&e->wtx_sm, WTX_POLLED);
	}

	if (frames_out != NULL) {
		*frames_out = frames;
	} else {
		for (uint32_t i = 0; i < len; i++) {
			sqlite3_free(frames[i].data);
		}
		sqlite3_free(frames);
	}
	if (sl_out != NULL) {
		*sl_out = sl;
	}
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
	/* TODO maybe can "followerize" this and get rid of vfs2_unadd_after? */
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
		dqlite_vfs_frame *f =
		    sqlite3_malloc64(txns[i].meta.len * sizeof(*f));
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
		bool from_wal_cur =
		    salts_equal(txns[i].meta.salts, e->wal_cur_hdr.salts);
		bool from_wal_prev =
		    salts_equal(txns[i].meta.salts, e->wal_prev_hdr.salts);
		assert(from_wal_cur ^ from_wal_prev);
		if (from_wal_cur) {
			rv = vfs2_pseudo_read_begin(file, e->wal_cursor,
						    &read_lock);
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
			sqlite3_int64 off =
			    wal_offset_from_cursor(e->page_size, start + j);
			struct wal_frame_hdr fhdr;
			rv =
			    wal->pMethods->xRead(wal, &fhdr, sizeof(fhdr), off);
			if (rv != SQLITE_OK) {
				return 1;
			}
			off += (sqlite3_int64)sizeof(fhdr);
			rv = wal->pMethods->xRead(wal, txns[i].frames[j].data,
						  page_size, off);
			if (rv != SQLITE_OK) {
				return 1;
			}
			txns[i].frames[j].page_number =
			    ByteGetBe32(fhdr.page_number);
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

static int write_one_frame(struct entry *e,
			   struct wal_frame_hdr hdr,
			   void *data)
{
	int rv;

	sqlite3_int64 off = wal_offset_from_cursor(e->page_size, e->wal_cursor);
	rv = e->wal_cur->pMethods->xWrite(e->wal_cur, &hdr, sizeof(hdr), off);
	if (rv != SQLITE_OK) {
		return rv;
	}
	off += (sqlite3_int64)sizeof(hdr);
	rv = e->wal_cur->pMethods->xWrite(e->wal_cur, data, (int)e->page_size,
					  off);
	if (rv != SQLITE_OK) {
		return rv;
	}
	e->wal_cursor += 1;
	return SQLITE_OK;
}

/**
 * Create a valid WAL header from the specified fields.
 */
static struct wal_hdr make_wal_hdr(uint32_t magic,
				   uint32_t page_size,
				   uint32_t ckpoint_seqno,
				   struct vfs2_salts salts)
{
	struct wal_hdr ret;
	BytePutBe32(magic, ret.magic);
	BytePutBe32(3007000, ret.version);
	BytePutBe32(page_size, ret.page_size);
	BytePutBe32(ckpoint_seqno, ret.ckpoint_seqno);
	ret.salts = salts;
	struct cksums sums = {};
	const uint8_t *p = (const uint8_t *)&ret;
	size_t len = offsetof(struct wal_hdr, cksum1);
	update_cksums(p, len, &sums);
	BytePutBe32(sums.cksum1, ret.cksum1);
	BytePutBe32(sums.cksum2, ret.cksum2);
	POST(wal_hdr_is_valid(&ret));
	return ret;
}

static struct wal_hdr initial_wal_hdr(uint32_t page_size)
{
	struct vfs2_salts salts;
	sqlite3_randomness(sizeof(salts.salt1), (void *)&salts.salt1);
	sqlite3_randomness(sizeof(salts.salt2), (void *)&salts.salt2);
	return make_wal_hdr(is_bigendian() ? BE_MAGIC : LE_MAGIC, page_size, 0,
			    salts);
}

/**
 * Derive the next header that should be written to start a new WAL.
 *
 * To get the next header, we start with the header of WAL-cur, increment
 * salt1 and ckpoint_seqno, and randomize salt2.
 */
static struct wal_hdr next_wal_hdr(const struct entry *e)
{
	struct wal_hdr old = e->wal_cur_hdr;
	uint32_t magic = is_bigendian() ? BE_MAGIC : LE_MAGIC;
	uint32_t ckpoint_seqno = ByteGetBe32(old.ckpoint_seqno) + 1;
	/* salt2 is randomized every time we generate a new WAL header.
	 * We don't use the xRandomness method of the base VFS to do this,
	 * because it always translates to a syscall (getrandom), and
	 * SQLite intends that this should only be used for seeding the
	 * internal PRNG. Instead, we call sqlite3_randomness, which gives
	 * us access to this PRNG, seeded from the default (unix) VFS. */
	struct vfs2_salts salts;
	if (ckpoint_seqno == 1) {
		sqlite3_randomness(sizeof(salts.salt1), (void *)&salts.salt1);
	} else {
		BytePutBe32(get_salt1(old.salts) + 1, salts.salt1);
	}
	sqlite3_randomness(sizeof(salts.salt2), (void *)&salts.salt2);
	return make_wal_hdr(magic, e->page_size, ckpoint_seqno, salts);
}

static struct wal_frame_hdr txn_frame_hdr(struct entry *e,
					  struct cksums sums,
					  const dqlite_vfs_frame *frame,
					  uint32_t commit)
{
	struct wal_frame_hdr fhdr;

	BytePutBe32((uint32_t)frame->page_number, fhdr.page_number);
	BytePutBe32(commit, fhdr.commit);
	update_cksums((const void *)(&fhdr), 8, &sums);
	update_cksums(frame->data, e->page_size, &sums);
	fhdr.salts = e->wal_cur_hdr.salts;
	BytePutBe32(sums.cksum1, fhdr.cksum1);
	BytePutBe32(sums.cksum2, fhdr.cksum2);
	return fhdr;
}

int vfs2_add_uncommitted(sqlite3_file *file,
			 uint32_t page_size,
			 const dqlite_vfs_frame *frames,
			 unsigned len,
			 struct vfs2_wal_slice *out)
{
	struct file *xfile = (struct file *)file;
	PRE(xfile->flags & SQLITE_OPEN_MAIN_DB);
	struct entry *e = xfile->entry;
	int rv;

	/* TODO(cole) roll back wal_cursor and release the write lock if one of
	 * the writes fails. */

	PRE(len > 0);

	/* We require the page size to have been set by the pragma before this
	 * point. */
	PRE(is_valid_page_size(page_size));

	/* Sanity check that the leader isn't sending us pages of the wrong
	 * size. */
	PRE(page_size == e->page_size);

	/* The write lock is always held if there is at least one
	 * uncommitted frame in WAL-cur. In FOLLOWING state, we allow
	 * adding more frames to WAL-cur even if there are already
	 * some uncommitted frames. Hence we don't check the write
	 * lock here before "acquiring" it, we just make sure that
	 * it's held before returning.
	 *
	 * The write lock will be released when a call to vfs2_apply
	 * or vfs2_unadd causes the number of committed frames in
	 * WAL-cur (mxFrame) to equal the number of applies frames
	 * (wal_cursor). */
	if (e->shm_locks[WAL_WRITE_LOCK] == 0) {
		sm_move(&e->wlk_sm, WLK_LOCKED);
	}
	e->shm_locks[WAL_WRITE_LOCK] = VFS2_EXCLUSIVE;

	/* This paragraph accomplishes a few related things: initializing the
	 * shm if necessary, figuring out where to place the frames of the new
	 * transaction in the WAL, and, if necessary, writing a new WAL header
	 * and doing work related to the WAL swap.
	 *
	 * The shm will need to be initialized if this wasn't already done by
	 * either open_entry or SQLite itself. This happens when we open the
	 * database for the first time (so WAL-cur is empty) and then run
	 * add_uncommitted before anything else.
	 *
	 * The new transaction will be placed at the offset given by
	 * `e->wal_cursor`, after possibly resetting this to zero (when the WAL
	 * has been fully backfilled).
	 *
	 * If the new transaction is to be placed at offset zero, we also write
	 * a new WAL header, reset the shared memory (unless it was just
	 * initialized for the first time!), and swap the WALs.
	 */
	PRE(ERGO(e->shm.num_regions == 0, e->wal_cursor == 0));
	struct wal_index_full_hdr *ihdr =
	    e->shm.num_regions > 0 ? &e->shm.regions[0].hdr : NULL;
	uint32_t mx = ihdr != NULL ? ihdr->basic[0].mxFrame : 0;
	uint32_t backfill = ihdr != NULL ? ihdr->nBackfill : 0;
	if (e->wal_cursor == mx && mx == backfill) {
		struct wal_hdr new_wal_hdr;
		if (ihdr != NULL) {
			new_wal_hdr = next_wal_hdr(e);
			shm_restart(&e->shm, new_wal_hdr);
		} else {
			new_wal_hdr = initial_wal_hdr(e->page_size);
			rv = shm_init(&e->shm, new_wal_hdr);
			if (rv != SQLITE_OK) {
				return 1;
			}
		}
		wal_swap(e, &new_wal_hdr);
		rv = e->wal_cur->pMethods->xWrite(e->wal_cur, &new_wal_hdr,
						  sizeof(new_wal_hdr), 0);
		if (rv != SQLITE_OK) {
			return 1;
		}
	}

	uint32_t start = e->wal_cursor;
	struct cksums sums;
	uint32_t db_size;
	if (start > 0) {
		/* There's already a transaction in the WAL. In this case
		 * we initialize the rolling checksum and database size
		 * calculation from the header of the last (commit) frame in
		 * this transaction. */
		/* TODO cache this in the entry? */
		struct wal_frame_hdr prev_fhdr;
		sqlite3_int64 off =
		    wal_offset_from_cursor(e->page_size, e->wal_cursor - 1);
		rv = e->wal_cur->pMethods->xRead(e->wal_cur, &prev_fhdr,
						 sizeof(prev_fhdr), off);
		if (rv != SQLITE_OK) {
			return 1;
		}
		sums.cksum1 = ByteGetBe32(prev_fhdr.cksum1);
		sums.cksum2 = ByteGetBe32(prev_fhdr.cksum2);
		db_size = ByteGetBe32(prev_fhdr.commit);
	} else {
		/* This is the first transaction in this WAL. In this case
		 * we initialize the rolling checksum from the checksum in
		 * the WAL header, and read the actual database file to
		 * initialize the running database size. */
		sums.cksum1 = ByteGetBe32(e->wal_cur_hdr.cksum1);
		sums.cksum2 = ByteGetBe32(e->wal_cur_hdr.cksum2);
		/* The database size in pages is kept in a field of the database
		 * header. */
		uint8_t b[DB_FILE_HEADER_SIZE];
		rv =
		    xfile->orig->pMethods->xRead(xfile->orig, &b, sizeof(b), 0);
		/* TODO(cole) this can't fail provided that the main file
		 * has been created; ensure that this is the case even if
		 * we haven't run a checkpoint yet. */
		assert(rv == SQLITE_OK);
		db_size = ByteGetBe32(b + DB_FILE_HEADER_NPAGES_OFFSET);
	}
	POST(db_size > 0);

	/* Record the new frame in the appropriate page number array in the WAL
	 * index. Note that this doesn't make the frame visible to readers: that
	 * only happens once it is also recorded in the WAL-index hash array and
	 * mxFrame exceeds the frame index. The hash array is updated only when
	 * we increase mxFrame, so that we don't have to deal with the added
	 * complexity of removing things from the hash table when frames are
	 * overwritten before being committed. Updating the page number array
	 * "early" like this is harmless and saves us from having to stash the
	 * page numbers somewhere else in memory in between add_uncommitted and
	 * apply, or (worse) read them back from WAL-cur. */
	shm_add_pgno(&e->shm, e->wal_cursor, (uint32_t)frames[0].page_number);

	uint32_t commit = len == 1 ? db_size : 0;
	struct wal_frame_hdr fhdr = txn_frame_hdr(e, sums, &frames[0], commit);
	rv = write_one_frame(e, fhdr, frames[0].data);
	if (rv != SQLITE_OK) {
		return 1;
	}

	for (unsigned i = 1; i < len; i++) {
		PRE(e->wal_cursor < SHM_SHORT_PGNOS_LEN);
		shm_add_pgno(&e->shm, e->wal_cursor,
			     (uint32_t)frames[i].page_number);
		sums.cksum1 = ByteGetBe32(fhdr.cksum1);
		sums.cksum2 = ByteGetBe32(fhdr.cksum2);
		commit = i == len - 1 ? db_size : 0;
		fhdr = txn_frame_hdr(e, sums, &frames[i], commit);
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
	for (unsigned i = 1; i < WAL_NREADER; i++) {
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

void vfs2_ut_sm_relate(sqlite3_file *orig, sqlite3_file *targ)
{
	struct file *forig = (struct file *)orig;
	PRE(forig->flags & SQLITE_OPEN_MAIN_DB);
	struct file *ftarg = (struct file *)targ;
	PRE(ftarg->flags & SQLITE_OPEN_MAIN_DB);
	sm_relate(&forig->entry->wtx_sm, &ftarg->entry->wtx_sm);
}

void vfs2_ut_make_wal_hdr(uint8_t *buf,
			  uint32_t page_size,
			  uint32_t ckpoint_seqno,
			  uint32_t salt1,
			  uint32_t salt2)
{
	struct wal_hdr hdr =
	    make_wal_hdr(is_bigendian() ? BE_MAGIC : LE_MAGIC, page_size,
			 ckpoint_seqno, make_salts(salt1, salt2));
	memcpy(buf, &hdr, sizeof(hdr));
}
