#ifndef _GNU_SOURCE
# define _GNU_SOURCE
#endif

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <threads.h>
#include <unistd.h>

#include <sqlite3.h>

#include "../include/dqlite.h"

#include "lib/byte.h"

#include "format.h"
#include "tracing.h"
#include "vfs.h"

/* tinycc doesn't have this builtin, nor the warning that it's meant to silence.
 */
#ifdef __TINYC__
#define __builtin_assume_aligned(x, y) x
#endif

/* Byte order */
#if defined(DQLITE_LITTLE_ENDIAN)
#define VFS__BIGENDIAN 0
#elif defined(DQLITE_BIG_ENDIAN)
#define VFS__BIGENDIAN 1
#else
const int vfsOne = 1;
#define VFS__BIGENDIAN (*(char *)(&vfsOne) == 0)
#endif

/* Maximum pathname length supported by this VFS. */
#define VFS__MAX_PATHNAME 512

/* WAL magic value. Either this value, or the same value with the least
 * significant bit also set (FORMAT__WAL_MAGIC | 0x00000001) is stored in 32-bit
 * big-endian format in the first 4 bytes of a WAL file.
 *
 * If the LSB is set, then the checksums for each frame within the WAL file are
 * calculated by treating all data as an array of 32-bit big-endian
 * words. Otherwise, they are calculated by interpreting all data as 32-bit
 * little-endian words. */
#define VFS__WAL_MAGIC 0x377f0682

/* WAL format version (same for WAL index). */
#define VFS__WAL_VERSION 3007000

/* Index of the write lock in the WAL-index header locks area. */
#define VFS__WAL_WRITE_LOCK 0
#define VFS__WAL_CKPT_LOCK  1
#define VFS__WAL_RECOVER_LOCK  2
#define VFS__WAL_READ_LOCK(I) (3+(I))

/* Write ahead log header size. */
#define VFS__WAL_HEADER_SIZE 32

/* Write ahead log frame header size. */
#define VFS__FRAME_HEADER_SIZE 24

/* Size of the first part of the WAL index header. */
#define VFS__WAL_INDEX_HEADER_SIZE 48

/* Size of a single memory-mapped WAL index region. */
#define VFS__WAL_INDEX_REGION_SIZE 32768

/* Offset of the "in header database size" field in the main database file. */
#define VFS__IN_HEADER_DATABASE_SIZE_OFFSET 28


/******************************************************************************/
/*                                   Helpers                                  */
/******************************************************************************/

#define vfsFrameSize(PAGE_SIZE) (VFS__FRAME_HEADER_SIZE + PAGE_SIZE)

/*
 * Return a size that can be used to allocate at least one of 32KiB shm regions,
 * assuming that each mapping must be an integer multiple of the current system
 * page-size.
 *
 * Usually, this is 32KiB. The exception seems to be systems that are configured
 * to use 64KB pages like POWER architecture, where the default is 64KiB - in
 * this case each mapping must cover at least two regions or the mmap call will
 * fail.
 *
 * See also https://github.com/sqlite/sqlite/blob/eb8089e/src/os_unix.c#L4546
 */
static size_t vfsGetMapSize(void)
{
	const int os_page_size = (int)sysconf(_SC_PAGESIZE);
	/* Page size must be a power of 2 */
	PRE(((os_page_size - 1) & os_page_size) == 0);

	if (os_page_size < VFS__WAL_INDEX_REGION_SIZE) {
		PRE((VFS__WAL_INDEX_REGION_SIZE % os_page_size) == 0);
		return VFS__WAL_INDEX_REGION_SIZE;
	} else {
		PRE((os_page_size % VFS__WAL_INDEX_REGION_SIZE) == 0);
		return (size_t)os_page_size;
	}
}

/*
 * Generate or extend an 8 byte checksum based on the data in array data[] and
 * the initial values of in[0] and in[1] (or initial values of 0 and 0 if
 * in==NULL).
 *
 * The checksum is written back into out[] before returning.
 *
 * n must be a positive multiple of 8. */
static void vfsChecksum(
    uint8_t *data, /* Content to be checksummed */
    unsigned n,    /* Bytes of content in a[].  Must be a multiple of 8. */
    const uint32_t in[2], /* Initial checksum value input */
    uint32_t out[2]       /* OUT: Final checksum value output */
)
{
	assert((((uintptr_t)data) % sizeof(uint32_t)) == 0);

	uint32_t s1, s2;
	uint32_t *cur =
	    (uint32_t *)__builtin_assume_aligned(data, sizeof(uint32_t));
	uint32_t *end =
	    (uint32_t *)__builtin_assume_aligned(&data[n], sizeof(uint32_t));

	if (in) {
		s1 = in[0];
		s2 = in[1];
	} else {
		s1 = s2 = 0;
	}

	assert(n >= 8);
	assert((n & 0x00000007) == 0);
	assert(n <= 65536);

	do {
		s1 += *cur++ + s2;
		s2 += *cur++ + s1;
	} while (cur < end);

	out[0] = s1;
	out[1] = s2;
}

/* Return the page number field stored in the header of the given frame. */
#define vfsFrameGetPageNumber(f) ByteGetBe32(&((f)->header[0]))

/* Return the database size field stored in the header of the given frame. */
#define vfsFrameGetDatabaseSize(f) ByteGetBe32(&((f)->header[4]))

/* Return the checksum-1 field stored in the header of the given frame. */
#define vfsFrameGetChecksum1(f) ByteGetBe32(&((f)->header[16]))

/* Return the checksum-2 field stored in the header of the given frame. */
#define vfsFrameGetChecksum2(f) ByteGetBe32(&((f)->header[20]))

/* Return the salt-1 field stored in the WAL header.*/
#define vfsWalGetSalt1(w) *(uint32_t *)__builtin_assume_aligned(&(w)->hdr[16], sizeof(uint32_t))

/* Return the salt-2 field stored in the WAL header.*/
#define vfsWalGetSalt2(w) *(uint32_t *)__builtin_assume_aligned(&(w)->hdr[20], sizeof(uint32_t))

/* Return the checksum-1 field stored in the WAL header.*/
#define vfsWalGetChecksum1(w) ByteGetBe32(&(w)->hdr[24])

/* Return the checksum-2 field stored in the WAL header.*/
#define vfsWalGetChecksum2(w) ByteGetBe32(&(w)->hdr[28])

/* Parse the page size ("Must be a power of two between 512 and 32768
 * inclusive, or the value 1 representing a page size of 65536").
 *
 * Return 0 if the page size is out of bound. */
static uint32_t vfsParsePageSize(uint32_t page_size)
{
	if (page_size == 1) {
		page_size = FORMAT__PAGE_SIZE_MAX;
	} else if (page_size < FORMAT__PAGE_SIZE_MIN) {
		page_size = 0;
	} else if (page_size > (FORMAT__PAGE_SIZE_MAX / 2)) {
		page_size = 0;
	} else if (((page_size - 1) & page_size) != 0) {
		page_size = 0;
	}

	return page_size;
}

static bool vfsFilenameEndsWith(const char *filename, const char *suffix)
{
	size_t n_filename = strlen(filename);
	size_t n_suffix = strlen(suffix);
	if (n_suffix > n_filename) {
		return false;
	}
	return strncmp(filename + n_filename - n_suffix, suffix, n_suffix) == 0;
}

/******************************************************************************/
/*                            Main data structures                            */
/******************************************************************************/

/* Hold the content of a single WAL frame. */
struct vfsFrame
{
	uint8_t header[VFS__FRAME_HEADER_SIZE];
	uint8_t *page; /* Content of the page. */
};

/* Create a new frame of a WAL file. */
static struct vfsFrame *vfsFrameCreate(unsigned size)
{
	struct vfsFrame *f;

	assert(size > 0);

	f = sqlite3_malloc(sizeof *f);
	if (f == NULL) {
		goto oom;
	}

	f->page = sqlite3_malloc64(size);
	if (f->page == NULL) {
		goto oom_after_page_alloc;
	}

	memset(f->header, 0, FORMAT__WAL_FRAME_HDR_SIZE);
	memset(f->page, 0, (size_t)size);

	return f;

oom_after_page_alloc:
	sqlite3_free(f);
oom:
	return NULL;
}

/* Fill the header and the content of a WAL frame. The given checksum is the
 * rolling one of all preceeding frames and is updated by this function. */
static void vfsFrameFill(struct vfsFrame *f,
			 uint32_t page_number,
			 uint32_t database_size,
			 uint32_t salt[2],
			 uint32_t checksum[2],
			 uint8_t *page,
			 uint32_t page_size)
{
	BytePutBe32(page_number, &f->header[0]);
	BytePutBe32(database_size, &f->header[4]);

	vfsChecksum(f->header, 8, checksum, checksum);
	vfsChecksum(page, page_size, checksum, checksum);

	memcpy(&f->header[8], &salt[0], sizeof salt[0]);
	memcpy(&f->header[12], &salt[1], sizeof salt[1]);

	BytePutBe32(checksum[0], &f->header[16]);
	BytePutBe32(checksum[1], &f->header[20]);

	memcpy(f->page, page, page_size);
}

/* Destroy a WAL frame */
static void vfsFrameDestroy(struct vfsFrame *f)
{
	assert(f != NULL);
	assert(f->page != NULL);

	sqlite3_free(f->page);
	sqlite3_free(f);
}

/* Hold content for a shared memory mapping. */
struct vfsShm {
	mtx_t mtx;  /* Mutex to protect fields below. */
	int fd;     /* Read-only */
	off_t size; /* Total size of the in-memory file */
	/* Lock array. Each of these lock has the following semantics:
	 *  -  0 means unlocked;
	 *  - -1 means exclusive locked;
	 *  - >0 means shared locked and the value is the count of
	 *       shared locks taken.
	 */
	int lock[SQLITE_SHM_NLOCK];
};

/* Initialize the shared memory mapping of a database file. */
static int vfsShmInit(struct vfsShm *s)
{
	int fd = memfd_create("dqlite-shm", 0);
	if (fd < 0) {
		return SQLITE_NOMEM;
	}
	*s = (struct vfsShm){
		.fd = fd,
	};
	return SQLITE_OK;
}

static int vfsShmLock(struct vfsShm *s, int ofst, int n, bool exclusive)
{
	PRE(ofst >= 0 && n > 0 && ofst + n <= SQLITE_SHM_NLOCK);

	mtx_lock(&s->mtx);
	for (int i = ofst; i < ofst + n; i++) {
		if (exclusive && s->lock[i] != 0) {
			mtx_unlock(&s->mtx);
			return SQLITE_BUSY;
		} else if (!exclusive && s->lock[i] < 0) {
			mtx_unlock(&s->mtx);
			return SQLITE_BUSY;
		}
	}

	for (int i = ofst; i < ofst + n; i++) {
		if (exclusive) {
			s->lock[i] = -1;
		} else {
			s->lock[i]++;
		}
	}

	mtx_unlock(&s->mtx);
	return SQLITE_OK;
}

static int vfsShmUnlock(struct vfsShm *s, int ofst, int n, bool exclusive)
{
	PRE(ofst >= 0 && n > 0 && ofst + n <= SQLITE_SHM_NLOCK);

	mtx_lock(&s->mtx);
	for (int i = ofst; i < ofst + n; i++) {
		if (exclusive) {
			PRE(s->lock[i] < 0);
			s->lock[i] = 0;
		} else {
			PRE(s->lock[i] > 0);
			s->lock[i]--;
		}
	}
	mtx_unlock(&s->mtx);
	return SQLITE_OK;
}

/* Release all resources used by a shared memory mapping. */
static void vfsShmClose(struct vfsShm *s)
{
	int rv = close(s->fd);
	if (rv != 0 && errno != EINTR) {
		tracef("closing shared memory failed: %d", errno);
	}
}

/* WAL-specific content.
 * Watch out when changing the members of this struct, see
 * comment in `formatWalChecksumBytes`. */
struct vfsWal
{
	uint8_t hdr[VFS__WAL_HEADER_SIZE]; /* Header. */

	/* Transaction-related frames are private to the connection owning the
	 * WRITE lock. They don't need synchronization as such */
	struct vfsFrame **tx; /* Frames added by a transaction. */
	unsigned n_tx;        /* Number of added frames. */

	mtx_t mtx;                /* Lock for fields below. */
	struct vfsFrame **frames; /* All frames committed. */
	unsigned n_frames;        /* Number of committed frames. */
};

/* Initialize a new WAL object. */
static void vfsWalInit(struct vfsWal *w)
{
	*w = (struct vfsWal){};
	int rv = mtx_init(&w->mtx, mtx_plain);
	assert(rv == 0);
}

/* Lookup a frame from the WAL, returning NULL if it doesn't exist. */
static struct vfsFrame *vfsWalFrameLookup(struct vfsWal *w, unsigned n)
{
	struct vfsFrame *frame;

	assert(w != NULL);
	assert(n > 0);

	mtx_lock(&w->mtx);
	if (n <= w->n_frames) {
		frame = w->frames[n - 1];
	} else {
		frame = NULL;
	}
	mtx_unlock(&w->mtx);

	/* In case of a cache spill, it is possible that SQLite will try to
	 * re-populate the cache reading from the uncommitted part of the WAL. 
	 * Unfortunately, it is not possible to reliably link a WAL file to
	 * its main file and checking this way that this is the connection
	 * holding the write lock. As such, the best we can do is to trust
	 * SQLite. This is also the reason why this code is in an unprotected
	 * section (outside the protection of the mtx). */
	if (frame == NULL && w->n_tx > 0) {
		n -= w->n_frames;
		if (n <= w->n_tx) {
			frame = w->tx[n - 1];
		}
	}

	return frame;
}

/* Get the page size stored in the WAL header. */
static uint32_t vfsWalGetPageSize(struct vfsWal *w)
{
	/* The page size is stored in the 4 bytes starting at 8
	 * (big-endian) */
	return vfsParsePageSize(ByteGetBe32(&w->hdr[8]));
}

/* Return the size of the WAL file in bytes. */
static int64_t vfsWalSize(struct vfsWal *w)
{
	mtx_lock(&w->mtx);
	int64_t size = 0;
	if (w->n_frames > 0) {
		uint32_t page_size;
		page_size = vfsWalGetPageSize(w);
		size += VFS__WAL_HEADER_SIZE;
		size += (int64_t)w->n_frames *
			(int64_t)(FORMAT__WAL_FRAME_HDR_SIZE + page_size);
	}
	mtx_unlock(&w->mtx);
	/* TODO dqlite is limited to a max database size of SIZE_MAX */
	assert(size >= 0 && (uint64_t)size <= SIZE_MAX);
	return size;
}

static void vfsWalTruncate(struct vfsWal *w)
{
	mtx_lock(&w->mtx);
	PRE(w->n_tx == 0);
	PRE(w->tx == NULL);
	for (unsigned i = 0; i < w->n_frames; i++) {
		vfsFrameDestroy(w->frames[i]);
	}
	sqlite3_free(w->frames);

	w->frames = NULL;
	w->n_frames = 0;
	mtx_unlock(&w->mtx);
}

/* Release all memory used by a WAL object. */
static void vfsWalClose(struct vfsWal *w)
{
	for (unsigned i = 0; i < w->n_tx; i++) {
		vfsFrameDestroy(w->tx[i]);
	}
	sqlite3_free(w->tx);
	w->tx = NULL;
	w->n_tx = 0;

	vfsWalTruncate(w);
	mtx_destroy(&w->mtx);
}

/* Database-specific content */
struct vfsDatabase
{
	char *name;         /* Database name. Read only. */
	struct vfsShm shm;  /* Shared memory. */
	struct vfsWal wal;  /* Associated WAL. */
	int open_count; /* Number of (sqlite3) file descriptor opened for this
			database. */

	mtx_t mtx;
	void **pages;       /* All database. */
	unsigned n_pages;   /* Number of pages. */
};

/*
 * Comment copied entirely for sqlite source code, it is safe to assume
 * the value 0x40000000 will never change. dq_sqlite_pending_byte is global
 * to be able to adapt it in the unittest, the value must never be changed.
 *
 * ==BEGIN COPY==
 * The value of the "pending" byte must be 0x40000000 (1 byte past the
 * 1-gibabyte boundary) in a compatible database.  SQLite never uses
 * the database page that contains the pending byte.  It never attempts
 * to read or write that page.  The pending byte page is set aside
 * for use by the VFS layers as space for managing file locks.
 *
 * During testing, it is often desirable to move the pending byte to
 * a different position in the file.  This allows code that has to
 * deal with the pending byte to run on files that are much smaller
 * than 1 GiB.  The sqlite3_test_control() interface can be used to
 * move the pending byte.
 *
 * IMPORTANT:  Changing the pending byte to any value other than
 * 0x40000000 results in an incompatible database file format!
 * Changing the pending byte during operation will result in undefined
 * and incorrect behavior.
 * ==END COPY==
 */
DQLITE_VISIBLE_TO_TESTS unsigned dq_sqlite_pending_byte = 0x40000000;

/* Initialize a new database object. */
static int vfsDatabaseInit(struct vfsDatabase *d, const char *name)
{
	char *dbname = sqlite3_malloc((int)strlen(name) + 1);
	if (dbname == NULL) {
		return SQLITE_NOMEM;
	}
	strcpy(dbname, name);

	*d = (struct vfsDatabase){
		.name = dbname,
	};
	int rv = vfsShmInit(&d->shm);
	if (rv != SQLITE_OK) {
		sqlite3_free(dbname);
		return rv;
	}
	vfsWalInit(&d->wal);
	rv = mtx_init(&d->mtx, mtx_plain);
	assert(rv == 0);
	return SQLITE_OK;
}

/* Get a page from the given database, possibly creating a new one. */
static int vfsDatabaseGetPage(struct vfsDatabase *d,
			      uint32_t page_size,
			      unsigned pgno,
			      void **page)
{
	int rc;

	assert(d != NULL);
	assert(pgno > 0);

	/* SQLite should access pages progressively, without jumping more than
	 * one page after the end unless one would attempt to access a page at
	 * `sqlite_pending_byte` offset, skipping a page is permitted then. */
	bool pending_byte_page_reached =
	    (page_size * d->n_pages == dq_sqlite_pending_byte);
	if ((pgno > d->n_pages + 1) && !pending_byte_page_reached) {
		tracef("page number greater than length (requested %d, last %d)",
		       pgno, d->n_pages);
		*page = NULL;
		return SQLITE_IOERR_WRITE;
	}

	if (pgno <= d->n_pages) {
		/* Return the existing page. */
		assert(d->pages != NULL);
		*page = d->pages[pgno - 1];
		return SQLITE_OK;
	}

	/* Create a new page, grow the page array, and append the
	 * new page to it. */
	*page = sqlite3_malloc64(page_size);
	if (*page == NULL) {
		rc = SQLITE_NOMEM;
		goto err;
	}

	mtx_lock(&d->mtx);
	void **pages = sqlite3_realloc64(d->pages, sizeof *pages * pgno);
	if (pages == NULL) {
		rc = SQLITE_NOMEM;
		goto err_after_vfs_page_create;
	}

	pages[pgno - 1] = *page;

	/* Allocate a page to store the pending_byte */
	if (pending_byte_page_reached) {
		void *pending_byte_page = sqlite3_malloc64(page_size);
		if (pending_byte_page == NULL) {
			rc = SQLITE_NOMEM;
			goto err_after_pending_byte_page;
		}
		pages[d->n_pages] = pending_byte_page;
	}

	/* Update the page array. */
	d->pages = pages;
	d->n_pages = pgno;
	mtx_unlock(&d->mtx);
	return SQLITE_OK;

err_after_pending_byte_page:
	d->pages = pages;

err_after_vfs_page_create:
	mtx_unlock(&d->mtx);
	sqlite3_free(*page);
err:
	*page = NULL;
	return rc;
}

/* Lookup a page from the given database, returning NULL if it doesn't exist. */
static void *vfsDatabasePageLookup(struct vfsDatabase *d, unsigned pgno)
{
	/* == Safety==
	 * See vfsMainFileRead and vfsMainFileWrite for a rationale
	 * on why this logic is unguarded by d->mtx. */
	void *page;

	assert(d != NULL);
	assert(pgno > 0);

	if (pgno > d->n_pages) {
		/* This page hasn't been written yet. */
		return NULL;
	}

	page = d->pages[pgno - 1];

	assert(page != NULL);

	return page;
}

static uint32_t vfsDatabaseGetPageSize(struct vfsDatabase *d)
{
	uint8_t *page;

	mtx_lock(&d->mtx);
	assert(d->n_pages > 0);
	page = d->pages[0];
	mtx_unlock(&d->mtx);

	/* The page size is stored in the 16th and 17th bytes of the first
	 * database page (big-endian) */
	return vfsParsePageSize(ByteGetBe16(&page[16]));
}

/* Return the size of the database file in bytes. */
static int64_t vfsDatabaseFileSize(struct vfsDatabase *d)
{
	mtx_lock(&d->mtx);
	int64_t pages = (int64_t)d->n_pages;
	mtx_unlock(&d->mtx);

	if (pages == 0) {
		return 0;
	}
	return pages * (int64_t)vfsDatabaseGetPageSize(d);
}

/* Truncate a database file to be exactly the given number of pages. */
static int vfsDatabaseTruncate(struct vfsDatabase *d, sqlite_int64 size)
{
	if (d->n_pages == 0) {
		if (size > 0) {
			return SQLITE_IOERR_TRUNCATE;
		}
		return SQLITE_OK;
	}

	/* Since the file size is not zero, some content must
	 * have been written and the page size must be known. */
	uint32_t page_size = vfsDatabaseGetPageSize(d);
	assert(page_size > 0);

	if ((size % page_size) != 0) {
		return SQLITE_IOERR_TRUNCATE;
	}

	unsigned n_pages = (unsigned)(size / page_size);

	/* We expect callers to only invoke us if some actual content has been
	 * written already. */
	assert(d->n_pages > 0);

	/* Truncate should always shrink a file. */
	assert(n_pages <= d->n_pages);
	assert(d->pages != NULL);

	mtx_lock(&d->mtx);
	/* Destroy pages beyond pages_len. */
	void **cursor = d->pages + n_pages;
	for (unsigned i = 0; i < (d->n_pages - n_pages); i++) {
		sqlite3_free(*cursor);
		cursor++;
	}

	/* Shrink the page array, possibly to 0. */
	void **pages = sqlite3_realloc64(d->pages, sizeof *d->pages * n_pages);
	if (pages != NULL || n_pages == 0) {
		/* Failing to shrink will not release memory, but that memory could be
		 * reused later: it is not an error to hold a bigger memory segment than
		 * we need (and likely to happen nevertheless). */
		d->pages = pages;
	}

	/* Update the page count. */
	d->n_pages = n_pages;
	mtx_unlock(&d->mtx);

	return SQLITE_OK;
}

/* Release all memory used by a database object. */
static void vfsDatabaseClose(struct vfsDatabase *d)
{
	for (unsigned i = 0; d->pages != NULL && i < d->n_pages; i++) {
		sqlite3_free(d->pages[i]);
	}
	sqlite3_free(d->pages);
	sqlite3_free(d->name);
	vfsWalClose(&d->wal);
	vfsShmClose(&d->shm);
	mtx_destroy(&d->mtx);
}

/* Custom dqlite VFS. Contains pointers to all databases that were created. */
struct vfs
{
	struct vfsDatabase **databases; /* Database objects */
	unsigned n_databases;           /* Number of databases */
	int error;                      /* Last error occurred. */
	struct sqlite3_vfs *base_vfs; /* Base VFS. */

	void (*delete_hook)(void *, const char*);
	void *delete_hook_data;
};

/* Create a new vfs object. */
static struct vfs *vfsCreate(void)
{
	struct vfs *v;

	v = sqlite3_malloc(sizeof *v);
	if (v == NULL) {
		return NULL;
	}

	*v = (struct vfs) {
		.base_vfs = sqlite3_vfs_find("unix"),
	};
	assert(v->base_vfs != NULL);
	return v;
}

/* Create a database object and add it to the databases array. */
static struct vfsDatabase *vfsCreateDatabase(struct vfs *v, const char *name)
{
	unsigned n = v->n_databases + 1;
	struct vfsDatabase **databases;
	struct vfsDatabase *d;

	assert(name != NULL);

	/* Create a new entry. */
	databases = sqlite3_realloc64(v->databases, sizeof *databases * n);
	if (databases == NULL) {
		return NULL;
	}
	v->databases = databases;

	d = sqlite3_malloc(sizeof *d);
	if (d == NULL) {
		return NULL;
	}

	int rv = vfsDatabaseInit(d, name);
	if (rv != SQLITE_OK) {
		sqlite3_free(d);
		return NULL;
	}

	v->databases[n - 1] = d;
	v->n_databases = n;

	return d;
}

/* Find the database object associated with the given filename. */
static struct vfsDatabase *vfsDatabaseLookup(struct vfs *v,
					     const char *filename)
{
	size_t n = strlen(filename);
	unsigned i;

	assert(v != NULL);
	assert(filename != NULL);

	if (vfsFilenameEndsWith(filename, "-wal")) {
		n -= strlen("-wal");
	}
	if (vfsFilenameEndsWith(filename, "-journal")) {
		n -= strlen("-journal");
	}

	for (i = 0; i < v->n_databases; i++) {
		struct vfsDatabase *database = v->databases[i];
		if (strlen(database->name) == n &&
		    strncmp(database->name, filename, n) == 0) {
			return database;
		}
	}

	return NULL;
}

static int vfsDeleteDatabase(struct vfs *vfs, struct vfsDatabase *database)
{
	tracef("[%s] finalizing database delete", database->name);
	for (unsigned i = 0; i < vfs->n_databases; i++) {
		if (vfs->databases[i] != database) {
			continue;
		}

		if (vfs->delete_hook != NULL) {
			vfs->delete_hook(vfs->delete_hook_data, database->name);
		}
		vfsDatabaseClose(database);
		sqlite3_free(database);

		/* Shift all other contents objects. */
		for (unsigned j = i + 1; j < vfs->n_databases; j++) {
			vfs->databases[j - 1] = vfs->databases[j];
		}
		vfs->n_databases--;

		return SQLITE_OK;
	}

	vfs->error = ENOENT;
	return SQLITE_IOERR_DELETE_NOENT;
}

/* Release the memory used internally by the VFS object.
 *
 * All file content will be de-allocated, so dangling open FDs against
 * those files will be broken.
 */
static void vfsDestroy(struct vfs *r)
{
	for (unsigned i = 0; i < r->n_databases; i++) {
		struct vfsDatabase *database = r->databases[i];
		vfsDatabaseClose(database);
		sqlite3_free(database);
	}
	sqlite3_free(r->databases);
}

/******************************************************************************/
/*                           SQLite3 implementation                           */
/******************************************************************************/

typedef sqlite3_file vfsNoopFile;

static int vfsNoopClose(sqlite3_file *file)
{
	(void)file;
	return SQLITE_OK;
}

static int vfsNoopTruncate(sqlite3_file *file, sqlite3_int64 size)
{
	(void)file;
	(void)size;
	return SQLITE_IOERR_TRUNCATE;
}

static int vfsNoopSync(sqlite3_file *file, int flags)
{
	(void)file;
	(void)flags;
	return SQLITE_IOERR;
}

static int vfsNoopFileSize(sqlite3_file *file, sqlite3_int64 *pSize)
{
	(void)file;
	(void)pSize;
	*pSize = 0;
	return SQLITE_OK;
}

static int vfsNoopLock(sqlite3_file *file, int lockType)
{
	(void)file;
	(void)lockType;
	return SQLITE_OK;
}

static int vfsNoopUnlock(sqlite3_file *file, int lockType)
{
	(void)file;
	(void)lockType;
	return SQLITE_OK;
}

static int vfsNoopFileControl(sqlite3_file *file, int op, void *pArg)
{
	(void)file;
	(void)op;
	(void)pArg;
	return SQLITE_NOTFOUND;
}

static int vfsNoopSectorSize(sqlite3_file *file)
{
	(void)file;
	return 0;
}

static int vfsNoopRead(sqlite3_file *file,
		       void *data,
		       int iAmt,
		       sqlite3_int64 iOfst)
{
	(void)file;
	(void)iOfst;
	memset(data, 0, (size_t)iAmt);
	return SQLITE_OK;
}

static int vfsNoopWrite(sqlite3_file *file,
			const void *data,
			int iAmt,
			sqlite3_int64 iOfst)
{
	(void)file;
	(void)data;
	(void)iAmt;
	(void)iOfst;
	return SQLITE_OK;
}

static int vfsNoopCheckReservedLock(sqlite3_file *file, int *pResOut)
{
	(void)file;
	*pResOut = 0;
	return SQLITE_OK;
}

static int vfsNoopDeviceCharacteristics(sqlite3_file *file)
{
	(void)file;
	return SQLITE_IOCAP_ATOMIC | SQLITE_IOCAP_SAFE_APPEND |
	       SQLITE_IOCAP_SEQUENTIAL | SQLITE_IOCAP_POWERSAFE_OVERWRITE;
}

static const sqlite3_io_methods vfsNoopMethods = {
	.iVersion = 1,
	.xClose = vfsNoopClose,
	.xRead = vfsNoopRead,
	.xWrite = vfsNoopWrite,
	.xTruncate = vfsNoopTruncate,
	.xSync = vfsNoopSync,
	.xFileSize = vfsNoopFileSize,
	.xLock = vfsNoopLock,
	.xUnlock = vfsNoopUnlock,
	.xCheckReservedLock = vfsNoopCheckReservedLock,
	.xFileControl = vfsNoopFileControl,
	.xSectorSize = vfsNoopSectorSize,
	.xDeviceCharacteristics = vfsNoopDeviceCharacteristics,
};

struct vfsWalFile
{
	sqlite3_file base;            /* Base class. Must be first. */
	struct vfsDatabase *database; /* Underlying database file. */
};

static int vfsWalFileRead(sqlite3_file* file, void* buf, int amount, sqlite3_int64 offset)
{
	struct vfsWalFile *f = (struct vfsWalFile *)file;
	/* == Safety ==
	 * This method can be called by any read or write transaction.
	 * The vfsWal datastructure is split in two parts:
	 * - the unpolled transaction (which should be private to the only
	 *   connection holding the WRITE lock)
	 * - the "committed" WAL which can only be updated by VfsApply which
	 *   always runs from libuv thread, which might run concurrently to
	 *   read transactions, in an increase-only fashion. As such, it needs
	 *   a mutex guard as resizing the array might invalidate the pointer.
	 *   Last, truncation can happen only while holding all locks exclusively
	 *   and as such it can never happen concurrently on this mehtods (so the
	 *   increase-only invariant holds for this method).
	 */

	uint32_t page_size;
	unsigned index;
	struct vfsFrame *frame;

	if (offset == 0) {
		/* Read the header. */
		assert(amount == VFS__WAL_HEADER_SIZE);
		memcpy(buf, f->database->wal.hdr, VFS__WAL_HEADER_SIZE);
		return SQLITE_OK;
	}

	page_size = vfsWalGetPageSize(&f->database->wal);
	assert(page_size > 0);

	/* For any other frame, we expect either a header read,
	 * a checksum read, a page read or a full frame read. */
	if (amount == FORMAT__WAL_FRAME_HDR_SIZE) {
		assert(((offset - VFS__WAL_HEADER_SIZE) %
			((int)page_size + FORMAT__WAL_FRAME_HDR_SIZE)) == 0);
		index =
		    (unsigned)formatWalCalcFrameIndex((int)page_size, offset);
	} else if (amount == sizeof(uint32_t) * 2) {
		if (offset == FORMAT__WAL_FRAME_HDR_SIZE) {
			/* Read the checksum from the WAL
			 * header. */
			memcpy(buf, f->database->wal.hdr + offset, (size_t)amount);
			return SQLITE_OK;
		}
		assert(((offset - 16 - VFS__WAL_HEADER_SIZE) %
			((int)page_size + FORMAT__WAL_FRAME_HDR_SIZE)) == 0);
		index =
		    (unsigned)((offset - 16 - VFS__WAL_HEADER_SIZE) /
			       ((int)page_size + FORMAT__WAL_FRAME_HDR_SIZE)) +
		    1;
	} else if (amount == (int)page_size) {
		assert(((offset - VFS__WAL_HEADER_SIZE -
			 FORMAT__WAL_FRAME_HDR_SIZE) %
			((int)page_size + FORMAT__WAL_FRAME_HDR_SIZE)) == 0);
		index =
		    (unsigned)formatWalCalcFrameIndex((int)page_size, offset);
	} else {
		assert(amount == (FORMAT__WAL_FRAME_HDR_SIZE + (int)page_size));
		index =
		    (unsigned)formatWalCalcFrameIndex((int)page_size, offset);
	}

	if (index == 0) {
		/* From SQLite docs:
		*
		*   If xRead() returns SQLITE_IOERR_SHORT_READ it must also fill
		*   in the unread portions of the buffer with zeros.  A VFS that
		*   fails to zero-fill short reads might seem to work.  However,
		*   failure to zero-fill short reads will eventually lead to
		*   database corruption.
		*/
		memset(buf, 0, (size_t)amount);
		return SQLITE_IOERR_SHORT_READ;
	}

	/* If ever really needed, there exists lock-free algorithms. I think
	 * that here contention will always be very small the only thing that
	 * can be contended is the access to the wal page pointer and not
	 * on the content: it is guaranteed by the sqlite3 WAL protocol that
	 * the WAL will not be restarted or checkpointed if a READ lock is
	 * held, which is always true when reading the WAL. */
	frame = vfsWalFrameLookup(&f->database->wal, index);

	if (frame == NULL) {
		/* From SQLite docs:
		*
		*   If xRead() returns SQLITE_IOERR_SHORT_READ it must also fill
		*   in the unread portions of the buffer with zeros.  A VFS that
		*   fails to zero-fill short reads might seem to work.  However,
		*   failure to zero-fill short reads will eventually lead to
		*   database corruption.
		*/
		memset(buf, 0, (size_t)amount);
		return SQLITE_IOERR_SHORT_READ;
	}

	if (amount == FORMAT__WAL_FRAME_HDR_SIZE) {
		memcpy(buf, frame->header, (size_t)amount);
	} else if (amount == sizeof(uint32_t) * 2) {
		memcpy(buf, frame->header + 16, (size_t)amount);
	} else if (amount == (int)page_size) {
		memcpy(buf, frame->page, (size_t)amount);
	} else {
		memcpy(buf, frame->header, FORMAT__WAL_FRAME_HDR_SIZE);
		memcpy(buf + FORMAT__WAL_FRAME_HDR_SIZE, frame->page,
		       page_size);
	}

	return SQLITE_OK;
}

static int vfsWalFileWrite(sqlite3_file* file, const void* buf, int amount, sqlite3_int64 offset)
{
	struct vfsWalFile *f = (struct vfsWalFile *)file;
	/* == Safety ==
	 * See vfsWalFileRead.
	 */
	uint32_t page_size;
	unsigned index;
	struct vfsFrame *frame;

	/* WAL header. */
	if (offset == 0) {
		/* We expect the data to contain exactly 32
		 * bytes. */
		assert(amount == VFS__WAL_HEADER_SIZE);

		memcpy(f->database->wal.hdr, buf, (size_t)amount);
		return SQLITE_OK;
	}

	/* There is no need to lock here as only one writer is allowed to
	 * ever change the WAL and this routine is that writer. */
	page_size = vfsWalGetPageSize(&f->database->wal);
	assert(page_size > 0);

	/* It is expected that once committed the WAL cannot be changed. 
	 * As such, its index must be above the already committed part */
	/* FIXME: this should likely be called "pageno" as it's 1-based. */
	index = (unsigned)formatWalCalcFrameIndex((int)page_size, offset);
	if (index <= f->database->wal.n_frames) {
		return SQLITE_IOERR_WRITE;
	}

	unsigned tx_index = index - f->database->wal.n_frames;
	if (tx_index > f->database->wal.n_tx) {
		/* SQLite should access frames progressively, without jumping more than
		 * one page after the end. */
		unsigned new_n_tx = f->database->wal.n_tx + 1;
		assert(tx_index == new_n_tx);
		/* Also, new frames always start by writing the header first */
		assert(amount == FORMAT__WAL_FRAME_HDR_SIZE);

		frame = vfsFrameCreate(page_size);
		if (frame == NULL) {
			return SQLITE_NOMEM;
		}
		/* TODO: use a capacity so that we don't reallocate for every frame */
		struct vfsFrame **new_tx = sqlite3_realloc(f->database->wal.tx, (int)(sizeof(*new_tx) * new_n_tx));
		if (new_tx == NULL) {
			return SQLITE_NOMEM;
		}
		new_tx[f->database->wal.n_tx] = frame;
		f->database->wal.tx = new_tx;
		f->database->wal.n_tx = new_n_tx;
	}

	frame = f->database->wal.tx[tx_index - 1];
	assert(frame != NULL);

	/* This is a WAL frame write. We expect either a frame
	 * header or page write. */
	if (amount == FORMAT__WAL_FRAME_HDR_SIZE) {
		/* Frame header write. */
		assert(((offset - VFS__WAL_HEADER_SIZE) %
			((int)page_size + FORMAT__WAL_FRAME_HDR_SIZE)) == 0);
		memcpy(frame->header, buf, FORMAT__WAL_FRAME_HDR_SIZE);
	} else {
		/* Frame page write. */
		assert(amount == (int)page_size);
		assert(((offset - VFS__WAL_HEADER_SIZE -
			 FORMAT__WAL_FRAME_HDR_SIZE) %
			((int)page_size + FORMAT__WAL_FRAME_HDR_SIZE)) == 0);
		memcpy(frame->page, buf, (size_t)amount);
	}

	return SQLITE_OK;
}

static int vfsWalFileTruncate(sqlite3_file* file, sqlite3_int64 size)
{
	struct vfsWalFile *f = (struct vfsWalFile *)file;

	/* We expect SQLite to only truncate to zero, after a
	 * full checkpoint. */
	if (size != 0) {
		return SQLITE_PROTOCOL;
	}

	vfsWalTruncate(&f->database->wal);
	return SQLITE_OK;
}

static int vfsWalFileSize(sqlite3_file *file, sqlite3_int64 *pSize)
{
	struct vfsWalFile *f = (struct vfsWalFile *)file;
	*pSize = (sqlite3_int64)vfsWalSize(&f->database->wal);
	return SQLITE_OK;
}

static const sqlite3_io_methods vfsWalFileMethods = {
	.iVersion = 1,
	.xClose = vfsNoopClose,
	.xRead = vfsWalFileRead,
	.xWrite = vfsWalFileWrite,
	.xTruncate = vfsWalFileTruncate,
	.xSync = vfsNoopSync,
	.xFileSize = vfsWalFileSize,
	.xLock = vfsNoopLock,
	.xUnlock = vfsNoopUnlock,
	.xCheckReservedLock = vfsNoopCheckReservedLock,
	.xFileControl = vfsNoopFileControl,
	.xSectorSize = vfsNoopSectorSize,
	.xDeviceCharacteristics = vfsNoopDeviceCharacteristics,
};

#define VFS__CHECKPOINT_MASK 0xff

enum vfsMainFileState {
	NORMAL = 0,

	/* The connection was polled. Marking the connection in the leader
	 * allows us to ensure that the connection that is polled is the one
	 * applied or aborted.*/
	POLLED,

	/* A deletion was requested. This will create a transaction that will be
	 * distributed as a normal transaction and will force all replicas to
	 * delete the database at the first chance. */
	DELETING,
};

/* Implementation of the abstract sqlite3_file base class.
 * for the main database file */
struct vfsMainFile {
	sqlite3_file base;            /* Base class. Must be first. */
	struct vfs *vfs;              /* Pointer to volatile VFS data. */
	struct vfsDatabase *database; /* Underlying database content. */
	enum vfsMainFileState state;  /* The current connection status. */
	uint16_t sharedMask; /* Mask of shared locks held */
	uint16_t exclMask;   /* Mask of exclusive locks held. The special value
		     `VFS__CHECKPOINT_MASK` is used during a
		     checkpoint. See VfsCheckpoint for details. */
	struct {
		void **ptr;
		int len, cap;
	} mappedShmRegions;
};

static uint32_t vfsDatabaseNumPages(struct vfsDatabase *database, bool use_wal);

static int vfsMainFileClose(sqlite3_file *file)
{
	/* == Safety==
	 * This method can only run in the libuv loop thread. This means that in
	 * the meanwhile no new connection can happen and no
	 * VfsPoll/VfsAbort/VfsCheckpoint/VfsApply can happen. */
	struct vfsMainFile *f = (struct vfsMainFile *)file;

	PRE(f->database->open_count > 0);
	f->database->open_count--;
	if (f->database->open_count > 0) {
		/* Some other connection is still open, no need to run any
		 * finalizer here. */
		return SQLITE_OK;
	}

	PRE(f->database->shm.lock[VFS__WAL_WRITE_LOCK] == 0);
	PRE(f->database->wal.n_tx == 0);

	/* This was the last open connection for this database. It is
	 * possible now to run finalizers for this database. */
	if (vfsDatabaseNumPages(f->database, true) != 1) {
		/* A deleted database only has 1 page. */
		return SQLITE_OK;
	}

	/* A deleted database has the in-header size set to 0. */
	uint8_t *header;
	if (f->database->wal.n_frames == 0) {
		assert(f->database->n_pages > 0);
		header = f->database->pages[0];
	} else {
		struct vfsFrame *frame =
		    f->database->wal.frames[f->database->wal.n_frames - 1];
		PRE(vfsFrameGetPageNumber(frame) == 1);
		header = frame->page;
	}

	/* If the in-header database size is not 0, then the database is still
	 * alive and it must not be removed */
	uint32_t in_header_size = ByteGetBe32(&header[VFS__IN_HEADER_DATABASE_SIZE_OFFSET]);
	if (in_header_size != 0) {
		return SQLITE_OK;
	}

	int rv = vfsDeleteDatabase(f->vfs, f->database);
	assert(rv == SQLITE_OK);
	return SQLITE_OK;
}

static int vfsMainFileRead(sqlite3_file *file,
			   void *buf,
			   int amount,
			   sqlite_int64 offset)
{
	struct vfsMainFile *f = (struct vfsMainFile *)file;

	/* == Safety==
	 * It is expected for this call to be done in two different cases:
	 * - when the database is empty, while holding an EXCLUSIVE DELETE-mode
	 *   lock on the file, during the transition from DELETE-mode to
	 *   WAL-mode; in that case, no synchronization is necessary as there
	 *   can only ever be one connection open (on the libuv thread).
	 * - when holding a shared memory read-lock; in this case, the guarantee
	 *   we get from SQLite is that pages accessed are never changed. While
	 *   for a file that is enough, it is not enough for an array in memory
	 *   that might be resized (as in realloc). So, while the content of
	 *   each page in f->database->pages can be considered constant, the
	 *   array holding each page might change. The resizing of this array
	 *   and/or the overwriting of unread pages is always done during a
	 *   checkpoint, holding the checkpoint lock. On top of these guarantees
	 *   from SQLite, dqlite adds a more strict behaviour: only TRUNCATE
	 *   checkpoint are allowed and they always block all readers (i.e. they
	 *   hold all locks exclusively). This means that during this call it is
	 *   not necessary to synchronize at all.
	 *
	 * Regarding reading values from memory shared across multiple threads,
	 * it is guaranteed that after all checkpoints, a full memory barrier is
	 * issued from the checkpoint thread (the libuv main thread). A full
	 * memory barrier is also issued by the thead the transaction runs in
	 * when starting a read transaction. This creates a thread-fence chain
	 * that guarantees that no stale data is read after a checkpoint by read
	 * transactions.
	 */

	int page_size;
	unsigned pgno;
	const char *page;

	if (f->database->n_pages == 0) {
		/* From SQLite docs:
		*
		*   If xRead() returns SQLITE_IOERR_SHORT_READ it must also fill
		*   in the unread portions of the buffer with zeros.  A VFS that
		*   fails to zero-fill short reads might seem to work.  However,
		*   failure to zero-fill short reads will eventually lead to
		*   database corruption.
		*/
		memset(buf, 0, (size_t)amount);
		return SQLITE_IOERR_SHORT_READ;
	}

	/* If the main database file is not empty, we expect the
	 * page size to have been set by an initial write. */
	uint32_t page_size_u32 = vfsDatabaseGetPageSize(f->database);
	assert(page_size_u32 > 0 && page_size_u32 <= INT_MAX);
	page_size = (int)page_size_u32;

	if (offset < page_size) {
		/* Reading from page 1. We expect the read to be
		 * at most page_size bytes. */
		assert(amount <= page_size);
		pgno = 1;
	} else {
		/* For pages greater than 1, we expect an offset
		 * that starts exactly at a page boundary. The read
		 * size can be less than a full page when SQLite
		 * is compiled with SQLITE_DIRECT_OVERFLOW_READ
		 * (enabled by default since 3.45.1). */
		assert(amount <= page_size);

		assert((offset % page_size) == 0);
		pgno = (unsigned)(offset / page_size) + 1;
	}

	assert(pgno > 0);
	page = vfsDatabasePageLookup(f->database, pgno);

	if (page == NULL) {
		/* From SQLite docs:
		 *
		 *   If xRead() returns SQLITE_IOERR_SHORT_READ it must also
		 *   fill in the unread portions of the buffer with zeros.  A VFS
		 *   that fails to zero-fill short reads might seem to work.
		 *   However, failure to zero-fill short reads will eventually
		 *   lead to database corruption.
		 */
		memset(buf, 0, (size_t)amount);
		return SQLITE_IOERR_SHORT_READ;
	}

	memcpy(buf, pgno == 1 ? page + offset : page, (size_t)amount);
	return SQLITE_OK;
}

static int vfsMainFileWrite(sqlite3_file *file,
			    const void *buf,
			    int amount,
			    sqlite_int64 offset)
{
	struct vfsMainFile *f = (struct vfsMainFile *)file;

	/* == Safety==
	 * It is expected for this call to be done in two different cases:
	 * - when the database is empty, while holding an EXCLUSIVE DELETE-mode
	 *   lock on the file, during the transition from DELETE-mode to
	 *   WAL-mode; in that case, no synchronization is necessary as there
	 *   can only ever be one connection open (on the libuv thread).
	 * - when holding all locks exclusively (see VfsCheckpoint). In this case,
	 *   no readers are allowed and no other operation on the database is allowed.
	 *   Moreover, this logic will always run in the libuv thread.
	 *
	 * The above reasoning would look like synchronization through mutexes is not
	 * strictly needed here, however that is only partially true: SQLite might 
	 * use the `xFileSize` (vfsMainFileSize) to check for file existance without
	 * taking any lock first (i.e. the only locking guarantee is on read and write).
	 * As such, the code below will still use a (likely uncontended) mutex to guard
	 * resizing of the page array. See `vfsDatabaseGetPage` and `vfsDatabaseTruncate`.
	 */
	assert(buf != NULL);
	assert(amount > 0);

	unsigned pgno;
	uint32_t page_size;
	void *page;

	if (offset == 0) {
		const uint8_t *header = buf;

		/* This is the first database page. We expect
		 * the data to contain at least the header. */
		assert(amount >= FORMAT__DB_HDR_SIZE);

		/* Extract the page size from the header. */
		page_size = vfsParsePageSize(ByteGetBe16(&header[16]));
		if (page_size == 0) {
			return SQLITE_CORRUPT;
		}

		pgno = 1;
	} else {
		page_size = vfsDatabaseGetPageSize(f->database);

		/* The header must have been written and the page size set. */
		assert(page_size > 0);

		/* For pages beyond the first we expect offset to be a multiple
		 * of the page size. */
		assert((offset % (int)page_size) == 0);

		/* We expect that SQLite writes a page at time. */
		assert(amount == (int)page_size);

		pgno = ((unsigned)(offset / (int)page_size)) + 1;
	}

	int rv = vfsDatabaseGetPage(f->database, page_size, pgno, &page);
	if (rv != SQLITE_OK) {
		return rv;
	}

	assert(page != NULL);
	memcpy(page, buf, (size_t)amount);
	return SQLITE_OK;
}

static int vfsMainFileTruncate(sqlite3_file *file, sqlite_int64 size)
{
	struct vfsMainFile *f = (struct vfsMainFile *)file;
	/* == Safety==
	 * See vfsMainFileWrite.
	 */

	int rv = vfsDatabaseTruncate(f->database, size);
	return rv;
}

static int vfsMainFileSize(sqlite3_file *file, sqlite_int64 *size)
{
	struct vfsMainFile *f = (struct vfsMainFile *)file;
	/* == Safety==
	 * See vfsMainFileWrite.
	 */
	*size = vfsDatabaseFileSize(f->database);
	return SQLITE_OK;
}

static int vfsWalAppend(struct vfsDatabase *d,
	const struct vfsTransaction *transaction);

/* Forges a header for deleteion. The starting point is the database
 * header and not the WAL header. The reasoning is that most of the fields
 * will be reset to 0 or to the default value, with the exception of:
 * - The header string;
 * - The database page size in bytes;
 * - ThefFile format read/write version;
 * - The maximum embedded payload fraction;
 * - The minimum embedded payload fraction;
 * - The leaf payload fraction;
 * - The file change counter;
 * - The database text encoding;
 * - The version-valid-for number and the version number.
 *
 * The file change counter can be kept still as it is only updated during a
 * checkpoint (so it should match the value of any page-1 frame in the
 * WAL).
 * The schema cookie must be updated to make sure statements are
 * reprepared. To achive this, this method takes a worst-case approach:
 * the worst case is that all frames in the WAL are schema changes
 * that only touch page 1, as such the schema cookie can only be incremented
 * that number of times. */
static void vfsResetDatabaseFileHeader(uint8_t *header, uint32_t max_transaction_count)
{
	static const uint8_t le_one[] = { 1, 0, 0, 0 };

	/* See https://sqlite.org/fileformat2.html for details. */

	/* Set the file size to 0 */
	const size_t file_size_offset = 28;
	memset(header + file_size_offset, 0, 4);

	/* Set freelist head to 0 */
	const size_t freelist_head_offset = 32;
	memset(header + freelist_head_offset, 0, 4);

	/* Set the freelist count to 0 */
	const size_t freelist_count_offset = 36;
	memset(header + freelist_count_offset, 0, 4);

	/* To achive this logic takes a worst-case approach: the worst case is
	 * that all frames in the WAL are schema changes that only touch page 1,
	 * as such the schema cookie can only be incremented at most that number
	 * of times. */
	const size_t schema_cookie_offset = 40;
	const uint32_t old_schema_cookie =
	    ByteGetBe32(header + schema_cookie_offset);
	BytePutBe32(old_schema_cookie + max_transaction_count + 1,
		    header + schema_cookie_offset);

	/* Set default page cache to 0 */
	const size_t default_page_cache_size_offset = 48;
	memset(header + default_page_cache_size_offset, 0, 4);

	/* Set autovacuum root pointer to 0 */
	const size_t autovacuum_root_pointer_offset = 52;
	memset(header + autovacuum_root_pointer_offset, 0, 4);

	/* Set user_version to 0 */	
	const size_t user_version_offset = 60;
	memset(header + user_version_offset, 0, 4);

	/* Switch off autovacuum mode */
	const size_t autovacuum_mode_offset = 64;
	memset(header + autovacuum_mode_offset, 0, 4);

	/* Set application id to 0 */
	const size_t application_id_offset = 68;
	memset(header + application_id_offset, 0, 4);

	/* Copy the change counter to version-valid-for number. */
	const size_t version_valid_for_number_offset = 92;
	memcpy(header + version_valid_for_number_offset, le_one, 4); 
}

static int vfsPragmaDeleteDatabase(struct vfsMainFile *f, char **fcntl)
{
	if (fcntl[2]) {
		/* Support only `PRAGMA delete_database` syntax */
		return SQLITE_NOTFOUND;
	}

	/* To correctly sequence this transaction with other ones, the
	 * connection must already hold the write lock */
	if ((f->exclMask & (1 << VFS__WAL_WRITE_LOCK)) == 0) {
		fcntl[0] = sqlite3_mprintf(
		    "PRAGMA delete_database must be run in a write "
		    "transaction. Use BEGIN IMMEDIATE to start one.");
		return SQLITE_ERROR;
	}

	/* The transaction must not contain any other change to the database. 
	 * As such, it will be necessary for the client to perform precisely
	 * this sequence of commands:
	 *   BEGIN IMMEDIATE;
	 *   PRAGMA delete_database;
	 *   COMMIT;
	 */
	if (f->database->wal.n_tx != 0) {
		/* This check is weak as this will only be true if a cache spill
		 * occurs. A more strict check about the current transaction is done
		 * in vfsFinalizeTransaction. */
		fcntl[0] = sqlite3_mprintf(
		    "PRAGMA delete_database must be the only statement run in "
		    "the transaction.");
		return SQLITE_ERROR;
	}

	PRE(f->state == NORMAL);
	f->state = DELETING;

	return SQLITE_OK;
}

static int vfsPragmaPageSize(struct vfsMainFile *f, char **fcntl)
{
	if (fcntl[2] == NULL) {
		/* Support only the setter. */
		return SQLITE_NOTFOUND;
	}
	/* When the user executes 'PRAGMA page_size=N' we save the
	 * size internally.
	 *
	 * The page size must be between 512 and 65536, and be a
	 * power of two. The check below was copied from
	 * sqlite3BtreeSetPageSize in btree.c.
	 *
	 * Invalid sizes are simply ignored, SQLite will do the same.
	 *
	 * It's not possible to change the size after it's set.
	 */
	int page_size = atoi(fcntl[2]);

	if (page_size < FORMAT__PAGE_SIZE_MIN ||
	    page_size > FORMAT__PAGE_SIZE_MAX) {
		fcntl[0] = sqlite3_mprintf("page size outside of valid range");
		return SQLITE_IOERR;
	}

	if (!is_po2((unsigned long)page_size)) {
		fcntl[0] = sqlite3_mprintf("page size must be a power of 2");
		return SQLITE_IOERR;
	}

	if (f->database->n_pages > 0) {
		int db_page_size = (int)vfsDatabaseGetPageSize(f->database);
		if (db_page_size != page_size) {
			fcntl[0] = sqlite3_mprintf(
			    "changing page size is not supported");
			return SQLITE_IOERR;
		}
	}

	/* Read case not handled */
	return SQLITE_NOTFOUND;
}

/* Handle pragma a pragma file control. See the xFileControl
 * docstring in sqlite.h for more details. */
static int vfsFileControlPragma(struct vfsMainFile *f, char **fcntl)
{
	PRE(f && fcntl && fcntl[1]);
	const char *pragma_name = fcntl[1];

	if (sqlite3_stricmp(pragma_name, "page_size") == 0) {
		return vfsPragmaPageSize(f, fcntl);
	}
	if (sqlite3_stricmp(pragma_name, "delete_database") == 0) {
		return vfsPragmaDeleteDatabase(f, fcntl);
	}

	return SQLITE_NOTFOUND;
}

static int vfsFileControlPersistWal(struct vfsMainFile *f, int *arg)
{
	(void)f;
	/* This prevents SQLite from deleting the WAL after the
	 * last connection is closed. */
	switch (*(int *)(arg)) {
		case 0:
			return SQLITE_MISUSE;
		case -1:
			*(int *)(arg) = 1;
			return SQLITE_OK;
		case 1:
		default:
			return SQLITE_OK;
	}
}

static int vfsMainFileControl(sqlite3_file *file, int op, void *arg)
{
	struct vfsMainFile *f = (struct vfsMainFile *)file;

	switch (op) {
		case SQLITE_FCNTL_PRAGMA:
			return vfsFileControlPragma(f, arg);
		case SQLITE_FCNTL_PERSIST_WAL:
			return vfsFileControlPersistWal(f, arg);
		default:
			return SQLITE_OK;
	}
}

/* Simulate shared memory by allocating on the C heap. */
static int vfsMainFileShmMap(sqlite3_file *file, /* Handle open on database file */
			 int region_index,   /* Region to retrieve */
			 int region_size,    /* Size of regions */
			 int extend, /* True to extend file if necessary */
			 void volatile **out /* OUT: Mapped memory */
)
{
	const off_t offset = (off_t)(region_index * region_size);
	struct vfsMainFile *f = (struct vfsMainFile *)file;
	struct vfsShm *s = &f->database->shm;

	PRE(region_size == VFS__WAL_INDEX_REGION_SIZE);
	if (region_index < f->mappedShmRegions.len) {
		*out = f->mappedShmRegions.ptr[region_index];
		return SQLITE_OK;
	}

	const size_t map_size = vfsGetMapSize();
	/* If an allocation is required, then it must be aligned with the
	 * mapping size. */
	PRE(((size_t)(region_index * VFS__WAL_INDEX_REGION_SIZE) % map_size) == 0);

	int rv;
	mtx_lock(&s->mtx);
	const off_t min_file_size = offset + (off_t)map_size;
	if (s->size < min_file_size) {
		if (extend) {
			/* The file is not big enough */
			rv = ftruncate(s->fd, min_file_size);
			if (rv < 0) {
				mtx_unlock(&s->mtx);
				return SQLITE_NOMEM;
			}
			s->size = min_file_size;
		} else {
			*out = NULL;
			mtx_unlock(&s->mtx);
			return SQLITE_OK;
		}
	}
	mtx_unlock(&s->mtx);

	const int region_count = (int)(map_size / VFS__WAL_INDEX_REGION_SIZE);
	while (f->mappedShmRegions.cap < (region_index + region_count)) {
		int newCap = 2 * f->mappedShmRegions.cap + 1;
		void **newPtr = sqlite3_realloc64(
		    f->mappedShmRegions.ptr,
		    (sqlite3_uint64)sizeof(*f->mappedShmRegions.ptr) *
			(sqlite3_uint64)newCap);
		if (newPtr == NULL) {
			return SQLITE_NOMEM;
		}
		memset(newPtr + f->mappedShmRegions.cap, 0,
		       sizeof(void *) *
			   (size_t)(newCap - f->mappedShmRegions.cap));
		f->mappedShmRegions.cap = newCap;
		f->mappedShmRegions.ptr = newPtr;
	}

	PRE(region_index == f->mappedShmRegions.len);
	void *region =
	    mmap(NULL, (size_t)map_size, PROT_READ | PROT_WRITE,
		 f->exclMask ? MAP_PRIVATE : MAP_SHARED, s->fd,
		 region_index * VFS__WAL_INDEX_REGION_SIZE);
	if (region == MAP_FAILED) {
		return SQLITE_IOERR_SHMMAP;
	}

	for (int i = 0; i < region_count; i++) {
		f->mappedShmRegions.ptr[region_index + i] = region + (i * VFS__WAL_INDEX_REGION_SIZE);
	}
	f->mappedShmRegions.len = region_index + region_count;
	*out = region;
	return SQLITE_OK;
}

/* Forges a transaction that will:
 *  - reset the header to the default;
 *  - remove all content from the database.
 * The result will be a pristine database with only one empty leaf page. */
static void vfsForgeDeleteTransaction(struct vfsMainFile *f)
{
	PRE(f->exclMask & (1 << VFS__WAL_WRITE_LOCK));
	PRE(f->state == DELETING);

	/* Deleting a database in dqlite means just resetting it to a 0-page
	 * file. This allows other (read) transactions to properly cohexist. The
	 * real deletion will happen later on when the number of connections to
	 * the database becomes 0. If some other write statements are executed
	 * on that database in the meanwhile, deletion will not happen and the
	 * database will act as-if a database was deleted and recreated from
	 * scratch.
	 *
	 * The above is achieved by abusing the in-header size field in the
	 * header:
	 *  - for historical reasons, SQLite will accept a 0-value and use the file
	 *    size instead in that case but SQLite will never write 0 in that field
	 *    see https://sqlite.org/fileformat2.html#in_header_database_size
	 *  - the VfsApply logic will use 1 as size for the commit record in the
	 *    WAL, forcing all pages except the header to be ignored by new
	 *    readers (like a database reset) and forcing the next checkpoint to
	 *    reset the database to a single page
	 *  - when closing the last file handle, if the database is only 1 page
	 *    and that page contains 0 in that field, then the database can be 
	 *    removed as no connection is open
	 */
	if (f->database->wal.n_tx != 0) {
		/* The transaction in which the delete was asked was polluted by
		 * other changes. The behaviour here is then to just ignore that
		 * request. */
		tracef("[%s] delete_database PRAGMA ignored: dirty transaction",
		       f->database->name);
		return;
	}

	const size_t header_size = 100;
	uint32_t page_size = vfsDatabaseGetPageSize(f->database);
	assert(page_size > header_size);

	struct vfsFrame **frames = sqlite3_malloc(sizeof(struct vfsFrame *));
	if (frames == NULL) {
		return;
	}

	frames[0] = vfsFrameCreate(page_size);
	if (frames[0] == NULL) {
		sqlite3_free(frames);
		return;
	}

	uint8_t *page = frames[0]->page;

	assert(f->database->n_pages > 0);

	memcpy(page, f->database->pages[0], header_size);
	vfsResetDatabaseFileHeader(page, (uint32_t)f->database->wal.n_frames);

	/* frame->page 1 is used as the b-tree root for the sqlite_schema table.
	 * Since this logic removes everything, it is ok to just mark it
	 * as a blank leaf frame->page. */
	memset(page + header_size, 0, page_size - header_size);

	const uint8_t leaf_table_page_type = 0x0d;
	page[header_size] = leaf_table_page_type;

	const size_t leaf_table_page_cell_content_area_start_offset = 5;
	if (page_size != FORMAT__PAGE_SIZE_MAX) {
		page[header_size +
		     leaf_table_page_cell_content_area_start_offset] =
		    (uint8_t)(page_size >> 8);
		page[header_size +
		     leaf_table_page_cell_content_area_start_offset + 1] =
		    (uint8_t)(page_size);
	}

	/* Fill the header with just about the information necessary for the
	 * VfsPoll to succeed as it is never expected for this frame to be read
	 * by SQLite. See VfsPoll */
	BytePutBe32(1, &frames[0]->header[0]);
	BytePutBe32(1, &frames[0]->header[4]);

	f->database->wal.n_tx = 1;
	f->database->wal.tx = frames;

	/* Now restart the header, so that new transactions will notice the new
	 * page (including this one). This is the same logic found in
	 * vfsInvalidateWalIndexHeader except that it is executed on the private
	 * map of this file, so that it is "invisible" to other transaction
	 * until VfsApply is executed */
	PRE(f->mappedShmRegions.len > 0);
	uint8_t *walIndex = f->mappedShmRegions.ptr[0];
	walIndex[0] = 1;
	walIndex[VFS__WAL_INDEX_HEADER_SIZE] = 0;
}

/* Finalizes a transaction by removing unused WAL frames and forging delete
 * requests, if any. */
static void vfsFinalizeTransaction(struct vfsMainFile *f)
{
	struct vfsWal *w = &f->database->wal;
	
	if (f->state == DELETING) {
		tracef("[%s] forged delete transaction", f->database->name);
		vfsForgeDeleteTransaction(f);
		f->state = NORMAL;
	}

	if (w->n_tx == 0) {
		return;
	}

	struct vfsFrame *last = w->tx[w->n_tx - 1];
	uint32_t commit = vfsFrameGetDatabaseSize(last);

	if (commit > 0) {
		return;
	}

	tracef("rollback n_tx: %d", w->n_tx);
	for (unsigned i = 0; i < w->n_tx; i++) {
		vfsFrameDestroy(w->tx[i]);
	}
	sqlite3_free(w->tx);
	w->n_tx = 0;
	w->tx = NULL;
}

/* vfsRedirectShm will turn the shared memory held by the SQLite connection
 * using this file into a private mapping, so that changes to this region will
 * not be seen by other connections open on the same file.
 *
 * The reson to use this kind of dark magic is that SQLite stores a pointer to
 * the shared memory inside the connection and performs reads and writes
 * directly.
 *
 * See also vfsPublishShm and vfsRollbackShm. */
static int vfsRedirectShm(struct vfsMainFile *f)
{
	const size_t map_size = vfsGetMapSize();

	/* When taking a write lock also make all mappings private.
	 * This effectively shadows the transaction as all other
	 * connections cannot see the update to the mxFrames field. */
	int region_per_map = (int)(map_size / VFS__WAL_INDEX_REGION_SIZE);
	for (int regionIndex = 0; regionIndex < f->mappedShmRegions.len;
	     regionIndex += region_per_map) {
		void *region = f->mappedShmRegions.ptr[regionIndex];
		void *new_region =
		    mmap(NULL, map_size, PROT_READ | PROT_WRITE, MAP_PRIVATE,
			 f->database->shm.fd,
			 regionIndex * VFS__WAL_INDEX_REGION_SIZE);
		if (new_region == MAP_FAILED) {
			/* This should never happen. Also, this means that we
			 * might leave the connection in a weird state. */
			return SQLITE_NOMEM;
		}
		void *remapped = mremap(new_region, map_size, map_size,
					MREMAP_MAYMOVE | MREMAP_FIXED, region);
		assert(remapped == region);
	}
	return SQLITE_OK;
}

/* vfsPublishShm will make the changes made to the shared memory by a write
 * transaction visible to the other open SQLite connections on the same
 * database.
 *
 * See vfsRedirectShm for a rationale. */
static int vfsPublishShm(struct vfsMainFile *f)
{
	/* Releasing memory means "publishing" changes back to the
	 * shared map and remove the COW behavior from the regions. */
	if (f->mappedShmRegions.len == 0) {
		return SQLITE_OK;
	}

	/* The shared memory needs to be written in the proper order so that we
	 * don't end up with concurrency issues. It is possible to split the
	 * shared memory in 2 parts:
	 *  - the header, composed by
	 *    - two copies actually of the WAL Index Information section
	 *    - a synchronization section (read marks and backfill);
	 *  - the hash map array
	 * The right order to do that is to first write the hash map array, then
	 * header. Also, only the backfill of the synchronization section of the
	 * header should be written and only when appropriate to do so. */
	const size_t map_size = vfsGetMapSize();
	const int region_per_map = (int)(map_size / VFS__WAL_INDEX_REGION_SIZE);
	PRE((f->mappedShmRegions.len % region_per_map) == 0);

	/* Copy the hash map array in all the secondary regions */
	for (int i = region_per_map; i < f->mappedShmRegions.len;
	     i += region_per_map) {
		void *region = mmap(
		    NULL, map_size, PROT_READ | PROT_WRITE, MAP_SHARED,
		    f->database->shm.fd, i * VFS__WAL_INDEX_REGION_SIZE);
		assert(region != MAP_FAILED);
		memcpy(region, f->mappedShmRegions.ptr[i], map_size);
		void *remapped = mremap(
		    region, map_size, map_size,
		    MREMAP_MAYMOVE | MREMAP_FIXED, f->mappedShmRegions.ptr[i]);
		assert(remapped == f->mappedShmRegions.ptr[i]);
	}

	const size_t ckptInfoSize = 40;
	const size_t headerSize = VFS__WAL_INDEX_HEADER_SIZE * 2 + ckptInfoSize;

	void *first_region_private = f->mappedShmRegions.ptr[0];
	void *first_region_shared = mmap(NULL, map_size, PROT_READ | PROT_WRITE,
					 MAP_SHARED, f->database->shm.fd, 0);
	assert(first_region_shared != MAP_FAILED);

	/* Copy the hash map array for the first region */
	memcpy(first_region_shared + headerSize,
	       first_region_private + headerSize, map_size - headerSize);
	atomic_thread_fence(memory_order_relaxed);

	/* Finally publish the two copies of the WAL Index information and
	 * remap. */
	memcpy(first_region_shared + VFS__WAL_INDEX_HEADER_SIZE,
	       f->mappedShmRegions.ptr[0] + VFS__WAL_INDEX_HEADER_SIZE,
	       VFS__WAL_INDEX_HEADER_SIZE);
	/* See https://github.com/sqlite/sqlite/blob/1ea6a53/src/wal.c#L2585 */
	atomic_thread_fence(memory_order_seq_cst);
	memcpy(first_region_shared, f->mappedShmRegions.ptr[0],
	       VFS__WAL_INDEX_HEADER_SIZE);

	/* Check that no checkpointing happened during the write transaction. */

	/* Now it is necessary to merge the synchronization part.
	 * Read marks are never changed by a write transaction as a write
	 * transaction can only be started after a read transaction and as such
	 * the marker and the read lock is already acquired. For the other
	 * fields, quoting the documentation: > The nBackfill can only be
	 * increased while holding the WAL_CKPT_LOCK. However, nBackfill > is
	 * changed to zero during a WAL reset, and this happens while holding
	 * the WAL_WRITE_LOCK. And also:` > The mxFrame value is always greater
	 * than or equal to both nBackfill and nBackfillAttempted. Which means
	 * that only when holding the checkpoint lock those fields should be
	 * published */
	if (f->exclMask & (1 << VFS__WAL_CKPT_LOCK)) {
		const size_t nBackfillOffset = 96;
		int32_t *privateBackfill =
		    (first_region_private + nBackfillOffset);
		/* We must use atomic write here as it's read atomically by the
		 * SQLite checkpoint logic which might happen in a different
		 * thread. */
		_Atomic int32_t *sharedBackfill =
		    (first_region_shared + nBackfillOffset);
		atomic_store_explicit(sharedBackfill, *privateBackfill,
				      memory_order_relaxed);

		const size_t nBackfillAttemptedOffset = 128;
		int32_t *privateBackfillAttempted =
		    (first_region_private + nBackfillAttemptedOffset);
		int32_t *sharedBackfillAttempted =
		    (first_region_shared + nBackfillAttemptedOffset);
		*sharedBackfillAttempted = *privateBackfillAttempted;
	}

	void *remapped =
	    mremap(first_region_shared, map_size, map_size,
		   MREMAP_MAYMOVE | MREMAP_FIXED, f->mappedShmRegions.ptr[0]);
	assert(remapped == f->mappedShmRegions.ptr[0]);

	return SQLITE_OK;
}

/* vfsRollbackShm will discard the changes made to the shared memory by a write
 * transaction.
 *
 * See vfsRedirectShm for a rationale. */
static int vfsRollbackShm(struct vfsMainFile *f) {
	const size_t map_size = vfsGetMapSize();
	const int region_per_map = (int)(map_size / VFS__WAL_INDEX_REGION_SIZE);
	PRE((f->mappedShmRegions.len % region_per_map) == 0);

	for (int i = 0; i < f->mappedShmRegions.len; i += region_per_map) {
		void *new_region =
		    mmap(NULL, map_size, PROT_READ | PROT_WRITE, MAP_SHARED,
			 f->database->shm.fd, i * VFS__WAL_INDEX_REGION_SIZE);
		if (new_region == MAP_FAILED) {
			/* This should never happen. Also, this means that we
			 * might leave the connection in a weird state. */
			return SQLITE_NOMEM;
		}
		/* Now the private map can be unmapped safely going back to the
		 * shared one. */
		void *remapped = mremap(new_region, map_size, map_size,
					MREMAP_MAYMOVE | MREMAP_FIXED,
					f->mappedShmRegions.ptr[i]);
		assert(remapped == f->mappedShmRegions.ptr[i]);
	}
	return SQLITE_OK;
}

static int vfsMainFileShmLock(sqlite3_file *file, int ofst, int n, int flags)
{
	struct vfsMainFile *f = (struct vfsMainFile *)file;
	const uint16_t mask =
	    (uint16_t)((1 << (ofst + n)) -
		       (1 << ofst)); /* Mask of locks to take or release */

	/* Legal values for the offset and the range */
	PRE(n > 0);
	PRE(ofst >= 0 && ofst + n <= SQLITE_SHM_NLOCK);

	/* Legal values for the flags.
	 *
	 * See https://sqlite.org/c3ref/c_shm_exclusive.html. */
	PRE(flags == (SQLITE_SHM_LOCK | SQLITE_SHM_SHARED) ||
	    flags == (SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE) ||
	    flags == (SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED) ||
	    flags == (SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE));

	PRE((f->exclMask & f->sharedMask) == 0);

	/* In case a checkpoint is running, the locking logic is overridden, to
	 * make sure nothing can happen concurrently. As such, it is just enough
	 * to always return SQLITE_OK in this case. */
	if (f->exclMask == VFS__CHECKPOINT_MASK) {
		return SQLITE_OK;
	}

	int rv = SQLITE_OK;
	if (
		((flags & SQLITE_SHM_UNLOCK) && ((f->exclMask | f->sharedMask) & mask)) ||
	    (flags == (SQLITE_SHM_SHARED | SQLITE_SHM_LOCK) && 0 == (f->sharedMask & mask)) ||
	    (flags == (SQLITE_SHM_EXCLUSIVE | SQLITE_SHM_LOCK))) {

		if (flags & SQLITE_SHM_UNLOCK) {
			PRE(!(flags & SQLITE_SHM_EXCLUSIVE) ||
			    (f->exclMask & mask) == mask);
			PRE(!(flags & SQLITE_SHM_SHARED) ||
			    (f->sharedMask & mask) == mask);

			if (ofst == VFS__WAL_WRITE_LOCK && flags & SQLITE_SHM_EXCLUSIVE) {
				vfsFinalizeTransaction(f);
				/* Keep the lock if not polled. It will be released later after
				 * during VfsAbort. */
				if (f->database->wal.n_tx > 0) {
					return SQLITE_OK;
				}
			}

			rv = vfsShmUnlock(&f->database->shm, ofst, n, flags & SQLITE_SHM_EXCLUSIVE);
			if (rv == SQLITE_OK) {
				f->exclMask &= (uint16_t)~mask;
				f->sharedMask &= (uint16_t)~mask;
			}
		} else if (flags & SQLITE_SHM_SHARED) {
			rv = vfsShmLock(&f->database->shm, ofst, n, false);
			if (rv == SQLITE_OK) {
				f->sharedMask |= mask;
			}
		} else {
			PRE(flags & SQLITE_SHM_EXCLUSIVE);
			PRE((f->sharedMask & mask) == 0);
			PRE((f->exclMask & mask) == 0);

			/* When acquiring a write lock, make sure there's no
			 * transaction that hasn't been rolled back or polled.
			 * If there is, just return SQLITE_BUSY. */
			if (ofst == VFS__WAL_WRITE_LOCK &&
			    f->database->wal.n_tx > 0) {
				rv = SQLITE_BUSY;
			} else {
				rv = vfsShmLock(&f->database->shm, ofst, n,
						true);
				if (rv == SQLITE_OK) {
					f->exclMask |= mask;
				}
			}
		}

		/* This code makes sure that when taking a lock no transaction
		 * is pending. */
		if (rv == SQLITE_OK && ofst == VFS__WAL_WRITE_LOCK) {
			if (flags == (SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)) {
				assert(f->database->wal.n_tx == 0);
			}
		}
	}

	if (rv == SQLITE_OK && ofst == VFS__WAL_WRITE_LOCK && n == 1 && flags & SQLITE_SHM_EXCLUSIVE) {
		rv = (flags & SQLITE_SHM_LOCK) ? vfsRedirectShm(f) : vfsPublishShm(f);
	}

	POST((f->exclMask & f->sharedMask) == 0);
	return rv;
}

static void vfsMainFileShmBarrier(sqlite3_file *file)
{
	(void)file;
	atomic_thread_fence(memory_order_seq_cst);
}

static int vfsMainFileShmUnmap(sqlite3_file *file, int delete_flag)
{
	(void)delete_flag;

	struct vfsMainFile *f = (struct vfsMainFile *)file;
	const size_t map_size = vfsGetMapSize();
	const int region_per_map = (int)(map_size / VFS__WAL_INDEX_REGION_SIZE);
	PRE((f->mappedShmRegions.len % region_per_map) == 0);
	for (int i = 0; i < f->mappedShmRegions.len; i += region_per_map) {
		if (f->mappedShmRegions.ptr[i] != NULL) {
			int rv = munmap(f->mappedShmRegions.ptr[i], map_size);
			assert(rv == 0);
		}
	}
	sqlite3_free(f->mappedShmRegions.ptr);
	f->mappedShmRegions.ptr = NULL;
	f->mappedShmRegions.len = 0;
	f->mappedShmRegions.cap = 0;
	return SQLITE_OK;
}

static const sqlite3_io_methods vfsMainFileMethods = {
	.iVersion = 2,
	.xClose = vfsMainFileClose,
	.xRead = vfsMainFileRead,
	.xWrite = vfsMainFileWrite,
	.xTruncate = vfsMainFileTruncate,
	.xSync = vfsNoopSync,
	.xFileSize = vfsMainFileSize,
	.xLock = vfsNoopLock,
	.xUnlock = vfsNoopUnlock,
	.xCheckReservedLock = vfsNoopCheckReservedLock,
	.xFileControl = vfsMainFileControl,
	.xSectorSize = vfsNoopSectorSize,
	.xDeviceCharacteristics = vfsNoopDeviceCharacteristics,
	.xShmMap = vfsMainFileShmMap,
	.xShmLock = vfsMainFileShmLock,
	.xShmBarrier = vfsMainFileShmBarrier,
	.xShmUnmap = vfsMainFileShmUnmap,
};

static int vfsOpen(sqlite3_vfs *vfs,
		   const char *filename,
		   sqlite3_file *file,
		   int flags,
		   int *out_flags)
{
	struct vfs *v = vfs->pAppData;
	struct vfsDatabase *database;
	int exclusive = flags & SQLITE_OPEN_EXCLUSIVE;
	int create = flags & SQLITE_OPEN_CREATE;

	(void)out_flags;

	assert(vfs != NULL);
	assert(vfs->pAppData != NULL);
	assert(file != NULL);

	/* From sqlite3.h.in:
	 *
	 *   The SQLITE_OPEN_EXCLUSIVE flag is always used in conjunction with
	 *   the SQLITE_OPEN_CREATE flag, which are both directly analogous to
	 *   the O_EXCL and O_CREAT flags of the POSIX open() API.  The
	 *   SQLITE_OPEN_EXCLUSIVE flag, when paired with the
	 *   SQLITE_OPEN_CREATE, is used to indicate that file should always be
	 *   created, and that it is an error if it already exists.  It is not
	 *   used to indicate the file should be opened for exclusive access.
	 */
	assert(!exclusive || create);

	/* From SQLite documentation:
	 *
	 * If the zFilename parameter to xOpen is a NULL pointer then xOpen
	 * must invent its own temporary name for the file. Whenever the
	 * xFilename parameter is NULL it will also be the case that the
	 * flags parameter will include SQLITE_OPEN_DELETEONCLOSE.
	 */
	if (filename == NULL) {
		assert(flags & SQLITE_OPEN_DELETEONCLOSE);

		/* Open an actual temporary file. */
		vfs = sqlite3_vfs_find("unix");
		assert(vfs != NULL);
		return vfs->xOpen(vfs, NULL, file, flags, out_flags);
	} else if (flags & SQLITE_OPEN_MAIN_JOURNAL) {
		/* Journal file is just a noop file as only WAL mode is supported */
		file->pMethods = &vfsNoopMethods;
		return SQLITE_OK;
	}

	assert((flags & SQLITE_OPEN_DELETEONCLOSE) == 0);

	/* This tells SQLite to not call Close() in case we return an error. */
	file->pMethods = 0;

	/* Search if the database object exists already. */
	database = vfsDatabaseLookup(v, filename);

	if ((flags & (SQLITE_OPEN_MAIN_DB | SQLITE_OPEN_WAL)) == 0) {
		v->error = ENOENT;
		return SQLITE_CANTOPEN;
	}

	/* If file exists, and the exclusive flag is on, return an error. */
	if (database != NULL && exclusive && create && (flags & SQLITE_OPEN_MAIN_DB)) {
		v->error = EEXIST;
		return SQLITE_CANTOPEN;
	}

	if (flags & SQLITE_OPEN_WAL) {
		/* When opening the WAL file we expect the main
		 * database file to have already been created. */
		if (database == NULL) {
			v->error = ENOENT;
			return SQLITE_CANTOPEN;
		}

		struct vfsWalFile *walFile = (struct vfsWalFile *)file;
		*walFile = (struct vfsWalFile){
			.base = {
				.pMethods = &vfsWalFileMethods,
			},
			.database = database,
		};

		return SQLITE_OK;
	}

	assert(flags & SQLITE_OPEN_MAIN_DB);

	if (database == NULL) {
		if (!create) {
			v->error = ENOENT;
			return SQLITE_CANTOPEN;
		}

		database = vfsCreateDatabase(v, filename);
		if (database == NULL) {
			v->error = ENOMEM;
			return SQLITE_CANTOPEN;
		}
	}

	*(struct vfsMainFile *)file = (struct vfsMainFile){
		.base = {
			.pMethods = &vfsMainFileMethods,
		},
		.vfs = v,
		.database = database,
	};
	database->open_count++;
	return SQLITE_OK;
}

static int vfsDelete(sqlite3_vfs *vfs, const char *filename, int dir_sync)
{
	(void)dir_sync;
	struct vfs *v = vfs->pAppData;
	assert(v != NULL);

	if (vfsFilenameEndsWith(filename, "-journal")) {
		return SQLITE_OK;
	}

	if (vfsFilenameEndsWith(filename, "-wal")) {
		return SQLITE_OK;
	}

	/* This should never happen. */
	assert(false && "impossible");
	return SQLITE_PROTOCOL;
}

static int vfsAccess(sqlite3_vfs *vfs,
		     const char *filename,
		     int flags,
		     int *result)
{
	struct vfs *v;
	struct vfsDatabase *database;

	(void)flags;

	assert(vfs != NULL);
	assert(vfs->pAppData != NULL);

	v = (struct vfs *)(vfs->pAppData);

	/* If the database object exists, we consider all associated files as
	 * existing and accessible. */
	database = vfsDatabaseLookup(v, filename);
	if (database == NULL) {
		*result = 0;
	} else {
		*result = 1;
	}

	return SQLITE_OK;
}

static int vfsFullPathname(sqlite3_vfs *vfs,
			   const char *filename,
			   int pathname_len,
			   char *pathname)
{
	(void)vfs;

	/* Just return the path unchanged. */
	sqlite3_snprintf(pathname_len, pathname, "%s", filename);
	return SQLITE_OK;
}

static void *vfsDlOpen(sqlite3_vfs *vfs, const char *filename)
{
	(void)vfs;
	(void)filename;

	return 0;
}

static void vfsDlError(sqlite3_vfs *vfs, int nByte, char *zErrMsg)
{
	(void)vfs;

	sqlite3_snprintf(nByte, zErrMsg,
			 "Loadable extensions are not supported");
	zErrMsg[nByte - 1] = '\0';
}

static void (*vfsDlSym(sqlite3_vfs *vfs, void *pH, const char *z))(void)
{
	(void)vfs;
	(void)pH;
	(void)z;

	return 0;
}

static void vfsDlClose(sqlite3_vfs *vfs, void *pHandle)
{
	(void)vfs;
	(void)pHandle;

	return;
}

static int vfsRandomness(sqlite3_vfs *vfs, int nByte, char *zByte)
{
	(void)vfs;

	ssize_t rv = getrandom(zByte, (size_t)nByte, 0);
	if (rv == -1) {
		/* Ignore failed attempts */
		return 0;
	}
	return (int)rv;
}

static int vfsSleep(sqlite3_vfs *vfs, int microseconds)
{
	(void)vfs;

	struct timespec sp;
	sp.tv_sec = microseconds / 1000000;
	sp.tv_nsec = (microseconds % 1000000) * 1000;
	nanosleep(&sp, NULL);

	return microseconds;
}

static int vfsCurrentTimeInt64(sqlite3_vfs *vfs, sqlite3_int64 *piNow)
{
	static const sqlite3_int64 unixEpoch =
	    24405875 * (sqlite3_int64)8640000;
	struct timeval now;

	(void)vfs;

	gettimeofday(&now, 0);
	*piNow =
	    unixEpoch + 1000 * (sqlite3_int64)now.tv_sec + now.tv_usec / 1000;
	return SQLITE_OK;
}

static int vfsCurrentTime(sqlite3_vfs *vfs, double *piNow)
{
	sqlite3_int64 iNow;
	int rc = vfsCurrentTimeInt64(vfs, &iNow);
	if (rc == SQLITE_OK) {
		*piNow = ((double)iNow) / 86400000.0;
	}
	return rc;
}

static int vfsGetLastError(sqlite3_vfs *vfs, int x, char *y)
{
	struct vfs *v = (struct vfs *)(vfs->pAppData);
	int rc;

	(void)vfs;
	(void)x;
	(void)y;

	rc = v->error;

	return rc;
}

int VfsInit(struct sqlite3_vfs *vfs, const char *name)
{
	tracef("vfs init");

	vfs->iVersion = 2;
	vfs->mxPathname = VFS__MAX_PATHNAME;
	vfs->pNext = NULL;

	struct vfs *v = vfsCreate();
	if (v == NULL) {
		return DQLITE_NOMEM;
	}
	vfs->pAppData = v;
	vfs->szOsFile = MAX(sizeof(struct vfsMainFile), sizeof(struct vfsWalFile));
	if (vfs->szOsFile < v->base_vfs->szOsFile) {
		vfs->szOsFile = v->base_vfs->szOsFile;
	}
	vfs->xOpen = vfsOpen;
	vfs->xDelete = vfsDelete;
	vfs->xAccess = vfsAccess;
	vfs->xFullPathname = vfsFullPathname;
	vfs->xDlOpen = vfsDlOpen;
	vfs->xDlError = vfsDlError;
	vfs->xDlSym = vfsDlSym;
	vfs->xDlClose = vfsDlClose;
	vfs->xRandomness = vfsRandomness;
	vfs->xSleep = vfsSleep;
	vfs->xCurrentTime = vfsCurrentTime;
	vfs->xGetLastError = vfsGetLastError;
	vfs->xCurrentTimeInt64 = vfsCurrentTimeInt64;
	vfs->zName = name;

	return 0;
}

void VfsDeleteHook(struct sqlite3_vfs *vfs,
		  void (*hook)(void *, const char *),
		  void *data)
{
	struct vfs *v = vfs->pAppData;
	v->delete_hook = hook;
	v->delete_hook_data = data;
}

void VfsClose(struct sqlite3_vfs *vfs)
{
	tracef("vfs close");
	struct vfs *v = vfs->pAppData;
	vfsDestroy(v);
	sqlite3_free(v);
}

int VfsPoll(sqlite3 *conn, struct vfsTransaction *transaction)
{
	sqlite3_file *file;
	struct vfsMainFile *f;
	struct vfsFrame *last;
	uint32_t commit;
	unsigned i;
	int rv;

	rv = sqlite3_file_control(conn, NULL, SQLITE_FCNTL_FILE_POINTER, &file);
	assert(rv == SQLITE_OK);
	f = (struct vfsMainFile*)file;
	tracef("vfs poll filename:%s", f->database->name);

	if (!(f->exclMask & (1 << VFS__WAL_WRITE_LOCK))) {
		/* There could be an in-progress transaction, but not from this
		 * connection. */
		*transaction = (struct vfsTransaction){};
		return SQLITE_OK;
	}

	if (f->database->wal.n_tx == 0) {
		*transaction = (struct vfsTransaction){};
		return SQLITE_OK;
	}

	/* Check if the last frame in the transaction has the commit marker. */
	last = f->database->wal.tx[f->database->wal.n_tx - 1];
	commit = vfsFrameGetDatabaseSize(last);

	if (commit == 0) {
		*transaction = (struct vfsTransaction){};
		return SQLITE_OK;
	}

	uint64_t *numbers = sqlite3_malloc64(sizeof(*numbers) * f->database->wal.n_tx);
	if (numbers == NULL) {
		return SQLITE_NOMEM;
	}

	void **pages = sqlite3_malloc64(sizeof(*pages) * f->database->wal.n_tx);
	if (pages == NULL) {
		sqlite3_free(numbers);
		return SQLITE_NOMEM;
	}

	for (i = 0; i < f->database->wal.n_tx; i++) {
		numbers[i] = vfsFrameGetPageNumber(f->database->wal.tx[i]);
		pages[i] = f->database->wal.tx[i]->page;
		/* Release the vfsFrame object, but not its buf attribute, since
		 * responsibility for that memory has been transferred to the
		 * caller. */
		sqlite3_free(f->database->wal.tx[i]);
	}
	sqlite3_free(f->database->wal.tx);
	*transaction = (struct vfsTransaction) {
		.n_pages     = f->database->wal.n_tx,
		.page_numbers = numbers,
		.pages   = pages,
	};
	f->database->wal.n_tx = 0;
	f->database->wal.tx = NULL;
	f->state = POLLED;

	return SQLITE_OK;
}

/* Append the given pages as new frames. */
static int vfsWalAppend(struct vfsDatabase *d,
	const struct vfsTransaction *transaction)
{
	struct vfsWal *w = &d->wal;
	struct vfsFrame **frames; /* New frames array. */
	uint32_t page_size;
	uint32_t database_size;
	unsigned i;
	unsigned j;
	uint32_t salt[2];
	uint32_t checksum[2];

	/* No pending transactions. */
	assert(w->n_tx == 0);

	page_size = vfsWalGetPageSize(w);
	assert(page_size > 0);

	/* Get the salt from the WAL header. */
	salt[0] = vfsWalGetSalt1(w);
	salt[1] = vfsWalGetSalt2(w);

	/* If there are currently no frames in the WAL, the starting database
	 * size will be equal to the current number of pages in the main
	 * database, and the starting checksum should be set to the one stored
	 * in the WAL header. Otherwise, the starting database size and checksum
	 * will be the ones stored in the last frame of the WAL. */
	if (w->n_frames == 0) {
		database_size = d->n_pages;
		checksum[0] = vfsWalGetChecksum1(w);
		checksum[1] = vfsWalGetChecksum2(w);
	} else {
		struct vfsFrame *frame = w->frames[w->n_frames - 1];
		checksum[0] = vfsFrameGetChecksum1(frame);
		checksum[1] = vfsFrameGetChecksum2(frame);
		database_size = vfsFrameGetDatabaseSize(frame);
	}

	mtx_lock(&w->mtx);
	frames = sqlite3_realloc64(
	    w->frames, sizeof(*frames) * (w->n_frames + transaction->n_pages));
	if (frames == NULL) {
		mtx_unlock(&w->mtx);
		goto oom;
	}
	w->frames = frames;
	mtx_unlock(&w->mtx);

	for (i = 0; i < transaction->n_pages; i++) {
		struct vfsFrame *frame = vfsFrameCreate(page_size);
		uint32_t page_number = (uint32_t)transaction->page_numbers[i];
		uint32_t commit = 0;
		uint8_t *page = transaction->pages[i];

		if (frame == NULL) {
			goto oom_after_frames_alloc;
		}

		/* When writing the SQLite database header, make sure to sync 
		 * the file size to the logical database size. */
		if (page_number == 1) {
			database_size = ByteGetBe32(&page[VFS__IN_HEADER_DATABASE_SIZE_OFFSET]);
			/* Delete database page found! */
			if (database_size == 0) {
				assert(transaction->n_pages == 1);
				database_size = 1;
			}
		}

		/* For commit records, the size of the database file in pages
		 * after the commit. For all other records, zero. */
		if (i == transaction->n_pages - 1) {
			commit = database_size;
		}

		vfsFrameFill(frame, page_number, commit, salt, checksum, page,
			     page_size);

		frames[w->n_frames + i] = frame;
	}

	mtx_lock(&w->mtx);
	w->n_frames += transaction->n_pages;
	mtx_unlock(&w->mtx);

	return 0;

oom_after_frames_alloc:
	for (j = 0; j < i; j++) {
		vfsFrameDestroy(frames[w->n_frames + j]);
	}
oom:
	return DQLITE_NOMEM;
}

/* Write the header of a brand new WAL file image. */
static void vfsWalStartHeader(struct vfsWal *w, uint32_t page_size)
{
	assert(page_size > 0);
	uint32_t checksum[2] = {0, 0};
	/* SQLite calculates checksums for the WAL header and frames either
	 * using little endian or big endian byte order when adding up 32-bit
	 * words. The byte order that should be used is recorded in the WAL file
	 * header by setting the least significant bit of the magic value stored
	 * in the first 32 bits. This allows portability of the WAL file across
	 * hosts with different native byte order.
	 *
	 * When creating a brand new WAL file, SQLite will set the byte order
	 * bit to match the host's native byte order, so checksums are a bit
	 * more efficient.
	 *
	 * In Dqlite the WAL file image is always generated at run time on the
	 * host, so we can always use the native byte order. */
	BytePutBe32(VFS__WAL_MAGIC | VFS__BIGENDIAN, &w->hdr[0]);
	BytePutBe32(VFS__WAL_VERSION, &w->hdr[4]);
	BytePutBe32(page_size, &w->hdr[8]);
	BytePutBe32(0, &w->hdr[12]);
	sqlite3_randomness(8, &w->hdr[16]);
	vfsChecksum(w->hdr, 24, checksum, checksum);
	BytePutBe32(checksum[0], w->hdr + 24);
	BytePutBe32(checksum[1], w->hdr + 28);
}

/* Invalidate the WAL index header, forcing the next connection that tries to
 * start a read transaction to rebuild the WAL index by reading the WAL.
 *
 * No read or write lock must be currently held. */
static void vfsInvalidateWalIndexHeader(struct vfsDatabase *d)
{
	struct vfsShm *shm = &d->shm;

	PRE(shm->lock[VFS__WAL_WRITE_LOCK] < 0);
	PRE(shm->lock[VFS__WAL_CKPT_LOCK] >= 0);
	PRE(shm->lock[VFS__WAL_RECOVER_LOCK] >= 0);
	PRE(shm->size >= VFS__WAL_INDEX_HEADER_SIZE * 2);

	/* The walIndexTryHdr function in sqlite/wal.c (which is indirectly
	 * called by sqlite3WalBeginReadTransaction), compares the first and
	 * second copy of the WAL index header to see if it is valid. Changing
	 * the first byte of each of the two copies is enough to make the check
	 * fail. */
	uint8_t buffer[1] = { 1 };
	ssize_t rv = pwrite(shm->fd, buffer, 1, 0);
	assert(rv == 1);

	buffer[0] = 0;
	rv = pwrite(shm->fd, buffer, 1, VFS__WAL_INDEX_HEADER_SIZE);
	assert(rv == 1);
}

int VfsApply(sqlite3 *conn, const struct vfsTransaction *transaction)
{
	sqlite3_file *file;
	struct vfsMainFile *f;
	int rv;

	rv = sqlite3_file_control(conn, NULL, SQLITE_FCNTL_FILE_POINTER, &file);
	assert(rv == SQLITE_OK);
	f = (struct vfsMainFile*)file;
	tracef("vfs apply on %s %u pages", f->database->name, transaction->n_pages);

	
	if (f->state != POLLED) {
		/* If this connection wasn't the one originating the transaction and there is 
		* another on-going write transaction it is not possible to change the underlying 
		* WAL object as that would cause corruption of the database. */
		rv = vfsShmLock(&f->database->shm, VFS__WAL_WRITE_LOCK, 1, true);
		if (rv != SQLITE_OK) {
			return rv;
		}
	}
	PRE(f->database->shm.lock[VFS__WAL_WRITE_LOCK] < 0);

	/* If there's no page size set in the WAL header, it must mean that WAL
	 * file was never written. In that case we need to initialize the WAL
	 * header. */
	if (vfsWalGetPageSize(&f->database->wal) == 0) {
		vfsWalStartHeader(&f->database->wal, vfsDatabaseGetPageSize(f->database));
	}

	rv = vfsWalAppend(f->database, transaction);
	if (rv != 0) {
		tracef("wal append failed rv:%d n_pages:%u n:%u", rv,
		       f->database->n_pages, transaction->n_pages);
		return rv;
	}

	/* If this is the connection that orginated this commit and on which
	 * dqlite_vfs_poll() was called. The lock must be released and the WAL
	 * index can be published.
	 *
	 * Otherwise, if the WAL index header is mapped it means that this VFS
	 * has one or more open connections even if it's not the one that
	 * originated the transaction (this can happen for example when applying
	 * a Raft barrier and replaying the Raft log in order to serve a request
	 * of a newly connected client). */
	if (f->state == POLLED) {
		PRE(f->exclMask & (1 << VFS__WAL_WRITE_LOCK));
		vfsMainFileShmLock(file, VFS__WAL_WRITE_LOCK, 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE);
		f->state = NORMAL;
	} else {
		if (f->database->shm.size > 0) {
			vfsInvalidateWalIndexHeader(f->database);
		}
		vfsShmUnlock(&f->database->shm, VFS__WAL_WRITE_LOCK, 1, true);
	}

	return SQLITE_OK;
}

int VfsAbort(sqlite3 *conn)
{
	sqlite3_file *file;
	int rv = sqlite3_file_control(conn, NULL, SQLITE_FCNTL_FILE_POINTER, &file);
	assert(rv == SQLITE_OK);
	struct vfsMainFile *f = (struct vfsMainFile*)file;

	if (f->state == POLLED) {
		/* The write lock must be held if the transaction was polled. */
		PRE(f->exclMask & (1 << VFS__WAL_WRITE_LOCK));
		/* This logic should then:
		 *  - drop the changes to the shared memory
		 *  - release the write lock */
		rv = vfsRollbackShm(f);
		if (rv != SQLITE_OK) {
			return rv;
		}
		rv = vfsShmUnlock(&f->database->shm, VFS__WAL_WRITE_LOCK, 1,
				  true);
		if (rv != SQLITE_OK) {
			tracef("shm unlock failed %d", rv);
		} else {
			f->exclMask &= (uint16_t)(~(1 << VFS__WAL_WRITE_LOCK));
			f->state = NORMAL;
		}
		return rv;
	}

	return SQLITE_OK;
}

int VfsCheckpoint(sqlite3 *conn, unsigned int threshold)
{
	sqlite3_file *file;
	int rv = sqlite3_file_control(conn, NULL, SQLITE_FCNTL_FILE_POINTER, &file);
	assert(rv == SQLITE_OK);
	struct vfsMainFile *f = (struct vfsMainFile*)file;

	PRE(f->sharedMask == 0);
	PRE(f->exclMask == 0);
	tracef("[database %p] checkpoint start", f->database);

	/* Try to lock everything, so that nothing can proceed. */
	rv = vfsShmLock(&f->database->shm, 0, SQLITE_SHM_NLOCK, true);
	if (rv != SQLITE_OK) {
		tracef("[database %p] checkpoint busy", f->database);
		return rv;
	}

	PRE(f->database->wal.n_tx == 0);
	if (f->database->wal.n_frames < threshold) {
		rv = vfsShmUnlock(&f->database->shm, 0, SQLITE_SHM_NLOCK, true);
		assert(rv == SQLITE_OK);
		tracef("[database %p] checkpoint below threshold (%d < %d)", f->database, f->database->wal.n_frames, threshold);
		return SQLITE_OK;
	}

	int wal_size;
	int ckpt;
	f->exclMask = VFS__CHECKPOINT_MASK;
	rv = sqlite3_wal_checkpoint_v2(conn, NULL, SQLITE_CHECKPOINT_TRUNCATE,
				       &wal_size, &ckpt);
	/* Since no reader transaction is in progress, we must be able to
	 * checkpoint the entire WAL */
	assert(rv == SQLITE_OK);
	assert(wal_size == 0);
	assert(ckpt == 0);
	tracef("[database %p] checkpointed");

	f->exclMask = 0;
	rv = vfsShmUnlock(&f->database->shm, 0, SQLITE_SHM_NLOCK, true);
	assert(rv == SQLITE_OK);

	return SQLITE_OK;
}

/* Extract the number of pages field from the database header. */
static uint32_t vfsDatabaseGetNumberOfPages(struct vfsDatabase *d)
{
	if (d->n_pages == 0) {
		return 0;
	}

	/* The page size is stored in the 16th and 17th bytes of the first
	 * database page (big-endian) */
	uint8_t *page = d->pages[0];
	return ByteGetBe32(&page[VFS__IN_HEADER_DATABASE_SIZE_OFFSET]);
}

static uint32_t vfsDatabaseNumPages(struct vfsDatabase *database, bool use_wal) {
	uint32_t n;
	if (use_wal && database->wal.n_frames > 0) {
		n = vfsFrameGetDatabaseSize(database->wal.frames[database->wal.n_frames-1]);
		/* If the result is zero, it means that the WAL contains
		 * uncommitted transactions. */
		POST(n > 0);
	} else {
		n = vfsDatabaseGetNumberOfPages(database);
	}
	return n;
}

int VfsDatabaseNumPages(sqlite3_vfs *vfs,
			const char *filename,
			bool use_wal,
			uint32_t *n)
{
	struct vfs *v;
	struct vfsDatabase *d;

	v = (struct vfs *)(vfs->pAppData);
	d = vfsDatabaseLookup(v, filename);
	if (d == NULL) {
		return -1;
	}

	*n = vfsDatabaseNumPages(d, use_wal);
	return 0;
}

int VfsAcquireSnapshot(sqlite3 *conn, struct vfsSnapshot *snapshot)
{
	sqlite3_file *file;
	int rv = sqlite3_file_control(conn, NULL, SQLITE_FCNTL_FILE_POINTER, &file);
	assert(rv == SQLITE_OK);
	struct vfsMainFile *f = (struct vfsMainFile*)file;

	/* Acquire read lock 0. This prevents any checkpointing to succeed, even
	 * a PASSIVE one. */
	rv = vfsMainFileShmLock(file, VFS__WAL_READ_LOCK(0), 1, SQLITE_SHM_LOCK | SQLITE_SHM_SHARED);
	if (rv != SQLITE_OK) {
		return rv;
	}

	uint32_t page_size = vfsDatabaseGetPageSize(f->database);
	if (vfsWalSize(&f->database->wal) == 0) {
		/* Fast path: when the WAL is empty, there is no need to
		 * allocate anything */
		*snapshot = (struct vfsSnapshot) {
			.main = {
				.pages = f->database->pages,
				.page_count = f->database->n_pages,
				.page_size = page_size,
			},
			.wal = {
				.pages = NULL,
				.page_count = 0,
				.page_size = page_size
			},
		};
		return SQLITE_OK;
	}

	uint32_t page_count = vfsDatabaseNumPages(f->database, true);
	void **pages = sqlite3_malloc64(sizeof(void*) * page_count);
	if (pages == NULL) {
		rv = vfsMainFileShmLock(file, VFS__WAL_READ_LOCK(0), 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED);
		assert(rv == SQLITE_OK);
		return SQLITE_NOMEM;
	}

	/* == Safety==
	 * While holding READ_LOCK(0) the database is guaranteed not to change.
	 */
	for (unsigned i = 0; i < f->database->n_pages; i++) {
		pages[i] = f->database->pages[i];
	}

	/* == Safety==
	 * It is expected for this routine to run on the main libuv thread. This
	 * means that no VfsApply or VfsCheckpoint can run. As such, accessing
	 * frames is safe. Holding READ_LOCK(0) will also prevent any checkpoint
	 * from running and as such, *the content* of the frames is safe to
	 * access on any thread, until VfsReleaseSnapshot is called.
	 */
	for (unsigned i = 0; i < f->database->wal.n_frames; i++) {
		struct vfsFrame *frame = f->database->wal.frames[i];
		uint32_t page_number = vfsFrameGetPageNumber(frame);
		if (page_number > page_count) {
			continue;
		}
		pages[page_number - 1] = frame->page;
	}

	*snapshot = (struct vfsSnapshot) {
		.main = {
			.pages = pages,
			.page_count = page_count,
			.page_size = page_size,
		},
		.wal = {
			.pages = NULL,
			.page_count = 0,
			.page_size = page_size
		},
	};
	return SQLITE_OK;
}

int VfsReleaseSnapshot(sqlite3 *conn, struct vfsSnapshot *snapshot)
{
	sqlite3_file *file;
	int rv = sqlite3_file_control(conn, NULL, SQLITE_FCNTL_FILE_POINTER, &file);
	assert(rv == SQLITE_OK);
	struct vfsMainFile *f = (struct vfsMainFile*)file;

	PRE(f->sharedMask & (1 << VFS__WAL_READ_LOCK(0)));

	if (snapshot->main.pages != f->database->pages) {
		assert(f->database->wal.n_frames > 0);

		sqlite3_free(snapshot->main.pages);
	}

	*snapshot = (struct vfsSnapshot){};

	/* Release the lock */
	rv = vfsMainFileShmLock(file, VFS__WAL_READ_LOCK(0), 1, SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED);
	assert (rv == SQLITE_OK);

	return SQLITE_OK;
}

static int vfsDatabaseRestore(struct vfsDatabase *d, const struct vfsSnapshotFile *file)
{
	if (file->page_count == 0) {
		return SQLITE_CORRUPT;
	}

	void **pages = sqlite3_malloc64(sizeof(void*) * file->page_count);
	if (pages == NULL) {
		return SQLITE_NOMEM;
	}

	for (size_t i = 0; i < file->page_count; i++) {
		void *page = sqlite3_malloc64(file->page_size);
		if (page == NULL) {
			for (size_t j = 0; j < i; j++) {
				sqlite3_free(pages[j]);
			}
			sqlite3_free(pages);
			return SQLITE_NOMEM;
		}

		pages[i] = page;
		memcpy(page, file->pages[i], file->page_size);
	}

	/* Truncate any existing content. */
	int rv = vfsDatabaseTruncate(d, 0);
	assert(rv == 0);

	d->pages = pages;
	d->n_pages = (unsigned)file->page_count;

	return SQLITE_OK;
}

int VfsRestore(sqlite3 *conn, const struct vfsSnapshot *snapshot)
{
	sqlite3_file *file;
	int rv = sqlite3_file_control(conn, NULL, SQLITE_FCNTL_FILE_POINTER, &file);
	assert(rv == SQLITE_OK);
	struct vfsMainFile *f = (struct vfsMainFile*)file;

	assert(snapshot->wal.page_count == 0);

	tracef("restore %s of %zd pages", f->database->name, snapshot->main.page_count);

	/* Lock the database. The locking scheme here is similar to the one used
	 * when transitioning from WAL to DELETE mode. The WAL-Index recovery is
	 * not enough as it lets connections read the content of the main file.
	 * This means that all WAL-Index locks must be held exclusively,
	 * including READ(0) to simulate a hard lock on the file.
	 */
	rv = vfsShmLock(&f->database->shm, 0, SQLITE_SHM_NLOCK, true);
	if (rv != SQLITE_OK) {
		return rv;
	}

	rv = vfsDatabaseRestore(f->database, &snapshot->main);
	if (rv != SQLITE_OK) {
		tracef("database restore failed %d", rv);
		goto err_locked;
	}

	for (int i = 0; i < f->mappedShmRegions.len; i++) {
		memset(f->mappedShmRegions.ptr[i], 0, VFS__WAL_INDEX_REGION_SIZE);
	}

	vfsWalClose(&f->database->wal);
	vfsWalInit(&f->database->wal);

err_locked:
	vfsShmUnlock(&f->database->shm, 0, SQLITE_SHM_NLOCK, true);
	return rv;
}

uint64_t VfsDatabaseSize(sqlite3_vfs *vfs,
			 const char *path,
			 unsigned n,
			 unsigned page_size)
{
	struct vfs *v;
	struct vfsDatabase *database;
	struct vfsWal *wal;
	uint64_t new_wal_size;

	v = (struct vfs *)(vfs->pAppData);
	database = vfsDatabaseLookup(v, path);
	assert(database != NULL);

	wal = &database->wal;
	new_wal_size = (uint64_t)vfsWalSize(wal);
	if (new_wal_size == 0) {
		new_wal_size += (uint64_t)VFS__WAL_HEADER_SIZE;
	}
	new_wal_size += (uint64_t)n * (uint64_t)vfsFrameSize(page_size);
	return (uint64_t)vfsDatabaseFileSize(database) + new_wal_size;
}

uint64_t VfsDatabaseSizeLimit(sqlite3_vfs *vfs)
{
	(void)vfs;
	return (uint64_t)SIZE_MAX;
}
