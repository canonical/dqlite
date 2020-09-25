#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include <raft.h>

#include <sqlite3.h>

#include "../include/dqlite.h"

#include "lib/assert.h"

#include "format.h"
#include "vfs.h"

/* Byte order */
#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __LITTLE_ENDIAN__)
#define VFS__BIGENDIAN 0
#elif defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __BIG_ENDIAN__)
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

/* Write ahead log header size. */
#define VFS__WAL_HEADER_SIZE 32

/* Write ahead log frame header size. */
#define VFS__FRAME_HEADER_SIZE 24

/* Size of the first part of the WAL index header. */
#define VFS__WAL_INDEX_HEADER_SIZE 48

/* Size of a single memory-mapped WAL index region. */
#define VFS__WAL_INDEX_REGION_SIZE 32768

#define vfsFrameSize(PAGE_SIZE) (VFS__FRAME_HEADER_SIZE + PAGE_SIZE)

/* Hold content for a shared memory mapping. */
struct vfsShm
{
	void **regions;     /* Pointers to shared memory regions. */
	unsigned n_regions; /* Number of shared memory regions. */
	unsigned refcount;  /* Number of outstanding mappings. */
	unsigned shared[SQLITE_SHM_NLOCK];    /* Count of shared locks */
	unsigned exclusive[SQLITE_SHM_NLOCK]; /* Count of exclusive locks */
};

/* Hold the content of a single WAL frame. */
struct vfsFrame
{
	uint8_t header[VFS__FRAME_HEADER_SIZE];
	uint8_t *page; /* Content of the page. */
};

/* WAL-specific content */
struct vfsWal
{
	uint8_t hdr[VFS__WAL_HEADER_SIZE]; /* Header. */
	struct vfsFrame **frames;          /* All frames committed. */
	unsigned n_frames;                 /* Number of committed frames. */
	struct vfsFrame **tx;              /* Frames added by a transaction. */
	unsigned n_tx;                     /* Number of added frames. */
};

/* Database-specific content */
struct vfsDatabase
{
	char *name;        /* Database name. */
	void **pages;      /* All database. */
	unsigned n_pages;  /* Number of pages. */
	struct vfsShm shm; /* Shared memory. */
	struct vfsWal wal; /* Associated WAL. */
};

/* Flip a 16-bit number back and forth to or from big-endian representation. */
static uint16_t vfsFlip16(uint16_t v)
{
#if defined(__BYTE_ORDER) && (__BYTE_ORDER == __BIG_ENDIAN)
	return v;
#elif defined(__BYTE_ORDER) && (__BYTE_ORDER == __LITTLE_ENDIAN) && \
    defined(__GNUC__) && __GNUC__ >= 4 && __GNUC_MINOR__ >= 8
	return __builtin_bswap16(v);
#else
	union {
		uint16_t u;
		uint8_t v[4];
	} s;

	s.v[0] = (uint8_t)(v >> 8);
	s.v[1] = (uint8_t)v;

	return s.u;
#endif
}

/* Flip a 32-bit number back and forth to or from big-endian representation. */
static uint32_t vfsFlip32(uint32_t v)
{
#if defined(__BYTE_ORDER) && (__BYTE_ORDER == __BIG_ENDIAN)
	return v;
#elif defined(__BYTE_ORDER) && (__BYTE_ORDER == __LITTLE_ENDIAN) && \
    defined(__GNUC__) && __GNUC__ >= 4 && __GNUC_MINOR__ >= 8
	return __builtin_bswap32(v);
#else
	union {
		uint32_t u;
		uint8_t v[4];
	} s;

	s.v[0] = (uint8_t)(v >> 24);
	s.v[1] = (uint8_t)(v >> 16);
	s.v[2] = (uint8_t)(v >> 8);
	s.v[3] = (uint8_t)v;

	return s.u;
#endif
}

/* Load a 16-bit number stored in big-endian representation. */
static uint32_t vfsGet16(const uint8_t *buf)
{
	return vfsFlip16(*(const uint16_t *)buf);
}

/* Load a 32-bit number stored in big-endian representation. */
static uint32_t vfsGet32(const uint8_t *buf)
{
	return vfsFlip32(*(const uint32_t *)buf);
}

/* Store a 32-bit number in big endian format */
static void vfsPut32(uint32_t v, uint8_t *buf)
{
	uint32_t u = vfsFlip32(v);
	memcpy(buf, &u, sizeof u);
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
	uint32_t s1, s2;
	uint32_t *cur = (uint32_t *)data;
	uint32_t *end = (uint32_t *)&data[n];

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

/* Destroy a WAL frame */
static void vfsFrameDestroy(struct vfsFrame *f)
{
	assert(f != NULL);
	assert(f->page != NULL);

	sqlite3_free(f->page);
	sqlite3_free(f);
}

/* Initialize the shared memory mapping of a database file. */
static void vfsShmInit(struct vfsShm *s)
{
	int i;

	s->regions = NULL;
	s->n_regions = 0;
	s->refcount = 0;

	for (i = 0; i < SQLITE_SHM_NLOCK; i++) {
		s->shared[i] = 0;
		s->exclusive[i] = 0;
	}
}

/* Release all resources used by a shared memory mapping. */
static void vfsShmClose(struct vfsShm *s)
{
	void *region;
	unsigned i;

	assert(s != NULL);

	/* Free all regions. */
	for (i = 0; i < s->n_regions; i++) {
		region = *(s->regions + i);
		assert(region != NULL);
		sqlite3_free(region);
	}

	/* Free the shared memory region array. */
	if (s->regions != NULL) {
		sqlite3_free(s->regions);
	}
}

/* Revert the shared mamory to its initial state. */
static void vfsShmReset(struct vfsShm *s)
{
	vfsShmClose(s);
	vfsShmInit(s);
}

/* Initialize a new WAL object. */
static void vfsWalInit(struct vfsWal *w)
{
	memset(w->hdr, 0, VFS__WAL_HEADER_SIZE);
	w->frames = NULL;
	w->n_frames = 0;
	w->tx = NULL;
	w->n_tx = 0;
}

/* Initialize a new database object. */
static void vfsDatabaseInit(struct vfsDatabase *d)
{
	d->pages = NULL;
	d->n_pages = 0;
	vfsShmInit(&d->shm);
	vfsWalInit(&d->wal);
}

/* Release all memory used by a WAL object. */
static void vfsWalClose(struct vfsWal *w)
{
	unsigned i;
	for (i = 0; i < w->n_frames; i++) {
		vfsFrameDestroy(w->frames[i]);
	}
	if (w->frames != NULL) {
		sqlite3_free(w->frames);
	}
	for (i = 0; i < w->n_tx; i++) {
		vfsFrameDestroy(w->tx[i]);
	}
	if (w->tx != NULL) {
		sqlite3_free(w->tx);
	}
}

/* Release all memory used by a database object. */
static void vfsDatabaseClose(struct vfsDatabase *d)
{
	unsigned i;
	for (i = 0; i < d->n_pages; i++) {
		sqlite3_free(d->pages[i]);
	}
	if (d->pages != NULL) {
		sqlite3_free(d->pages);
	}
	vfsShmClose(&d->shm);
	vfsWalClose(&d->wal);
}

/* Destroy the content of a database object. */
static void vfsDatabaseDestroy(struct vfsDatabase *d)
{
	assert(d != NULL);

	sqlite3_free(d->name);

	vfsDatabaseClose(d);
	sqlite3_free(d);
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
	 * one page after the end. */
	if (pgno > d->n_pages + 1) {
		rc = SQLITE_IOERR_WRITE;
		goto err;
	}

	if (pgno == d->n_pages + 1) {
		/* Create a new page, grow the page array, and append the
		 * new page to it. */
		void **pages; /* New page array. */

		*page = sqlite3_malloc64(page_size);
		if (*page == NULL) {
			rc = SQLITE_NOMEM;
			goto err;
		}

		pages = sqlite3_realloc64(d->pages, sizeof *pages * pgno);
		if (pages == NULL) {
			rc = SQLITE_NOMEM;
			goto err_after_vfs_page_create;
		}

		/* Append the new page to the new page array. */
		*(pages + pgno - 1) = *page;

		/* Update the page array. */
		d->pages = pages;
		d->n_pages = pgno;
	} else {
		/* Return the existing page. */
		assert(d->pages != NULL);
		*page = d->pages[pgno - 1];
	}

	return SQLITE_OK;

err_after_vfs_page_create:
	sqlite3_free(*page);
err:
	*page = NULL;
	return rc;
}

/* Get a frame from the current transaction, possibly creating a new one. */
static int vfsWalFrameGet(struct vfsWal *w,
			  unsigned index,
			  uint32_t page_size,
			  struct vfsFrame **frame)
{
	int rv;

	assert(w != NULL);
	assert(index > 0);

	/* SQLite should access pages progressively, without jumping more than
	 * one page after the end. */
	if (index > w->n_frames + w->n_tx + 1) {
		rv = SQLITE_IOERR_WRITE;
		goto err;
	}

	if (index == w->n_frames + w->n_tx + 1) {
		/* Create a new frame, grow the transaction array, and append
		 * the new frame to it. */
		struct vfsFrame **tx;

		/* We assume that the page size has been set, either by
		 * intervepting the first main database file write, or by
		 * handling a 'PRAGMA page_size=N' command in
		 * vfs__file_control(). This assumption is enforved in
		 * vfsFileWrite(). */
		assert(page_size > 0);

		*frame = vfsFrameCreate(page_size);
		if (*frame == NULL) {
			rv = SQLITE_NOMEM;
			goto err;
		}

		tx = sqlite3_realloc64(w->tx, sizeof *tx * w->n_tx + 1);
		if (tx == NULL) {
			rv = SQLITE_NOMEM;
			goto err_after_vfs_frame_create;
		}

		/* Append the new page to the new page array. */
		tx[index - w->n_frames - 1] = *frame;

		/* Update the page array. */
		w->tx = tx;
		w->n_tx++;
	} else {
		/* Return the existing page. */
		assert(w->tx != NULL);
		*frame = w->tx[index - w->n_frames - 1];
	}

	return SQLITE_OK;

err_after_vfs_frame_create:
	vfsFrameDestroy(*frame);
err:
	*frame = NULL;
	return rv;
}

/* Lookup a page from the given database, returning NULL if it doesn't exist. */
static void *vfsDatabasePageLookup(struct vfsDatabase *d, unsigned pgno)
{
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

/* Lookup a frame from the WAL, returning NULL if it doesn't exist. */
static struct vfsFrame *vfsWalFrameLookup(struct vfsWal *w, unsigned n)
{
	struct vfsFrame *frame;

	assert(w != NULL);
	assert(n > 0);

	if (n > w->n_frames + w->n_tx) {
		/* This page hasn't been written yet. */
		return NULL;
	}
	if (n <= w->n_frames) {
		frame = w->frames[n - 1];
	} else {
		frame = w->tx[n - w->n_frames - 1];
	}

	assert(frame != NULL);

	return frame;
}

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

static uint32_t vfsDatabaseGetPageSize(struct vfsDatabase *d)
{
	uint8_t *page;

	assert(d->n_pages > 0);

	page = d->pages[0];

	/* The page size is stored in the 16th and 17th bytes of the first
	 * database page (big-endian) */
	return vfsParsePageSize(vfsGet16(&page[16]));
}

/* Truncate a database file to be exactly the given number of pages. */
static int vfsDatabaseTruncate(struct vfsDatabase *d, sqlite_int64 size)
{
	void **cursor;
	uint32_t page_size;
	unsigned n_pages;
	unsigned i;

	if (d->n_pages == 0) {
		if (size > 0) {
			return SQLITE_IOERR_TRUNCATE;
		}
		return SQLITE_OK;
	}

	/* Since the file size is not zero, some content must
	 * have been written and the page size must be known. */
	page_size = vfsDatabaseGetPageSize(d);
	assert(page_size > 0);

	if ((size % page_size) != 0) {
		return SQLITE_IOERR_TRUNCATE;
	}

	n_pages = (unsigned)(size / page_size);

	/* We expect callers to only invoke us if some actual content has been
	 * written already. */
	assert(d->n_pages > 0);

	/* Truncate should always shrink a file. */
	assert(n_pages <= d->n_pages);
	assert(d->pages != NULL);

	/* Destroy pages beyond pages_len. */
	cursor = d->pages + n_pages;
	for (i = 0; i < (d->n_pages - n_pages); i++) {
		sqlite3_free(*cursor);
		cursor++;
	}

	/* Shrink the page array, possibly to 0.
	 *
	 * TODO: in principle realloc could fail also when shrinking. */
	d->pages = sqlite3_realloc64(d->pages, sizeof *d->pages * n_pages);

	/* Update the page count. */
	d->n_pages = n_pages;

	return SQLITE_OK;
}

/* Truncate a WAL file to zero. */
static int vfsWalTruncate(struct vfsWal *w, sqlite3_int64 size)
{
	unsigned i;

	/* We expect SQLite to only truncate to zero, after a
	 * full checkpoint.
	 *
	 * TODO: figure out other case where SQLite might
	 * truncate to a different size.
	 */
	if (size != 0) {
		return SQLITE_PROTOCOL;
	}

	if (w->n_frames == 0) {
		return SQLITE_OK;
	}

	assert(w->frames != NULL);

	/* Restart the header. */
	formatWalRestartHeader(w->hdr);

	/* Destroy all frames. */
	for (i = 0; i < w->n_frames; i++) {
		vfsFrameDestroy(w->frames[i]);
	}
	sqlite3_free(w->frames);

	w->frames = NULL;
	w->n_frames = 0;

	return SQLITE_OK;
}

enum vfsFileType {
	VFS__DATABASE, /* Main database file */
	VFS__JOURNAL,  /* Default SQLite journal file */
	VFS__WAL       /* Write-Ahead Log */
};

/* Implementation of the abstract sqlite3_file base class. */
struct vfsFile
{
	sqlite3_file base;            /* Base class. Must be first. */
	struct vfs *vfs;              /* Pointer to volatile VFS data. */
	enum vfsFileType type;        /* Associated file (main db or WAL). */
	struct vfsDatabase *database; /* Underlying database content. */
	int flags;                    /* Flags passed to xOpen */
	sqlite3_file *temp;           /* For temp-files, actual VFS. */
};

/* Custom dqlite VFS. Contains pointers to all databases that were created. */
struct vfs
{
	struct vfsDatabase **databases; /* Database objects */
	unsigned n_databases;           /* Number of databases */
	int error;                      /* Last error occurred. */
};

/* Create a new vfs object. */
static struct vfs *vfsCreate(void)
{
	struct vfs *v;

	v = sqlite3_malloc(sizeof *v);
	if (v == NULL) {
		return NULL;
	}

	v->databases = NULL;
	v->n_databases = 0;

	return v;
}

/* Release the memory used internally by the VFS object.
 *
 * All file content will be de-allocated, so dangling open FDs against
 * those files will be broken.
 */
static void vfsDestroy(struct vfs *r)
{
	unsigned i;

	assert(r != NULL);

	for (i = 0; i < r->n_databases; i++) {
		struct vfsDatabase *database = r->databases[i];
		vfsDatabaseDestroy(database);
	}

	if (r->databases != NULL) {
		sqlite3_free(r->databases);
	}
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
		if (strncmp(database->name, filename, n) == 0) {
			// Found matching file.
			return database;
		}
	}

	return NULL;
}

static int vfsDeleteDatabase(struct vfs *r, const char *name)
{
	unsigned i;

	for (i = 0; i < r->n_databases; i++) {
		struct vfsDatabase *database = r->databases[i];
		unsigned j;

		if (strcmp(database->name, name) != 0) {
			continue;
		}

		/* Free all memory allocated for this file. */
		vfsDatabaseDestroy(database);

		/* Shift all other contents objects. */
		for (j = i + 1; j < r->n_databases; j++) {
			r->databases[j - 1] = r->databases[j];
		}
		r->n_databases--;

		return SQLITE_OK;
	}

	r->error = ENOENT;
	return SQLITE_IOERR_DELETE_NOENT;
}

static int vfsFileClose(sqlite3_file *file)
{
	int rc = SQLITE_OK;
	struct vfsFile *f = (struct vfsFile *)file;
	struct vfs *v = (struct vfs *)(f->vfs);

	if (f->temp != NULL) {
		/* Close the actual temporary file. */
		rc = f->temp->pMethods->xClose(f->temp);
		sqlite3_free(f->temp);

		return rc;
	}

	if (f->flags & SQLITE_OPEN_DELETEONCLOSE) {
		rc = vfsDeleteDatabase(v, f->database->name);
	}

	return rc;
}

/* Read data from the main database. */
static int vfsDatabaseRead(struct vfsDatabase *d,
			   void *buf,
			   int amount,
			   sqlite_int64 offset)
{
	unsigned page_size;
	unsigned pgno;
	void *page;

	if (d->n_pages == 0) {
		return SQLITE_IOERR_SHORT_READ;
	}

	/* If the main database file is not empty, we expect the
	 * page size to have been set by an initial write. */
	page_size = vfsDatabaseGetPageSize(d);
	assert(page_size > 0);

	if (offset < page_size) {
		/* Reading from page 1. We expect the read to be
		 * at most page_size bytes. */
		assert(amount <= (int)page_size);
		pgno = 1;
	} else {
		/* For pages greater than 1, we expect a full
		 * page read, with an offset that starts exectly
		 * at the page boundary. */
		assert(amount == (int)page_size);
		assert(((unsigned)offset % page_size) == 0);
		pgno = ((unsigned)offset / page_size) + 1;
	}

	assert(pgno > 0);

	page = vfsDatabasePageLookup(d, pgno);

	if (pgno == 1) {
		/* Read the desired part of page 1. */
		memcpy(buf, page + offset, (size_t)amount);
	} else {
		/* Read the full page. */
		memcpy(buf, page, (size_t)amount);
	}

	return SQLITE_OK;
}

/* Get the page size stored in the WAL header. */
static uint32_t vfsWalGetPageSize(struct vfsWal *w)
{
	/* The page size is stored in the 4 bytes starting at 8
	 * (big-endian) */
	return vfsParsePageSize(vfsGet32(&w->hdr[8]));
}

/* Read data from the WAL. */
static int vfsWalRead(struct vfsWal *w,
		      void *buf,
		      int amount,
		      sqlite_int64 offset)
{
	uint32_t page_size;
	unsigned index;
	struct vfsFrame *frame;

	if (w->n_frames == 0) {
		return SQLITE_IOERR_SHORT_READ;
	}

	if (offset == 0) {
		/* Read the header. */
		assert(amount == VFS__WAL_HEADER_SIZE);
		memcpy(buf, w->hdr, VFS__WAL_HEADER_SIZE);
		return SQLITE_OK;
	}

	page_size = vfsWalGetPageSize(w);
	assert(page_size > 0);

	/* For any other frame, we expect either a header read,
	 * a checksum read, a page read or a full frame read. */
	if (amount == FORMAT__WAL_FRAME_HDR_SIZE) {
		assert(((offset - VFS__WAL_HEADER_SIZE) %
			(page_size + FORMAT__WAL_FRAME_HDR_SIZE)) == 0);
		index = formatWalCalcFrameIndex(page_size, (unsigned)offset);
	} else if (amount == sizeof(uint32_t) * 2) {
		if (offset == FORMAT__WAL_FRAME_HDR_SIZE) {
			/* Read the checksum from the WAL
			 * header. */
			memcpy(buf, w->hdr + offset, (size_t)amount);
			return SQLITE_OK;
		}
		assert(((offset - 16 - VFS__WAL_HEADER_SIZE) %
			(page_size + FORMAT__WAL_FRAME_HDR_SIZE)) == 0);
		index = ((unsigned)offset - 16 - VFS__WAL_HEADER_SIZE) /
			    (page_size + FORMAT__WAL_FRAME_HDR_SIZE) +
			1;
	} else if (amount == (int)page_size) {
		assert(((offset - VFS__WAL_HEADER_SIZE -
			 FORMAT__WAL_FRAME_HDR_SIZE) %
			(page_size + FORMAT__WAL_FRAME_HDR_SIZE)) == 0);
		index = formatWalCalcFrameIndex(page_size, (unsigned)offset);
	} else {
		assert(amount == (FORMAT__WAL_FRAME_HDR_SIZE + (int)page_size));
		index = formatWalCalcFrameIndex(page_size, (unsigned)offset);
	}

	if (index == 0) {
		// This is an attempt to read a page that was
		// never written.
		memset(buf, 0, (size_t)amount);
		return SQLITE_IOERR_SHORT_READ;
	}

	frame = vfsWalFrameLookup(w, index);

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

static int vfsFileRead(sqlite3_file *file,
		       void *buf,
		       int amount,
		       sqlite_int64 offset)
{
	struct vfsFile *f = (struct vfsFile *)file;
	int rv;

	assert(buf != NULL);
	assert(amount > 0);
	assert(f != NULL);

	if (f->temp != NULL) {
		/* Read from the actual temporary file. */
		return f->temp->pMethods->xRead(f->temp, buf, amount, offset);
	}

	switch (f->type) {
		case VFS__DATABASE:
			rv = vfsDatabaseRead(f->database, buf, amount, offset);
			break;
		case VFS__WAL:
			rv = vfsWalRead(&f->database->wal, buf, amount, offset);
			break;
		default:
			rv = SQLITE_IOERR_READ;
			break;
	}

	/* From SQLite docs:
	 *
	 *   If xRead() returns SQLITE_IOERR_SHORT_READ it must also fill
	 *   in the unread portions of the buffer with zeros.  A VFS that
	 *   fails to zero-fill short reads might seem to work.  However,
	 *   failure to zero-fill short reads will eventually lead to
	 *   database corruption.
	 */
	if (rv == SQLITE_IOERR_SHORT_READ) {
		memset(buf, 0, (size_t)amount);
	}

	return rv;
}

static int vfsDatabaseWrite(struct vfsDatabase *d,
			    const void *buf,
			    int amount,
			    sqlite_int64 offset)
{
	unsigned pgno;
	uint32_t page_size;
	void *page;
	int rc;

	if (offset == 0) {
		const uint8_t *header = buf;

		/* This is the first database page. We expect
		 * the data to contain at least the header. */
		assert(amount >= FORMAT__DB_HDR_SIZE);

		/* Extract the page size from the header. */
		page_size = vfsParsePageSize(vfsGet16(&header[16]));
		if (page_size == 0) {
			return SQLITE_CORRUPT;
		}

		pgno = 1;
	} else {
		page_size = vfsDatabaseGetPageSize(d);

		/* The header must have been written and the page size set. */
		assert(page_size > 0);

		/* For pages beyond the first we expect offset to be a multiple
		 * of the page size. */
		assert((offset % page_size) == 0);

		/* We expect that SQLite writes a page at time. */
		assert(amount == (int)page_size);

		pgno = ((unsigned)offset / page_size) + 1;
	}

	rc = vfsDatabaseGetPage(d, page_size, pgno, &page);
	if (rc != SQLITE_OK) {
		return rc;
	}

	assert(page != NULL);

	memcpy(page, buf, (size_t)amount);

	return SQLITE_OK;
}

static int vfsWalWrite(struct vfsWal *w,
		       const void *buf,
		       int amount,
		       sqlite_int64 offset)
{
	uint32_t page_size;
	unsigned index;
	struct vfsFrame *frame;

	/* WAL header. */
	if (offset == 0) {
		/* We expect the data to contain exactly 32
		 * bytes. */
		assert(amount == VFS__WAL_HEADER_SIZE);

		memcpy(w->hdr, buf, (size_t)amount);
		return SQLITE_OK;
	}

	page_size = vfsWalGetPageSize(w);
	assert(page_size > 0);

	/* This is a WAL frame write. We expect either a frame
	 * header or page write. */
	if (amount == FORMAT__WAL_FRAME_HDR_SIZE) {
		/* Frame header write. */
		assert(((offset - VFS__WAL_HEADER_SIZE) %
			(page_size + FORMAT__WAL_FRAME_HDR_SIZE)) == 0);

		index = formatWalCalcFrameIndex(page_size, (unsigned)offset);

		vfsWalFrameGet(w, index, page_size, &frame);
		if (frame == NULL) {
			return SQLITE_NOMEM;
		}
		memcpy(frame->header, buf, (size_t)amount);
	} else {
		/* Frame page write. */
		assert(amount == (int)page_size);
		assert(((offset - VFS__WAL_HEADER_SIZE -
			 FORMAT__WAL_FRAME_HDR_SIZE) %
			(page_size + FORMAT__WAL_FRAME_HDR_SIZE)) == 0);

		index = formatWalCalcFrameIndex(page_size, (unsigned)offset);

		/* The header for the this frame must already
		 * have been written, so the page is there. */
		frame = vfsWalFrameLookup(w, index);

		assert(frame != NULL);

		memcpy(frame->page, buf, (size_t)amount);
	}

	return SQLITE_OK;
}

static int vfsFileWrite(sqlite3_file *file,
			const void *buf,
			int amount,
			sqlite_int64 offset)
{
	struct vfsFile *f = (struct vfsFile *)file;
	int rv;

	assert(buf != NULL);
	assert(amount > 0);
	assert(f != NULL);

	if (f->temp != NULL) {
		/* Write to the actual temporary file. */
		return f->temp->pMethods->xWrite(f->temp, buf, amount, offset);
	}

	switch (f->type) {
		case VFS__DATABASE:
			rv = vfsDatabaseWrite(f->database, buf, amount, offset);
			break;
		case VFS__WAL:
			rv =
			    vfsWalWrite(&f->database->wal, buf, amount, offset);
			break;
		case VFS__JOURNAL:
			/* Silently swallow writes to the journal */
			rv = SQLITE_OK;
			break;
		default:
			rv = SQLITE_IOERR_WRITE;
			break;
	}

	return rv;
}

static int vfsFileTruncate(sqlite3_file *file, sqlite_int64 size)
{
	struct vfsFile *f = (struct vfsFile *)file;
	int rv;

	assert(f != NULL);

	switch (f->type) {
		case VFS__DATABASE:
			rv = vfsDatabaseTruncate(f->database, size);
			break;

		case VFS__WAL:
			rv = vfsWalTruncate(&f->database->wal, size);
			break;

		default:
			rv = SQLITE_IOERR_TRUNCATE;
			break;
	}

	return rv;
}

static int vfsFileSync(sqlite3_file *file, int flags)
{
	(void)file;
	(void)flags;

	return SQLITE_IOERR_FSYNC;
}

/* Return the size of the database file in bytes. */
static size_t vfsDatabaseFileSize(struct vfsDatabase *d)
{
	size_t size = 0;
	if (d->n_pages > 0) {
		size = d->n_pages * vfsDatabaseGetPageSize(d);
	}
	return size;
}

/* Return the size of the WAL file in bytes. */
static size_t vfsWalFileSize(struct vfsWal *w)
{
	size_t size = 0;
	if (w->n_frames > 0) {
		uint32_t page_size;
		page_size = vfsWalGetPageSize(w);
		size += VFS__WAL_HEADER_SIZE;
		size += w->n_frames * (FORMAT__WAL_FRAME_HDR_SIZE + page_size);
	}
	return size;
}

static int vfsFileSize(sqlite3_file *file, sqlite_int64 *size)
{
	struct vfsFile *f = (struct vfsFile *)file;
	size_t n;

	switch (f->type) {
		case VFS__DATABASE:
			n = vfsDatabaseFileSize(f->database);
			break;
		case VFS__WAL:
			/* TODO? here we assume that FileSize() is never invoked
			 * between a header write and a page write. */
			n = vfsWalFileSize(&f->database->wal);
			break;
		default:
			n = 0;
			break;
	}

	*size = (sqlite3_int64)n;

	return SQLITE_OK;
}

/* Locking a file is a no-op, since no other process has visibility on it. */
static int vfsFileLock(sqlite3_file *file, int lock)
{
	(void)file;
	(void)lock;

	return SQLITE_OK;
}

/* Unlocking a file is a no-op, since no other process has visibility on it. */
static int vfsFileUnlock(sqlite3_file *file, int lock)
{
	(void)file;
	(void)lock;

	return SQLITE_OK;
}

/* We always report that a lock is held. This routine should be used only in
 * journal mode, so it doesn't matter. */
static int vfsFileCheckReservedLock(sqlite3_file *file, int *result)
{
	(void)file;

	*result = 1;
	return SQLITE_OK;
}

/* Handle pragma a pragma file control. See the xFileControl
 * docstring in sqlite.h.in for more details. */
static int vfsFileControlPragma(struct vfsFile *f, char **fnctl)
{
	const char *left;
	const char *right;

	assert(f != NULL);
	assert(fnctl != NULL);

	left = fnctl[1];
	right = fnctl[2];

	assert(left != NULL);

	if (strcmp(left, "page_size") == 0 && right) {
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
		int page_size = atoi(right);

		if (page_size >= FORMAT__PAGE_SIZE_MIN &&
		    page_size <= FORMAT__PAGE_SIZE_MAX &&
		    ((page_size - 1) & page_size) == 0) {
			if (f->database->n_pages > 0 &&
			    page_size !=
				(int)vfsDatabaseGetPageSize(f->database)) {
				fnctl[0] =
				    "changing page size is not supported";
				return SQLITE_IOERR;
			}
		}
	} else if (strcmp(left, "journal_mode") == 0 && right) {
		/* When the user executes 'PRAGMA journal_mode=x' we ensure
		 * that the desired mode is 'wal'. */
		if (strcasecmp(right, "wal") != 0) {
			fnctl[0] = "only WAL mode is supported";
			return SQLITE_IOERR;
		}
	}

	/* We're returning NOTFOUND here to tell SQLite that we wish it to go on
	 * with its own handling as well. If we returned SQLITE_OK the page size
	 * of the journal mode wouldn't be effectively set, as the processing of
	 * the PRAGMA would stop here. */
	return SQLITE_NOTFOUND;
}

/* Return the page number field stored in the header of the given frame. */
static uint32_t vfsFrameGetPageNumber(struct vfsFrame *f)
{
	return vfsGet32(&f->header[0]);
}

/* Return the database size field stored in the header of the given frame. */
static uint32_t vfsFrameGetDatabaseSize(struct vfsFrame *f)
{
	return vfsGet32(&f->header[4]);
}

/* Return the checksum-1 field stored in the header of the given frame. */
static uint32_t vfsFrameGetChecksum1(struct vfsFrame *f)
{
	return vfsGet32(&f->header[16]);
}

/* Return the checksum-2 field stored in the header of the given frame. */
static uint32_t vfsFrameGetChecksum2(struct vfsFrame *f)
{
	return vfsGet32(&f->header[20]);
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
	vfsPut32(page_number, &f->header[0]);
	vfsPut32(database_size, &f->header[4]);

	vfsChecksum(f->header, 8, checksum, checksum);
	vfsChecksum(page, page_size, checksum, checksum);

	memcpy(&f->header[8], &salt[0], sizeof salt[0]);
	memcpy(&f->header[12], &salt[1], sizeof salt[1]);

	vfsPut32(checksum[0], &f->header[16]);
	vfsPut32(checksum[1], &f->header[20]);

	memcpy(f->page, page, page_size);
}

/* This function modifies part of the WAL index header to reflect the current
 * content of the WAL.
 *
 * It is called in two cases. First, after a write transaction gets completed
 * and the SQLITE_FCNTL_COMMIT_PHASETWO file control op code is triggered, in
 * order to "rewind" the mxFrame and szPage fields of the WAL index header back
 * to when the write transaction started, effectively "shadowing" the
 * transaction, which will be replicated asynchronously. Second, when the
 * replication actually succeeds and dqlite_vfs_apply() is called on the VFS
 * that originated the transaction, in order to make the transaction visible.
 *
 * Note that the hash table contained in the WAL index does not get modified,
 * and even after a rewind following a write transaction it will still contain
 * entries for the frames committed by the transaction. That's safe because
 * mxFrame will make clients ignore those hash table entries. However it means
 * that in case the replication is not actually successful and
 * dqlite_vfs_abort() is called the WAL index must be invalidated.
 **/
static void vfsAmendWalIndexHeader(struct vfsDatabase *d)
{
	struct vfsShm *shm = &d->shm;
	struct vfsWal *wal = &d->wal;
	uint8_t *index;
	uint32_t frame_checksum[2] = {0, 0};
	uint32_t n_pages = (uint32_t)d->n_pages;
	uint32_t checksum[2] = {0, 0};

	if (wal->n_frames > 0) {
		struct vfsFrame *last = wal->frames[wal->n_frames - 1];
		frame_checksum[0] = vfsFrameGetChecksum1(last);
		frame_checksum[1] = vfsFrameGetChecksum2(last);
		n_pages = vfsFrameGetDatabaseSize(last);
	}

	assert(shm->n_regions > 0);
	index = shm->regions[0];

	assert(*(uint32_t *)(&index[0]) == VFS__WAL_VERSION); /* iVersion */
	assert(index[12] == 1);                               /* isInit */
	assert(index[13] == VFS__BIGENDIAN);                  /* bigEndCksum */

	*(uint32_t *)(&index[16]) = wal->n_frames;
	*(uint32_t *)(&index[20]) = n_pages;
	*(uint32_t *)(&index[24]) = frame_checksum[0];
	*(uint32_t *)(&index[28]) = frame_checksum[1];

	vfsChecksum(index, 40, checksum, checksum);

	*(uint32_t *)(&index[40]) = checksum[0];
	*(uint32_t *)(&index[44]) = checksum[1];

	/* Update the second copy of the first part of the WAL index header. */
	memcpy(index + VFS__WAL_INDEX_HEADER_SIZE, index,
	       VFS__WAL_INDEX_HEADER_SIZE);
}

/* The SQLITE_FCNTL_COMMIT_PHASETWO file control op code is trigged by the
 * SQLite pager after completing a transaction. */
static int vfsFileControlCommitPhaseTwo(struct vfsFile *f)
{
	struct vfsDatabase *database = f->database;
	struct vfsWal *wal = &database->wal;
	if (wal->n_tx > 0) {
		vfsAmendWalIndexHeader(database);
	}
	return 0;
}

static int vfsFileControl(sqlite3_file *file, int op, void *arg)
{
	struct vfsFile *f = (struct vfsFile *)file;
	int rv;

	assert(f->type == VFS__DATABASE);

	switch (op) {
		case SQLITE_FCNTL_PRAGMA:
			rv = vfsFileControlPragma(f, arg);
			break;
		case SQLITE_FCNTL_COMMIT_PHASETWO:
			rv = vfsFileControlCommitPhaseTwo(f);
			break;
		case SQLITE_FCNTL_PERSIST_WAL:
			/* This prevents SQLite from deleting the WAL after the
			 * last connection is closed. */
			*(int *)(arg) = 1;
			rv = SQLITE_OK;
			break;
		default:
			rv = SQLITE_OK;
			break;
	}

	return rv;
}

static int vfsFileSectorSize(sqlite3_file *file)
{
	(void)file;

	return 0;
}

static int vfsFileDeviceCharacteristics(sqlite3_file *file)
{
	(void)file;

	return 0;
}

static int vfsShmMap(struct vfsShm *s,
		     unsigned region_index,
		     unsigned region_size,
		     bool extend,
		     void volatile **out)
{
	void *region;
	int rv;

	if (s->regions != NULL && region_index < s->n_regions) {
		/* The region was already allocated. */
		region = s->regions[region_index];
		assert(region != NULL);
	} else {
		if (extend) {
			void **regions;

			/* We should grow the map one region at a time. */
			assert(region_size == VFS__WAL_INDEX_REGION_SIZE);
			assert(region_index == s->n_regions);
			region = sqlite3_malloc64(region_size);
			if (region == NULL) {
				rv = SQLITE_NOMEM;
				goto err;
			}

			memset(region, 0, region_size);

			regions = sqlite3_realloc64(
			    s->regions,
			    sizeof *s->regions * (s->n_regions + 1));

			if (regions == NULL) {
				rv = SQLITE_NOMEM;
				goto err_after_region_malloc;
			}

			s->regions = regions;
			s->regions[region_index] = region;
			s->n_regions++;

		} else {
			/* The region was not allocated and we don't have to
			 * extend the map. */
			region = NULL;
		}
	}

	*out = region;

	if (region_index == 0 && region != NULL) {
		s->refcount++;
	}

	return SQLITE_OK;

err_after_region_malloc:
	sqlite3_free(region);
err:
	assert(rv != SQLITE_OK);
	*out = NULL;
	return rv;
}

/* Simulate shared memory by allocating on the C heap. */
static int vfsFileShmMap(sqlite3_file *file, /* Handle open on database file */
			 int region_index,   /* Region to retrieve */
			 int region_size,    /* Size of regions */
			 int extend, /* True to extend file if necessary */
			 void volatile **out /* OUT: Mapped memory */
)
{
	struct vfsFile *f = (struct vfsFile *)file;

	assert(f->type == VFS__DATABASE);

	return vfsShmMap(&f->database->shm, (unsigned)region_index,
			 (unsigned)region_size, extend != 0, out);
}

static int vfsShmLock(struct vfsShm *s, int ofst, int n, int flags)
{
	int i;

	if (flags & SQLITE_SHM_EXCLUSIVE) {
		/* No shared or exclusive lock must be held in the region. */
		for (i = ofst; i < ofst + n; i++) {
			if (s->shared[i] > 0 || s->exclusive[i] > 0) {
				return SQLITE_BUSY;
			}
		}

		for (i = ofst; i < ofst + n; i++) {
			assert(s->exclusive[i] == 0);
			s->exclusive[i] = 1;
		}
	} else {
		/* No exclusive lock must be held in the region. */
		for (i = ofst; i < ofst + n; i++) {
			if (s->exclusive[i] > 0) {
				return SQLITE_BUSY;
			}
		}

		for (i = ofst; i < ofst + n; i++) {
			s->shared[i]++;
		}
	}

	return SQLITE_OK;
}

static int vfsShmUnlock(struct vfsShm *s, int ofst, int n, int flags)
{
	unsigned *these_locks;
	unsigned *other_locks;
	int i;

	if (flags & SQLITE_SHM_SHARED) {
		these_locks = s->shared;
		other_locks = s->exclusive;
	} else {
		these_locks = s->exclusive;
		other_locks = s->shared;
	}

	for (i = ofst; i < ofst + n; i++) {
		/* Sanity check that no lock of the other type is held in this
		 * region. */
		assert(other_locks[i] == 0);

		/* Only decrease the lock count if it's positive. In other words
		 * releasing a never acquired lock is legal and idemponent. */
		if (these_locks[i] > 0) {
			these_locks[i]--;
		}
	}

	return SQLITE_OK;
}

/* If there's a uncommitted transaction, roll it back. */
static void vfsWalRollbackIfUncommitted(struct vfsWal *w)
{
	struct vfsFrame *last;
	uint32_t commit;
	unsigned i;

	if (w->n_tx == 0) {
		return;
	}

	last = w->tx[w->n_tx - 1];
	commit = vfsFrameGetDatabaseSize(last);

	if (commit > 0) {
		return;
	}

	for (i = 0; i < w->n_tx; i++) {
		vfsFrameDestroy(w->tx[i]);
	}

	w->n_tx = 0;
}

static int vfsFileShmLock(sqlite3_file *file, int ofst, int n, int flags)
{
	struct vfsFile *f;
	struct vfsShm *shm;
	struct vfsWal *wal;
	int rv;

	assert(file != NULL);
	assert(ofst >= 0);
	assert(n >= 0);

	/* Legal values for the offset and the range */
	assert(ofst >= 0 && ofst + n <= SQLITE_SHM_NLOCK);
	assert(n >= 1);
	assert(n == 1 || (flags & SQLITE_SHM_EXCLUSIVE) != 0);

	/* Legal values for the flags.
	 *
	 * See https://sqlite.org/c3ref/c_shm_exclusive.html. */
	assert(flags == (SQLITE_SHM_LOCK | SQLITE_SHM_SHARED) ||
	       flags == (SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE) ||
	       flags == (SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED) ||
	       flags == (SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE));

	/* This is a no-op since shared-memory locking is relevant only for
	 * inter-process concurrency. See also the unix-excl branch from
	 * upstream (git commit cda6b3249167a54a0cf892f949d52760ee557129). */

	f = (struct vfsFile *)file;

	assert(f->type == VFS__DATABASE);
	assert(f->database != NULL);

	shm = &f->database->shm;
	if (flags & SQLITE_SHM_UNLOCK) {
		rv = vfsShmUnlock(shm, ofst, n, flags);
	} else {
		rv = vfsShmLock(shm, ofst, n, flags);
	}

	wal = &f->database->wal;
	if (rv == SQLITE_OK && ofst == VFS__WAL_WRITE_LOCK) {
		assert(n == 1);
		/* When acquiring the write lock, make sure there's no
		 * transaction that hasn't been rolled back or polled. */
		if (flags == (SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)) {
			assert(wal->n_tx == 0);
		}
		/* When releasing the write lock, if we find a pending
		 * uncommitted transaction then a rollback must have occurred.
		 * In that case we delete the pending transaction. */
		if (flags == (SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE)) {
			vfsWalRollbackIfUncommitted(wal);
		}
	}

	return rv;
}

static void vfsFileShmBarrier(sqlite3_file *file)
{
	(void)file;
	/* This is a no-op since we expect SQLite to be compiled with mutex
	 * support (i.e. SQLITE_MUTEX_OMIT or SQLITE_MUTEX_NOOP are *not*
	 * defined, see sqliteInt.h). */
}

static void vfsShmUnmap(struct vfsShm *s)
{
	s->refcount--;
	if (s->refcount == 0) {
		vfsShmReset(s);
	}
}

static int vfsFileShmUnmap(sqlite3_file *file, int delete_flag)
{
	struct vfsFile *f = (struct vfsFile *)file;
	(void)delete_flag;
	vfsShmUnmap(&f->database->shm);
	return SQLITE_OK;
}

static const sqlite3_io_methods vfsFileMethods = {
    2,                             // iVersion
    vfsFileClose,                  // xClose
    vfsFileRead,                   // xRead
    vfsFileWrite,                  // xWrite
    vfsFileTruncate,               // xTruncate
    vfsFileSync,                   // xSync
    vfsFileSize,                   // xFileSize
    vfsFileLock,                   // xLock
    vfsFileUnlock,                 // xUnlock
    vfsFileCheckReservedLock,      // xCheckReservedLock
    vfsFileControl,                // xFileControl
    vfsFileSectorSize,             // xSectorSize
    vfsFileDeviceCharacteristics,  // xDeviceCharacteristics
    vfsFileShmMap,                 // xShmMap
    vfsFileShmLock,                // xShmLock
    vfsFileShmBarrier,             // xShmBarrier
    vfsFileShmUnmap,               // xShmUnmap
    0,
    0,
};

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
		goto oom;
	}
	v->databases = databases;

	d = sqlite3_malloc(sizeof *d);
	if (d == NULL) {
		goto oom;
	}

	d->name = sqlite3_malloc64(strlen(name) + 1);
	if (d->name == NULL) {
		goto oom_after_database_malloc;
	}
	strcpy(d->name, name);

	vfsDatabaseInit(d);

	v->databases[n - 1] = d;
	v->n_databases = n;

	return d;

oom_after_database_malloc:
	sqlite3_free(d);
oom:
	return NULL;
}

static int vfsOpen(sqlite3_vfs *vfs,
		   const char *filename,
		   sqlite3_file *file,
		   int flags,
		   int *out_flags)
{
	struct vfs *v;
	struct vfsFile *f;
	struct vfsDatabase *database;
	enum vfsFileType type;
	bool exists;
	int exclusive = flags & SQLITE_OPEN_EXCLUSIVE;
	int create = flags & SQLITE_OPEN_CREATE;
	int rc;

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

	v = (struct vfs *)(vfs->pAppData);
	f = (struct vfsFile *)file;

	/* This tells SQLite to not call Close() in case we return an error. */
	f->base.pMethods = 0;
	f->temp = NULL;

	/* Save the flags */
	f->flags = flags;

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

		f->temp = sqlite3_malloc(vfs->szOsFile);
		if (f->temp == NULL) {
			v->error = ENOENT;
			return SQLITE_CANTOPEN;
		}
		rc = vfs->xOpen(vfs, NULL, f->temp, flags, out_flags);
		if (rc != SQLITE_OK) {
			sqlite3_free(f->temp);
			return rc;
		}

		f->base.pMethods = &vfsFileMethods;
		f->vfs = NULL;
		f->database = NULL;

		return SQLITE_OK;
	}

	/* Search if the database object exists already. */
	database = vfsDatabaseLookup(v, filename);
	exists = database != NULL;

	if (flags & SQLITE_OPEN_MAIN_DB) {
		type = VFS__DATABASE;
	} else if (flags & SQLITE_OPEN_MAIN_JOURNAL) {
		type = VFS__JOURNAL;
	} else if (flags & SQLITE_OPEN_WAL) {
		type = VFS__WAL;
	} else {
		v->error = ENOENT;
		return SQLITE_CANTOPEN;
	}

	/* If file exists, and the exclusive flag is on, return an error. */
	if (exists && exclusive && create && type == VFS__DATABASE) {
		v->error = EEXIST;
		rc = SQLITE_CANTOPEN;
		goto err;
	}

	if (!exists) {
		/* When opening a WAL or journal file we expect the main
		 * database file to have already been created. */
		if (type == VFS__WAL || type == VFS__JOURNAL) {
			v->error = ENOENT;
			rc = SQLITE_CANTOPEN;
			goto err;
		}

		assert(type == VFS__DATABASE);

		/* Check the create flag. */
		if (!create) {
			v->error = ENOENT;
			rc = SQLITE_CANTOPEN;
			goto err;
		}

		database = vfsCreateDatabase(v, filename);
		if (database == NULL) {
			v->error = ENOMEM;
			rc = SQLITE_CANTOPEN;
			goto err;
		}
	}

	/* Populate the new file handle. */
	f->base.pMethods = &vfsFileMethods;
	f->vfs = v;
	f->type = type;
	f->database = database;

	return SQLITE_OK;

err:
	assert(rc != SQLITE_OK);
	return rc;
}

static int vfsDelete(sqlite3_vfs *vfs, const char *filename, int dir_sync)
{
	struct vfs *v;

	(void)dir_sync;

	assert(vfs != NULL);
	assert(vfs->pAppData != NULL);

	if (vfsFilenameEndsWith(filename, "-journal")) {
		return SQLITE_OK;
	}
	if (vfsFilenameEndsWith(filename, "-wal")) {
		return SQLITE_OK;
	}

	v = (struct vfs *)(vfs->pAppData);

	return vfsDeleteDatabase(v, filename);
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
	(void)nByte;
	(void)zByte;

	/* TODO (is this needed?) */
	return SQLITE_OK;
}

static int vfsSleep(sqlite3_vfs *vfs, int microseconds)
{
	(void)vfs;

	/* TODO (is this needed?) */
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
	// TODO: check if it's always safe to cast a double* to a
	// sqlite3_int64*.
	return vfsCurrentTimeInt64(vfs, (sqlite3_int64 *)piNow);
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

static int vfsInit(struct sqlite3_vfs *vfs, const char *name)
{
	vfs->iVersion = 2;
	vfs->szOsFile = sizeof(struct vfsFile);
	vfs->mxPathname = VFS__MAX_PATHNAME;
	vfs->pNext = NULL;

	vfs->pAppData = vfsCreate();
	if (vfs->pAppData == NULL) {
		return DQLITE_NOMEM;
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

int VfsInit(struct sqlite3_vfs *vfs, const char *name)
{
	return vfsInit(vfs, name);
}

void VfsClose(struct sqlite3_vfs *vfs)
{
	struct vfs *v = vfs->pAppData;
	vfsDestroy(v);
	sqlite3_free(v);
}

static int vfsWalPoll(struct vfsWal *w, dqlite_vfs_frame **frames, unsigned *n)
{
	struct vfsFrame *last;
	uint32_t commit;
	unsigned i;

	if (w->n_tx == 0) {
		*frames = NULL;
		*n = 0;
		return 0;
	}

	/* Check if the last frame in the transaction has the commit marker. */
	last = w->tx[w->n_tx - 1];
	commit = vfsFrameGetDatabaseSize(last);

	if (commit == 0) {
		*frames = NULL;
		*n = 0;
		return 0;
	}

	*frames = sqlite3_malloc64(sizeof **frames * w->n_tx);
	if (*frames == NULL) {
		return DQLITE_NOMEM;
	}
	*n = w->n_tx;

	for (i = 0; i < w->n_tx; i++) {
		dqlite_vfs_frame *frame = &(*frames)[i];
		uint32_t page_number = vfsFrameGetPageNumber(w->tx[i]);
		frame->data = w->tx[i]->page;
		frame->page_number = page_number;
		/* Release the vfsFrame object, but not its buf attribute, since
		 * responsibility for that memory has been transferred to the
		 * caller. */
		sqlite3_free(w->tx[i]);
	}

	w->n_tx = 0;

	return 0;
}

int VfsPoll(sqlite3_vfs *vfs,
	    const char *filename,
	    dqlite_vfs_frame **frames,
	    unsigned *n)
{
	struct vfs *v;
	struct vfsDatabase *database;
	struct vfsShm *shm;
	struct vfsWal *wal;
	int rv;

	v = (struct vfs *)(vfs->pAppData);
	database = vfsDatabaseLookup(v, filename);

	if (database == NULL) {
		return DQLITE_ERROR;
	}

	shm = &database->shm;
	wal = &database->wal;

	if (wal == NULL) {
		*frames = NULL;
		*n = 0;
		return 0;
	}

	rv = vfsWalPoll(wal, frames, n);
	if (rv != 0) {
		return rv;
	}

	/* If some frames have been written take the write lock. */
	if (*n > 0) {
		rv = vfsShmLock(shm, 0, 1, SQLITE_SHM_EXCLUSIVE);
		if (rv != 0) {
			return rv;
		}
		vfsAmendWalIndexHeader(database);
	}

	return 0;
}

/* Return the salt-1 field stored in the WAL header.*/
static uint32_t vfsWalGetSalt1(struct vfsWal *w)
{
	return *(uint32_t *)(&w->hdr[16]);
}

/* Return the salt-2 field stored in the WAL header.*/
static uint32_t vfsWalGetSalt2(struct vfsWal *w)
{
	return *(uint32_t *)(&w->hdr[20]);
}

/* Return the checksum-1 field stored in the WAL header.*/
static uint32_t vfsWalGetChecksum1(struct vfsWal *w)
{
	return vfsGet32(&w->hdr[24]);
}

/* Return the checksum-2 field stored in the WAL header.*/
static uint32_t vfsWalGetChecksum2(struct vfsWal *w)
{
	return vfsGet32(&w->hdr[28]);
}

/* Append the given pages as new frames. */
static int vfsWalAppend(struct vfsWal *w,
			unsigned database_n_pages,
			unsigned n,
			unsigned long *page_numbers,
			uint8_t *pages)
{
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
		database_size = (uint32_t)database_n_pages;
		checksum[0] = vfsWalGetChecksum1(w);
		checksum[1] = vfsWalGetChecksum2(w);
	} else {
		struct vfsFrame *frame = w->frames[w->n_frames - 1];
		checksum[0] = vfsFrameGetChecksum1(frame);
		checksum[1] = vfsFrameGetChecksum2(frame);
		database_size = vfsFrameGetDatabaseSize(frame);
	}

	frames =
	    sqlite3_realloc64(w->frames, sizeof *frames * (w->n_frames + n));
	if (frames == NULL) {
		goto oom;
	}
	w->frames = frames;

	for (i = 0; i < n; i++) {
		struct vfsFrame *frame = vfsFrameCreate(page_size);
		uint32_t page_number = (uint32_t)page_numbers[i];
		uint32_t commit = 0;
		uint8_t *page = &pages[i * page_size];

		if (frame == NULL) {
			goto oom_after_frames_alloc;
		}

		if (page_number > database_size) {
			database_size = page_number;
		}

		/* For commit records, the size of the database file in pages
		 * after the commit. For all other records, zero. */
		if (i == n - 1) {
			commit = database_size;
		}

		vfsFrameFill(frame, page_number, commit, salt, checksum, page,
			     page_size);

		frames[w->n_frames + i] = frame;
	}

	w->n_frames += n;

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
	vfsPut32(VFS__WAL_MAGIC | VFS__BIGENDIAN, &w->hdr[0]);
	vfsPut32(VFS__WAL_VERSION, &w->hdr[4]);
	vfsPut32(page_size, &w->hdr[8]);
	vfsPut32(0, &w->hdr[12]);
	sqlite3_randomness(8, &w->hdr[16]);
	vfsChecksum(w->hdr, 24, checksum, checksum);
	vfsPut32(checksum[0], w->hdr + 24);
	vfsPut32(checksum[1], w->hdr + 28);
}

/* Invalidate the WAL index header, forcing the next connection that tries to
 * start a read transaction to rebuild the WAL index by reading the WAL.
 *
 * No read or write lock must be currently held. */
static void vfsInvalidateWalIndexHeader(struct vfsDatabase *d)
{
	struct vfsShm *shm = &d->shm;
	uint8_t *header = shm->regions[0];
	unsigned i;

	for (i = 0; i < SQLITE_SHM_NLOCK; i++) {
		assert(shm->shared[i] == 0);
		assert(shm->exclusive[i] == 0);
	}

	/* The walIndexTryHdr function in sqlite/wal.c (which is indirectly
	 * called by sqlite3WalBeginReadTransaction), compares the first and
	 * second copy of the WAL index header to see if it is valid. Changing
	 * the first byte of each of the two copies is enough to make the check
	 * fail. */
	header[0] = 1;
	header[VFS__WAL_INDEX_HEADER_SIZE] = 0;
}

int VfsApply(sqlite3_vfs *vfs,
	     const char *filename,
	     unsigned n,
	     unsigned long *page_numbers,
	     void *frames)
{
	struct vfs *v;
	struct vfsDatabase *database;
	struct vfsWal *wal;
	struct vfsShm *shm;
	int rv;

	v = (struct vfs *)(vfs->pAppData);
	database = vfsDatabaseLookup(v, filename);

	assert(database != NULL);

	wal = &database->wal;
	shm = &database->shm;

	/* If there's no page size set in the WAL header, it must mean that WAL
	 * file was never written. In that case we need to initialize the WAL
	 * header. */
	if (vfsWalGetPageSize(wal) == 0) {
		vfsWalStartHeader(wal, vfsDatabaseGetPageSize(database));
	}

	rv = vfsWalAppend(wal, database->n_pages, n, page_numbers, frames);
	if (rv != 0) {
		return rv;
	}

	/* If a write lock is held it means that this is the VFS that orginated
	 * this commit and on which dqlite_vfs_poll() was called. In that case
	 * we release the lock and update the WAL index.
	 *
	 * Otherwise, if the WAL index header is mapped it means that this VFS
	 * has one or more open connections even if it's not the one that
	 * originated the transaction (this can happen for example when applying
	 * a Raft barrier and replaying the Raft log in order to serve a request
	 * of a newly connected client). */
	if (shm->exclusive[0] == 1) {
		shm->exclusive[0] = 0;
		vfsAmendWalIndexHeader(database);
	} else {
		if (shm->n_regions > 0) {
			vfsInvalidateWalIndexHeader(database);
		}
	}

	return 0;
}

int VfsAbort(sqlite3_vfs *vfs, const char *filename)
{
	struct vfs *v;
	struct vfsDatabase *database;
	int rv;

	v = (struct vfs *)(vfs->pAppData);
	database = vfsDatabaseLookup(v, filename);

	rv = vfsShmUnlock(&database->shm, 0, 1, SQLITE_SHM_EXCLUSIVE);
	if (rv != 0) {
		return rv;
	}

	return 0;
}

/* Extract the number of pages field from the database header. */
static uint32_t vfsDatabaseGetNumberOfPages(struct vfsDatabase *d)
{
	uint8_t *page;

	assert(d->n_pages > 0);

	page = d->pages[0];

	/* The page size is stored in the 16th and 17th bytes of the first
	 * database page (big-endian) */
	return vfsGet32(&page[28]);
}

static void vfsDatabaseSnapshot(struct vfsDatabase *d, uint8_t **cursor)
{
	uint32_t page_size;
	unsigned i;

	page_size = vfsDatabaseGetPageSize(d);
	assert(page_size > 0);
	assert(d->n_pages == vfsDatabaseGetNumberOfPages(d));

	for (i = 0; i < d->n_pages; i++) {
		memcpy(*cursor, d->pages[i], page_size);
		*cursor += page_size;
	}
}

static void vfsWalSnapshot(struct vfsWal *w, uint8_t **cursor)
{
	uint32_t page_size;
	unsigned i;

	if (w->n_frames == 0) {
		return;
	}

	memcpy(*cursor, w->hdr, VFS__WAL_HEADER_SIZE);
	*cursor += VFS__WAL_HEADER_SIZE;

	page_size = vfsWalGetPageSize(w);
	assert(page_size > 0);

	for (i = 0; i < w->n_frames; i++) {
		struct vfsFrame *frame = w->frames[i];
		memcpy(*cursor, frame->header, FORMAT__WAL_FRAME_HDR_SIZE);
		*cursor += FORMAT__WAL_FRAME_HDR_SIZE;
		memcpy(*cursor, frame->page, page_size);
		*cursor += page_size;
	}
}

int VfsSnapshot(sqlite3_vfs *vfs, const char *filename, void **data, size_t *n)
{
	struct vfs *v;
	struct vfsDatabase *database;
	struct vfsWal *wal;
	uint8_t *cursor;

	v = (struct vfs *)(vfs->pAppData);
	database = vfsDatabaseLookup(v, filename);

	if (database == NULL) {
		*data = NULL;
		*n = 0;
		return 0;
	}

	if (database->n_pages != vfsDatabaseGetNumberOfPages(database)) {
		return SQLITE_CORRUPT;
	}

	wal = &database->wal;

	*n = vfsDatabaseFileSize(database) + vfsWalFileSize(wal);
	/* TODO: we should fix the tests and use sqlite3_malloc instead. */
	*data = raft_malloc(*n);
	if (*data == NULL) {
		return DQLITE_NOMEM;
	}

	cursor = *data;

	vfsDatabaseSnapshot(database, &cursor);
	vfsWalSnapshot(wal, &cursor);

	return 0;
}

static int vfsDatabaseRestore(struct vfsDatabase *d,
			      const uint8_t *data,
			      size_t n)
{
	uint32_t page_size = vfsParsePageSize(vfsGet16(&data[16]));
	unsigned n_pages;
	void **pages;
	unsigned i;
	int rv;

	assert(page_size > 0);

	/* Check that the page size of the snapshot is consistent with what we
	 * have here. */
	assert(vfsDatabaseGetPageSize(d) == page_size);

	n_pages = (unsigned)vfsGet32(&data[28]);

	if (n < n_pages * page_size) {
		return DQLITE_ERROR;
	}

	pages = sqlite3_malloc64(sizeof *pages * n_pages);
	if (pages == NULL) {
		goto oom;
	}

	for (i = 0; i < n_pages; i++) {
		void *page = sqlite3_malloc64(page_size);
		if (page == NULL) {
			unsigned j;
			for (j = 0; j < i; j++) {
				sqlite3_free(pages[j]);
			}
			goto oom_after_pages_alloc;
		}
		pages[i] = page;
		memcpy(page, &data[i * page_size], page_size);
	}

	/* Truncate any existing content. */
	rv = vfsDatabaseTruncate(d, 0);
	assert(rv == 0);

	d->pages = pages;
	d->n_pages = n_pages;

	return 0;

oom_after_pages_alloc:
	sqlite3_free(pages);
oom:
	return DQLITE_NOMEM;
}

static int vfsWalRestore(struct vfsWal *w,
			 const uint8_t *data,
			 size_t n,
			 uint32_t page_size)
{
	struct vfsFrame **frames;
	unsigned n_frames;
	unsigned i;
	int rv;

	if (n == 0) {
		return 0;
	}

	assert(w->n_tx == 0);

	assert(n > VFS__WAL_HEADER_SIZE);
	assert(((n - VFS__WAL_HEADER_SIZE) % vfsFrameSize(page_size)) == 0);

	n_frames =
	    (unsigned)((n - VFS__WAL_HEADER_SIZE) / vfsFrameSize(page_size));

	frames = sqlite3_malloc64(sizeof *frames * n_frames);
	if (frames == NULL) {
		goto oom;
	}

	for (i = 0; i < n_frames; i++) {
		struct vfsFrame *frame = vfsFrameCreate(page_size);
		const uint8_t *p;

		if (frame == NULL) {
			unsigned j;
			for (j = 0; j < i; j++) {
				vfsFrameDestroy(frames[j]);
				goto oom_after_frames_alloc;
			}
		}
		frames[i] = frame;

		p = &data[VFS__WAL_HEADER_SIZE + i * vfsFrameSize(page_size)];
		memcpy(frame->header, p, VFS__FRAME_HEADER_SIZE);
		memcpy(frame->page, p + VFS__FRAME_HEADER_SIZE, page_size);
	}

	memcpy(w->hdr, data, VFS__WAL_HEADER_SIZE);

	rv = vfsWalTruncate(w, 0);
	assert(rv == 0);

	w->frames = frames;
	w->n_frames = n_frames;

	return 0;

oom_after_frames_alloc:
	sqlite3_free(frames);
oom:
	return DQLITE_NOMEM;
}

int VfsRestore(sqlite3_vfs *vfs,
	       const char *filename,
	       const void *data,
	       size_t n)
{
	struct vfs *v;
	struct vfsDatabase *database;
	struct vfsWal *wal;
	uint32_t page_size;
	size_t offset;
	int rv;

	v = (struct vfs *)(vfs->pAppData);
	database = vfsDatabaseLookup(v, filename);
	assert(database != NULL);

	wal = &database->wal;

	/* Truncate any existing content. */
	rv = vfsWalTruncate(wal, 0);
	if (rv != 0) {
		return rv;
	}

	/* Restore the content of the main database and of the WAL. */
	rv = vfsDatabaseRestore(database, data, n);
	if (rv != 0) {
		return rv;
	}

	page_size = vfsDatabaseGetPageSize(database);
	offset = database->n_pages * page_size;

	rv = vfsWalRestore(wal, data + offset, n - offset, page_size);
	if (rv != 0) {
		return rv;
	}

	return 0;
}
