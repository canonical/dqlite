#include <assert.h>
#include <errno.h>
#include <stddef.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include <sqlite3.h>

/* Maximum pathname length supported by this VFS. */
#define DQLITE__VFS_MAX_PATHNAME 512

/* Maximum number of files this VFS can create. */
#define DQLITE__VFS_MAX_FILES 64

/* Default, minumum and maximum page size (copied from SQLite). */
#define DQLITE__VFS_PAGE_MIN_SIZE 512
#define DQLITE__VFS_PAGE_MAX_SIZE 65536

/* Possible file content types. */
#define DQLITE__VFS_CONTENT_MAIN_DB      0
#define DQLITE__VFS_CONTENT_WAL          1
#define DQLITE__VFS_CONTENT_OTHER        2

/* Size of the database header (copied from SQLite). */
#define DQLITE__VFS_MAIN_DB_HDRSIZE 100

/* Size of write ahead log header (copied from SQLite) */
#define DQLITE__VFS_WAL_HDRSIZE 32

// Size of header before each frame in wal
#define DQLITE__VFS_WAL_FRAME_HDRSIZE 24

/* Hold content for a single page or frame in a volatile file. */
struct dqlite__vfs_page {
	void *buf;             /* Content of the page. */
	void *hdr;             /* Page header (only for WAL pages). */
	void *dirty_mask;      /* Bit mask of dirty buf bytes to be re-written (only for WAL pages) */
	int   dirty_mask_size; /* Number of bytes in the dirty_mask array. */
	void *dirty_buf;       /* List of dirty buf bytes, one for each bit with value 1 in dirty_mask. */
};

/* Initialize a new volatile page for a database or WAL file.
 *
 * If it's a page for a WAL file, the WAL header will
 * also be allocated.
 */
static int dqlite__vfs_page_init(struct dqlite__vfs_page *page, int page_size, int wal)
{
	assert(page_size > 0);
	assert(wal == 0 || wal == 1 );

	page->buf = sqlite3_malloc(page_size);
	if (page->buf == NULL)
		goto err_buf_malloc;
	memset(page->buf, 0, page_size);

	if( wal ){
		page->hdr = sqlite3_malloc(DQLITE__VFS_WAL_FRAME_HDRSIZE);
		if(page->hdr == NULL)
			goto err_hdr_malloc;
		memset(page->hdr, 0, DQLITE__VFS_WAL_FRAME_HDRSIZE);

		page->dirty_mask_size = page_size / sizeof(char);
		page->dirty_mask = sqlite3_malloc(page->dirty_mask_size);
		if(page->dirty_mask == NULL)
			goto err_dirty_mask_malloc;
		memset(page->dirty_mask, 0, page->dirty_mask_size);
	}else{
		page->hdr = NULL;
	}

	page->dirty_buf = NULL;

	return SQLITE_OK;

 err_dirty_mask_malloc:
	sqlite3_free(page->hdr);

 err_hdr_malloc:
	sqlite3_free(page->buf);

 err_buf_malloc:
	return SQLITE_NOMEM;

}

/* Release the memory of a volatile page */
static void dqlite__vfs_page_close(struct dqlite__vfs_page *page)
{
	assert(page != NULL);
	assert(page->buf != NULL);

	sqlite3_free(page->buf);

	if (page->hdr != NULL) {
		assert(page->dirty_mask );

		sqlite3_free(page->hdr);
		sqlite3_free(page->dirty_mask);
	}
}

/* Hold content for a single file in the volatile file system. */
struct dqlite__vfs_content {
	char *filename;                  /* Name of the file. */
	void *hdr;                       /* File header (only for WAL files). */
	struct dqlite__vfs_page **pages; /* Pointers to all pages in the file. */
	int pages_len;                   /* Number of pages in the file. */
	int page_size;                   /* Size of page->buf for each page. */
	int refcount;                    /* Number of open FDs referencing this file. */
	int type;                        /* Content type (either main db or WAL). */
	void **shm_regions;              /* Pointers to shared memory regions. */
	int shm_regions_len;             /* Number of shared memory regions. */
	int shm_refcount;                /* Number of opened files using the shared memory. */
	struct dqlite__vfs_content *wal; /* Handle to the WAL file content (for database files). */
};

/* Initialize the content structure for a new volatile file. */
static int dqlite__vfs_content_init(
	struct dqlite__vfs_content *content,
	const char *filename,
	int type)
{
	assert(filename != NULL);
	assert(
		type == DQLITE__VFS_CONTENT_MAIN_DB ||
		type == DQLITE__VFS_CONTENT_WAL     ||
		type == DQLITE__VFS_CONTENT_OTHER);

	// Copy the name, since when called from Go, the pointer will be freed.
	content->filename = (char*)(sqlite3_malloc(strlen(filename) + 1));
	if (content->filename == NULL)
		goto err_filename_malloc;

	content->filename = strncpy(content->filename, filename, strlen(filename) + 1);

	// For WAL files, also allocate the WAL file header.
	if (type == DQLITE__VFS_CONTENT_WAL) {
		content->hdr = sqlite3_malloc(DQLITE__VFS_WAL_HDRSIZE);
		if (content->hdr == NULL)
			goto err_hdr_malloc;
		memset(content->hdr, 0, DQLITE__VFS_WAL_HDRSIZE);
	}else {
		content->hdr = NULL;
	}

	content->pages = 0;
	content->pages_len = 0;
	content->page_size = 0;
	content->refcount = 0;
	content->type = type;
	content->shm_regions = 0;
	content->shm_regions_len = 0;
	content->shm_refcount = 0;
	content->wal = 0;

	return SQLITE_OK;

 err_hdr_malloc:
	sqlite3_free(content->filename);

 err_filename_malloc:
	return SQLITE_NOMEM;
}

/* Release the memory used for the content of a volatile file. */
static void dqlite__vfs_content_close(
	struct dqlite__vfs_content *content,
	int force)
{
	int i;
	struct dqlite__vfs_page *page;
	void *shm_region;

	assert(content != NULL);
	assert(content->filename != NULL);
	assert(force == 0 || force == 1);
	assert(force == 1 || content->refcount == 0);

	// Free the name.
	sqlite3_free(content->filename);

	// Free the header if it's a WAL file.
	if (content->type == DQLITE__VFS_CONTENT_WAL){
		assert(content->hdr != NULL);
		sqlite3_free(content->hdr);
	} else {
		assert(content->hdr == NULL);
	}

	// Free all pages.
	for(i = 0; i < content->pages_len; i++) {
		page = *(content->pages + i);
		assert(page != NULL);
		dqlite__vfs_page_close(page);
		sqlite3_free(page);
	}

	// Free the page array.
	if (content->pages != NULL){
		sqlite3_free(content->pages);
	}

	// Free all shared memory regions.
	for (i = 0; i < content->shm_regions_len; i++) {
		shm_region = *(content->shm_regions + i);
		assert( shm_region );
		sqlite3_free(shm_region);
	}

	// Free the shared memory region array.
	if( content->shm_regions ){
		sqlite3_free(content->shm_regions);
	}
}

/* Return 1 if this file has no content. */
static int dqlite__vfs_content_is_empty(struct dqlite__vfs_content *content){
	assert(content != NULL);

	if (content->pages_len == 0) {
		assert(content->pages == NULL);
		return 1;
	}

	// If it was written, a page list and a page size must have been set.
	assert(
		content->pages != NULL &&
		content->pages_len > 0 &&
		content->page_size > 0);

	return 0;
}

// Get a page from this file, possibly creating a new one.
static struct dqlite__vfs_page *dqlite__vfs_content_page_get(
	struct dqlite__vfs_content *content,
	int pgno)
{
	struct dqlite__vfs_page *page;
	int err;
	int is_wal;

	assert(content != NULL);
	assert(pgno > 0);

	is_wal = content->type == DQLITE__VFS_CONTENT_WAL;

	/* At most one new page should be appended. */
	assert(pgno <= (content->pages_len + 1));

	if (pgno == (content->pages_len + 1)) {
		/* Create a new page, grow the page array, and append the
		 * new page to it. */
		struct dqlite__vfs_page **pages; /* New page array. */

		/* We assume that the page size has been set, either by inteerrepting
		 * the first main database file write, or by handling a 'PRAGMA page_size=N'
		 * command in dqlite__vfs_file_control(). */
		assert (content->page_size > 0);

		page = (struct dqlite__vfs_page*)sqlite3_malloc(sizeof(*page));
		if (page == NULL)
			goto err_page_malloc;

		err = dqlite__vfs_page_init(page, content->page_size, is_wal);
		if (err != SQLITE_OK){
			assert(err == SQLITE_NOMEM);
			goto err_page_init;
		}

		pages = (struct dqlite__vfs_page**)sqlite3_realloc(
			content->pages, sizeof(struct dqlite__vfs_page*) * pgno);
		if (pages == NULL)
			goto err_pages_malloc;

		/* Append the new page to the new page array. */
		*(pages + pgno - 1) = page;

		/* Update the page array. */
		content->pages = pages;
		content->pages_len = pgno;
	}else{
		/* Return the existing page. */
		assert(content->pages != NULL);
		page = *(content->pages + pgno - 1);
	}

	return page;

 err_pages_malloc:
	dqlite__vfs_page_close(page);

 err_page_init:
	sqlite3_free(page);

 err_page_malloc:
	return NULL;
}

/* Lookup a page from this file, returning NULL if it doesn't exist. */
static struct dqlite__vfs_page* dqlite__vfs_content_page_lookup(
	struct dqlite__vfs_content *content,
	int pgno)
{
	struct dqlite__vfs_page* page;

	assert(content != NULL);

	if(pgno > content->pages_len) {
		/* This page hasn't been written yet. */
		return NULL;
	}

	page = *(content->pages + pgno - 1);

	assert(page != NULL);

	if (content->type == DQLITE__VFS_CONTENT_WAL)
		assert(page->hdr != NULL);

	return page;
}

// Truncate the file to be exactly the given number of pages.
static void dqlite__vfs_content_truncate(
	struct dqlite__vfs_content *content,
	int pages_len
	)
{
	struct dqlite__vfs_page **cursor;
	int i;

	assert(content->pages_len > 0);

	/* Truncate should always shrink a file. */
	assert(pages_len <= content->pages_len);
	assert(content->pages != NULL);

	/* Destroy pages beyond pages_len. */
	cursor = content->pages + pages_len;
	for (i=0; i < (content->pages_len - pages_len); i++ ) {
		dqlite__vfs_page_close(*cursor);
		sqlite3_free(*cursor);
		cursor++;
	}

	/* Reset the file header (for WAL files). */
	if (content->type == DQLITE__VFS_CONTENT_WAL) {
		assert(content->hdr  != NULL);
		memset(content->hdr, 0, DQLITE__VFS_WAL_HDRSIZE);
	}else {
		assert(content->hdr == NULL);
	}

	// Shrink the page array, possibly to 0.
	content->pages = (struct dqlite__vfs_page**)sqlite3_realloc(
		content->pages, sizeof(struct dqlite__vfs_page*) * pages_len);

	/* Update the page count. */
	content->pages_len = pages_len;
}

// Root of the volatile file system. Contains pointers to the content
// of all files that were created.
struct dqlite__vfs_root {
	struct dqlite__vfs_content **contents;     /* Pointers to files in the file system */
	int                          contents_len; /* Number of files in the file system */
	pthread_mutex_t              mutex;        /* Serialize to access this object */
	int                          error;        /* Last error occurred. */
};

/* Initialize a new dqlite__vfs_root object. */
static int dqlite__vfs_root_init(struct dqlite__vfs_root *r)
{
	int err;
	int contents_size = sizeof(struct dqlite__vfs_content*) * DQLITE__VFS_MAX_FILES;

	assert(r != NULL);

	r->contents_len = DQLITE__VFS_MAX_FILES;

	r->contents = (struct dqlite__vfs_content**)(sqlite3_malloc(contents_size));
	if (r->contents == NULL)
		goto err_contents_malloc;
	memset(r->contents, 0, contents_size);

	err = pthread_mutex_init(&r->mutex, NULL);
	assert(err == 0); /* Docs say that pthread_mutex_init can't fail */

	return SQLITE_OK;

 err_contents_malloc:
	return SQLITE_NOMEM;
}

/* Release the memory used internally by dqlite__vfs_root object.
 *
 * All file content will be de-allocated, so dangling open FDs against
 * those files will be broken.
 */
static void dqlite__vfs_root_close(struct dqlite__vfs_root *r)
{
	struct dqlite__vfs_content **cursor; /* Iterator for r->contents */
	int i;

	assert(r != NULL);
	assert(r->contents != NULL);

	cursor = r->contents;

	/* The content array has been allocated and has at least one slot. */
	assert(cursor != NULL);
	assert(r->contents_len > 0);

	for (i = 0; i < r->contents_len; i++) {
		struct dqlite__vfs_content *content = *cursor;
		if( content ){
			dqlite__vfs_content_close(content, 1);
			sqlite3_free(content);
		}
		cursor++;
	}

	sqlite3_free(r->contents);
}

/* Find a content object by name.
 *
 * Fill ppContent and return its index if found, otherwise return the index
 * of a free slot (or -1, if there are no free slots).
 */
static int dqlite__vfs_root_content_lookup(
	struct dqlite__vfs_root *r,
	const char *filename,
	struct dqlite__vfs_content **out // OUT: content object or NULL
	)
{
	struct dqlite__vfs_content **cursor; /* Iterator for r->contents */
	int i;
	int free_slot = -1; /* Index of the content or of a free slot in the contents array. */

	assert(r != NULL);
	assert(filename != NULL);

	cursor = r->contents;

	// The content array has been allocated and has at least one slot.
	assert(cursor != NULL);
	assert(r->contents_len > 0);

	for(i = 0; i < r->contents_len; i++ ) {
		struct dqlite__vfs_content *content = *cursor;
		if (content && strcmp(content->filename, filename) == 0) {
			// Found matching file.
			*out = content;
			return i;
		}
		if (content == NULL && free_slot==-1) {
			// Keep track of the index of this empty slot.
			free_slot = i;
		}
		cursor++;
	}

	// No matching content object.
	*out = 0;

	return free_slot;
}

/* Find the database content object associated with the given WAL file name. */
static int dqlite__vfs_root_database_content_lookup(
	struct dqlite__vfs_root *r,
	const char *wal_filename,
	struct dqlite__vfs_content **out
	)
{
	struct dqlite__vfs_content *content;
	int main_filename_len;
	char *main_filename;

	assert(r != NULL);
	assert(wal_filename != NULL);
	assert(out != NULL);

	*out = NULL; /* In case of errors */

	main_filename_len = strlen(wal_filename) - strlen("-wal") + 1;
	main_filename = sqlite3_malloc(main_filename_len);

	if (main_filename == NULL)
		return SQLITE_NOMEM;

	strncpy(main_filename, wal_filename, main_filename_len - 1);
	main_filename[main_filename_len - 1] = '\0';

	dqlite__vfs_root_content_lookup(r, main_filename, &content);

	sqlite3_free(main_filename);

	assert(content != NULL);

	*out = content;

	return SQLITE_OK;
}

/* Return the size of the database file whose WAL file has the given name.
 *
 * The size must have been previously set when this routine is called.
 */
static int dqlite__vfs_root_database_page_size(
	struct dqlite__vfs_root *r,
	const char *wal_filename,
	int *page_size
	)
{
	struct dqlite__vfs_content *content;
	int err;

	assert(r != NULL);
	assert(wal_filename != NULL);
	assert(page_size != NULL);

	*page_size = 0; // In case of errors.

	err = dqlite__vfs_root_database_content_lookup(r, wal_filename, &content);
	if (err != SQLITE_OK) {
		return err;
	}

	assert(content->page_size > 0);

	*page_size = content->page_size;

	return SQLITE_OK;
}

/* Extract the page size from the content of the first database page. */
static unsigned dqlite__vfs_parse_database_page_size(void *buf)
{
	unsigned page_size;

	assert(buf != NULL);

	page_size = (((char*)buf)[16] << 8) + ((char*)buf)[17];

	/* Validate the page size, see https://www.sqlite.org/fileformat2.html. */
	if (page_size == 1) {
		page_size = DQLITE__VFS_PAGE_MAX_SIZE;
	} else {
		assert(
			page_size >= DQLITE__VFS_PAGE_MIN_SIZE &&
			page_size <= (DQLITE__VFS_PAGE_MAX_SIZE / 2) &&
			((page_size-1) & page_size)==0);
	}

	return page_size;
}

/* Extract the page size from the content of the WAL header. */
static unsigned dqlite__vfs_parse_wal_page_size(const void *buf)
{
	unsigned page_size;

	assert( buf );

	/* See wal.c for a description of the WAL header format. */
	page_size = (
		( ((char*)buf)[8]  << 24 ) +
		( ((char*)buf)[9]  << 16 ) +
		( ((char*)buf)[10] << 8  ) +
		((char*)buf)[11]
		);

	/* Validate the page size, see https://www.sqlite.org/fileformat2.html. */
	if (page_size == 1){
		page_size = DQLITE__VFS_PAGE_MAX_SIZE;
	} else {
		assert( page_size>=DQLITE__VFS_PAGE_MIN_SIZE
			&& page_size<=(DQLITE__VFS_PAGE_MAX_SIZE / 2)
			&& ((page_size-1)&page_size)==0
			);
	}

	return page_size;
}

struct dqlite__vfs_file {
	sqlite3_file base;                   /* Base class. Must be first. */
	struct dqlite__vfs_root *root;       /* Pointer to our volatile VFS instance data. */
	struct dqlite__vfs_content *content; /* Handle to the file content. */
};

static int dqlite__vfs_close(sqlite3_file *file)
{
	struct dqlite__vfs_file *f = (struct dqlite__vfs_file*)file;
	struct dqlite__vfs_root *root = (struct dqlite__vfs_root*)(f->root);

	pthread_mutex_lock(&root->mutex);

	assert( f->content->refcount );
	f->content->refcount--;

	pthread_mutex_unlock(&root->mutex);

	return SQLITE_OK;
}

static int dqlite__vfs_read(
	sqlite3_file *file,
	void *buf,
	int amount,
	sqlite_int64 offset)
{
	struct dqlite__vfs_file *f = (struct dqlite__vfs_file*)file;
	int pgno;
	struct dqlite__vfs_page *page;

	assert(buf != NULL);
	assert(amount > 0);
	assert(f != NULL );
	assert(f->content != NULL );
	assert(f->content->filename != NULL);
	assert(f->content->refcount > 0);

	/* From SQLite docs:
	 *
	 *   If xRead() returns SQLITE_IOERR_SHORT_READ it must also fill
	 *   in the unread portions of the buffer with zeros.  A VFS that
	 *   fails to zero-fill short reads might seem to work.  However,
	 *   failure to zero-fill short reads will eventually lead to
	 *   database corruption.
	 */

	/* Check if the file is empty. */
	if (dqlite__vfs_content_is_empty(f->content)) {
		memset(buf, 0, amount);
		return SQLITE_IOERR_SHORT_READ;
	}

	/* From this point on we can assume that the file was written at least
	 * once. */

	/* Since writes to all files other than the main database or the WAL are
	 * no-ops and the associated content object remains empty, we expect
	 * the content type to be either DQLITE__VFS_CONTENT_MAIN_DB or DQLITE__VFS_CONTENT_WAL. */
	assert(
		f->content->type == DQLITE__VFS_CONTENT_MAIN_DB ||
		f->content->type == DQLITE__VFS_CONTENT_WAL);

	switch (f->content->type) {
	case DQLITE__VFS_CONTENT_MAIN_DB:
		/* Main database */

		/* If the main database file is not empty, we expect the page size
		 * to have been set by an initial write. */
		assert (f->content->page_size >0);

		if (offset < f->content->page_size) {
			/* This is page 1 read. We expect the read to be at most page_size
			 * bytes. */
			assert (amount <= f->content->page_size);

			pgno = 1;
		}else{
			/* For pages greater than 1, we expect a full page read, with
			 * an offset that starts exectly at the page boundary. */
			assert(amount == f->content->page_size);
			assert((offset % f->content->page_size) == 0);

			pgno = (offset / f->content->page_size) + 1;
		}

		assert(pgno > 0);

		if (pgno == 0) {
			/* This is an attempt to read a page that was never written. */
			memset(buf, 0, amount);
			return SQLITE_IOERR_SHORT_READ;
		}

		page = dqlite__vfs_content_page_lookup(f->content, pgno);

		if (pgno == 1){
			/* Read the desired part of page 1. */
			memcpy(buf, page->buf + offset, amount);
		}else{
			/* Read the full page. */
			memcpy(buf, page->buf, amount);
		}
		return SQLITE_OK;

	case DQLITE__VFS_CONTENT_WAL:
		/* WAL file */

		if (f->content->page_size == 0) {
			/* If the page size hasn't been set yet, set it by copy the one from
			 * the associated main database file. */
			int err = dqlite__vfs_root_database_page_size(
				f->root, f->content->filename, &f->content->page_size);
			if (err != 0) {
				return err;
			}
		}

		if (offset ==0) {
			/* Read the header. */
			assert(amount==DQLITE__VFS_WAL_HDRSIZE );
			assert( f->content->hdr );
			memcpy(buf, f->content->hdr, DQLITE__VFS_WAL_HDRSIZE);
			return SQLITE_OK;
		}

		/* For any other frame, we expect either a header read, a
		 * checksum read, a page read or a full frame read. */
		if (amount == DQLITE__VFS_WAL_FRAME_HDRSIZE) {
			assert( ((offset-DQLITE__VFS_WAL_HDRSIZE) % (f->content->page_size+DQLITE__VFS_WAL_FRAME_HDRSIZE))==0 );
			pgno = ((offset-DQLITE__VFS_WAL_HDRSIZE) / (f->content->page_size+DQLITE__VFS_WAL_FRAME_HDRSIZE)) + 1;
		}else if (amount == sizeof(uint32_t)*2) {
			if (offset == DQLITE__VFS_WAL_FRAME_HDRSIZE) {
				/* Read the checksum from the WAL header. */
				memcpy(buf, f->content->hdr + offset, amount);
				return SQLITE_OK;
			}
			assert(((offset-16-DQLITE__VFS_WAL_HDRSIZE) % (f->content->page_size+DQLITE__VFS_WAL_FRAME_HDRSIZE)) == 0);
			pgno = (offset-16-DQLITE__VFS_WAL_HDRSIZE) / (f->content->page_size+DQLITE__VFS_WAL_FRAME_HDRSIZE) + 1;
		}else if( amount==f->content->page_size ){
			assert( ((offset-DQLITE__VFS_WAL_HDRSIZE-DQLITE__VFS_WAL_FRAME_HDRSIZE) % (f->content->page_size+DQLITE__VFS_WAL_FRAME_HDRSIZE))==0 );
			pgno = ((offset-DQLITE__VFS_WAL_HDRSIZE-DQLITE__VFS_WAL_FRAME_HDRSIZE) / (f->content->page_size+DQLITE__VFS_WAL_FRAME_HDRSIZE)) + 1;
		}else{
			assert( amount==(DQLITE__VFS_WAL_FRAME_HDRSIZE+f->content->page_size) );
			pgno = ((offset-DQLITE__VFS_WAL_HDRSIZE) / (f->content->page_size+DQLITE__VFS_WAL_FRAME_HDRSIZE)) + 1;
		}

		if(pgno == 0){
			// This is an attempt to read a page that was never written.
			memset(buf, 0, amount);
			return SQLITE_IOERR_SHORT_READ;
		}

		page = dqlite__vfs_content_page_lookup(f->content, pgno);

		if (amount == DQLITE__VFS_WAL_FRAME_HDRSIZE) {
			memcpy(buf, page->hdr, amount);
		} else if (amount == sizeof(uint32_t)*2) {
			memcpy(buf, page->hdr + 16, amount);
		} else if (amount == f->content->page_size) {
			memcpy(buf, page->buf, amount);
		} else {
			memcpy(buf, page->hdr, DQLITE__VFS_WAL_FRAME_HDRSIZE);
			memcpy(buf+DQLITE__VFS_WAL_FRAME_HDRSIZE, page->buf, f->content->page_size);
		}

		return SQLITE_OK;
	}

	return SQLITE_IOERR_READ;
}

static int dqlite__vfs_write(
	sqlite3_file *file,
	const void *buf,
	int amount,
	sqlite_int64 offset
	){
	struct dqlite__vfs_file *f = (struct dqlite__vfs_file*)file;
	unsigned pgno;
	struct dqlite__vfs_page *page;

	assert(buf != NULL);
	assert(amount > 0);
	assert(f != NULL );
	assert(f->content != NULL );
	assert(f->content->filename != NULL);
	assert(f->content->refcount > 0);

	switch (f->content->type) {
	case DQLITE__VFS_CONTENT_MAIN_DB:
		/* Main database. */

		if (offset == 0) {
			int page_size;

			/* This is the first database page. We expect the data to contain at
			 * least the header. */
			assert(amount >= DQLITE__VFS_MAIN_DB_HDRSIZE);

			/* Extract the page size from the header. */
			page_size = dqlite__vfs_parse_database_page_size((void*)buf);

			if (f->content->page_size > 0) {
				/* Check that the given page size actually matches what we have
				 * recorded. Since we make 'PRAGMA page_size=N' fail if the page
				 * is already set (see struct dqlite__vfs_fileControl), there should
				 * be no way for the user to change it. */
				assert (page_size == f->content->page_size);
			} else {
				/* This must be the very first write to the database. Keep track
				 * of the page size. */
				f->content->page_size = page_size;
			}

			pgno = 1;
		} else {
			/* The header must have been written and the page size set. */
			assert(f->content->page_size > 0);

			/* For pages beyond the first we expect offset to be a multiple of
			 * the page size. */
			assert((offset % f->content->page_size) == 0);

			/* We expect that SQLite writes a page at time. */
			assert(amount == f->content->page_size );

			pgno = (offset / f->content->page_size) + 1;
		}

		page = dqlite__vfs_content_page_get(f->content, pgno);
		if (page == NULL){
			return SQLITE_NOMEM;
		}

		assert(page->buf != NULL);

		memcpy(page->buf, buf, amount);

		return SQLITE_OK;

	case DQLITE__VFS_CONTENT_WAL:
		/* WAL file. */

		if (f->content->page_size == 0) {
			/* If the page size hasn't been set yet, set it by copy the one from
			 * the associated main database file. */
			int err = dqlite__vfs_root_database_page_size(
				f->root, f->content->filename, &f->content->page_size);
			if (err != 0 ) {
				return err;
			}
		}

		if (offset ==0 ) {
			/* This is the WAL header. */

			/* We expect the data to contain exactly 32 bytes. */
			assert(amount == DQLITE__VFS_WAL_HDRSIZE);

			/* The page size indicated in the header must match the one of the database file. */
			assert((int)dqlite__vfs_parse_wal_page_size(buf) == f->content->page_size);

			memcpy(f->content->hdr, buf, amount);
			return SQLITE_OK;
		}

		assert(f->content->page_size > 0);

		/* This is a WAL frame write. We expect either a frame header or page
		 * write. */
		if (amount == DQLITE__VFS_WAL_FRAME_HDRSIZE) {
			/* Frame header write. */
			assert( ((offset-DQLITE__VFS_WAL_HDRSIZE) % (f->content->page_size+DQLITE__VFS_WAL_FRAME_HDRSIZE))==0 );
			pgno = ((offset-DQLITE__VFS_WAL_HDRSIZE) / (f->content->page_size+DQLITE__VFS_WAL_FRAME_HDRSIZE)) + 1;

			page = dqlite__vfs_content_page_get(f->content, pgno);
			if (page == NULL){
				return SQLITE_NOMEM;
			}
			memcpy(page->hdr, buf, amount);
		} else {
			/* Frame page write. */
			assert(amount == f->content->page_size);
			assert( ((offset-DQLITE__VFS_WAL_HDRSIZE-DQLITE__VFS_WAL_FRAME_HDRSIZE) % (f->content->page_size+DQLITE__VFS_WAL_FRAME_HDRSIZE))==0 );

			pgno = ((offset-DQLITE__VFS_WAL_HDRSIZE-DQLITE__VFS_WAL_FRAME_HDRSIZE) / (f->content->page_size+DQLITE__VFS_WAL_FRAME_HDRSIZE)) + 1;

			// The header for the this frame must already have been written,
			// so the page is there.
			page = dqlite__vfs_content_page_lookup(f->content, pgno);

			assert(page != NULL);

			memcpy(page->buf, buf, amount);
		}

		return SQLITE_OK;

	case DQLITE__VFS_CONTENT_OTHER:
		// Silently swallow writes to any other file.
		return SQLITE_OK;
	}

	return SQLITE_IOERR_WRITE;
}

static int dqlite__vfs_truncate(sqlite3_file *file, sqlite_int64 size)
{
	struct dqlite__vfs_file *f = (struct dqlite__vfs_file*)file;
	int pgno;

	assert(f != NULL);
	assert(f->content != NULL);

	/* We expect calls to xTruncate only for database and WAL files. */
	assert(
		f->content->type == DQLITE__VFS_CONTENT_MAIN_DB	||
		f->content->type==DQLITE__VFS_CONTENT_WAL);

	/* Check if this file empty.*/
	if (dqlite__vfs_content_is_empty(f->content)) {
		/* We don't expect SQLite to grow empty files. */
		assert(size == 0);
		return SQLITE_OK;
	}

	switch (f->content->type) {
	case DQLITE__VFS_CONTENT_MAIN_DB:
		/* Main database. */
		assert(f->content->page_size > 0);
		assert((size % f->content->page_size) == 0);
		pgno = size / f->content->page_size;
		break;

	case DQLITE__VFS_CONTENT_WAL:
		/* WAL file. */

		/* We expect SQLite to only truncate to zero, after a full checkpoint.
		 *
		 * TODO: figure out other case where SQLite might truncate to a
		 *       different size.
		 */
		assert(size == 0);
		pgno = 0;
		break;
	}

	dqlite__vfs_content_truncate(f->content, pgno);

	return SQLITE_OK;
}

static int dqlite__vfs_sync(sqlite3_file *file, int flags){
	(void)file;
	(void)flags;

	return SQLITE_OK;
}

static int dqlite__vfs_file_size(sqlite3_file *file, sqlite_int64 *size)
{
	struct dqlite__vfs_file *f = (struct dqlite__vfs_file*)file;

	/* Check if this file empty. */
	if (dqlite__vfs_content_is_empty(f->content)) {
		*size = 0;
		return SQLITE_OK;
	}

	/* Since we don't allow writing any other file, this must be
	 * either a database file or WAL file. */
	assert(
		f->content->type == DQLITE__VFS_CONTENT_MAIN_DB ||
		f->content->type == DQLITE__VFS_CONTENT_WAL);

	/* Since this file is not empty, the page size must have been set. */
	assert(f->content->page_size > 0);

	switch (f->content->type) {
	case DQLITE__VFS_CONTENT_MAIN_DB:
		*size = f->content->pages_len * f->content->page_size;
		break;

	case DQLITE__VFS_CONTENT_WAL:
		/* TODO? here we assume that FileSize() is never invoked between
		 *       a header write and a page write. */
		*size = DQLITE__VFS_WAL_HDRSIZE + (f->content->pages_len * (DQLITE__VFS_WAL_FRAME_HDRSIZE + f->content->page_size));
		break;
	}

	return SQLITE_OK;
}

/* Locking a file is a no-op, since no other process has visibility on it. */
static int dqlite__vfs_lock(sqlite3_file *file, int lock)
{
	(void)file;
	(void)lock;

	return SQLITE_OK;
}

/* Unlocking a file is a no-op, since no other process has visibility on it. */
static int dqlite__vfs_unlock(sqlite3_file *file, int lock)
{
	(void)file;
	(void)lock;

	return SQLITE_OK;
}

/* We always report that a lock is held. This routine should be used only in
 * journal mode, so it doesn't matter. */
static int dqlite__vfs_check_reserved_lock(sqlite3_file *file, int *result)
{
	(void)file;

	*result = 1;
	return SQLITE_OK;
}

static int dqlite__vfs_file_control(sqlite3_file *file, int op, void *arg)
{
	struct dqlite__vfs_file *f = (struct dqlite__vfs_file*)file;

	if (op==SQLITE_FCNTL_PRAGMA) {
		// Handle pragma a pragma file control. See the xFileControl docstring
		// in sqlite.h.in for more details.
		char **pragma_fnctl;
		const char *pragma_left;
		const char *pragma_right;

		pragma_fnctl = (char**)arg;
		assert( pragma_fnctl );

		pragma_left = pragma_fnctl[1];
		pragma_right = pragma_fnctl[2];
		assert( pragma_left );

		if (strcmp(pragma_left, "page_size")==0 && pragma_right) {
			/* When the user executes 'PRAGMA page_size=N' we save the size internally.
			 *
			 * The page size must be between 512 and 65536, and be a power of two. The
			 * check below was copied from sqlite3BtreeSetPageSize in btree.c.
			 *
			 * Invalid sizes are simply ignored, SQLite will do the same.
			 *
			 * It's not possible to change the size after it's set.
			 */
			int page_size = atoi(pragma_right);

			if (
				page_size >= DQLITE__VFS_PAGE_MIN_SIZE &&
				page_size <= DQLITE__VFS_PAGE_MAX_SIZE &&
				((page_size-1)&page_size) == 0 ) {

				if (f->content->page_size && page_size != f->content->page_size) {
					pragma_fnctl[0] = "changing page size is not supported";
					return SQLITE_ERROR;
				}
				f->content->page_size = page_size;
			}
		}else if (strcmp(pragma_left, "journal_mode")==0 && pragma_right) {
			/* When the user executes 'PRAGMA journal_mode=x' we ensure that the
			 * desired mode is 'wal'. */
			if (strcasecmp(pragma_right, "wal") != 0) {
				pragma_fnctl[0] = "only WAL mode is supported";
				return SQLITE_ERROR;
			}
		}
		return SQLITE_NOTFOUND;
	}
	return SQLITE_OK;
}

static int dqlite__vfs_sector_size(sqlite3_file *file)
{
	(void)file;

	return 0;
}

static int dqlite__vfs_device_characteristics(sqlite3_file *file)
{
	(void)file;

	return 0;
}

/* Simulate shared memory by allocating on the C heap. */
static int dqlite__vfs_shm_map(
	sqlite3_file *file, /* Handle open on database file */
	int region_index,   /* Region to retrieve */
	int region_size,    /* Size of regions */
	int extend,         /* True to extend file if necessary */
	void volatile **out /* OUT: Mapped memory */
	){
	struct dqlite__vfs_file *f = (struct dqlite__vfs_file*)file;
	void *region;
	int err = SQLITE_OK;

	if (f->content->shm_regions != NULL && region_index < f->content->shm_regions_len) {
		/* The region was already allocated. */
		region = *(f->content->shm_regions + region_index);
		assert (region != NULL);
	} else {
		if (extend) {
			/* We should grow the map one region at a time. */
			assert(region_index == f->content->shm_regions_len);

			region = sqlite3_malloc(region_size);
			if (region != NULL) {
				memset(region, 0, region_size);
				f->content->shm_regions = (void**)sqlite3_realloc(
					f->content->shm_regions, sizeof(void*) * (region_index + 1));
				if (f->content->shm_regions == NULL) {
					sqlite3_free(region);
					region = 0;
					err = SQLITE_NOMEM;
				} else {
					*(f->content->shm_regions + region_index) = region;
					f->content->shm_regions_len++;
				}
			} else {
				err = SQLITE_NOMEM;
			}
		} else {
			/* The region was not allocated and we don't have to
			 * extend the map. */
			region = NULL;
		}
	}

	if (region) {
		f->content->shm_refcount++;
	}

	*out = region;

	return err;
}

static int dqlite__vfs_shm_lock(sqlite3_file *file, int ofst, int n, int flags)
{
	(void)file;
	(void)ofst;
	(void)n;
	(void)flags;

	/* This is a no-op since shared-memory locking is relevant only for
	 * inter-process concurrency. See also the unix-excl branch from upstream
	 * (git commit cda6b3249167a54a0cf892f949d52760ee557129). */
	return SQLITE_OK;
}

static void dqlite__vfs_shm_barrier(sqlite3_file *file)
{
	(void)file;
	/* This is a no-op since we expect SQLite to be compiled with mutex
	 * support (i.e. SQLITE_MUTEX_OMIT or SQLITE_MUTEX_NOOP are *not*
	 * defined, see sqliteInt.h). */
}

static int dqlite__vfs_shm_unmap(sqlite3_file *file, int delete_flag)
{
	struct dqlite__vfs_file *f;;
	void **cursor;
	int i;

	assert(file != NULL);

	(void)delete_flag;

	f = (struct dqlite__vfs_file*)file;

	// If reference count is 0, no shared memory map is set.
	if (f->content->shm_refcount == 0){
		return SQLITE_OK;
	}

	f->content->shm_refcount--;

	/* If we got zero references, free the entire map. */
	if (f->content->shm_refcount == 0) {
		cursor = f->content->shm_regions;
		for (i = 0; i < f->content->shm_regions_len; i++){
			sqlite3_free(*cursor);
			cursor++;
		}
		sqlite3_free(f->content->shm_regions);
		f->content->shm_regions = 0;
		f->content->shm_regions_len = 0;
	}

	return SQLITE_OK;
}

static int dqlite__vfs_open(
	sqlite3_vfs *vfs,     /* VFS */
	const char *filename, /* File to open, or 0 for a temp file */
	sqlite3_file *file,   /* Pointer to DemoFile struct to populate */
	int flags,            /* Input SQLITE_OPEN_XXX flags */
	int *out_flags        /* Output SQLITE_OPEN_XXX flags (or NULL) */
	)
{
	assert(vfs != NULL);
	assert(file != NULL );
	assert(filename != NULL);

	(void)out_flags;

	struct dqlite__vfs_root *root = (struct dqlite__vfs_root*)(vfs->pAppData);
	struct dqlite__vfs_file *f = (struct dqlite__vfs_file*)file;

	assert( root );

	struct dqlite__vfs_content *content;

	int free_slot = -1; /* Index of a free slot in the root->acontent array. */
	int exists = 0;     /* Whether the file exists already. */
	int type;           /* File content type (e.g. database or WAL). */
	int rc;             /* Return code. */

	/* Flags */
	int exclusive = flags & SQLITE_OPEN_EXCLUSIVE;
	int create    = flags & SQLITE_OPEN_CREATE;

	/* This signals SQLite to not call Close() in case we return an error. */
	f->base.pMethods = 0;

	pthread_mutex_lock(&root->mutex);

	/* Search if the file exists already, and (if it doesn't) if there are
	 * free slots. */
	free_slot = dqlite__vfs_root_content_lookup(root, filename, &content);
	exists = content != 0;

	/* If file exists, and the exclusive flag is on, then return an error.
	 *
	 * From sqlite3.h.in:
	 *
	 *   The SQLITE_OPEN_EXCLUSIVE flag is always used in conjunction with
	 *   the SQLITE_OPEN_CREATE flag, which are both directly analogous to
	 *   the O_EXCL and O_CREAT flags of the POSIX open() API.  The
	 *   SQLITE_OPEN_EXCLUSIVE flag, when paired with the
	 *   SQLITE_OPEN_CREATE, is used to indicate that file should always be
	 *   created, and that it is an error if it already exists.  It is not
	 *   used to indicate the file should be opened for exclusive access.
	 */
	if (exists && exclusive && create) {
		root->error = EEXIST;
		pthread_mutex_unlock(&root->mutex);
		return SQLITE_CANTOPEN;
	}

	if (!exists) {
		/* Check the create flag. */
		if (!create) {
			root->error = ENOENT;
			pthread_mutex_unlock(&root->mutex);
			return SQLITE_CANTOPEN;
		}

		/* This is a new file, so try to create a new entry. */
		if (free_slot == -1) {
			/* No more file content slots available. */
			root->error = ENFILE;
			pthread_mutex_unlock(&root->mutex);
			return SQLITE_CANTOPEN;
		}

		if (flags & SQLITE_OPEN_MAIN_DB ) {
			type = DQLITE__VFS_CONTENT_MAIN_DB;
		} else if (flags & SQLITE_OPEN_WAL) {
			type = DQLITE__VFS_CONTENT_WAL;
		} else {
			type = DQLITE__VFS_CONTENT_OTHER;
		}

		content = (struct dqlite__vfs_content*)sqlite3_malloc(sizeof(*content));
		if (content == NULL) {
			root->error = ENOMEM;
			pthread_mutex_unlock(&root->mutex);
			return SQLITE_NOMEM;
		}

		rc = dqlite__vfs_content_init(content, filename, type);
		if (rc != SQLITE_OK) {
			root->error = ENOMEM;
			pthread_mutex_unlock(&root->mutex);
			return SQLITE_NOMEM;
		}

		if (type == DQLITE__VFS_CONTENT_WAL) {
			/* An associated database file must have been opened. */
			struct dqlite__vfs_content *database;
			rc = dqlite__vfs_root_database_content_lookup(root, filename, &database);
			if (rc != SQLITE_OK) {
				root->error = ENOMEM;
				pthread_mutex_unlock(&root->mutex);
				return rc;
			}
			database->wal = content;
		}

		/* Save the new file content in a free entry of the root file
		 * content array. */
		*(root->contents + free_slot) = content;
	}

	// Populate the new file handle.
	static const sqlite3_io_methods io = {
		2,                                       // iVersion
		dqlite__vfs_close,                    // xClose
		dqlite__vfs_read,                     // xRead
		dqlite__vfs_write,                    // xWrite
		dqlite__vfs_truncate,                 // xTruncate
		dqlite__vfs_sync,                     // xSync
		dqlite__vfs_file_size,                 // xFileSize
		dqlite__vfs_lock,                     // xLock
		dqlite__vfs_unlock,                   // xUnlock
		dqlite__vfs_check_reserved_lock,        // xCheckReservedLock
		dqlite__vfs_file_control,              // xFileControl
		dqlite__vfs_sector_size,               // xSectorSize
		dqlite__vfs_device_characteristics,    // xDeviceCharacteristics
		dqlite__vfs_shm_map,                   // xShmMap
		dqlite__vfs_shm_lock,                  // xShmLock
		dqlite__vfs_shm_barrier,               // xShmBarrier
		dqlite__vfs_shm_unmap,                  // xShmUnmap
		0,
		0,
	};

	f->base.pMethods = &io;
	f->root = root;
	f->content = content;

	content->refcount++;

	pthread_mutex_unlock(&root->mutex);

	return SQLITE_OK;
}

static int dqlite__vfs_delete(
	sqlite3_vfs *vfs,
	const char *filename, int dir_sync)
{
	struct dqlite__vfs_root *root;
	struct dqlite__vfs_content *content;
	int content_index;

	assert(vfs != NULL);
	assert(vfs->pAppData != NULL);

	root = (struct dqlite__vfs_root*)(vfs->pAppData);

	(void)dir_sync;

	pthread_mutex_lock(&root->mutex);

	/* Check if the file exists. */
	content_index = dqlite__vfs_root_content_lookup(root, filename, &content);
	if (content == NULL) {
		root->error = ENOENT;
		pthread_mutex_unlock(&root->mutex);
		return SQLITE_IOERR_DELETE_NOENT;
	}

	/* Check that there are no consumers of this file. */
	if (content->refcount > 0) {
		root->error = EBUSY;
		pthread_mutex_unlock(&root->mutex);
		return SQLITE_IOERR_DELETE;
	}

	// Free all memory allocated for this file.
	dqlite__vfs_content_close(content, 0);
	sqlite3_free(content);

	// Reset the file content slot.
	*(root->contents + content_index) = 0;

	pthread_mutex_unlock(&root->mutex);

	return SQLITE_OK;
}

static int dqlite__vfs_access(
	sqlite3_vfs *vfs,
	const char *filename,
	int flags,
	int *result)
{
	struct dqlite__vfs_root *root;
	struct dqlite__vfs_content *content;

	assert(vfs != NULL);
	assert(vfs->pAppData != NULL);

	(void)flags;

	root = (struct dqlite__vfs_root*)(vfs->pAppData);

	pthread_mutex_lock(&root->mutex);

	/* If the file exists, access is always granted. */
	dqlite__vfs_root_content_lookup(root, filename, &content);
	if (content == NULL) {
		root->error = ENOENT;
		*result = 0;
	} else {
		*result = 1;
	}

	pthread_mutex_unlock(&root->mutex);

	return SQLITE_OK;
}

static int dqlite__vfs_full_pathname(
	sqlite3_vfs *vfs,     /* VFS */
	const char *filename, /* Input path (possibly a relative path) */
	int pathname_len,     /* Size of output buffer in bytes */
	char *pathname)       /* Pointer to output buffer */
{
	(void)vfs;

	// Just return the path unchanged.
	sqlite3_snprintf(pathname_len, pathname, "%s", filename);
	return SQLITE_OK;
}

static void* dqlite__vfs_dl_open(sqlite3_vfs *vfs, const char *filename)
{
	(void)vfs;
	(void)filename;

	return 0;
}

static void dqlite__vfs_dl_error(sqlite3_vfs *vfs, int nByte, char *zErrMsg)
{
	(void)vfs;

	sqlite3_snprintf(nByte, zErrMsg, "Loadable extensions are not supported");
	zErrMsg[nByte-1] = '\0';
}

static void (*dqlite__vfs_dl_sym(sqlite3_vfs *vfs, void *pH, const char *z))(void)
{
	(void)vfs;
	(void)pH;
	(void)z;

	return 0;
}

static void dqlite__vfs_dl_close(sqlite3_vfs *vfs, void *pHandle)
{
	(void)vfs;
	(void)pHandle;

	return;
}

static int dqlite__vfs_randomness(sqlite3_vfs *vfs, int nByte, char *zByte)
{
	(void)vfs;
	(void)nByte;
	(void)zByte;

	return SQLITE_OK;
	// TODO return volatileRandomness(nByte, zByte);
}

static int dqlite__vfs_sleep(sqlite3_vfs *vfs, int microseconds)
{
	(void)vfs;

	// Sleep in Go, to avoid the scheduler unconditionally preempting the
	// SQLite API call being invoked.
	return microseconds;
	// TODO return volatileSleep(microseconds);
}

static int dqlite__vfs_current_time_int64(sqlite3_vfs *vfs, sqlite3_int64 *piNow)
{
	static const sqlite3_int64 unixEpoch = 24405875*(sqlite3_int64)8640000;
	struct timeval now;

	(void)vfs;

	gettimeofday(&now, 0);
	*piNow = unixEpoch + 1000*(sqlite3_int64)now.tv_sec + now.tv_usec/1000;
	return SQLITE_OK;
}

static int dqlite__vfs_current_time(sqlite3_vfs *vfs, double *piNow)
{
	// TODO: check if it's always safe to cast a double* to a sqlite3_int64*.
	return dqlite__vfs_current_time_int64(vfs, (sqlite3_int64*)piNow);
}

static int dqlite__vfs_get_last_error(sqlite3_vfs *vfs, int NotUsed2, char *NotUsed3)
{
	struct dqlite__vfs_root *root = (struct dqlite__vfs_root*)(vfs->pAppData);
	int rc;

	(void)vfs;
	(void)NotUsed2;
	(void)NotUsed3;

	pthread_mutex_lock(&root->mutex);
	rc = root->error;
	pthread_mutex_unlock(&root->mutex);

	return rc;
}

int dqlite_vfs_register(const char *name, sqlite3_vfs **out) {
	sqlite3_vfs* vfs;
	char *zName;
	struct dqlite__vfs_root *root;
	int err;

	assert(name != NULL);
	assert(out != NULL);

	*out = 0; // In case of errors

	vfs = sqlite3_vfs_find(name);
	if (vfs != NULL) {
		return SQLITE_ERROR;
	}

	vfs = (sqlite3_vfs*)sqlite3_malloc(sizeof(sqlite3_vfs));
	if (vfs == NULL) {
		err = SQLITE_NOMEM;
		goto err_vfs_malloc;
	}

	root = (struct dqlite__vfs_root*)sqlite3_malloc(sizeof(*root));
	if (root == NULL) {
		err = SQLITE_NOMEM;
		goto err_vfs_root_malloc;
	}

	err = dqlite__vfs_root_init(root);
	if (err != 0) {
		assert(err == SQLITE_NOMEM);
		goto err_vfs_root_init;
	}

	zName = sqlite3_malloc(strlen(name) + 1);
	if (zName == NULL) {
		err = SQLITE_NOMEM;
		goto err_zname_malloc;
	}

	strcpy(zName, name);

	vfs->iVersion =          2;
	vfs->szOsFile =          sizeof(struct dqlite__vfs_file);
	vfs->mxPathname =        DQLITE__VFS_MAX_PATHNAME;
	vfs->pNext =             0;
	vfs->zName =             (const char*)zName;
	vfs->pAppData =          (void*)root;
	vfs->xOpen =             dqlite__vfs_open;
	vfs->xDelete =           dqlite__vfs_delete;
	vfs->xAccess =           dqlite__vfs_access;
	vfs->xFullPathname =     dqlite__vfs_full_pathname;
	vfs->xDlOpen =           dqlite__vfs_dl_open;
	vfs->xDlError =          dqlite__vfs_dl_error;
	vfs->xDlSym =            dqlite__vfs_dl_sym;
	vfs->xDlClose =          dqlite__vfs_dl_close;
	vfs->xRandomness =       dqlite__vfs_randomness;
	vfs->xSleep =            dqlite__vfs_sleep;
	vfs->xCurrentTime =      dqlite__vfs_current_time;
	vfs->xGetLastError =     dqlite__vfs_get_last_error;
	vfs->xCurrentTimeInt64 = dqlite__vfs_current_time_int64;

	sqlite3_vfs_register(vfs, 0);

	*out = vfs;

	return SQLITE_OK;

 err_zname_malloc:
 err_vfs_root_init:
	sqlite3_free(root);

 err_vfs_root_malloc:
	sqlite3_free(vfs);

 err_vfs_malloc:
	return err;
}

void dqlite_vfs_unregister(sqlite3_vfs* vfs) {
	struct dqlite__vfs_root *root;

	assert(vfs != NULL);

	sqlite3_vfs_unregister(vfs);

	root = (struct dqlite__vfs_root*)(vfs->pAppData);

	dqlite__vfs_root_close(root);

	sqlite3_free(root);
	sqlite3_free((char *)vfs->zName);
	sqlite3_free(vfs);
}
