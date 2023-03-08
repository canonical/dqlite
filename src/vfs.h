#ifndef VFS_H_
#define VFS_H_

#include "sqlite3.h"

#include "config.h"

#ifndef EXPOSE_VFS_STUFF

/**
 * A single WAL frame to be replicated.
 */
struct dqlite_vfs_frame
{
	unsigned long page_number; /* Database page number. */
	void *data;                /* Content of the database page. */
};
typedef struct dqlite_vfs_frame dqlite_vfs_frame;

/**
 * A data buffer.
 */
struct dqlite_buffer
{
	void *base; /* Pointer to the buffer data. */
	size_t len; /* Length of the buffer. */
};

#endif

/* Initialize the given SQLite VFS interface with dqlite's custom
 * implementation. */
int VfsInit(struct sqlite3_vfs *vfs, const char *name);

/* Release all memory associated with the given dqlite in-memory VFS
 * implementation.
 *
 * This function also automatically unregister the implementation from the
 * SQLite global registry. */
void VfsClose(struct sqlite3_vfs *vfs);

/* Check if the last sqlite3_step() call triggered a write transaction, and
 * return its content if so. */
int VfsPoll(sqlite3_vfs *vfs,
	    const char *database,
	    dqlite_vfs_frame **frames,
	    unsigned *n);

/* Append the given frames to the WAL. */
int VfsApply(sqlite3_vfs *vfs,
	     const char *filename,
	     unsigned n,
	     unsigned long *page_numbers,
	     void *frames);

/* Cancel a pending transaction. */
int VfsAbort(sqlite3_vfs *vfs, const char *filename);

/* Make a full snapshot of a database. */
int VfsSnapshot(sqlite3_vfs *vfs, const char *filename, void **data, size_t *n);

/* Makes a full, shallow snapshot of a database. The first n-1 buffers will each
 * contain a pointer to the actual database pages, while the n'th buffer
 * will contain a copy of the wal. `bufs` MUST point to an array of n
 * `dqlite_buffer` structs and n MUST equal 1 + the number of pages in
 * the database. */
int VfsShallowSnapshot(sqlite3_vfs *vfs, const char *filename, struct dqlite_buffer bufs[], uint32_t n);

/* Restore a database snapshot. */
int VfsRestore(sqlite3_vfs *vfs,
	       const char *filename,
	       const void *data,
	       size_t n);

/* Number of pages in the database. */
int VfsDatabaseNumPages(sqlite3_vfs *vfs,
			const char* filename,
			uint32_t *n);

#endif /* VFS_H_ */
