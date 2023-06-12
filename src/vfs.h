#ifndef VFS_H_
#define VFS_H_

#include <sqlite3.h>

#include "config.h"

/* Initialize the given SQLite VFS interface with dqlite's custom
 * implementation. */
int VfsInit(struct sqlite3_vfs *vfs, const char *name);

int VfsEnableDisk(struct sqlite3_vfs *vfs);

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
 * will contain a copy of the WAL. `bufs` MUST point to an array of n
 * `dqlite_buffer` structs and n MUST equal 1 + the number of pages in
 * the database. */
int VfsShallowSnapshot(sqlite3_vfs *vfs, const char *filename, struct dqlite_buffer bufs[], uint32_t n);

/* Copies the WAL into buf */
int VfsDiskSnapshotWal(sqlite3_vfs *vfs, const char *path, struct dqlite_buffer *buf);

/* `mmap` the database into buf. */
int VfsDiskSnapshotDb(sqlite3_vfs *vfs, const char *path, struct dqlite_buffer *buf);

/* Restore a database snapshot. */
int VfsRestore(sqlite3_vfs *vfs,
	       const char *filename,
	       const void *data,
	       size_t n);

/* Restore a disk database snapshot. */
int VfsDiskRestore(sqlite3_vfs *vfs,
	       const char *path,
	       const void *data,
	       size_t main_size,
	       size_t wal_size);

/* Number of pages in the database. */
int VfsDatabaseNumPages(sqlite3_vfs *vfs,
			const char* filename,
			uint32_t *n);

/* Returns the resulting size of the main file, wal file and n additional WAL
 * frames with the specified page_size. */
uint64_t VfsDatabaseSize(sqlite3_vfs *vfs, const char *path, unsigned n, unsigned page_size);

/* Returns the the maximum size of the main file and wal file. */
uint64_t VfsDatabaseSizeLimit(sqlite3_vfs *vfs);

#endif /* VFS_H_ */
