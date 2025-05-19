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

struct vfsTransaction {
	uint32_t    n_pages;      /* Number of pages in the transaction. */
	uint64_t   *page_numbers; /* Page number for each page. */
	void      **pages;        /* Content of the pages. */
};

/* Check if the last sqlite3_step() call triggered a write transaction, and
 * return its content if so. */
int VfsPoll(sqlite3_vfs *vfs,
	    const char *database,
		struct vfsTransaction *transaction);

/* Append the given frames to the WAL. */
int VfsApply(sqlite3_vfs *vfs,
		const char *filename,
		const struct vfsTransaction *transaction);

/* Cancel a pending transaction. */
int VfsAbort(sqlite3_vfs *vfs, const char *filename);

/* Make a full snapshot of a database. */
int VfsSnapshot(sqlite3_vfs *vfs, const char *filename, void **data, size_t *n);

/**
 * Prepare a snapshot of the selected database, borrowing from the in-memory
 * state of the VFS.
 *
 * The provided array of buffers will be populated with pointers to the
 * in-memory database held by the VFS. It's forbidden to checkpoint the
 * database while these pointers are still in use. VfsDatabaseNumPages (with
 * `use_wal = true`) should be used to determine how many buffers are needed.
 */
int VfsShallowSnapshot(sqlite3_vfs *vfs,
		       const char *filename,
		       struct dqlite_buffer bufs[],
		       uint32_t n);

/* Copies the WAL into buf */
int VfsDiskSnapshotWal(sqlite3_vfs *vfs,
		       const char *path,
		       struct dqlite_buffer *buf);

/* `mmap` the database into buf. */
int VfsDiskSnapshotDb(sqlite3_vfs *vfs,
		      const char *path,
		      struct dqlite_buffer *buf);

int VfsSnapshotDisk(sqlite3_vfs *vfs,
		    const char *filename,
		    struct dqlite_buffer bufs[],
		    uint32_t n);

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

/**
 * Number of pages in the database.
 *
 * If `use_wal` is set, returns the number of pages that the database would have
 * after fully checkpointing the WAL.
 */
int VfsDatabaseNumPages(sqlite3_vfs *vfs,
			const char *filename,
			bool use_wal,
			uint32_t *n);

/* Returns the resulting size of the main file, wal file and n additional WAL
 * frames with the specified page_size. */
uint64_t VfsDatabaseSize(sqlite3_vfs *vfs,
			 const char *path,
			 unsigned n,
			 unsigned page_size);

/* Returns the the maximum size of the main file and wal file. */
uint64_t VfsDatabaseSizeLimit(sqlite3_vfs *vfs);

#endif /* VFS_H_ */
