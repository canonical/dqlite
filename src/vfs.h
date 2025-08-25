#ifndef VFS_H_
#define VFS_H_

#include <sqlite3.h>

#include "config.h"

/* Initialize the given SQLite VFS interface with dqlite's custom
 * implementation. */
int VfsInit(struct sqlite3_vfs *vfs, const char *name);

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
int VfsPoll(sqlite3 *conn, struct vfsTransaction *transaction);

/* Append the given transaction to the WAL. */
int VfsApply(sqlite3 *conn, const struct vfsTransaction *transaction);

/* Cancel a pending transaction. */
int VfsAbort(sqlite3 *conn);

/* Performs a controlled checkpoint on conn */
int VfsCheckpoint(sqlite3 *conn, unsigned int threshold);

struct vfsSnapshotFile {
	void **pages;
	size_t page_count;
	size_t page_size;
};

struct vfsSnapshot {
	struct vfsSnapshotFile main;
	struct vfsSnapshotFile wal;
};

/* Acquires a snapshot from the connection conn. The snapshot wil be valid until
 * VfsReleaseSnapshot is called.
 *
 * An acquired snapshot will take relevant lock on the database to make sure
 * that memory remains valid until released.
 *
 * The logic will also attempt a checkpoint before returning to reduce the
 * snapshot size.
 */
int VfsAcquireSnapshot(sqlite3 *conn, struct vfsSnapshot *snapshot);

/* Releases a snapshot taken on conn. This means both releasing the locks on the
 * database and the memory associated with the snapshot. */
int VfsReleaseSnapshot(sqlite3 *conn, struct vfsSnapshot *snapshot);

/* Restore a database snapshot. */
int VfsRestore(sqlite3 *conn, const struct vfsSnapshot *snapshot);

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
