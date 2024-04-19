#ifndef DQLITE_VFS2_H
#define DQLITE_VFS2_H

#include <sqlite3.h>

#include <stddef.h>
#include <stdint.h>

/**
 * Create a new VFS object that wraps the given VFS object.
 *
 * The returned VFS is allocated on the heap and lives until vfs2_destroy is
 * called.  Its methods are thread-safe if those of the wrapped VFS are, but
 * the methods of the sqlite3_file objects it creates are not thread-safe.
 * Therefore, a database connection that's created using this VFS should only
 * be used on the thread that opened it. The functions below that operate on
 * sqlite3_file objects created by this VFS should also only be used on that
 * thread.
 */
sqlite3_vfs *vfs2_make(sqlite3_vfs *orig, const char *name);

struct vfs2_salts {
	uint8_t salt1[4];
	uint8_t salt2[4];
};

/**
 * Identifying information about a write transaction.
 */
struct vfs2_wal_slice {
	struct vfs2_salts salts;
	uint32_t start;
	uint32_t len;
};

struct vfs2_wal_frame {
	uint32_t page_number;
	uint32_t commit;
	void *page;
};

/**
 * Retrieve frames that were appended to the WAL by the last write transaction,
 * and reacquire the write lock.
 *
 * Call this on the database main file object (SQLITE_FCNTL_FILE_POINTER).
 *
 * Polling the same transaction more than once is an error.
 */
int vfs2_poll(sqlite3_file *file, struct vfs2_wal_frame **frames, unsigned *n, struct vfs2_wal_slice *sl);

int vfs2_unhide(sqlite3_file *file);

int vfs2_commit(sqlite3_file *file, struct vfs2_wal_slice stop);

int vfs2_commit_barrier(sqlite3_file *file);

int vfs2_apply_uncommitted(sqlite3_file *file, uint32_t page_size, const struct vfs2_wal_frame *frames, unsigned n, struct vfs2_wal_slice *out);

int vfs2_unapply(sqlite3_file *file, struct vfs2_wal_slice stop);

struct vfs2_wal_txn {
	struct vfs2_wal_slice meta;
	struct vfs2_wal_frame *frames;
};

/**
 * Synchronously read some transaction data directly from the WAL.
 *
 * Fill the `meta` field of each vfs2_wal_txn with a slice that was previously
 * returned by vfs2_shallow_poll. On return, this function will set the `frames`
 * field of each vfs2_wal_txn, using memory from the SQLite allocator that the
 * caller must free, if the transaction was read successfully. Setting this
 * field to NULL means that the transaction couldn't be read.
 */
int vfs2_read_wal(sqlite3_file *file,
		  struct vfs2_wal_txn *txns,
		  size_t txns_len);

/**
 * Cancel a pending transaction and release the write lock.
 *
 * Call this on the database main file object (SQLITE_FCNTL_FILE_POINTER).
 *
 * Calling this function when there is no pending transaction is an error.
 * It's okay to call it whether or not the transaction has been polled.
 */
int vfs2_abort(sqlite3_file *file);

int vfs2_pseudo_read_begin(sqlite3_file *file, uint32_t target, unsigned *out);

int vfs2_pseudo_read_end(sqlite3_file *file, unsigned i);

/**
 * Destroy the VFS object.
 *
 * Call this from the same thread that called vfs2_make. No connection may be
 * open that uses this VFS.
 */
void vfs2_destroy(sqlite3_vfs *vfs);

// TODO access read marks and shm_locks
// TODO access information about checkpoints

#endif
