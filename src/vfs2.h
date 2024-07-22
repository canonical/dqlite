#ifndef DQLITE_VFS2_H
#define DQLITE_VFS2_H

#include "../include/dqlite.h" /* dqlite_vfs_frame */

#include <sqlite3.h>

#include <stddef.h>
#include <stdint.h>

/**
 * Create a new VFS object that wraps the given VFS object.
 *
 * The returned VFS is allocated on the heap and lives until vfs2_destroy is
 * called. The provided name is not copied or freed, and must outlive the VFS.
 *
 * The methods of the resulting sqlite3_vfs object are thread-safe, but the
 * xOpen method creates sqlite3_file objects whose methods are not thread-safe.
 * Therefore, when a database connection is opened using the returned VFS, it
 * must not subsequently be used on other threads without additional
 * synchronization. This also goes for the functions below that operate directly
 * on sqlite3_file.
 */
sqlite3_vfs *vfs2_make(sqlite3_vfs *orig, const char *name);

/**
 * A pair of salt values from the header of a WAL file.
 */
struct vfs2_salts {
	uint8_t salt1[4];
	uint8_t salt2[4];
};

/**
 * Identifying information about a write transaction.
 *
 * The salts identify a WAL file. The `start` and `len` fields have units of
 * frames and identify a range of frames within that WAL file that represent a
 * transaction.
 */
struct vfs2_wal_slice {
	struct vfs2_salts salts;
	uint32_t start;
	uint32_t len;
};

/**
 * Retrieve a description of a write transaction that was just written to the
 * WAL.
 *
 * The first argument is the main file handle for the database. This function
 * also acquires the WAL write lock, preventing another write transaction from
 * overwriting the first one until vfs2_unhide is called.
 *
 * The `frames` out argument is populated with an array containing
 * the frames of the transaction, and the `sl` out argument is populated with a
 * WAL slice describing the transaction. The length of the frames array is
 * `sl.len`.
 *
 * Polling the same transaction more than once is an error.
 */
int vfs2_poll(sqlite3_file *file,
	      dqlite_vfs_frame **frames,
	      struct vfs2_wal_slice *sl);

/**
 * Mark a write transaction that was previously polled as committed, making it
 * visible to future transactions.
 *
 * This function also releases the WAL write lock, allowing another write
 * transaction to execute. It should be called on the main file handle
 * (SQLITE_FCNTL_FILE_POINTER).
 *
 * It's an error to call this function if no write transaction is currently
 * pending due to vfs2_poll.
 */
int vfs2_unhide(sqlite3_file *file);

/**
 * Make some transactions in the WAL visible to readers.
 *
 * The first argument is the main file for the database
 * (SQLITE_FCNTL_FILE_POINTER). All transactions up to and including the one
 * described by the second argument will be marked as committed and made
 * visible. The WAL write lock is also released if there are no uncommitted
 * transactions left in the WAL.
 *
 * The affected transactions must have been added to the WAL by
 * vfs2_add_committed.
 */
int vfs2_apply(sqlite3_file *file, struct vfs2_wal_slice stop);

/**
 * Add the frames of a write transaction directly to the end of the WAL.
 *
 * The first argument is the main file handle for the database. On success, the
 * WAL write lock is acquired if it was not held already. The added frames are
 * initially invisible to readers, and must be made visible by calling
 * vfs2_commit or removed from the WAL by calling vfs2_unadd.
 *
 * A WAL slice describing the new transaction is written to the last argument.
 *
 * The `page_size` for the new frames must match the page size already set for
 * this database.
 */
int vfs2_add_uncommitted(sqlite3_file *file,
			 uint32_t page_size,
			 const dqlite_vfs_frame *frames,
			 unsigned n,
			 struct vfs2_wal_slice *out);

/**
 * Remove some transactions from the WAL.
 *
 * The first argument is the main file handle for the database
 * (SQLITE_FCNTL_FILE_POINTER). The second argument is a WAL slice. The
 * transaction described by this slice, and all following transactions, will be
 * removed from the WAL. The WAL write lock will be released if there are no
 * uncommitted transactions in the WAL afterward.
 *
 * All removed transactions must have been added to the WAL by
 * vfs2_add_uncommitted, and must not have been made visible using vfs2_commit.
 */
int vfs2_unadd(sqlite3_file *file, struct vfs2_wal_slice stop);

/**
 * Request to read a specific transaction from a WAL file.
 */
struct vfs2_wal_txn {
	struct vfs2_wal_slice meta;
	dqlite_vfs_frame *frames;
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

/**
 * Try to set a read lock at a fixed place in the WAL-index.
 *
 * The first argument is the main file handle for the database
 * (SQLITE_FCNTL_FILE_POINTER). The second argument is the desired value for the
 * read mark, in units of frames. On success, the index of the read mark is
 * written to the last argument, and the corresponding WAL read lock is held.
 *
 * This function may fail if all read marks are in used when it is called.
 */
int vfs2_pseudo_read_begin(sqlite3_file *file, uint32_t target, unsigned *out);

/**
 * Unset a read mark that was set by vfs2_pseudo_read_begin.
 *
 * This also releases the corresponding read lock.
 */
int vfs2_pseudo_read_end(sqlite3_file *file, unsigned i);

/**
 * Destroy the VFS object.
 *
 * Call this from the same thread that called vfs2_make. No connection may be
 * open that uses this VFS.
 */
void vfs2_destroy(sqlite3_vfs *vfs);

/**
 * Declare a relationship between two databases opened by (possibly distinct)
 * vfs2 instances.
 *
 * Intended for use by unit tests. The arguments are main file handles
 * (SQLITE_FCNTL_FILE_POINTER).
 */
void vfs2_ut_sm_relate(sqlite3_file *orig, sqlite3_file *targ);

#endif
