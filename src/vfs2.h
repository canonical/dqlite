#ifndef DQLITE_VFS2_H
#define DQLITE_VFS2_H

#include "../include/dqlite.h"

#include <sqlite3.h>

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
sqlite3_vfs *vfs2_make(sqlite3_vfs *orig, const char *name, unsigned page_size);

/**
 * Retrieve frames that were appended to the WAL by the last write transaction,
 * and reacquire the write lock.
 *
 * Call this on the database main file object (SQLITE_FCNTL_FILE_POINTER).
 *
 * Polling the same transaction more than once in any way is an error, and you
 * must choose only one of vfs2_poll or vfs2_shallow_poll.
 */
int vfs2_poll(sqlite3_file *file, dqlite_vfs_frame **frames, unsigned *n);

/**
 * Identifying information about a write transaction.
 */
struct vfs2_wal_slice
{
	uint8_t salt1[4];
	uint8_t salt2[4];
	uint32_t start;
	uint32_t len;
};

/**
 * Retrieve information about frames that the last write transaction appended to
 * the WAL, and reacquire the write lock.
 *
 * Call this on the database main file object (SQLITE_FCNTL_FILE_POINTER).
 *
 * Polling the same transaction more than once in any way is an error, and you
 * must choose only one of vfs2_poll or vfs2_shallow_poll.
 */
int vfs2_shallow_poll(sqlite3_file *file, struct vfs2_wal_slice *out);

/**
 * Unhide a write transaction that's been committed in Raft, and release the
 * write lock.
 *
 * Call this on the database main file object (SQLITE_FCNTL_FILE_POINTER).
 *
 * Calling this function when there is no pending transaction is an error.
 * It's okay to call it without polling the transaction.
 */
int vfs2_apply(sqlite3_file *file);

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
 * Destroy the VFS object.
 *
 * Call this from the same thread that called vfs2_make. No connection may be
 * open that uses this VFS.
 */
void vfs2_destroy(sqlite3_vfs *vfs);

// TODO access read marks and locks
// TODO access information about checkpoints

#endif
