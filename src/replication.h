/******************************************************************************
 *
 * Raft-based implementation of the SQLite replication interface.
 *
 *****************************************************************************/

#ifndef DQLITE_REPLICATION_H
#define DQLITE_REPLICATION_H

#ifdef DQLITE_EXPERIMENTAL

#include <libco.h>
#include <sqlite3.h>

/* Application context object for sqlite3_wal_replication. */
struct dqlite__replication_ctx {
	cothread_t main_coroutine;
};

/* Initialize a replication context object. */
void dqlite__replication_ctx_init(struct dqlite__replication_ctx *c);

/* Close a replication context object, releasing all associated resources. */
void dqlite__replication_ctx_close(struct dqlite__replication_ctx *r);

/* Implementation of the xBegin hook */
int dqlite__replication_begin(sqlite3_wal_replication *r, void *arg);

/* Implementation of the xAbort hook */
int dqlite__replication_abort(sqlite3_wal_replication *r, void *arg);

/* Implementation of the xFrames hook */
int dqlite__replication_frames(sqlite3_wal_replication *      r,
                               void *                         arg,
                               int                            page_size,
                               int                            n,
                               sqlite3_wal_replication_frame *frames,
                               unsigned                       truncate,
                               int                            commit);

/* Implementation of the xUndo hook */
int dqlite__replication_undo(sqlite3_wal_replication *r, void *arg);

/* Implementation of the xEnd hook */
int dqlite__replication_end(sqlite3_wal_replication *r, void *arg);

#endif /* DQLITE_EXPERIMENTAL */

#endif /* DQLITE_REPLICATION_H */
