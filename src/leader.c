#include <stdio.h>

#include "../include/dqlite.h"

#include "./lib/assert.h"

#include "command.h"
#include "leader.h"
#include "vfs.h"

static void maybeExecDone(struct exec *req)
{
	if (!req->done) {
		return;
	}
	req->leader->exec = NULL;
	if (req->cb != NULL) {
		req->cb(req, req->status);
	}
}

/* Open a SQLite connection and set it to leader replication mode. */
static int openConnection(const char *filename,
			  const char *vfs,
			  unsigned page_size,
			  sqlite3 **conn)
{
	char pragma[255];
	int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	char *msg = NULL;
	int rc;

	rc = sqlite3_open_v2(filename, conn, flags, vfs);
	if (rc != SQLITE_OK) {
		goto err;
	}

	/* Enable extended result codes */
	rc = sqlite3_extended_result_codes(*conn, 1);
	if (rc != SQLITE_OK) {
		goto err_after_open;
	}

	/* Set the page size. */
	sprintf(pragma, "PRAGMA page_size=%d", page_size);
	rc = sqlite3_exec(*conn, pragma, NULL, NULL, &msg);
	if (rc != SQLITE_OK) {
		goto err_after_open;
	}

	/* Disable syncs. */
	rc = sqlite3_exec(*conn, "PRAGMA synchronous=OFF", NULL, NULL, &msg);
	if (rc != SQLITE_OK) {
		goto err_after_open;
	}

	/* Set WAL journaling. */
	rc = sqlite3_exec(*conn, "PRAGMA journal_mode=WAL", NULL, NULL, &msg);
	if (rc != SQLITE_OK) {
		goto err_after_open;
	}

	rc = sqlite3_exec(*conn, "PRAGMA wal_autocheckpoint=0", NULL, NULL,
			  &msg);
	if (rc != SQLITE_OK) {
		goto err_after_open;
	}

	rc =
	    sqlite3_db_config(*conn, SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, 1, NULL);
	if (rc != SQLITE_OK) {
		goto err_after_open;
	}

	/* TODO: make setting foreign keys optional. */
	rc = sqlite3_exec(*conn, "PRAGMA foreign_keys=1", NULL, NULL, &msg);
	if (rc != SQLITE_OK) {
		goto err_after_open;
	}

	return 0;

err_after_open:
	sqlite3_close(*conn);
err:
	if (msg != NULL) {
		sqlite3_free(msg);
	}
	return rc;
}

/* Whether we need to submit a barrier request because there is no transaction
 * in progress in the underlying database and the FSM is behind the last log
 * index. */
static bool needsBarrier(struct leader *l)
{
	return l->db->tx_id == 0 &&
	       raft_last_applied(l->raft) < raft_last_index(l->raft);
}

int leader__init(struct leader *l, struct db *db, struct raft *raft)
{
	int rc;
	l->db = db;
	l->raft = raft;
	rc = openConnection(db->filename, db->config->name,
			    db->config->page_size, &l->conn);
	if (rc != 0) {
		return rc;
	}

	l->exec = NULL;
	l->apply.data = l;
	l->inflight = NULL;
	QUEUE__PUSH(&db->leaders, &l->queue);
	return 0;
}

void leader__close(struct leader *l)
{
	int rc;
	/* TODO: there shouldn't be any ongoing exec request. */
	if (l->exec != NULL) {
		assert(l->inflight == NULL);
		l->exec->done = true;
		l->exec->status = SQLITE_ERROR;
		maybeExecDone(l->exec);
	}
	rc = sqlite3_close(l->conn);
	assert(rc == 0);

	QUEUE__REMOVE(&l->queue);
}

static void leaderCheckpointApplyCb(struct raft_apply *req,
				    int status,
				    void *result)
{
	struct leader *l = req->data;
	(void)result;
	/* In case of failure, release the chekcpoint lock. */
	if (status != 0) {
		struct sqlite3_file *file;
		sqlite3_file_control(l->conn, "main", SQLITE_FCNTL_FILE_POINTER,
				     &file);
		file->pMethods->xShmLock(
		    file, 1 /* checkpoint lock */, 1,
		    SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE);
	}
	l->inflight = NULL;
	l->db->tx_id = 0;
	l->exec->done = true;
	maybeExecDone(l->exec);
}

/* Attempt to perform a checkpoint if possible. */
static bool leaderMaybeCheckpoint(struct leader *l)
{
	struct sqlite3_file *main;
	struct sqlite3_file *wal;
	struct raft_buffer buf;
	struct command_checkpoint command;
	volatile void *region;
	sqlite3_int64 size;
	unsigned page_size = l->db->config->page_size;
	unsigned pages;
	int i;
	int rv;

	/* Get the database file associated with this connection */
	rv = sqlite3_file_control(l->conn, "main", SQLITE_FCNTL_JOURNAL_POINTER,
				  &wal);
	assert(rv == SQLITE_OK); /* Should never fail */

	rv = wal->pMethods->xFileSize(wal, &size);
	assert(rv == SQLITE_OK); /* Should never fail */

	/* Calculate the number of frames. */
	pages = ((unsigned)size - 32) / (24 + page_size);

	/* Check if the size of the WAL is beyond the threshold. */
	if (pages < l->db->config->checkpoint_threshold) {
		return false;
	}

	/* Get the database file associated with this connection */
	rv = sqlite3_file_control(l->conn, "main", SQLITE_FCNTL_FILE_POINTER,
				  &main);
	assert(rv == SQLITE_OK); /* Should never fail */

	/* Get the first SHM region, which contains the WAL header. */
	rv = main->pMethods->xShmMap(main, 0, 0, 0, &region);
	assert(rv == SQLITE_OK); /* Should never fail */

	rv = main->pMethods->xShmUnmap(main, 0);
	assert(rv == SQLITE_OK); /* Should never fail */

	/* Try to acquire all locks. */
	for (i = 0; i < SQLITE_SHM_NLOCK; i++) {
		int flags = SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE;

		rv = main->pMethods->xShmLock(main, i, 1, flags);
		if (rv == SQLITE_BUSY) {
			/* There's a reader. Let's postpone the checkpoint
			 * for now. */
			return false;
		}

		/* Not locked. Let's release the lock we just
		 * acquired. */
		flags = SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE;
		main->pMethods->xShmLock(main, i, 1, flags);
	}

	/* Attempt to perfom a checkpoint across all nodes.
	 *
	 * TODO: reason about if it's indeed fine to ignore all kind of
	 * errors. */
	command.filename = l->db->filename;
	rv = command__encode(COMMAND_CHECKPOINT, &command, &buf);
	if (rv != 0) {
		goto abort;
	}

	rv = raft_apply(l->raft, &l->apply, &buf, 1, leaderCheckpointApplyCb);
	if (rv != 0) {
		goto abort_after_command_encode;
	}

	rv = main->pMethods->xShmLock(main, 1 /* checkpoint lock */, 1,
				      SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE);
	assert(rv == 0);

	return true;

abort_after_command_encode:
	raft_free(buf.base);
abort:
	assert(rv != 0);
	return false;
}

static void leaderApplyFramesCb(struct raft_apply *req,
				int status,
				void *result)
{
	struct apply *apply = req->data;
	struct leader *l = apply->leader;
	if (l == NULL) {
		raft_free(apply);
		return;
	}

	(void)result;

	if (status != 0) {
		sqlite3_vfs *vfs = sqlite3_vfs_find(l->db->config->name);
		switch (status) {
			case RAFT_LEADERSHIPLOST:
				l->exec->status = SQLITE_IOERR_LEADERSHIP_LOST;
				break;
			case RAFT_NOSPACE:
				l->exec->status = SQLITE_IOERR_WRITE;
				break;
			case RAFT_SHUTDOWN:
				/* If we got here it means we have manually
				 * fired the apply callback from
				 * gateway__close(). In this case we don't
				 * free() the apply object, since it will be
				 * freed when the callback is fired again by
				 * raft.
				 *
				 * TODO: we should instead make gatewa__close()
				 * itself asynchronous. */
				apply->leader = NULL;
				l->exec->status = SQLITE_ABORT;
				goto finish;
				break;
			default:
				l->exec->status = SQLITE_IOERR;
				break;
		}
		VfsAbort(vfs, l->db->filename);
	}

	raft_free(apply);

	if (status == 0 && leaderMaybeCheckpoint(l)) {
		/* Wait for the checkpoint to finish. */
		return;
	}

finish:
	l->inflight = NULL;
	l->db->tx_id = 0;
	l->exec->done = true;
	maybeExecDone(l->exec);
}

static int leaderApplyFrames(struct exec *req,
			     dqlite_vfs_frame *frames,
			     unsigned n)
{
	struct leader *l = req->leader;
	struct db *db = l->db;
	struct command_frames c;
	struct raft_buffer buf;
	struct apply *apply;
	int rv;

	c.filename = db->filename;
	c.tx_id = 0;
	c.truncate = 0;
	c.is_commit = 1;
	c.frames.n_pages = (uint32_t)n;
	c.frames.page_size = (uint16_t)db->config->page_size;
	c.frames.data = frames;

	apply = raft_malloc(sizeof *req);
	if (apply == NULL) {
		rv = DQLITE_NOMEM;
		goto err;
	}

	rv = command__encode(COMMAND_FRAMES, &c, &buf);
	if (rv != 0) {
		goto err_after_apply_alloc;
	}

	apply->leader = req->leader;
	apply->req.data = apply;
	apply->type = COMMAND_FRAMES;

	rv = raft_apply(l->raft, &apply->req, &buf, 1, leaderApplyFramesCb);
	if (rv != 0) {
		goto err_after_command_encode;
	}

	db->tx_id = 1;
	l->inflight = apply;

	return 0;

err_after_command_encode:
	raft_free(buf.base);
err_after_apply_alloc:
	raft_free(apply);
err:
	assert(rv != 0);
	return rv;
}

static void leaderExecV2(struct exec *req)
{
	struct leader *l = req->leader;
	struct db *db = l->db;
	sqlite3_vfs *vfs = sqlite3_vfs_find(db->config->name);
	dqlite_vfs_frame *frames;
	unsigned n;
	unsigned i;
	int rv;

	req->status = sqlite3_step(req->stmt);

	rv = VfsPoll(vfs, l->db->filename, &frames, &n);
	if (rv != 0 || n == 0) {
		goto finish;
	}

	rv = leaderApplyFrames(req, frames, n);
	for (i = 0; i < n; i++) {
		sqlite3_free(frames[i].data);
	}
	sqlite3_free(frames);
	if (rv != 0) {
		VfsAbort(vfs, l->db->filename);
		goto finish;
	}

	return;

finish:
	l->exec->done = true;
	if (rv != 0) {
		l->exec->status = rv;
	}
	maybeExecDone(l->exec);
}

static void execBarrierCb(struct barrier *barrier, int status)
{
	struct exec *req = barrier->data;
	struct leader *l = req->leader;
	if (status != 0) {
		l->exec->done = true;
		l->exec->status = status;
		maybeExecDone(l->exec);
		return;
	}
	leaderExecV2(req);
}

int leader__exec(struct leader *l,
		 struct exec *req,
		 sqlite3_stmt *stmt,
		 exec_cb cb)
{
	int rv;
	if (l->exec != NULL) {
		return SQLITE_BUSY;
	}
	l->exec = req;

	req->leader = l;
	req->stmt = stmt;
	req->cb = cb;
	req->done = false;
	req->barrier.data = req;

	rv = leader__barrier(l, &req->barrier, execBarrierCb);
	if (rv != 0) {
		return rv;
	}
	return 0;
}

static void raftBarrierCb(struct raft_barrier *req, int status)
{
	struct barrier *barrier = req->data;
	int rv = 0;
	if (status != 0) {
		if (status == RAFT_LEADERSHIPLOST) {
			rv = SQLITE_IOERR_LEADERSHIP_LOST;
		} else {
			rv = SQLITE_ERROR;
		}
	}
	barrier->cb(barrier, rv);
}

int leader__barrier(struct leader *l, struct barrier *barrier, barrier_cb cb)
{
	int rv;
	if (!needsBarrier(l)) {
		cb(barrier, 0);
		return 0;
	}
	barrier->cb = cb;
	barrier->leader = l;
	barrier->req.data = barrier;
	rv = raft_barrier(l->raft, &barrier->req, raftBarrierCb);
	if (rv != 0) {
		return rv;
	}
	return 0;
}
