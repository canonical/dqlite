#include <string.h>
#include <unistd.h>

#include "../lib/sm.h" /* struct sm */
#include "assert.h"
#include "byte.h"
#include "heap.h"
#include "uv.h"
#include "uv_encoding.h"

enum {
	TRUNC_START,
	TRUNC_BARRIER,
	TRUNC_WORK,
	TRUNC_LISTED,
	TRUNC_TRUNCATED,
	TRUNC_REMOVED,
	TRUNC_SYNCED,
	TRUNC_DONE,
	TRUNC_FAIL,
	TRUNC_NR,
};

static struct sm_conf trunc_states[TRUNC_NR] = {
	[TRUNC_START] = {
		.name = "start",
		.allowed = BITS(TRUNC_BARRIER)
			  |BITS(TRUNC_FAIL),
		.flags = SM_INITIAL,
	},
	[TRUNC_BARRIER] = {
		.name = "barrier",
		.allowed = BITS(TRUNC_WORK)
			  |BITS(TRUNC_DONE)
			  |BITS(TRUNC_FAIL),
	},
	[TRUNC_WORK] = {
		.name = "work",
		.allowed = BITS(TRUNC_LISTED)
			  |BITS(TRUNC_FAIL),

	},
	[TRUNC_LISTED] = {
		.name = "listed",
		.allowed = BITS(TRUNC_TRUNCATED)
			  |BITS(TRUNC_REMOVED)
			  |BITS(TRUNC_SYNCED)
			  |BITS(TRUNC_FAIL),
	},
	[TRUNC_TRUNCATED] = {
		.name = "truncated",
		.allowed = BITS(TRUNC_REMOVED)
			  |BITS(TRUNC_SYNCED)
			  |BITS(TRUNC_FAIL),
	},
	[TRUNC_REMOVED] = {
		.name = "truncated",
		.allowed = BITS(TRUNC_REMOVED)
			  |BITS(TRUNC_SYNCED)
			  |BITS(TRUNC_FAIL),
	},
	[TRUNC_SYNCED] = {
		.name = "synced",
		.allowed = BITS(TRUNC_DONE)
			  |BITS(TRUNC_FAIL),
	},
	[TRUNC_DONE] = {
		.name = "done",
		.flags = SM_FINAL,
	},
	[TRUNC_FAIL] = {
		.name = "fail",
		.flags = SM_FINAL|SM_FAILURE,
	},
};

static bool trunc_invariant(const struct sm *sm, int prev)
{
	(void)sm;
	(void)prev;
	return true;
}

/* Track a truncate request. */
struct uvTruncate
{
	struct uv *uv;
	struct UvBarrierReq barrier;
	raft_index index;
	struct raft_io_truncate *orig;
	int status;
};

static void truncate_done(struct uvTruncate *trunc, int status)
{
	if (status == 0) {
		sm_move(&trunc->orig->sm, TRUNC_DONE);
	} else {
		sm_fail(&trunc->orig->sm, TRUNC_FAIL, status);
	}
	sm_fini(&trunc->orig->sm);
	RaftHeapFree(trunc->orig);
	RaftHeapFree(trunc);
}

/* Execute a truncate request in a thread. */
static void uvTruncateWorkCb(uv_work_t *work)
{
	struct uvTruncate *truncate = work->data;
	struct uv *uv = truncate->uv;
	tracef("uv truncate work cb");
	struct uvSnapshotInfo *snapshots;
	struct uvSegmentInfo *segments;
	struct uvSegmentInfo *segment;
	size_t n_snapshots;
	size_t n_segments;
	size_t i;
	size_t j;
	char errmsg[RAFT_ERRMSG_BUF_SIZE];
	int rv;

	sm_move(&truncate->orig->sm, TRUNC_WORK);

	/* Load all segments on disk. */
	rv = UvList(uv, &snapshots, &n_snapshots, &segments, &n_segments,
		    errmsg);
	if (rv != 0) {
		goto err;
	}
	if (snapshots != NULL) {
		RaftHeapFree(snapshots);
	}
	assert(segments != NULL);

	sm_move(&truncate->orig->sm, TRUNC_LISTED);

	/* Find the segment that contains the truncate point. */
	segment = NULL; /* Suppress warnings. */
	for (i = 0; i < n_segments; i++) {
		segment = &segments[i];
		if (segment->is_open) {
			continue;
		}
		if (truncate->index >= segment->first_index &&
		    truncate->index <= segment->end_index) {
			break;
		}
	}
	assert(i < n_segments);

	/* If the truncate index is not the first of the segment, we need to
	 * truncate it. */
	if (truncate->index > segment->first_index) {
		rv = uvSegmentTruncate(uv, segment, truncate->index);
		if (rv != 0) {
			goto err_after_list;
		}
		sm_move(&truncate->orig->sm, TRUNC_TRUNCATED);
	}

	/* Remove all closed segments past the one containing the truncate
	 * index. */
	for (j = i; j < n_segments; j++) {
		segment = &segments[j];
		if (segment->is_open) {
			continue;
		}
		rv = UvFsRemoveFile(uv->dir, segment->filename, errmsg);
		if (rv != 0) {
			tracef("unlink segment %s: %s", segment->filename,
			       errmsg);
			rv = RAFT_IOERR;
			goto err_after_list;
		}
		sm_move(&truncate->orig->sm, TRUNC_REMOVED);
	}
	rv = UvFsSyncDir(uv->dir, errmsg);
	if (rv != 0) {
		tracef("sync data directory: %s", errmsg);
		rv = RAFT_IOERR;
		goto err_after_list;
	}
	sm_move(&truncate->orig->sm, TRUNC_SYNCED);

	RaftHeapFree(segments);
	truncate->status = 0;

	tracef("uv truncate work cb ok");
	return;

err_after_list:
	RaftHeapFree(segments);
err:
	assert(rv != 0);
	truncate->status = rv;
}

static void uvTruncateAfterWorkCb(uv_work_t *work, int status)
{
	assert(work != NULL);
	struct uvTruncate *truncate = work->data;
	assert(truncate != NULL);
	struct uv *uv = truncate->uv;
	assert(uv != NULL);
	tracef("uv truncate after work cb status:%d", status);
	assert(status == 0);
	if (truncate->status != 0) {
		uv->errored = true;
	}
	tracef("clear truncate work");
	uv->truncate_work.data = NULL;
	truncate_done(truncate, truncate->status);
	UvUnblock(uv);
}

static void uvTruncateBarrierCb(struct UvBarrierReq *barrier)
{
	struct uvTruncate *truncate = barrier->data;
	struct uv *uv = truncate->uv;
	tracef("uv truncate barrier cb");
	int rv;

	/* Ensure that we don't invoke this callback more than once. */
	barrier->cb = NULL;

	sm_move(&truncate->orig->sm, TRUNC_BARRIER);

	/* If we're closing, don't perform truncation at all and abort here. */
	if (uv->closing) {
		tracef("closing => don't truncate");
		truncate_done(truncate, 0);
		uvMaybeFireCloseCb(uv);
		return;
	}

	assert(queue_empty(&uv->append_writing_reqs));
	assert(queue_empty(&uv->finalize_reqs));
	assert(uv->finalize_work.data == NULL);
	assert(uv->truncate_work.data == NULL);

	tracef("set truncate work");
	uv->truncate_work.data = truncate;
	rv = uv_queue_work(uv->loop, &uv->truncate_work, uvTruncateWorkCb,
			   uvTruncateAfterWorkCb);
	if (rv != 0) {
		tracef("truncate index %lld: %s", truncate->index,
		       uv_strerror(rv));
		tracef("clear truncate work");
		uv->truncate_work.data = NULL;
		uv->errored = true;
	}
}

int UvTruncate(struct raft_io *io,
	       struct raft_io_truncate *orig,
	       raft_index index)
{
	struct uv *uv;
	struct uvTruncate *truncate;
	int rv;

	uv = io->impl;
	tracef("uv truncate %llu", index);
	assert(!uv->closing);

	/* We should truncate only entries that we were requested to append in
	 * the first place. */
	assert(index > 0);
	assert(index < uv->append_next_index);

	sm_init(&orig->sm, trunc_invariant, NULL, trunc_states, "trunc", TRUNC_START);
	truncate = RaftHeapMalloc(sizeof *truncate);
	if (truncate == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}
	truncate->uv = uv;
	truncate->index = index;
	truncate->barrier.data = truncate;
	truncate->barrier.blocking = true;
	truncate->barrier.cb = uvTruncateBarrierCb;
	truncate->orig = orig;

	/* Make sure that we wait for any inflight writes to finish and then
	 * close the current segment. */
	rv = UvBarrier(uv, index, &truncate->barrier);
	if (rv != 0) {
		goto err_after_req_alloc;
	}

	return 0;

err_after_req_alloc:
	RaftHeapFree(truncate);
err:
	assert(rv != 0);
	sm_fail(&orig->sm, TRUNC_FAIL, rv);
	return rv;
}

#undef tracef
