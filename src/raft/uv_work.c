#include "assert.h"
#include "heap.h"
#include "uv.h"

struct uvAsyncWork
{
	struct uv *uv;
	struct raft_io_async_work *req;
	struct uv_work_s work;
	int status;
	queue queue;
};

static void uvAsyncWorkCb(uv_work_t *work)
{
	struct uvAsyncWork *w = work->data;
	assert(w != NULL);
	int rv;
	rv = w->req->work(w->req);
	w->status = rv;
}

static void uvAsyncAfterWorkCb(uv_work_t *work, int status)
{
	struct uvAsyncWork *w = work->data;
	struct raft_io_async_work *req = w->req;
	int req_status = w->status;
	struct uv *uv = w->uv;
	assert(status == 0);

	queue_remove(&w->queue);
	RaftHeapFree(w);
	req->cb(req, req_status);
	uvMaybeFireCloseCb(uv);
}

int UvAsyncWork(struct raft_io *io,
		struct raft_io_async_work *req,
		raft_io_async_work_cb cb)
{
	struct uv *uv;
	struct uvAsyncWork *async_work;
	int rv;

	uv = io->impl;
	assert(!uv->closing);

	async_work = RaftHeapMalloc(sizeof *async_work);
	if (async_work == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}

	async_work->uv = uv;
	async_work->req = req;
	async_work->work.data = async_work;
	req->cb = cb;

	queue_insert_tail(&uv->async_work_reqs, &async_work->queue);
	rv = uv_queue_work(uv->loop, &async_work->work, uvAsyncWorkCb,
			   uvAsyncAfterWorkCb);
	if (rv != 0) {
		queue_remove(&async_work->queue);
		tracef("async work: %s", uv_strerror(rv));
		rv = RAFT_IOERR;
		goto err_after_req_alloc;
	}

	return 0;

err_after_req_alloc:
	RaftHeapFree(async_work);
err:
	assert(rv != 0);
	return rv;
}

