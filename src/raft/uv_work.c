#include "../lib/assert.h"
#include "heap.h"
#include "uv.h"

struct uvAsyncWork
{
	struct uv *uv;
	struct raft_io_async_work *req;
#ifdef DQLITE_DISABLE_SQLITE_THREADPOOL
	struct uv_idle_s idle;
#else
	struct uv_work_s work;
#endif
	int status;
	queue queue;
};

#ifdef DQLITE_DISABLE_SQLITE_THREADPOOL
static void uvAsyncCloseCb(uv_handle_t *handle)
{
	struct uvAsyncWork *w = handle->data;
	struct uv *uv = w->uv;
	struct raft_io_async_work *req = w->req;
	int status = w->status;

	queue_remove(&w->queue);
	RaftHeapFree(w);
	req->cb(req, status);
	uvMaybeFireCloseCb(uv);
}

static void uvAsyncWorkCb(uv_idle_t *idle)
{
	struct uvAsyncWork *w = idle->data;
	struct raft_io_async_work *req = w->req;
	int rv;

	uv_idle_stop(idle);
	rv = req->work(req);
	w->status = rv;
	uv_close((uv_handle_t *)idle, uvAsyncCloseCb);
}
#else
static void uvAsyncWorkCb(uv_work_t *work)
{
	struct uvAsyncWork *w = work->data;
	dqlite_assert(w != NULL);
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
	dqlite_assert(status == 0);

	queue_remove(&w->queue);
	RaftHeapFree(w);
	req->cb(req, req_status);
	uvMaybeFireCloseCb(uv);
}
#endif

int UvAsyncWork(struct raft_io *io,
		struct raft_io_async_work *req,
		raft_io_async_work_cb cb)
{
	struct uv *uv;
	struct uvAsyncWork *async_work;
	int rv;

	uv = io->impl;
	dqlite_assert(!uv->closing);

	async_work = RaftHeapMalloc(sizeof *async_work);
	if (async_work == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}

	async_work->uv = uv;
	async_work->req = req;
#ifdef DQLITE_DISABLE_SQLITE_THREADPOOL
	rv = uv_idle_init(uv->loop, &async_work->idle);
	if (rv != 0) {
		tracef("async work: %s", uv_strerror(rv));
		rv = RAFT_IOERR;
		goto err_after_req_alloc;
	}
	async_work->idle.data = async_work;
#else
	async_work->work.data = async_work;
#endif
	req->cb = cb;

	queue_insert_tail(&uv->async_work_reqs, &async_work->queue);
#ifdef DQLITE_DISABLE_SQLITE_THREADPOOL
	rv = uv_idle_start(&async_work->idle, uvAsyncWorkCb);
#else
	rv = uv_queue_work(uv->loop, &async_work->work, uvAsyncWorkCb,
			   uvAsyncAfterWorkCb);
#endif
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
	dqlite_assert(rv != 0);
	return rv;
}

