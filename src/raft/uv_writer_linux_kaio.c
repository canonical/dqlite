#include "uv_writer.h"

#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "../lib/assert.h"
#include "../raft.h"
#include "heap.h"
#include "uv_os.h"

#if !HAVE_LINUX_AIO_ABI_H
#error "Linux KAIO writer requires linux/aio_abi.h"
#endif

struct UvWriterLinuxKaio
{
	struct uv_loop_s *loop;
	uv_file fd;
	bool async;
	aio_context_t ctx;
	struct io_event *events;
	unsigned n_events;
	int event_fd;
	struct uv_poll_s event_poller;
	struct uv_check_s check;
	UvWriterCloseCb close_cb;
	queue poll_queue;
	queue work_queue;
	bool closing;
};

struct UvWriterLinuxKaioReq
{
	struct iocb iocb;
};

static size_t kaioLenOfBufs(const uv_buf_t bufs[], unsigned n)
{
	size_t len = 0;
	for (unsigned i = 0; i < n; i++) {
		len += bufs[i].len;
	}
	return len;
}

static void kaioReqTransferErrMsg(struct UvWriterReq *req)
{
	ErrMsgPrintf(req->writer->errmsg, "%s", req->errmsg);
}

static void kaioReqFreeImpl(struct UvWriterReq *req)
{
	RaftHeapFree(req->impl);
	req->impl = NULL;
}

static void kaioReqSetStatus(struct UvWriterReq *req, int result)
{
	if (result < 0) {
		ErrMsgPrintf(req->errmsg, "write failed: %d", result);
		req->status = RAFT_IOERR;
	} else if ((size_t)result < req->len) {
		ErrMsgPrintf(req->errmsg,
			     "short write: %d bytes instead of %zu", result,
			     req->len);
		req->status = RAFT_NOSPACE;
	} else {
		req->status = 0;
	}
}

static void kaioReqFinish(struct UvWriterReq *req)
{
	queue_remove(&req->queue);
	if (req->status != 0) {
		kaioReqTransferErrMsg(req);
	}
	kaioReqFreeImpl(req);
	req->cb(req, req->status);
}

static int kaioIoSetup(unsigned n, aio_context_t *ctx, char *errmsg)
{
	int rv = UvOsIoSetup(n, ctx);
	if (rv != 0) {
		switch (rv) {
			case UV_EAGAIN:
				ErrMsgPrintf(errmsg,
					     "AIO events user limit exceeded");
				rv = RAFT_TOOMANY;
				break;
			default:
				UvOsErrMsg(errmsg, "io_setup", rv);
				rv = RAFT_IOERR;
				break;
		}
		return rv;
	}
	return 0;
}

static void kaioWorkCb(uv_work_t *work)
{
	struct UvWriterReq *req = work->data;
	struct UvWriter *writer = req->writer;
	struct UvWriterLinuxKaio *impl = writer->impl;
	struct UvWriterLinuxKaioReq *req_impl = req->impl;
	aio_context_t ctx;
	struct iocb *iocbs = &req_impl->iocb;
	struct io_event event;
	int n_events;
	int rv;

	if (impl->n_events > 1) {
		ctx = 0;
		rv = kaioIoSetup(1, &ctx, req->errmsg);
		if (rv != 0) {
			goto out;
		}
	} else {
		ctx = impl->ctx;
	}

	rv = UvOsIoSubmit(ctx, 1, &iocbs);
	if (rv != 0) {
		UvOsErrMsg(req->errmsg, "io_submit", rv);
		rv = RAFT_IOERR;
		goto out_after_io_setup;
	}

	n_events = UvOsIoGetevents(ctx, 1, 1, &event, NULL);
	dqlite_assert(n_events == 1);
	if (n_events != 1) {
		rv = n_events >= 0 ? -1 : n_events;
	}

out_after_io_setup:
	if (impl->n_events > 1) {
		UvOsIoDestroy(ctx);
	}

out:
	if (rv != 0) {
		req->status = rv;
	} else {
		kaioReqSetStatus(req, (int)event.res);
	}
}

static void kaioAfterWorkCb(uv_work_t *work, int status)
{
	struct UvWriterReq *req = work->data;
	dqlite_assert(status == 0);
	kaioReqFinish(req);
}

static void kaioPollCb(uv_poll_t *poller, int status, int events)
{
	struct UvWriter *writer = poller->data;
	struct UvWriterLinuxKaio *impl = writer->impl;
	uint64_t completed;
	unsigned i;
	int n_events;
	int rv;

	dqlite_assert(impl->event_fd >= 0);
	dqlite_assert(status == 0);
	if (status != 0) {
		goto fail_requests;
	}

	dqlite_assert(events & UV_READABLE);

	rv = (int)read(impl->event_fd, &completed, sizeof completed);
	if (rv != sizeof completed) {
		dqlite_assert(errno == EAGAIN);
		return;
	}

	n_events = UvOsIoGetevents(impl->ctx, 1, (long int)impl->n_events,
				     impl->events, NULL);
	dqlite_assert(n_events >= 1);
	if (n_events < 1) {
		status = n_events == 0 ? -1 : n_events;
		goto fail_requests;
	}

	for (i = 0; i < (unsigned)n_events; i++) {
		struct io_event *event = &impl->events[i];
		struct UvWriterReq *req = *((void **)&event->data);
		struct UvWriterLinuxKaioReq *req_impl = req->impl;

		if (event->res == -EAGAIN) {
			req_impl->iocb.aio_flags &= (unsigned)~IOCB_FLAG_RESFD;
			req_impl->iocb.aio_resfd = 0;
			req_impl->iocb.aio_rw_flags &= ~RWF_NOWAIT;
			dqlite_assert(req->uv.work.data == NULL);
			queue_remove(&req->queue);
			queue_insert_tail(&impl->work_queue, &req->queue);
			req->uv.work.data = req;
			rv = uv_queue_work(impl->loop, &req->uv.work, kaioWorkCb,
					   kaioAfterWorkCb);
			if (rv != 0) {
				UvOsErrMsg(req->errmsg, "uv_queue_work", rv);
				req->status = RAFT_IOERR;
				goto finish;
			}
			continue;
		}

		kaioReqSetStatus(req, (int)event->res);

	finish:
		kaioReqFinish(req);
	}

	return;

fail_requests:
	while (!queue_empty(&impl->poll_queue)) {
		queue *head = queue_head(&impl->poll_queue);
		struct UvWriterReq *req =
		    QUEUE_DATA(head, struct UvWriterReq, queue);
		kaioReqSetStatus(req, status);
		kaioReqFinish(req);
	}
}

static int kaioInit(struct UvWriter *writer,
		    struct uv_loop_s *loop,
		    uv_file fd,
		    const struct UvWriterOptions *options,
		    char *errmsg)
{
	int rv = 0;
	struct UvWriterLinuxKaio *impl = RaftHeapCalloc(1, sizeof *impl);
	if (impl == NULL) {
		ErrMsgOom(errmsg);
		return RAFT_NOMEM;
	}
	impl->loop = loop;
	impl->fd = fd;
	impl->async = options->async;
	impl->ctx = 0;
	impl->events = NULL;
	impl->n_events = options->max_concurrent_writes;
	impl->event_fd = -1;
	queue_init(&impl->poll_queue);
	queue_init(&impl->work_queue);
	writer->impl = impl;

	if (options->direct) {
		rv = UvOsSetDirectIo(impl->fd);
		if (rv != 0) {
			UvOsErrMsg(errmsg, "fcntl", rv);
			rv = RAFT_IOERR;
			goto err;
		}
	}

	rv = kaioIoSetup(impl->n_events, &impl->ctx, errmsg);
	if (rv != 0) {
		goto err;
	}

	impl->events = RaftHeapCalloc(impl->n_events, sizeof *impl->events);
	if (impl->events == NULL) {
		ErrMsgOom(errmsg);
		rv = RAFT_NOMEM;
		goto err_after_io_setup;
	}

	rv = UvOsEventfd(0, UV_FS_O_NONBLOCK);
	if (rv < 0) {
		UvOsErrMsg(errmsg, "eventfd", rv);
		rv = RAFT_IOERR;
		goto err_after_events_alloc;
	}
	impl->event_fd = rv;

	rv = uv_poll_init(loop, &impl->event_poller, impl->event_fd);
	if (rv != 0) {
		UvOsErrMsg(errmsg, "uv_poll_init", rv);
		rv = RAFT_IOERR;
		goto err_after_event_fd;
	}
	impl->event_poller.data = writer;

	rv = uv_check_init(loop, &impl->check);
	if (rv != 0) {
		UvOsErrMsg(errmsg, "uv_check_init", rv);
		rv = RAFT_IOERR;
		goto err_after_event_fd;
	}
	impl->check.data = writer;

	rv = uv_poll_start(&impl->event_poller, UV_READABLE, kaioPollCb);
	if (rv != 0) {
		UvOsErrMsg(errmsg, "uv_poll_start", rv);
		rv = RAFT_IOERR;
		goto err_after_event_fd;
	}

	return 0;

err_after_event_fd:
	UvOsClose(impl->event_fd);
err_after_events_alloc:
	RaftHeapFree(impl->events);
err_after_io_setup:
	UvOsIoDestroy(impl->ctx);
err:
	writer->impl = NULL;
	RaftHeapFree(impl);
	dqlite_assert(rv != 0);
	return rv;
}

static void kaioCleanUpAndFireCloseCb(struct UvWriter *writer)
{
	struct UvWriterLinuxKaio *impl = writer->impl;
	dqlite_assert(impl->closing);

	UvOsClose(impl->fd);
	RaftHeapFree(impl->events);
	UvOsIoDestroy(impl->ctx);

	if (impl->close_cb != NULL) {
		impl->close_cb(writer);
	}
	RaftHeapFree(impl);
	writer->impl = NULL;
}

static void kaioPollerCloseCb(struct uv_handle_s *handle)
{
	struct UvWriter *writer = handle->data;
	struct UvWriterLinuxKaio *impl = writer->impl;
	impl->event_poller.data = NULL;

	while (!queue_empty(&impl->poll_queue)) {
		queue *head = queue_head(&impl->poll_queue);
		struct UvWriterReq *req =
		    QUEUE_DATA(head, struct UvWriterReq, queue);
		dqlite_assert(req->uv.work.data == NULL);
		req->status = RAFT_CANCELED;
		kaioReqFinish(req);
	}

	if (impl->check.data != NULL) {
		return;
	}

	kaioCleanUpAndFireCloseCb(writer);
}

static void kaioCheckCloseCb(struct uv_handle_s *handle)
{
	struct UvWriter *writer = handle->data;
	struct UvWriterLinuxKaio *impl = writer->impl;
	impl->check.data = NULL;
	if (impl->event_poller.data != NULL) {
		return;
	}
	kaioCleanUpAndFireCloseCb(writer);
}

static void kaioCheckCb(struct uv_check_s *check)
{
	struct UvWriter *writer = check->data;
	struct UvWriterLinuxKaio *impl = writer->impl;
	if (!queue_empty(&impl->work_queue)) {
		return;
	}
	uv_close((struct uv_handle_s *)&impl->check, kaioCheckCloseCb);
}

static void kaioClose(struct UvWriter *writer, UvWriterCloseCb cb)
{
	int rv;
	struct UvWriterLinuxKaio *impl = writer->impl;
	dqlite_assert(impl != NULL);
	dqlite_assert(!impl->closing);
	dqlite_assert(!writer->closing);
	writer->closing = true;
	impl->closing = true;
	impl->close_cb = cb;

	UvOsClose(impl->event_fd);

	rv = uv_poll_stop(&impl->event_poller);
	dqlite_assert(rv == 0);

	uv_close((struct uv_handle_s *)&impl->event_poller, kaioPollerCloseCb);

	if (!queue_empty(&impl->work_queue)) {
		uv_check_start(&impl->check, kaioCheckCb);
	} else {
		uv_close((struct uv_handle_s *)&impl->check, kaioCheckCloseCb);
	}
}

static int kaioSubmit(struct UvWriter *writer,
		      struct UvWriterReq *req,
		      const uv_buf_t bufs[],
		      unsigned n,
		      size_t offset,
		      UvWriterReqCb cb)
{
	int rv = 0;
	struct UvWriterLinuxKaio *impl = writer->impl;
	struct UvWriterLinuxKaioReq *req_impl;
	struct iocb *iocbs;
	dqlite_assert(impl != NULL);
	dqlite_assert(!impl->closing);

	if (impl->n_events == 1) {
		dqlite_assert(queue_empty(&impl->poll_queue));
		dqlite_assert(queue_empty(&impl->work_queue));
	}

	dqlite_assert(impl->fd >= 0);
	dqlite_assert(impl->event_fd >= 0);
	dqlite_assert(impl->ctx != 0);
	dqlite_assert(req != NULL);
	dqlite_assert(bufs != NULL);
	dqlite_assert(n > 0);

	req_impl = RaftHeapCalloc(1, sizeof *req_impl);
	if (req_impl == NULL) {
		ErrMsgOom(writer->errmsg);
		return RAFT_NOMEM;
	}
	req->impl = req_impl;
	iocbs = &req_impl->iocb;

	req->writer = writer;
	req->len = kaioLenOfBufs(bufs, n);
	req->status = -1;
	req->uv.work.data = NULL;
	req->cb = cb;
	memset(req->errmsg, 0, sizeof req->errmsg);

	req_impl->iocb.aio_fildes = (uint32_t)impl->fd;
	req_impl->iocb.aio_lio_opcode = IOCB_CMD_PWRITEV;
	req_impl->iocb.aio_reqprio = 0;
	*((void **)(&req_impl->iocb.aio_buf)) = (void *)bufs;
	req_impl->iocb.aio_nbytes = n;
	req_impl->iocb.aio_offset = (int64_t)offset;
	*((void **)(&req_impl->iocb.aio_data)) = (void *)req;

	if (impl->async) {
		req_impl->iocb.aio_flags |= IOCB_FLAG_RESFD;
		req_impl->iocb.aio_resfd = (uint32_t)impl->event_fd;
		req_impl->iocb.aio_rw_flags |= RWF_NOWAIT;
	}

	if (impl->async) {
		queue_insert_tail(&impl->poll_queue, &req->queue);
		rv = UvOsIoSubmit(impl->ctx, 1, &iocbs);
		if (rv == 0) {
			goto done;
		}

		queue_remove(&req->queue);
		switch (rv) {
			case UV_EAGAIN:
				break;
			default:
				UvOsErrMsg(writer->errmsg, "io_submit", rv);
				rv = RAFT_IOERR;
				goto err;
		}

		req_impl->iocb.aio_flags &= (unsigned)~IOCB_FLAG_RESFD;
		req_impl->iocb.aio_resfd = 0;
		req_impl->iocb.aio_rw_flags &= ~RWF_NOWAIT;
	}

	queue_insert_tail(&impl->work_queue, &req->queue);
	req->uv.work.data = req;
	rv = uv_queue_work(impl->loop, &req->uv.work, kaioWorkCb,
			   kaioAfterWorkCb);
	if (rv != 0) {
		req->uv.work.data = NULL;
		queue_remove(&req->queue);
		UvOsErrMsg(writer->errmsg, "uv_queue_work", rv);
		rv = RAFT_IOERR;
		goto err;
	}

	done:
	return 0;

err:
	kaioReqFreeImpl(req);
	dqlite_assert(rv != 0);
	return rv;
}

const struct UvWriterBackend UvWriterLinuxKaioBackend = {
    kaioInit,
    kaioClose,
    kaioSubmit,
};
