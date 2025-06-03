#include "uv_writer.h"

#include <string.h>
#include <unistd.h>

#include "../raft.h"
#include "assert.h"
#include "heap.h"

/* Copy the error message from the request object to the writer object. */
static void uvWriterReqTransferErrMsg(struct UvWriterReq *req)
{
	ErrMsgPrintf(req->writer->errmsg, "%s", req->errmsg);
}

/* Set the request status according the given result code. */
static void uvWriterReqSetStatus(struct UvWriterReq *req, int result)
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

/* Remove the request from the queue of inflight writes and invoke the request
 * callback if set. */
static void uvWriterReqFinish(struct UvWriterReq *req)
{
	queue_remove(&req->queue);
	if (req->status != 0) {
		uvWriterReqTransferErrMsg(req);
	}
	req->cb(req, req->status);
}

/* Wrapper around the low-level OS syscall, providing a better error message. */
static int uvWriterIoSetup(unsigned n, aio_context_t *ctx, char *errmsg)
{
	int rv;
	rv = UvOsIoSetup(n, ctx);
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

/* Run blocking syscalls involved in a file write request.
 *
 * Perform a KAIO write request and synchronously wait for it to complete. */
static void uvWriterWorkCb(uv_work_t *work)
{
	struct UvWriterReq *req; /* Writer request object */
	struct UvWriter *w;      /* Writer object */
	aio_context_t ctx;       /* KAIO handle */
	struct iocb *iocbs;      /* Pointer to KAIO request object */
	struct io_event event;   /* KAIO response object */
	int n_events;
	int rv;

	req = work->data;
	w = req->writer;

	iocbs = &req->iocb;

	/* If more than one write in parallel is allowed, submit the AIO request
	 * using a dedicated context, to avoid synchronization issues between
	 * threads when multiple writes are submitted in parallel. This is
	 * suboptimal but in real-world users should use file systems and
	 * kernels with proper async write support. */
	if (w->n_events > 1) {
		ctx = 0;
		rv = uvWriterIoSetup(1 /* Maximum concurrent requests */, &ctx,
				     req->errmsg);
		if (rv != 0) {
			goto out;
		}
	} else {
		ctx = w->ctx;
	}

	/* Submit the request */
	rv = UvOsIoSubmit(ctx, 1, &iocbs);
	if (rv != 0) {
		/* UNTESTED: since we're not using NOWAIT and the parameters are
		 * valid, this shouldn't fail. */
		UvOsErrMsg(req->errmsg, "io_submit", rv);
		rv = RAFT_IOERR;
		goto out_after_io_setup;
	}

	/* Wait for the request to complete */
	n_events = UvOsIoGetevents(ctx, 1, 1, &event, NULL);
	assert(n_events == 1);
	if (n_events != 1) {
		/* UNTESTED */
		rv = n_events >= 0 ? -1 : n_events;
	}

out_after_io_setup:
	if (w->n_events > 1) {
		UvOsIoDestroy(ctx);
	}

out:
	if (rv != 0) {
		req->status = rv;
	} else {
		uvWriterReqSetStatus(req, (int)event.res);
	}

	return;
}

/* Callback run after writeWorkCb has returned. It normally invokes the write
 * request callback. */
static void uvWriterAfterWorkCb(uv_work_t *work, int status)
{
	struct UvWriterReq *req = work->data; /* Write file request object */
	assert(status == 0); /* We don't cancel worker requests */
	uvWriterReqFinish(req);
}

/* Callback fired when the event fd associated with AIO write requests should be
 * ready for reading (i.e. when a write has completed). */
static void uvWriterPollCb(uv_poll_t *poller, int status, int events)
{
	struct UvWriter *w = poller->data;
	uint64_t completed; /* True if the write is complete */
	unsigned i;
	int n_events;
	int rv;

	assert(w->event_fd >= 0);
	assert(status == 0);
	if (status != 0) {
		/* UNTESTED libuv docs: If an error happens while polling,
		 * status will be < 0 and corresponds with one of the UV_E*
		 * error codes. */
		goto fail_requests;
	}

	assert(events & UV_READABLE);

	/* Read the event file descriptor */
	rv = (int)read(w->event_fd, &completed, sizeof completed);
	if (rv != sizeof completed) {
		/* UNTESTED: According to eventfd(2) this is the only possible
		 * failure mode, meaning that epoll has indicated that the event
		 * FD is not yet ready. */
		assert(errno == EAGAIN);
		return;
	}

	/* TODO: this assertion fails in unit tests */
	/* assert(completed == 1); */

	/* Try to fetch the write responses.
	 *
	 * If we got here at least one write should have completed and io_events
	 * should return immediately without blocking. */
	n_events =
	    UvOsIoGetevents(w->ctx, 1, (long int)w->n_events, w->events, NULL);
	assert(n_events >= 1);
	if (n_events < 1) {
		/* UNTESTED */
		status = n_events == 0 ? -1 : n_events;
		goto fail_requests;
	}

	for (i = 0; i < (unsigned)n_events; i++) {
		struct io_event *event = &w->events[i];
		struct UvWriterReq *req = *((void **)&event->data);

		/* If we got EAGAIN, it means it was not possible to perform the
		 * write asynchronously, so let's fall back to the threadpool.
		 */
		if (event->res == -EAGAIN) {
			req->iocb.aio_flags &= (unsigned)~IOCB_FLAG_RESFD;
			req->iocb.aio_resfd = 0;
			req->iocb.aio_rw_flags &= ~RWF_NOWAIT;
			assert(req->work.data == NULL);
			req->work.data = req;
			rv = uv_queue_work(w->loop, &req->work, uvWriterWorkCb,
					   uvWriterAfterWorkCb);
			if (rv != 0) {
				/* UNTESTED: with the current libuv
				 * implementation this should never fail. */
				UvOsErrMsg(req->errmsg, "uv_queue_work", rv);
				req->status = RAFT_IOERR;
				goto finish;
			}
			continue;
		}

		uvWriterReqSetStatus(req, (int)event->res);

	finish:
		uvWriterReqFinish(req);
	}

	return;

fail_requests:
	while (!queue_empty(&w->poll_queue)) {
		queue *head;
		struct UvWriterReq *req;
		head = queue_head(&w->poll_queue);
		req = QUEUE_DATA(head, struct UvWriterReq, queue);
		uvWriterReqSetStatus(req, status);
		uvWriterReqFinish(req);
	}
}

int UvWriterInit(struct UvWriter *w,
		 struct uv_loop_s *loop,
		 uv_file fd,
		 bool direct /* Whether to use direct I/O */,
		 bool async /* Whether async I/O is available */,
		 unsigned max_concurrent_writes,
		 char *errmsg)
{
	void *data = w->data;
	int rv = 0;
	memset(w, 0, sizeof *w);
	w->data = data;
	w->loop = loop;
	w->fd = fd;
	w->async = async;
	w->ctx = 0;
	w->events = NULL;
	w->n_events = max_concurrent_writes;
	w->event_fd = -1;
	w->event_poller.data = NULL;
	w->check.data = NULL;
	w->close_cb = NULL;
	queue_init(&w->poll_queue);
	queue_init(&w->work_queue);
	w->closing = false;
	w->errmsg = errmsg;

	/* Set direct I/O if available. */
	if (direct) {
		rv = UvOsSetDirectIo(w->fd);
		if (rv != 0) {
			UvOsErrMsg(errmsg, "fcntl", rv);
			rv = RAFT_IOERR;
			goto err;
		}
	}

	/* Setup the AIO context. */
	rv = uvWriterIoSetup(w->n_events, &w->ctx, errmsg);
	if (rv != 0) {
		goto err;
	}

	/* Initialize the array of re-usable event objects. */
	w->events = RaftHeapCalloc(w->n_events, sizeof *w->events);
	if (w->events == NULL) {
		/* UNTESTED: todo */
		ErrMsgOom(errmsg);
		rv = RAFT_NOMEM;
		goto err_after_io_setup;
	}

	/* Create an event file descriptor to get notified when a write has
	 * completed. */
	rv = UvOsEventfd(0, UV_FS_O_NONBLOCK);
	if (rv < 0) {
		/* UNTESTED: should fail only with ENOMEM */
		UvOsErrMsg(errmsg, "eventfd", rv);
		rv = RAFT_IOERR;
		goto err_after_events_alloc;
	}
	w->event_fd = rv;

	rv = uv_poll_init(loop, &w->event_poller, w->event_fd);
	if (rv != 0) {
		/* UNTESTED: with the current libuv implementation this should
		 * never fail. */
		UvOsErrMsg(errmsg, "uv_poll_init", rv);
		rv = RAFT_IOERR;
		goto err_after_event_fd;
	}
	w->event_poller.data = w;

	rv = uv_check_init(loop, &w->check);
	if (rv != 0) {
		/* UNTESTED: with the current libuv implementation this should
		 * never fail. */
		UvOsErrMsg(errmsg, "uv_check_init", rv);
		rv = RAFT_IOERR;
		goto err_after_event_fd;
	}
	w->check.data = w;

	rv = uv_poll_start(&w->event_poller, UV_READABLE, uvWriterPollCb);
	if (rv != 0) {
		/* UNTESTED: with the current libuv implementation this should
		 * never fail. */
		UvOsErrMsg(errmsg, "uv_poll_start", rv);
		rv = RAFT_IOERR;
		goto err_after_event_fd;
	}

	return 0;

err_after_event_fd:
	UvOsClose(w->event_fd);
err_after_events_alloc:
	RaftHeapFree(w->events);
err_after_io_setup:
	UvOsIoDestroy(w->ctx);
err:
	assert(rv != 0);
	return rv;
}

static void uvWriterCleanUpAndFireCloseCb(struct UvWriter *w)
{
	assert(w->closing);

	UvOsClose(w->fd);
	RaftHeapFree(w->events);
	UvOsIoDestroy(w->ctx);

	if (w->close_cb != NULL) {
		w->close_cb(w);
	}
}

static void uvWriterPollerCloseCb(struct uv_handle_s *handle)
{
	struct UvWriter *w = handle->data;
	w->event_poller.data = NULL;

	/* Cancel all pending requests. */
	while (!queue_empty(&w->poll_queue)) {
		queue *head;
		struct UvWriterReq *req;
		head = queue_head(&w->poll_queue);
		req = QUEUE_DATA(head, struct UvWriterReq, queue);
		assert(req->work.data == NULL);
		req->status = RAFT_CANCELED;
		uvWriterReqFinish(req);
	}

	if (w->check.data != NULL) {
		return;
	}

	uvWriterCleanUpAndFireCloseCb(w);
}

static void uvWriterCheckCloseCb(struct uv_handle_s *handle)
{
	struct UvWriter *w = handle->data;
	w->check.data = NULL;
	if (w->event_poller.data != NULL) {
		return;
	}
	uvWriterCleanUpAndFireCloseCb(w);
}

static void uvWriterCheckCb(struct uv_check_s *check)
{
	struct UvWriter *w = check->data;
	if (!queue_empty(&w->work_queue)) {
		return;
	}
	uv_close((struct uv_handle_s *)&w->check, uvWriterCheckCloseCb);
}

void UvWriterClose(struct UvWriter *w, UvWriterCloseCb cb)
{
	int rv;
	assert(!w->closing);
	w->closing = true;
	w->close_cb = cb;

	/* We can close the event file descriptor right away, but we shouldn't
	 * close the main file descriptor or destroy the AIO context since there
	 * might be threadpool requests in flight. */
	UvOsClose(w->event_fd);

	rv = uv_poll_stop(&w->event_poller);
	assert(rv == 0); /* Can this ever fail? */

	uv_close((struct uv_handle_s *)&w->event_poller, uvWriterPollerCloseCb);

	/* If we have requests executing in the threadpool, we need to wait for
	 * them. That's done in the check callback. */
	if (!queue_empty(&w->work_queue)) {
		uv_check_start(&w->check, uvWriterCheckCb);
	} else {
		uv_close((struct uv_handle_s *)&w->check, uvWriterCheckCloseCb);
	}
}

/* Return the total lengths of the given buffers. */
static size_t lenOfBufs(const uv_buf_t bufs[], unsigned n)
{
	size_t len = 0;
	unsigned i;
	for (i = 0; i < n; i++) {
		len += bufs[i].len;
	}
	return len;
}

int UvWriterSubmit(struct UvWriter *w,
		   struct UvWriterReq *req,
		   const uv_buf_t bufs[],
		   unsigned n,
		   size_t offset,
		   UvWriterReqCb cb)
{
	int rv = 0;
	struct iocb *iocbs = &req->iocb;
	assert(!w->closing);

	/* TODO: at the moment we are not leveraging the support for concurrent
	 *       writes, so ensure that we're getting write requests
	 *       sequentially. */
	if (w->n_events == 1) {
		assert(queue_empty(&w->poll_queue));
		assert(queue_empty(&w->work_queue));
	}

	assert(w->fd >= 0);
	assert(w->event_fd >= 0);
	assert(w->ctx != 0);
	assert(req != NULL);
	assert(bufs != NULL);
	assert(n > 0);

	req->writer = w;
	req->len = lenOfBufs(bufs, n);
	req->status = -1;
	req->work.data = NULL;
	req->cb = cb;
	memset(&req->iocb, 0, sizeof req->iocb);
	memset(req->errmsg, 0, sizeof req->errmsg);

	req->iocb.aio_fildes = (uint32_t)w->fd;
	req->iocb.aio_lio_opcode = IOCB_CMD_PWRITEV;
	req->iocb.aio_reqprio = 0;
	*((void **)(&req->iocb.aio_buf)) = (void *)bufs;
	req->iocb.aio_nbytes = n;
	req->iocb.aio_offset = (int64_t)offset;
	*((void **)(&req->iocb.aio_data)) = (void *)req;

#if defined(RWF_HIPRI)
	/* High priority request, if possible */
	/* TODO: do proper kernel feature detection for this one. */
	/* req->iocb.aio_rw_flags |= RWF_HIPRI; */
#endif

#if defined(RWF_DSYNC)
	/* Use per-request synchronous I/O if available. Otherwise, we have
	 * opened the file with O_DSYNC. */
	/* TODO: do proper kernel feature detection for this one. */
	/* req->iocb.aio_rw_flags |= RWF_DSYNC; */
#endif

	/* If io_submit can be run in a 100% non-blocking way, we'll try to
	 * write without using the threadpool. */
	if (w->async) {
		req->iocb.aio_flags |= IOCB_FLAG_RESFD;
		req->iocb.aio_resfd = (uint32_t)w->event_fd;
		req->iocb.aio_rw_flags |= RWF_NOWAIT;
	}

	/* Try to submit the write request asynchronously */
	if (w->async) {
		queue_insert_tail(&w->poll_queue, &req->queue);
		rv = UvOsIoSubmit(w->ctx, 1, &iocbs);

		/* If no error occurred, we're done, the write request was
		 * submitted. */
		if (rv == 0) {
			goto done;
		}

		queue_remove(&req->queue);

		/* Check the reason of the error. */
		switch (rv) {
			case UV_EAGAIN:
				break;
			default:
				/* Unexpected error */
				UvOsErrMsg(w->errmsg, "io_submit", rv);
				rv = RAFT_IOERR;
				goto err;
		}

		/* Submitting the write would block, or NOWAIT is not
		 * supported. Let's run this request in the threadpool. */
		req->iocb.aio_flags &= (unsigned)~IOCB_FLAG_RESFD;
		req->iocb.aio_resfd = 0;
		req->iocb.aio_rw_flags &= ~RWF_NOWAIT;
	}

	/* If we got here it means we need to run io_submit in the threadpool.
	 */
	queue_insert_tail(&w->work_queue, &req->queue);
	req->work.data = req;
	rv = uv_queue_work(w->loop, &req->work, uvWriterWorkCb,
			   uvWriterAfterWorkCb);
	if (rv != 0) {
		/* UNTESTED: with the current libuv implementation this can't
		 * fail. */
		req->work.data = NULL;
		queue_remove(&req->queue);
		UvOsErrMsg(w->errmsg, "uv_queue_work", rv);
		rv = RAFT_IOERR;
		goto err;
	}

done:
	return 0;

err:
	assert(rv != 0);
	return rv;
}
