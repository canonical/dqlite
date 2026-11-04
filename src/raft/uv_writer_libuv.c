#include "uv_writer.h"

#include <string.h>

#include "../lib/assert.h"
#include "../raft.h"
#include "heap.h"

struct UvWriterLibuv
{
	struct uv_loop_s *loop;
	uv_file fd;
	uv_fs_t close_req;
	UvWriterCloseCb close_cb;
	queue pending;
	bool closing;
	bool close_started;
};

static size_t libuvLenOfBufs(const uv_buf_t bufs[], unsigned n)
{
	size_t len = 0;
	for (unsigned i = 0; i < n; i++) {
		len += bufs[i].len;
	}
	return len;
}

static void libuvErrMsg(char *errmsg, const char *call, int rv)
{
	ErrMsgPrintf(errmsg, "%s", uv_strerror(rv));
	ErrMsgWrapf(errmsg, "%s", call);
}

static void libuvCloseCb(uv_fs_t *close_req)
{
	struct UvWriter *writer = close_req->data;
	struct UvWriterLibuv *impl = writer->impl;
	if (close_req->result < 0) {
		libuvErrMsg(writer->errmsg, "uv_fs_close",
			    (int)close_req->result);
	}
	uv_fs_req_cleanup(close_req);

	if (impl->close_cb != NULL) {
		impl->close_cb(writer);
	}
	RaftHeapFree(impl);
	writer->impl = NULL;
}

static void libuvReqTransferErrMsg(struct UvWriterReq *req)
{
	ErrMsgPrintf(req->writer->errmsg, "%s", req->errmsg);
}

static void libuvMaybeFinishClose(struct UvWriter *writer)
{
	struct UvWriterLibuv *impl = writer->impl;
	if (!impl->closing || impl->close_started || !queue_empty(&impl->pending)) {
		return;
	}

	impl->close_started = true;
	impl->close_req.data = writer;
	int rv = uv_fs_close(impl->loop, &impl->close_req, impl->fd,
			      libuvCloseCb);
	if (rv != 0) {
		libuvErrMsg(writer->errmsg, "uv_fs_close", rv);
		if (impl->close_cb != NULL) {
			impl->close_cb(writer);
		}
		RaftHeapFree(impl);
		writer->impl = NULL;
	}
}

static void libuvReqSetStatus(struct UvWriterReq *req, ssize_t result)
{
	if (result < 0) {
		ErrMsgPrintf(req->errmsg, "write failed: %s",
			     uv_strerror((int)result));
		req->status = RAFT_IOERR;
	} else if ((size_t)result < req->len) {
		ErrMsgPrintf(req->errmsg,
			     "short write: %zd bytes instead of %zu", result,
			     req->len);
		req->status = RAFT_NOSPACE;
	} else {
		req->status = 0;
	}
}

static void libuvReqFinish(struct UvWriterReq *req)
{
	struct UvWriter *writer = req->writer;
	queue_remove(&req->queue);
	uv_fs_req_cleanup(&req->uv.fs_req);
	if (req->status != 0) {
		libuvReqTransferErrMsg(req);
	}
	req->cb(req, req->status);
	libuvMaybeFinishClose(writer);
}

static void libuvWriteCb(uv_fs_t *fs_req)
{
	struct UvWriterReq *req = fs_req->data;
	libuvReqSetStatus(req, fs_req->result);
	libuvReqFinish(req);
}

static int libuvInit(struct UvWriter *writer,
		     struct uv_loop_s *loop,
		     uv_file fd,
		     const struct UvWriterOptions *options,
		     char *errmsg)
{
	(void)options;
	struct UvWriterLibuv *impl = RaftHeapCalloc(1, sizeof *impl);
	if (impl == NULL) {
		ErrMsgOom(errmsg);
		return RAFT_NOMEM;
	}
	impl->loop = loop;
	impl->fd = fd;
	queue_init(&impl->pending);
	writer->impl = impl;
	return 0;
}

static void libuvClose(struct UvWriter *writer, UvWriterCloseCb cb)
{
	struct UvWriterLibuv *impl = writer->impl;
	dqlite_assert(impl != NULL);
	dqlite_assert(!impl->closing);
	dqlite_assert(!writer->closing);
	writer->closing = true;
	impl->closing = true;
	impl->close_cb = cb;
	libuvMaybeFinishClose(writer);
}

static int libuvSubmit(struct UvWriter *writer,
		       struct UvWriterReq *req,
		       const uv_buf_t bufs[],
		       unsigned n,
		       size_t offset,
		       UvWriterReqCb cb)
{
	struct UvWriterLibuv *impl = writer->impl;
	dqlite_assert(impl != NULL);
	dqlite_assert(!impl->closing);
	dqlite_assert(impl->fd >= 0);
	dqlite_assert(req != NULL);
	dqlite_assert(bufs != NULL);
	dqlite_assert(n > 0);

	req->writer = writer;
	req->len = libuvLenOfBufs(bufs, n);
	req->status = -1;
	req->cb = cb;
	req->impl = NULL;
	memset(&req->uv.fs_req, 0, sizeof req->uv.fs_req);
	memset(req->errmsg, 0, sizeof req->errmsg);
	req->uv.fs_req.data = req;

	queue_insert_tail(&impl->pending, &req->queue);
	int rv = uv_fs_write(impl->loop, &req->uv.fs_req, impl->fd, bufs, n,
			       (int64_t)offset, libuvWriteCb);
	if (rv != 0) {
		queue_remove(&req->queue);
		uv_fs_req_cleanup(&req->uv.fs_req);
		libuvErrMsg(writer->errmsg, "uv_fs_write", rv);
		return RAFT_IOERR;
	}

	return 0;
}

const struct UvWriterBackend UvWriterLibuvBackend = {
    libuvInit,
    libuvClose,
    libuvSubmit,
};
