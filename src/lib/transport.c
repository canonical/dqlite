#include <raft.h>

#include "../../include/dqlite.h"

#include "assert.h"
#include "transport.h"

/* Called to allocate a buffer for the next stream read. */
static void alloc_cb(uv_handle_t *stream, size_t suggested_size, uv_buf_t *buf)
{
	struct transport *t;
	(void)suggested_size;
	t = stream->data;
	assert(t->read.base != NULL);
	assert(t->read.len > 0);
	*buf = t->read;
}

/* Invoke the read callback. */
static void read_done(struct transport *t, ssize_t status)
{
	transport_read_cb cb;
	int rv;
	rv = uv_read_stop(t->stream);
	assert(rv == 0);
	cb = t->read_cb;
	assert(cb != NULL);
	t->read_cb = NULL;
	t->read.base = NULL;
	t->read.len = 0;
	cb(t, (int)status);
}

static void read_cb(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf)
{
	struct transport *t;
	(void)buf;

	t = stream->data;

	if (nread > 0) {
		size_t n = (size_t)nread;

		/* We shouldn't have read more data than the pending amount. */
		assert(n <= t->read.len);

		/* Advance the read window */
		t->read.base += n;
		t->read.len -= n;

		/* If there's more data to read in order to fill the current
		 * read buffer, just return, we'll be invoked again. */
		if (t->read.len > 0) {
			return;
		}

		/* Read completed, invoke the callback. */
		read_done(t, 0);
		return;
	}

	assert(nread <= 0);

	if (nread == 0) {
		/* Empty read */
		return;
	}

	assert(nread < 0);

	/* Failure. */
	read_done(t, nread);
}

int transport__stream(struct uv_loop_s *loop,
		      int fd,
		      struct uv_stream_s **stream)
{
	struct uv_pipe_s *pipe;
	struct uv_tcp_s *tcp;
	int rv;

	switch (uv_guess_handle(fd)) {
		case UV_TCP:
			tcp = raft_malloc(sizeof *tcp);
			if (tcp == NULL) {
				return DQLITE_NOMEM;
			}
			rv = uv_tcp_init(loop, tcp);
			assert(rv == 0);
			rv = uv_tcp_open(tcp, fd);
			if (rv != 0) {
				raft_free(tcp);
				return TRANSPORT__BADSOCKET;
			}
			*stream = (struct uv_stream_s *)tcp;
			break;
		case UV_NAMED_PIPE:
			pipe = raft_malloc(sizeof *pipe);
			if (pipe == NULL) {
				return DQLITE_NOMEM;
			}
			rv = uv_pipe_init(loop, pipe, 0);
			assert(rv == 0);
			rv = uv_pipe_open(pipe, fd);
			if (rv != 0) {
				raft_free(pipe);
				return TRANSPORT__BADSOCKET;
			}
			*stream = (struct uv_stream_s *)pipe;
			break;
		default:
			return TRANSPORT__BADSOCKET;
	};

	return 0;
}

int transport__init(struct transport *t, struct uv_stream_s *stream)
{
	t->stream = stream;
	t->stream->data = t;
	t->read.base = NULL;
	t->read.len = 0;
	t->write.data = t;
	t->read_cb = NULL;
	t->write_cb = NULL;
	t->close_cb = NULL;

	return 0;
}

static void close_cb(uv_handle_t *handle)
{
	struct transport *t = handle->data;
	raft_free(t->stream);
	if (t->close_cb != NULL) {
		t->close_cb(t);
	}
}

void transport__close(struct transport *t, transport_close_cb cb)
{
	assert(t->close_cb == NULL);
	t->close_cb = cb;
	uv_close((uv_handle_t *)t->stream, close_cb);
}

int transport__read(struct transport *t, uv_buf_t *buf, transport_read_cb cb)
{
	int rv;

	assert(t->read.base == NULL);
	assert(t->read.len == 0);
	t->read = *buf;
	t->read_cb = cb;
	rv = uv_read_start(t->stream, alloc_cb, read_cb);
	if (rv != 0) {
		return DQLITE_ERROR;
	}
	return 0;
}

static void write_cb(uv_write_t *req, int status)
{
	struct transport *t = req->data;
	transport_write_cb cb = t->write_cb;

	assert(cb != NULL);
	t->write_cb = NULL;

	cb(t, status);
}

int transport__write(struct transport *t, uv_buf_t *buf, transport_write_cb cb)
{
	int rv;
	assert(t->write_cb == NULL);
	t->write_cb = cb;
	rv = uv_write(&t->write, t->stream, buf, 1, write_cb);
	if (rv != 0) {
		return rv;
	}
	return 0;
}
