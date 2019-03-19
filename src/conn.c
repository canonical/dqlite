#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sqlite3.h>
#include <uv.h>

#include "../include/dqlite.h"

#include "./lib/assert.h"
#include "./lib/byte.h"

#include "conn.h"
#include "error.h"
#include "gateway.h"
#include "lifecycle.h"
#include "log.h"
#include "request.h"
#include "response.h"

/* Raft protocol tag */
#define RAFT_TAG 0x60c1f653be904bd1

#ifdef DQLITE_EXPERIMENTAL

/* Raft command codes */
enum { RAFT_CONNECT = 1, RAFT_JOIN, RAFT_LEAVE };

#endif /* DQLITE_EXPERIMENTAL */

/* Context attached to an uv_write_t write request */
struct conn__write_ctx
{
	struct conn *conn;
	struct response *response;
};

/* Forward declarations */
static void conn__alloc_cb(uv_handle_t *, size_t, uv_buf_t *);
static void conn__read_cb(uv_stream_t *, ssize_t, const uv_buf_t *);
static void conn__write_cb(uv_write_t *, int);

/* Write out a response for the client */
static int conn__write(struct conn *c, struct response *response)
{
	int err;
	struct conn__write_ctx *ctx;
	uv_write_t *req;
	uv_buf_t bufs[3];

	/* Create a write request UV handle */
	req = (uv_write_t *)sqlite3_malloc(sizeof(*req) + sizeof(*ctx));
	if (req == NULL) {
		err = DQLITE_NOMEM;
		dqlite__error_oom(&c->error,
				  "failed to start writing response");
		return err;
	}

	ctx = (struct conn__write_ctx *)(((char *)req) + sizeof(*req));

	ctx->conn = c;
	ctx->response = response;

	req->data = (void *)ctx;

	message__send_start(&response->message, bufs);

	assert(bufs[0].base != NULL);
	assert(bufs[0].len > 0);

	assert(bufs[1].base != NULL);
	assert(bufs[1].len > 0);

	err = uv_write(req, c->stream, bufs, 3, conn__write_cb);
	if (err != 0) {
		message__send_reset(&response->message);
		sqlite3_free(req);
		dqlite__error_uv(&c->error, err, "failed to write response");
		return err;
	}

	return 0;
}

/* Write out a failure response. */
static int conn__write_failure(struct conn *c, int code)
{
	int err;

	assert(c != NULL);
	assert(code != 0);

	dqlite__debugf(c, "failure (fd=%d code=%d msg=%s)", c->fd, code,
		       c->error);

	/* TODO: allocate the response object dynamically, to allow for
	 *       concurrent failures (e.g. the client issues a second failing
	 *       request before the response for the first failing request has
	 *       been completely written out. */
	c->response.type = DQLITE_RESPONSE_FAILURE;
	c->response.failure.code = code;
	c->response.failure.message = c->error;

	err = response_encode(&c->response);
	if (err != 0) {
		dqlite__error_wrapf(&c->error, &c->response.error,
				    "failed to encode failure response");
		return err;
	}

	err = conn__write(c, &c->response);
	if (err != 0) {
		/* NOTE: no need to set c->error since that's done by
		 * conn__write. */
		return err;
	}

	return 0;
}

static void conn__write_cb(uv_write_t *req, int status)
{
	struct conn__write_ctx *ctx;
	struct conn *c;
	struct response *response;

	assert(req != NULL);
	assert(req->data != NULL);

	ctx = (struct conn__write_ctx *)req->data;

	c = ctx->conn;
	response = ctx->response;

	assert(c != NULL);
	assert(response != NULL);

	message__send_reset(&response->message);

	/* From libuv docs about the uv_write_cb type: "status will be 0 in case
	 * of success, < 0 otherwise". */
	assert(status == 0 || status < 0);

	if (status) {
		dqlite__error_uv(&c->error, status, "response write error");
		gateway__aborted(&c->gateway, response);
		conn__abort(c);
	} else {
		/* In case this not our own failure response object, notify the
		 * gateway that we're done */
		if (response != &c->response) {
			gateway__flushed(&c->gateway, response);
		}

		/* If we had paused reading requests and we're not shutting
		 * down, let's resume. */
		if (c->paused && !c->aborting) {
			int err;
			err = uv_read_start(c->stream, conn__alloc_cb,
					    conn__read_cb);
			/* TODO: is it possible for uv_read_start to fail now?
			 */
			assert(err == 0);
		}
	}

	sqlite3_free(req);
}

/* Invoked by the gateway when a response for a request is ready to be flushed
 * and sent to the client. */
static void conn__flush_cb(void *arg, struct response *response)
{
	struct conn *c;
	int rc;

	assert(arg != NULL);
	assert(response != NULL);

	c = arg;

	rc = response_encode(response);
	if (rc != 0) {
		dqlite__error_wrapf(&c->error, &response->error,
				    "failed to encode response");
		goto response_failure;
	}

	rc = conn__write(c, response);
	if (rc != 0) {
		/* NOTE: no need to set c->error since that's done by
		 * conn__write. */
		goto response_failure;
	}

	if (c->metrics != NULL) {
		/* Update the metrics. */
		c->metrics->requests++;
		c->metrics->duration += uv_hrtime() - c->timestamp;
	}

	return;

response_failure:
	gateway__aborted(&c->gateway, response);
}

/* Initialize the connection read buffer, in preparation to the next read
 * phase. The read phases are defined by the connection finite state machine:
 *
 * 0) Handshake phase: 8 bytes are read, containing the protocol version.
 * 1) Request header: 8 bytes are read, containing the header of the next
 * request. 2) Reqest body: the body of the request is read.
 *
 * After 2) the state machine goes back to 1). */
static void conn__buf_init(struct conn *c, uv_buf_t *buf)
{
	assert(c != NULL);
	assert(buf->base != NULL);
	assert(buf->len > 0);

	assert(c->buf.base == NULL);
	assert(c->buf.len == 0);

	c->buf = *buf;
}

/* Reset the connection read buffer. This should be called at the end of a read
 * phase, to signal that the FSM should be advanced and next phase should
 * start (this is done by conn__alloc_cb). */
static void conn__buf_close(struct conn *c)
{
	assert(c != NULL);
	assert(c->buf.base != NULL);
	assert(c->buf.len == 0);

	c->buf.base = NULL;
}

#define CONN__HANDSHAKE 0
#define CONN__HEADER 1
#define CONN__BODY 2

static struct dqlite__fsm_state conn__states[] = {
    {CONN__HANDSHAKE, "handshake"},
    {CONN__HEADER, "header"},
    {CONN__BODY, "body"},
    {DQLITE__FSM_NULL, NULL},
};

#define CONN__ALLOC 0
#define CONN__READ 1

static struct dqlite__fsm_event conn__events[] = {
    {CONN__ALLOC, "alloc"},
    {CONN__READ, "read"},
    {DQLITE__FSM_NULL, NULL},
};

static int conn__handshake_alloc_cb(void *arg)
{
	struct conn *c;
	uv_buf_t buf;

	assert(arg != NULL);

	c = (struct conn *)arg;

	/* The handshake read buffer is simply the protocol field of the
	 * connection struct. */
	buf.base = (char *)(&c->protocol);
	buf.len = sizeof(c->protocol);

	conn__buf_init(c, &buf);

	return 0;
}

static int conn__handshake_read_cb(void *arg)
{
	int err;
	struct conn *c;

	assert(arg != NULL);

	c = (struct conn *)arg;

	/* The buffer must point to our protocol field */
	assert((c->buf.base - sizeof(c->protocol)) == (char *)(&c->protocol));

	c->protocol = byte__flip64(c->protocol);

	if (c->protocol != DQLITE_PROTOCOL_VERSION && c->protocol != RAFT_TAG) {
		err = DQLITE_PROTO;
		dqlite__error_printf(&c->error, "unknown protocol version: %lx",
				     c->protocol);
		return err;
	}

	return 0;
}

static struct dqlite__fsm_transition conn__transitions_handshake[] = {
    {CONN__ALLOC, conn__handshake_alloc_cb, CONN__HANDSHAKE},
    {CONN__READ, conn__handshake_read_cb, CONN__HEADER},
};

static int conn__header_alloc_cb(void *arg)
{
	struct conn *c;
	uv_buf_t buf;

	assert(arg != NULL);

	c = (struct conn *)arg;

#ifdef DQLITE_EXPERIMENTAL
	if (c->protocol == RAFT_TAG) {
		buf.base = (char *)c->raft.preamble;
		buf.len = sizeof c->raft.preamble;
		goto done;
	}
#endif /* DQLITE_EXPERIMENTAL */

	message__header_recv_start(&c->request.message, &buf);

#ifdef DQLITE_EXPERIMENTAL
done:
#endif /* DQLITE_EXPERIMENTAL */

	conn__buf_init(c, &buf);

	/* If metrics are enabled, keep track of the request start time. */
	if (c->metrics != NULL) {
		c->timestamp = uv_hrtime();
	}

	return 0;
}

static int conn__header_read_cb(void *arg)
{
	int err;
	struct conn *c;
	int ctx;

	assert(arg != NULL);

	c = (struct conn *)arg;

#ifdef DQLITE_EXPERIMENTAL
	if (c->protocol == RAFT_TAG) {
		c->raft.command = byte__flip64(c->raft.preamble[0]);
		c->raft.server_id = byte__flip64(c->raft.preamble[1]);
		c->raft.address.len = byte__flip64(c->raft.preamble[2]);
		return 0;
	}
#endif /* DQLITE_EXPERIMENTAL */

	err = message__header_recv_done(&c->request.message);
	if (err != 0) {
		/* At the moment DQLITE_PROTO is the only error that should be
		 * returned. */
		assert(err == DQLITE_PROTO);

		dqlite__error_wrapf(&c->error, &c->request.message.error,
				    "failed to parse request header");

		err = conn__write_failure(c, err);
		if (err != 0) {
			return err;
		}

		/* Instruct the fsm to skip receiving the message body */
		c->fsm.jump_state_id = CONN__HEADER;
	}

	/* If the gateway is currently busy handling a previous request,
	 * throttle the client. */
	ctx = gateway__ctx_for(&c->gateway, c->request.message.type);
	if (ctx == -1) {
		err = uv_read_stop(c->stream);
		if (err != 0) {
			dqlite__error_uv(&c->error, err,
					 "failed to pause reading");
			return err;
		}
		c->paused = 1;
	}

	return 0;
}

static struct dqlite__fsm_transition conn__transitions_header[] = {
    {CONN__ALLOC, conn__header_alloc_cb, CONN__HEADER},
    {CONN__READ, conn__header_read_cb, CONN__BODY},
};

static int conn__body_alloc_cb(void *arg)
{
	int err;
	struct conn *c;
	uv_buf_t buf;

	assert(arg != NULL);

	c = (struct conn *)arg;

#ifdef DQLITE_EXPERIMENTAL
	if (c->protocol == RAFT_TAG) {
		switch (c->raft.command) {
			case RAFT_CONNECT:
				c->raft.address.base =
				    sqlite3_malloc(c->raft.address.len);
				if (c->raft.address.base == NULL) {
					dqlite__error_oom(
					    &c->error,
					    "can't alloc server address");
					return DQLITE_NOMEM;
				}
				buf = c->raft.address;
				break;
			default:
				dqlite__error_printf(&c->error,
						     "bad raft command");
				return DQLITE_PROTO;
		}
		goto done;
	}
#endif /* DQLITE_EXPERIMENTAL */

	err = message__body_recv_start(&c->request.message, &buf);
	if (err != 0) {
		dqlite__error_wrapf(&c->error, &c->request.message.error,
				    "failed to start reading message body");
		return err;
	}

#ifdef DQLITE_EXPERIMENTAL
done:
#endif /* DQLITE_EXPERIMENTAL */
	conn__buf_init(c, &buf);

	return 0;
}

static int conn__body_read_cb(void *arg)
{
	int err;
	struct conn *c;

	assert(arg != NULL);

	c = (struct conn *)arg;

#ifdef DQLITE_EXPERIMENTAL
	if (c->protocol == RAFT_TAG) {
		switch (c->raft.command) {
			case RAFT_CONNECT:
				c->raft.cb(c->raft.transport, c->raft.server_id,
					   c->raft.address.base, c->stream);
				c->stream = NULL;
				sqlite3_free(c->raft.address.base);
				/* This makes the connection abort */
				dqlite__error_uv(&c->error, UV_EOF,
						 "handled to raft");
				return DQLITE_ERROR;
				break;
		}
	}
#endif /* DQLITE_EXPERIMENTAL */

	err = request_decode(&c->request);
	if (err != 0) {
		dqlite__error_wrapf(&c->error, &c->request.error,
				    "failed to decode request");
		goto request_failure;
	}

	c->request.timestamp = uv_now(c->loop);

	err = gateway__handle(&c->gateway, &c->request);
	if (err != 0) {
		dqlite__error_wrapf(&c->error, &c->gateway.error,
				    "failed to handle request");
		goto request_failure;
	}

	message__recv_reset(&c->request.message);

	return 0;

request_failure:
	assert(err != 0);

	err = conn__write_failure(c, err);
	if (err != 0) {
		return err;
	}

	return 0;
}

static struct dqlite__fsm_transition conn__transitions_body[] = {
    {CONN__ALLOC, conn__body_alloc_cb, CONN__BODY},
    {CONN__READ, conn__body_read_cb, CONN__HEADER},
};

static struct dqlite__fsm_transition *dqlite__transitions[] = {
    conn__transitions_handshake,
    conn__transitions_header,
    conn__transitions_body,
};

/* Called to allocate a buffer for the next stream read. */
static void conn__alloc_cb(uv_handle_t *stream, size_t _, uv_buf_t *buf)
{
	int err;
	struct conn *c;

	assert(stream != NULL);
	assert(buf != NULL);

	(void)_;

	c = (struct conn *)stream->data;

	assert(c != NULL);

	/* If this is the first read of the handshake or of a new message
	 * header, or of a message body, give to the relevant FSM callback a
	 * chance to initialize our read buffer. */
	if (c->buf.base == NULL) {
		assert(c->buf.len == 0);

		err = dqlite__fsm_step(&c->fsm, CONN__ALLOC, (void *)c);
		if (err != 0) {
			dqlite__errorf(c, "alloc error (fd=%d err=%d)", c->fd,
				       err);
			conn__abort(c);
			return;
		}

		assert(c->buf.base != NULL);
		assert(c->buf.len > 0);
	}

	*buf = c->buf;
}

static void conn__alive_cb(uv_timer_t *alive)
{
	uint64_t elapsed;
	struct conn *c;

	assert(alive != NULL);

	c = (struct conn *)alive->data;

	assert(c != NULL);

	elapsed = uv_now(c->loop) - c->gateway.heartbeat;

	/* If the last successful heartbeat happened more than heartbeat_timeout
	 * milliseconds ago, abort the connection. */
	if (elapsed > c->options->heartbeat_timeout) {
		// dqlite__error_printf(&c->error, "no heartbeat since %ld
		// milliseconds", elapsed); conn__abort(c);
	}
}

static void conn__read_cb(uv_stream_t *stream,
			  ssize_t nread,
			  const uv_buf_t *buf)
{
	int err;
	struct conn *c;

	assert(stream != NULL);
	assert(buf != NULL);

	c = (struct conn *)stream->data;

	assert(c != NULL);

	if (nread > 0) {
		size_t n = (size_t)nread;

		/* We shouldn't have read more data than the pending amount. */
		assert(n <= c->buf.len);

		/* Advance the read window */
		c->buf.base += n;
		c->buf.len -= n;

		/* If there's more data to read in order to fill the current
		 * read buffer, just return, we'll be invoked again. */
		if (c->buf.len > 0) {
			goto out;
		}

		/* Read completed, advance the FSM and reset the read buffer. */
		err = dqlite__fsm_step(&c->fsm, CONN__READ, (void *)c);
		conn__buf_close(c);

		/* If an error occurred, abort the connection. */
		if (err != 0) {
			goto abort;
		}

		goto out;
	}

	/* The if nread>0 condition above should always exit the function with a
	 * goto. */
	assert(nread <= 0);

	if (nread == 0) {
		/* Empty read */
		goto out;
	}

	/* The "if nread==0" condition above should always exit the function
	 * with a goto and never reach this point. */
	assert(nread < 0);

	/* Set the error and abort */
	dqlite__error_uv(&c->error, nread, "read error");

abort:
	conn__abort(c);
out:
	return;
}

void conn__init(struct conn *c,
		int fd,
		dqlite_logger *logger,
		dqlite_cluster *cluster,
		uv_loop_t *loop,
		struct options *options,
		struct dqlite__metrics *metrics)
{
	struct gateway__cbs callbacks;

	assert(c != NULL);
	assert(cluster != NULL);
	assert(loop != NULL);
	assert(options != NULL);

	callbacks.ctx = c;
	callbacks.xFlush = conn__flush_cb;

	c->logger = logger;
	c->protocol = 0;

	c->options = options;
	c->metrics = metrics;

	dqlite__error_init(&c->error);

	dqlite__fsm_init(&c->fsm, conn__states, conn__events,
			 dqlite__transitions);
	request_init(&c->request);

	gateway__init(&c->gateway, &callbacks, cluster, logger, options);
	response_init(&c->response);

	c->fd = fd;
	c->loop = loop;
	c->stream = NULL;

	c->buf.base = NULL;
	c->buf.len = 0;

	c->aborting = 0;
	c->paused = 0;
}

void conn__close(struct conn *c)
{
	assert(c != NULL);
	if (c->stream != NULL) {
		sqlite3_free(c->stream);
	}

	response_close(&c->response);
	gateway__close(&c->gateway);
	dqlite__fsm_close(&c->fsm);
	request_close(&c->request);
	dqlite__error_close(&c->error);
}

int conn__start(struct conn *c)
{
	struct uv_pipe_s *pipe;
	struct uv_tcp_s *tcp;
	int err;
	uint64_t heartbeat_timeout;

	assert(c != NULL);

	c->gateway.heartbeat = uv_now(c->loop);

	/* Start the alive timer, which will disconnect the client if no
	 * heartbeat is received within the timeout. */
	heartbeat_timeout = c->options->heartbeat_timeout;
	assert(heartbeat_timeout > 0);

	err = uv_timer_init(c->loop, &c->alive);
	if (err != 0) {
		dqlite__error_uv(&c->error, err, "failed to init alive timer");
		err = DQLITE_ERROR;
		goto err;
	}
	c->alive.data = (void *)c;

	err = uv_timer_start(&c->alive, conn__alive_cb, heartbeat_timeout,
			     heartbeat_timeout);
	if (err != 0) {
		dqlite__error_uv(&c->error, err, "failed to init alive timer");
		err = DQLITE_ERROR;
		goto err;
	}

	/* Start reading from the stream. */
	switch (uv_guess_handle(c->fd)) {
		case UV_TCP:
			tcp = sqlite3_malloc(sizeof *tcp);
			assert(tcp != NULL); /* TODO: handle OOM */
			err = uv_tcp_init(c->loop, tcp);
			if (err != 0) {
				dqlite__error_uv(&c->error, err,
						 "failed to init tcp stream");
				err = DQLITE_ERROR;
				sqlite3_free(tcp);
				goto err_after_timer_start;
			}

			err = uv_tcp_open(tcp, c->fd);
			if (err != 0) {
				dqlite__error_uv(&c->error, err,
						 "failed to open tcp stream");
				err = DQLITE_ERROR;
				sqlite3_free(tcp);
				goto err_after_timer_start;
			}

			c->stream = (struct uv_stream_s *)tcp;

			break;

		case UV_NAMED_PIPE:
			pipe = sqlite3_malloc(sizeof *pipe);
			assert(pipe != NULL); /* TODO: handle OOM */
			err = uv_pipe_init(c->loop, pipe, 0);
			if (err != 0) {
				dqlite__error_uv(&c->error, err,
						 "failed to init pipe stream");
				err = DQLITE_ERROR;
				sqlite3_free(pipe);
				goto err_after_timer_start;
			}

			err = uv_pipe_open(pipe, c->fd);
			if (err != 0) {
				dqlite__error_uv(&c->error, err,
						 "failed to open pipe stream");
				err = DQLITE_ERROR;
				sqlite3_free(pipe);
				goto err_after_timer_start;
			}

			c->stream = (struct uv_stream_s *)pipe;

			break;

		default:
			dqlite__error_printf(&c->error,
					     "unsupported stream type");
			err = DQLITE_ERROR;
			goto err_after_timer_start;
	}

	assert(c->stream != NULL);
	c->stream->data = (void *)c;

	err = uv_read_start(c->stream, conn__alloc_cb, conn__read_cb);
	if (err != 0) {
		dqlite__error_uv(&c->error, err,
				 "failed to start reading tcp stream");
		err = DQLITE_ERROR;
		goto err_after_stream_open;
	}

	return 0;

err_after_stream_open:
	uv_close((uv_handle_t *)c->stream, NULL);

err_after_timer_start:
	uv_close((uv_handle_t *)&c->alive, NULL);

err:
	assert(err != 0);
	return err;
}

static void conn__destroy(struct conn *c)
{
	conn__close(c);

	/* FIXME: this is broken, we should close the alive handle and *then*
	 * free the connection object in the close callback */
	sqlite3_free(c);
	uv_close((uv_handle_t *)(&c->alive), NULL);
}

static void conn__stream_close_cb(uv_handle_t *handle)
{
	struct conn *c;

	assert(handle != NULL);

	c = handle->data;
	conn__destroy(c);
}

/* Abort the connection, realeasing any memory allocated by the read buffer, and
 * closing the UV handle (which closes the underlying socket as well) */
void conn__abort(struct conn *c)
{
	const char *state;

	assert(c != NULL);

	if (c->aborting) {
		/* It might happen that a connection error occurs at the same
		 * time the loop gets stopped, and conn__abort is called twice
		 * in the same loop iteration. We just ignore the second call in
		 * that case.
		 */
		return;
	}

	c->aborting = 1;

	state = dqlite__fsm_state(&c->fsm);

#ifdef DQLITE_DEBUG
	/* In debug mode always log disconnections. */
	dqlite__debugf(c, "aborting (fd=%d state=%s msg=%s)", c->fd, state,
		       c->error);
#else
	/* If the error is not due to a client disconnection, log an error
	 * message */
	if (!dqlite__error_is_disconnect(&c->error)) {
		dqlite__errorf(c, "aborting (fd=%d state=%s msg=%s)", c->fd,
			       state, c->error);
	}
#endif

#ifdef DQLITE_EXPERIMENTAL
	if (c->stream == NULL) {
		conn__destroy(c);
		return;
	}
#endif /* DQLITE_EXPERIMENTAL */
	uv_close((uv_handle_t *)c->stream, conn__stream_close_cb);
}
