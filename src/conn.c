#define _GNU_SOURCE

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sqlite3.h>
#include <uv.h>

#include "../include/dqlite.h"

#include "binary.h"
#include "conn.h"
#include "error.h"
#include "fsm.h"
#include "gateway.h"
#include "lifecycle.h"
#include "log.h"
#include "request.h"
#include "response.h"

/* Context attached to an uv_write_t write request */
struct dqlite__conn_write_ctx {
	struct dqlite__conn *    conn;
	struct dqlite__response *response;
};

/* Forward declarations */
static void dqlite__conn_alloc_cb(uv_handle_t *, size_t, uv_buf_t *);
static void dqlite__conn_read_cb(uv_stream_t *, ssize_t, const uv_buf_t *);
static void dqlite__conn_write_cb(uv_write_t *, int);

/* Write out a response for the client */
static int dqlite__conn_write(struct dqlite__conn *    c,
                              struct dqlite__response *response)
{
	int                            err;
	struct dqlite__conn_write_ctx *ctx;
	uv_write_t *                   req;
	uv_buf_t                       bufs[3];

	/* Create a write request UV handle */
	req = (uv_write_t *)sqlite3_malloc(sizeof(*req) + sizeof(*ctx));
	if (req == NULL) {
		err = DQLITE_NOMEM;
		dqlite__error_oom(&c->error,
		                  "failed to start writing response");
		return err;
	}

	ctx = (struct dqlite__conn_write_ctx *)(((char *)req) + sizeof(*req));

	ctx->conn     = c;
	ctx->response = response;

	req->data = (void *)ctx;

	dqlite__message_send_start(&response->message, bufs);

	assert(bufs[0].base != NULL);
	assert(bufs[0].len > 0);

	assert(bufs[1].base != NULL);
	assert(bufs[1].len > 0);

	err = uv_write(req, &c->stream, bufs, 3, dqlite__conn_write_cb);
	if (err != 0) {
		dqlite__message_send_reset(&response->message);
		sqlite3_free(req);
		dqlite__error_uv(&c->error, err, "failed to write response");
		return err;
	}

	return 0;
}

/* Write out a failure response. */
static int dqlite__conn_write_failure(struct dqlite__conn *c, int code)
{
	int err;

	assert(c != NULL);
	assert(code != 0);

	dqlite__debugf(
	    c, "failure (fd=%d code=%d msg=%s)", c->fd, code, c->error);

	/* TODO: allocate the response object dynamically, to allow for
	 *       concurrent failures (e.g. the client issues a second failing
	 *       request before the response for the first failing request has
	 *       been completely written out. */
	c->response.type            = DQLITE_RESPONSE_FAILURE;
	c->response.failure.code    = code;
	c->response.failure.message = c->error;

	err = dqlite__response_encode(&c->response);
	if (err != 0) {
		dqlite__error_wrapf(&c->error,
		                    &c->response.error,
		                    "failed to encode failure response");
		return err;
	}

	err = dqlite__conn_write(c, &c->response);
	if (err != 0) {
		/* NOTE: no need to set c->error since that's done by
		 * dqlite__conn_write. */
		return err;
	}

	return 0;
}

static void dqlite__conn_write_cb(uv_write_t *req, int status)
{
	struct dqlite__conn_write_ctx *ctx;
	struct dqlite__conn *          c;
	struct dqlite__response *      response;

	assert(req != NULL);
	assert(req->data != NULL);

	ctx = (struct dqlite__conn_write_ctx *)req->data;

	c        = ctx->conn;
	response = ctx->response;

	assert(c != NULL);
	assert(response != NULL);

	dqlite__message_send_reset(&response->message);

	/* From libuv docs about the uv_write_cb type: "status will be 0 in case
	 * of success, < 0 otherwise". */
	assert(status == 0 || status < 0);

	if (status) {
		dqlite__error_uv(&c->error, status, "response write error");
		dqlite__gateway_aborted(&c->gateway, response);
		dqlite__conn_abort(c);
	} else {
		/* In case this not our own failure response object, notify the
		 * gateway that we're done */
		if (response != &c->response) {
			dqlite__gateway_flushed(&c->gateway, response);
		}

		/* If we had paused reading requests and we're not shutting
		 * down, let's resume. */
		if (c->paused && !c->aborting) {
			int err;
			err = uv_read_start(&c->stream,
			                    dqlite__conn_alloc_cb,
			                    dqlite__conn_read_cb);
			/* TODO: is it possible for uv_read_start to fail now?
			 */
			assert(err == 0);
		}
	}

	sqlite3_free(req);
}

/* Invoked by the gateway when a response for a request is ready to be flushed
 * and sent to the client. */
static void dqlite__conn_flush_cb(void *arg, struct dqlite__response *response)
{
	struct dqlite__conn *c;
	int                  rc;

	assert(arg != NULL);
	assert(response != NULL);

	c = arg;

	rc = dqlite__response_encode(response);
	if (rc != 0) {
		dqlite__error_wrapf(
		    &c->error, &response->error, "failed to encode response");
		goto response_failure;
	}

	rc = dqlite__conn_write(c, response);
	if (rc != 0) {
		/* NOTE: no need to set c->error since that's done by
		 * dqlite__conn_write. */
		goto response_failure;
	}

	if (c->metrics != NULL) {
		/* Update the metrics. */
		c->metrics->requests++;
		c->metrics->duration += uv_hrtime() - c->timestamp;
	}

	return;

response_failure:
	dqlite__gateway_aborted(&c->gateway, response);
}

/* Initialize the connection read buffer, in preparation to the next read
 * phase. The read phases are defined by the connection finite state machine:
 *
 * 0) Handshake phase: 8 bytes are read, containing the protocol version.
 * 1) Request header: 8 bytes are read, containing the header of the next
 * request. 2) Reqest body: the body of the request is read.
 *
 * After 2) the state machine goes back to 1). */
static void dqlite__conn_buf_init(struct dqlite__conn *c, uv_buf_t *buf)
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
 * start (this is done by dqlite__conn_alloc_cb). */
static void dqlite__conn_buf_close(struct dqlite__conn *c)
{
	assert(c != NULL);
	assert(c->buf.base != NULL);
	assert(c->buf.len == 0);

	c->buf.base = NULL;
}

#define DQLITE__CONN_HANDSHAKE 0
#define DQLITE__CONN_HEADER 1
#define DQLITE__CONN_BODY 2

static struct dqlite__fsm_state dqlite__conn_states[] = {
    {DQLITE__CONN_HANDSHAKE, "handshake"},
    {DQLITE__CONN_HEADER, "header"},
    {DQLITE__CONN_BODY, "body"},
    {DQLITE__FSM_NULL, NULL},
};

#define DQLITE__CONN_ALLOC 0
#define DQLITE__CONN_READ 1

static struct dqlite__fsm_event dqlite__conn_events[] = {
    {DQLITE__CONN_ALLOC, "alloc"},
    {DQLITE__CONN_READ, "read"},
    {DQLITE__FSM_NULL, NULL},
};

static int dqlite__conn_handshake_alloc_cb(void *arg)
{
	struct dqlite__conn *c;
	uv_buf_t             buf;

	assert(arg != NULL);

	c = (struct dqlite__conn *)arg;

	/* The handshake read buffer is simply the protocol field of the
	 * connection struct. */
	buf.base = (char *)(&c->protocol);
	buf.len  = sizeof(c->protocol);

	dqlite__conn_buf_init(c, &buf);

	return 0;
}

static int dqlite__conn_handshake_read_cb(void *arg)
{
	int                  err;
	struct dqlite__conn *c;

	assert(arg != NULL);

	c = (struct dqlite__conn *)arg;

	/* The buffer must point to our protocol field */
	assert((c->buf.base - sizeof(c->protocol)) == (char *)(&c->protocol));

	c->protocol = dqlite__flip64(c->protocol);

	if (c->protocol != DQLITE_PROTOCOL_VERSION) {
		err = DQLITE_PROTO;
		dqlite__error_printf(
		    &c->error, "unknown protocol version: %lx", c->protocol);
		return err;
	}

	return 0;
}

static struct dqlite__fsm_transition dqlite__conn_transitions_handshake[] = {
    {DQLITE__CONN_ALLOC,
     dqlite__conn_handshake_alloc_cb,
     DQLITE__CONN_HANDSHAKE},
    {DQLITE__CONN_READ, dqlite__conn_handshake_read_cb, DQLITE__CONN_HEADER},
};

static int dqlite__conn_header_alloc_cb(void *arg)
{
	struct dqlite__conn *c;
	uv_buf_t             buf;

	assert(arg != NULL);

	c = (struct dqlite__conn *)arg;

	dqlite__message_header_recv_start(&c->request.message, &buf);

	dqlite__conn_buf_init(c, &buf);

	/* If metrics are enabled, keep track of the request start time. */
	if (c->metrics != NULL) {
		c->timestamp = uv_hrtime();
	}

	return 0;
}

static int dqlite__conn_header_read_cb(void *arg)
{
	int                  err;
	struct dqlite__conn *c;
	int                  ctx;

	assert(arg != NULL);

	c = (struct dqlite__conn *)arg;

	err = dqlite__message_header_recv_done(&c->request.message);
	if (err != 0) {
		/* At the moment DQLITE_PROTO is the only error that should be
		 * returned. */
		assert(err == DQLITE_PROTO);

		dqlite__error_wrapf(&c->error,
		                    &c->request.message.error,
		                    "failed to parse request header");

		err = dqlite__conn_write_failure(c, err);
		if (err != 0) {
			return err;
		}

		/* Instruct the fsm to skip receiving the message body */
		c->fsm.jump_state_id = DQLITE__CONN_HEADER;
	}

	/* If the gateway is currently busy handling a previous request,
	 * throttle the client. */
	ctx = dqlite__gateway_ctx_for(&c->gateway, c->request.message.type);
	if (ctx == -1) {
		err = uv_read_stop(&c->stream);
		if (err != 0) {
			dqlite__error_uv(
			    &c->error, err, "failed to pause reading");
			return err;
		}
		c->paused = 1;
	}

	return 0;
}

static struct dqlite__fsm_transition dqlite__conn_transitions_header[] = {
    {DQLITE__CONN_ALLOC, dqlite__conn_header_alloc_cb, DQLITE__CONN_HEADER},
    {DQLITE__CONN_READ, dqlite__conn_header_read_cb, DQLITE__CONN_BODY},
};

static int dqlite__conn_body_alloc_cb(void *arg)
{
	int                  err;
	struct dqlite__conn *c;
	uv_buf_t             buf;

	assert(arg != NULL);

	c = (struct dqlite__conn *)arg;

	err = dqlite__message_body_recv_start(&c->request.message, &buf);
	if (err != 0) {
		dqlite__error_wrapf(&c->error,
		                    &c->request.message.error,
		                    "failed to start reading message body");
		return err;
	}

	dqlite__conn_buf_init(c, &buf);

	return 0;
}

static int dqlite__conn_body_read_cb(void *arg)
{
	int                  err;
	struct dqlite__conn *c;

	assert(arg != NULL);

	c = (struct dqlite__conn *)arg;

	err = dqlite__request_decode(&c->request);
	if (err != 0) {
		dqlite__error_wrapf(
		    &c->error, &c->request.error, "failed to decode request");
		goto request_failure;
	}

	c->request.timestamp = uv_now(c->loop);

	err = dqlite__gateway_handle(&c->gateway, &c->request);
	if (err != 0) {
		dqlite__error_wrapf(
		    &c->error, &c->gateway.error, "failed to handle request");
		goto request_failure;
	}

	dqlite__message_recv_reset(&c->request.message);

	return 0;

request_failure:
	assert(err != 0);

	err = dqlite__conn_write_failure(c, err);
	if (err != 0) {
		return err;
	}

	return 0;
}

static struct dqlite__fsm_transition dqlite__conn_transitions_body[] = {
    {DQLITE__CONN_ALLOC, dqlite__conn_body_alloc_cb, DQLITE__CONN_BODY},
    {DQLITE__CONN_READ, dqlite__conn_body_read_cb, DQLITE__CONN_HEADER},
};

static struct dqlite__fsm_transition *dqlite__transitions[] = {
    dqlite__conn_transitions_handshake,
    dqlite__conn_transitions_header,
    dqlite__conn_transitions_body,
};

/* Called to allocate a buffer for the next stream read. */
static void dqlite__conn_alloc_cb(uv_handle_t *stream, size_t _, uv_buf_t *buf)
{
	int                  err;
	struct dqlite__conn *c;

	assert(stream != NULL);
	assert(buf != NULL);

	(void)_;

	c = (struct dqlite__conn *)stream->data;

	assert(c != NULL);

	/* If this is the first read of the handshake or of a new message
	 * header, or of a message body, give to the relevant FSM callback a
	 * chance to initialize our read buffer. */
	if (c->buf.base == NULL) {
		assert(c->buf.len == 0);

		err = dqlite__fsm_step(&c->fsm, DQLITE__CONN_ALLOC, (void *)c);
		if (err != 0) {
			dqlite__errorf(
			    c, "alloc error (fd=%d err=%d)", c->fd, err);
			dqlite__conn_abort(c);
			return;
		}

		assert(c->buf.base != NULL);
		assert(c->buf.len > 0);
	}

	*buf = c->buf;
}

static void dqlite__conn_alive_cb(uv_timer_t *alive)
{
	uint64_t             elapsed;
	struct dqlite__conn *c;

	assert(alive != NULL);

	c = (struct dqlite__conn *)alive->data;

	assert(c != NULL);

	elapsed = uv_now(c->loop) - c->gateway.heartbeat;

	/* If the last successful heartbeat happened more than heartbeat_timeout
	 * milliseconds ago, abort the connection. */
	if (elapsed > c->options->heartbeat_timeout) {
		// dqlite__error_printf(&c->error, "no heartbeat since %ld
		// milliseconds", elapsed); dqlite__conn_abort(c);
	}
}

static void dqlite__conn_read_cb(uv_stream_t *   stream,
                                 ssize_t         nread,
                                 const uv_buf_t *buf)
{
	int                  err;
	struct dqlite__conn *c;

	assert(stream != NULL);
	assert(buf != NULL);

	c = (struct dqlite__conn *)stream->data;

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
		err = dqlite__fsm_step(&c->fsm, DQLITE__CONN_READ, (void *)c);
		dqlite__conn_buf_close(c);

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
	dqlite__conn_abort(c);
out:
	return;
}

void dqlite__conn_init(struct dqlite__conn *   c,
                       int                     fd,
                       dqlite_logger *         logger,
                       dqlite_cluster *        cluster,
                       uv_loop_t *             loop,
                       struct dqlite__options *options,
                       struct dqlite__metrics *metrics)
{
	struct dqlite__gateway_cbs callbacks;

	assert(c != NULL);
	assert(cluster != NULL);
	assert(loop != NULL);
	assert(options != NULL);

	callbacks.ctx    = c;
	callbacks.xFlush = dqlite__conn_flush_cb;

	/* The tcp and pipe handle structures are pointing to the same memory
	 * location as the abstract stream handle. */
	assert((uintptr_t)&c->tcp == (uintptr_t)&c->stream);
	assert((uintptr_t)&c->pipe == (uintptr_t)&c->stream);

	dqlite__lifecycle_init(DQLITE__LIFECYCLE_CONN);

	c->logger   = logger;
	c->protocol = 0;

	c->options = options;
	c->metrics = metrics;

	dqlite__error_init(&c->error);

	dqlite__fsm_init(&c->fsm,
	                 dqlite__conn_states,
	                 dqlite__conn_events,
	                 dqlite__transitions);
	dqlite__request_init(&c->request);

	dqlite__gateway_init(&c->gateway, &callbacks, cluster, logger, options);
	dqlite__response_init(&c->response);

	c->fd   = fd;
	c->loop = loop;

	c->buf.base = NULL;
	c->buf.len  = 0;

	c->aborting = 0;
	c->paused   = 0;
}

void dqlite__conn_close(struct dqlite__conn *c)
{
	assert(c != NULL);

	dqlite__response_close(&c->response);
	dqlite__gateway_close(&c->gateway);
	dqlite__fsm_close(&c->fsm);
	dqlite__request_close(&c->request);
	dqlite__error_close(&c->error);

	dqlite__lifecycle_close(DQLITE__LIFECYCLE_CONN);
}

int dqlite__conn_start(struct dqlite__conn *c)
{
	int      err;
	uint64_t heartbeat_timeout;

	assert(c != NULL);

#ifdef DQLITE_EXPERIMENTAL
	/* Start the gateway */
	err = dqlite__gateway_start(&c->gateway, uv_now(c->loop));
	if (err != 0) {
		dqlite__error_uv(
		    &c->error, err, "failed to start gateway coroutine");
		goto err;
	}
#else
	c->gateway.heartbeat = uv_now(c->loop);
#endif /* DQLITE_EXPERIMENTAL */

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

	err = uv_timer_start(&c->alive,
	                     dqlite__conn_alive_cb,
	                     heartbeat_timeout,
	                     heartbeat_timeout);
	if (err != 0) {
		dqlite__error_uv(&c->error, err, "failed to init alive timer");
		err = DQLITE_ERROR;
		goto err;
	}

	/* Start reading from the stream. */
	switch (uv_guess_handle(c->fd)) {
	case UV_TCP:
		err = uv_tcp_init(c->loop, &c->tcp);
		if (err != 0) {
			dqlite__error_uv(
			    &c->error, err, "failed to init tcp stream");
			err = DQLITE_ERROR;
			goto err_after_timer_start;
		}

		err = uv_tcp_open(&c->tcp, c->fd);
		if (err != 0) {
			dqlite__error_uv(
			    &c->error, err, "failed to open tcp stream");
			err = DQLITE_ERROR;
			goto err_after_timer_start;
		}

		break;

	case UV_NAMED_PIPE:
		err = uv_pipe_init(c->loop, &c->pipe, 0);
		if (err != 0) {
			dqlite__error_uv(
			    &c->error, err, "failed to init pipe stream");
			err = DQLITE_ERROR;
			goto err_after_timer_start;
		}

		err = uv_pipe_open(&c->pipe, c->fd);
		if (err != 0) {
			dqlite__error_uv(
			    &c->error, err, "failed to open pipe stream");
			err = DQLITE_ERROR;
			goto err_after_timer_start;
		}

		break;

	default:
		dqlite__error_printf(&c->error, "unsupported stream type");
		err = DQLITE_ERROR;
		goto err_after_timer_start;
	}

	c->stream.data = (void *)c;

	err = uv_read_start(
	    &c->stream, dqlite__conn_alloc_cb, dqlite__conn_read_cb);
	if (err != 0) {
		dqlite__error_uv(
		    &c->error, err, "failed to start reading tcp stream");
		err = DQLITE_ERROR;
		goto err_after_stream_open;
	}

	return 0;

err_after_stream_open:
	uv_close((uv_handle_t *)(&c->stream), NULL);

err_after_timer_start:
	uv_close((uv_handle_t *)(&c->alive), NULL);

err:
	assert(err != 0);
	return err;
}

static void dqlite__conn_stream_close_cb(uv_handle_t *handle)
{
	struct dqlite__conn *c;

	assert(handle != NULL);

	c = handle->data;

	dqlite__conn_close(c);
	sqlite3_free(c);
}

static void dqlite__conn_timer_close_cb(uv_handle_t *handle)
{
	struct dqlite__conn *c;

	assert(handle != NULL);

	c = handle->data;

	uv_close((uv_handle_t *)(&c->stream), dqlite__conn_stream_close_cb);
}

/* Abort the connection, realeasing any memory allocated by the read buffer, and
 * closing the UV handle (which closes the underlying socket as well) */
void dqlite__conn_abort(struct dqlite__conn *c)
{
	const char *state;

	assert(c != NULL);

	if (c->aborting) {
		/* It might happen that a connection error occurs at the same
		 *time
		 ** the loop gets stopped, and dqlite__conn_abort is called
		 *twice in
		 ** the same loop iteration. We just ignore the second call in
		 *that
		 ** case.
		 */
		return;
	}

	c->aborting = 1;

	state = dqlite__fsm_state(&c->fsm);

#ifdef DQLITE_DEBUG
	/* In debug mode always log disconnections. */
	dqlite__debugf(
	    c, "aborting (fd=%d state=%s msg=%s)", c->fd, state, c->error);
#else
	/* If the error is not due to a client disconnection, log an error
	 * message */
	if (!dqlite__error_is_disconnect(&c->error)) {
		dqlite__errorf(c,
		               "aborting (fd=%d state=%s msg=%s)",
		               c->fd,
		               state,
		               c->error);
	}
#endif

	uv_close((uv_handle_t *)(&c->alive), dqlite__conn_timer_close_cb);
}
