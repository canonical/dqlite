#define _GNU_SOURCE

#include <assert.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>

#include <uv.h>
#include <sqlite3.h>

#include "binary.h"
#include "conn.h"
#include "dqlite.h"
#include "error.h"
#include "fsm.h"
#include "gateway.h"
#include "lifecycle.h"
#include "log.h"
#include "request.h"
#include "response.h"

/* Context attached to an uv_write_t write request */
struct dqlite__conn_write_ctx {
	struct dqlite__conn     *conn;
	struct dqlite__response *response;
};

static void dqlite__conn_write_cb(uv_write_t* req, int status);

/* Write out a response for the client */
static int dqlite__conn_write(struct dqlite__conn *c, struct dqlite__response *response)
{
	int err;
	struct dqlite__conn_write_ctx *ctx;
	uv_write_t *req;
	uv_buf_t bufs[3];

	/* Create a write request UV handle */
	req = (uv_write_t*)sqlite3_malloc(sizeof(*req) + sizeof(*ctx));
	if (req == NULL) {
		err = DQLITE_NOMEM;
		dqlite__error_oom(&c->error, "failed to start writing response");
		return err;
	}

	ctx = (struct dqlite__conn_write_ctx*)(((char*)req) + sizeof(*req));
	ctx->conn = c;
	ctx->response = response;

	req->data = (void*)ctx;

	dqlite__message_send_start(&response->message, bufs);

	assert(bufs[0].base != NULL);
	assert(bufs[0].len > 0);

	assert(bufs[1].base != NULL);
	assert(bufs[1].len > 0);

	err = uv_write(req, (uv_stream_t*)(&c->tcp), bufs, 3, dqlite__conn_write_cb);
	if (err != 0) {
		dqlite__message_send_reset(&response->message);
		sqlite3_free(req);
		dqlite__error_uv(&c->error, err, "failed to write response");
		return err;
	}

	return 0;
}

/* Write out a failure response.
 *
 * This is used to inform the client about failures such as malformed requests
 * that failed to be parsed or handled because they are either malformed or
 * invalid (e.g. they contain a reference to an unknown prepared statement).
 */
static int dqlite__conn_write_failure(struct dqlite__conn *c, int code) {
	int err;

	assert(c != NULL);
	assert(code != 0);

	dqlite__debugf(c, "failure", "code=%d description=%s", code, c->error);

	/* TODO: allocate the response object dynamically, to allow for
	 *       concurrent failures (e.g. the client issues a second failing
	 *       request before the response for the first failing request has
	 *       been completely written out. */
	c->response.type = DQLITE_RESPONSE_FAILURE;
	c->response.failure.code = code;
	c->response.failure.description = c->error;

	err = dqlite__response_encode(&c->response);
	if (err != 0) {
		dqlite__error_wrapf(
			&c->error, &c->response.error,
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

static void dqlite__conn_write_cb(uv_write_t* req, int status)
{
	int err;
	struct dqlite__conn_write_ctx *ctx;
	struct dqlite__conn *c;
	struct dqlite__response *response;

	assert(req != NULL);
	assert(req->data != NULL);

	ctx = (struct dqlite__conn_write_ctx*)req->data;

	c = ctx->conn;
	response = ctx->response;

	assert(c != NULL);
	assert(response != NULL);

	dqlite__message_send_reset(&response->message);

	if (status) {
		dqlite__infof(c, "response write error", "socket=%d msg=%s", c->socket, uv_strerror(status));
		dqlite__gateway_abort(&c->gateway, response);
		dqlite__conn_abort(c);
	} else if (0) {
		//}else if( !dqlite__response_is_finished(response) ){
		dqlite__debugf(c, "continuing response", "socket=%d", c->socket);
		err = dqlite__gateway_continue(&c->gateway, response);
		if (err != 0) {
			assert(response == NULL);
			//dqlite__infof(c, "failed to handle request", "socket=%d msg=%s", c->socket, dqliteClientErrmsg(c->gateway));
			dqlite__conn_abort(c);
		} else {
			err = dqlite__conn_write(c, response);
			if (err != 0) {
				//dqlite__infof(c, "response error", "socket=%d msg=%s", c->socket, dqlite__errorMessage(&c->error));
				dqlite__gateway_abort(&c->gateway, response);
				dqlite__conn_abort(c);
			}
		}
	} else {
		/* In case this not a failure response, notify the gateway that
		 * we're done */
		if (response != &c->response) {
			dqlite__gateway_finish(&c->gateway, response);
		}
	}

	sqlite3_free(req);
}

static void dqlite__conn_buf_init(struct dqlite__conn *c, uv_buf_t *buf)
{
	assert(c != NULL);
	assert(buf->base != NULL);
	assert(buf->len > 0 );

	assert(c->buf.base == NULL);
	assert(c->buf.len == 0);

	c->buf = *buf;
}

static void dqlite__conn_buf_close(struct dqlite__conn *c)
{
	assert(c != NULL);
	assert(c->buf.base != NULL);
	assert(c->buf.len == 0);

	c->buf.base = NULL;
}

#define DQLITE__CONN_NULL DQLITE__FSM_NULL

#define DQLITE__CONN_HANDSHAKE 0
#define DQLITE__CONN_HEADER   1
#define DQLITE__CONN_BODY      2

static struct dqlite__fsm_state dqlite__conn_states[] = {
	{DQLITE__CONN_HANDSHAKE, "handshake"},
	{DQLITE__CONN_HEADER,    "message" },
	{DQLITE__CONN_BODY,      "data" },
	{DQLITE__CONN_NULL,      NULL      },
};

#define DQLITE__CONN_ALLOC 0
#define DQLITE__CONN_READ  1

static struct dqlite__fsm_event dqlite__conn_events[] = {
	{DQLITE__CONN_ALLOC, "alloc"},
	{DQLITE__CONN_READ,  "read" },
	{DQLITE__CONN_NULL,  NULL   },
};

static int dqlite__conn_handshake_alloc_cb(void *arg)
{
	struct dqlite__conn *c;
	uv_buf_t buf;

	assert(arg != NULL);

	c = (struct dqlite__conn*)arg;

	buf.base = (char*)(&c->protocol);
	buf.len = sizeof(c->protocol);

	dqlite__conn_buf_init(c, &buf);

	return 0;
}

static int dqlite__conn_handshake_read_cb(void *arg)
{
	int err;
	struct dqlite__conn *c;

	assert(arg != NULL);

	c = (struct dqlite__conn*)arg;

	/* The buffer must point to our version field */
	assert((c->buf.base - sizeof(c->protocol)) == (char*)(&c->protocol));

	c->protocol = dqlite__flip64(c->protocol);

	if (c->protocol != DQLITE_PROTOCOL_VERSION) {
		err = DQLITE_PROTO;
		dqlite__error_printf(&c->error, "unknown protocol version: %lx", c->protocol);
		return err;
	}

	return 0;
}

static struct dqlite__fsm_transition dqlite__conn_transitions_handshake[] = {
	{DQLITE__CONN_ALLOC, dqlite__conn_handshake_alloc_cb, DQLITE__CONN_HANDSHAKE},
	{DQLITE__CONN_READ,  dqlite__conn_handshake_read_cb,  DQLITE__CONN_HEADER},
};

static int dqlite__conn_header_alloc_cb(void *arg)
{
	struct dqlite__conn *c;
	uv_buf_t buf;

	assert(arg != NULL);

	c = (struct dqlite__conn*)arg;

	dqlite__message_header_recv_start(&c->request.message, &buf);

	dqlite__conn_buf_init(c, &buf);

	return 0;
}

static int dqlite__conn_header_read_cb(void *arg)
{
	int err;
	struct dqlite__conn *c;

	assert(arg != NULL);

	c = (struct dqlite__conn*)arg;

	err = dqlite__message_header_recv_done(&c->request.message);
	if (err != 0) {
		/* At the moment DQLITE_PROTO is the only error that should be
		 * returned. */
		assert(err == DQLITE_PROTO);

		dqlite__error_wrapf(
			&c->error,
			&c->request.message.error,
			"failed to parse request header");

		err = dqlite__conn_write_failure(c, err);
		if (err != 0) {
			return err;
		}

		/* Instruct the fsm to skip receiving the message body */
		c->fsm.jump_state_id = DQLITE__CONN_HEADER;
	}

	return 0;
}

static struct dqlite__fsm_transition dqlite__conn_transitions_header[] = {
	{DQLITE__CONN_ALLOC, dqlite__conn_header_alloc_cb, DQLITE__CONN_HEADER},
	{DQLITE__CONN_READ,  dqlite__conn_header_read_cb,  DQLITE__CONN_BODY},
};

static int dqlite__conn_body_alloc_cb(void *arg)
{
	int err;
	struct dqlite__conn *c;
	uv_buf_t buf;

	assert(arg != NULL);

	c = (struct dqlite__conn*)arg;

	err = dqlite__message_body_recv_start(&c->request.message, &buf);
	if (err != 0) {
		dqlite__error_wrapf(
			&c->error,
			&c->request.message.error, "failed to start reading message body");
		return err;
	}

	dqlite__conn_buf_init(c, &buf);

	return 0;
}

static int dqlite__conn_body_read_cb(void *arg)
{
	int err;
	struct dqlite__conn *c;
	struct dqlite__response *response;

	assert(arg != NULL);

	c = (struct dqlite__conn*)arg;

	err = dqlite__request_decode(&c->request);
	if (err != 0) {
		dqlite__error_wrapf(&c->error, &c->request.error, "failed to decode request");
		goto request_failure;
	}

	c->request.timestamp = uv_now(c->loop);

	err = dqlite__gateway_handle(&c->gateway, &c->request, &response);
	if (err != 0) {
		dqlite__error_wrapf(&c->error, &c->gateway.error, "failed to handle request");
		goto request_failure;
	}

	/* Currently all requests require a response */
	assert(response != NULL);

	dqlite__message_recv_reset(&c->request.message);

	err = dqlite__response_encode(response);
	if (err != 0) {
		dqlite__error_wrapf(&c->error, &response->error, "failed to encode response");
		goto response_failure;
	}

	err = dqlite__conn_write(c, response);
	if (err != 0) {
		/* NOTE: no need to set c->error since that's done by
		 * dqlite__conn_write. */
		goto response_failure;
	}

	return 0;

 response_failure:
	dqlite__gateway_abort(&c->gateway, response);

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
	{DQLITE__CONN_READ,  dqlite__conn_body_read_cb,  DQLITE__CONN_HEADER},
};

static struct dqlite__fsm_transition *dqlite__transitions[] = {
	dqlite__conn_transitions_handshake,
	dqlite__conn_transitions_header,
	dqlite__conn_transitions_body,
};

static void dqlite__conn_alloc_cb(uv_handle_t *tcp, size_t suggested_size, uv_buf_t *buf)
{
	int err;
	struct dqlite__conn *c;

	assert(tcp != NULL);
	assert(buf != NULL);

	(void)suggested_size; /* Unused */

	c = (struct dqlite__conn*)tcp->data;

	/* If this is the first read of the handshake or of a new message
	 * header, or of a message body, give the relevant FSM a chance
	 * initialize the read buffer. */
	if (c->buf.base == NULL) {

		assert(c->buf.len == 0);

		err = dqlite__fsm_step(&c->fsm, DQLITE__CONN_ALLOC, (void*)c);
		if (err != 0) {
			dqlite__debugf(c, "alloc failure", "socket=%d", c->socket);
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
	uint64_t elapsed;
	struct dqlite__conn *c;

	assert(alive != NULL);

	c = (struct dqlite__conn*)alive->data;

	assert(c != NULL);

	elapsed = uv_now(c->loop) - c->gateway.heartbeat;

	/* If the last successful heartbeat happened more than heartbeat_timeout
	 * milliseconds ago, abort the connection. */
	if (elapsed > c->gateway.heartbeat_timeout) {
		//dqlite__error_printf(&c->error, "no heartbeat since %ld milliseconds", elapsed);
		//dqlite__conn_abort(c);
	}
}

static void dqlite__conn_read_cb(uv_stream_t *tcp, ssize_t nread, const uv_buf_t *buf)
{
	int err;
	struct dqlite__conn *c;

	assert(tcp != NULL);
	assert(buf != NULL);

	c = (struct dqlite__conn*)tcp->data;

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

		/* Read completed, advance the FSM and reset the read buffer.. */
		err = dqlite__fsm_step(&c->fsm, DQLITE__CONN_READ, (void*)c);
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
		goto out;
	}

	/* The if nread==0 condition above should always exit the function with
	 * a goto and never reach this point. */
	assert(nread < 0);

	/* Set the error and abort */
	dqlite__error_uv(&c->error, nread, "read error");

 abort:
	dqlite__conn_abort(c);
 out:
	return;
}

void dqlite__conn_init(
	struct dqlite__conn *c,
	FILE *log,
	int socket,
	dqlite_cluster *cluster,
	uv_loop_t *loop)
{
	assert(c != NULL );
	assert(log != NULL );
	assert(cluster != NULL );
	assert(loop != NULL );

	dqlite__lifecycle_init(DQLITE__LIFECYCLE_CONN);

	dqlite__error_init(&c->error);

	c->protocol = 0;

	dqlite__fsm_init(&c->fsm, dqlite__conn_states, dqlite__conn_events, dqlite__transitions);
	dqlite__request_init(&c->request);
	dqlite__gateway_init(&c->gateway, log, cluster);
	dqlite__response_init(&c->response);

	c->log = log;
	c->socket = socket;
	c->loop = loop;

	c->buf.base = NULL;
	c->buf.len = 0;
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
	int err;
	uint64_t heartbeat_timeout;

	assert(c != NULL);

	/* Consider the initial connection as a heartbeat */
	c->gateway.heartbeat = uv_now(c->loop);

	/* Start the alive timer, which will disconnect the client if no
	 * heartbeat is received within the timeout. */

	heartbeat_timeout = c->gateway.heartbeat_timeout;

	assert(heartbeat_timeout > 0);

	err = uv_timer_init(c->loop, &c->alive);
	if (err != 0) {
		dqlite__error_uv(&c->error, err, "failed to init alive timer");
		err = DQLITE_ERROR;
		goto err_timer_init;
	}
	c->alive.data = (void*)c;

	err = uv_timer_start(&c->alive, dqlite__conn_alive_cb, heartbeat_timeout, heartbeat_timeout);
	if (err != 0) {
		dqlite__error_uv(&c->error, err, "failed to init alive timer");
		err = DQLITE_ERROR;
		goto err_timer_init;
	}

	/* Start reading from the TCP socket. */

	err = uv_tcp_init(c->loop, &c->tcp);
	if (err != 0) {
		dqlite__error_uv(&c->error, err, "failed to init tcp stream");
		err = DQLITE_ERROR;
		goto err_tcp_init;
	}

	err = uv_tcp_open(&c->tcp, c->socket);
	if (err != 0) {
		dqlite__error_uv(&c->error, err, "failed to open tcp stream");
		err = DQLITE_ERROR;
		goto err_tcp_open;
	}
	c->tcp.data = (void*)c;

	err = uv_read_start((uv_stream_t*)(&c->tcp), dqlite__conn_alloc_cb, dqlite__conn_read_cb);
	if (err != 0) {
		dqlite__error_uv(&c->error, err, "failed to start reading tcp stream");
		err = DQLITE_ERROR;
		goto err_tcp_start;
	}

	return 0;

 err_tcp_start:
 err_tcp_open:
	uv_close((uv_handle_t*)(&c->tcp), NULL);

 err_tcp_init:
	uv_close((uv_handle_t*)(&c->alive), NULL);

 err_timer_init:
	assert(err != 0);

	return err;
}

/* Abort the connection, realeasing any memory allocated by the read buffer, and
 * closing the UV handle (which closes the underlying socket as well) */
void dqlite__conn_abort(struct dqlite__conn *c)
{
	const char *conn_state;

	assert(c != NULL);

	if (uv_is_closing((uv_handle_t*)(&c->tcp))) {
		/* It might happen that a connection error occurs at the same time
		** the loop gets stopped, and dqlite__conn_abort is called twice in
		** the same loop iteration. We just ignore the second call in that
		** case.
		*/
		return;
	}

	conn_state = dqlite__fsm_state(&c->fsm);

	/* If the error is due to a client disconnection, log a debug
	 * message. Otherwise, log an error message */
	if (dqlite__error_is_disconnect(&c->error))
		dqlite__debugf(c,
			"aborting connection", "socket=%d conn_state=%s msg=%s"
			, c->socket, conn_state, c->error);
	else
		dqlite__errorf(c,
			"aborting connection", "socket=%d conn_state=%s msg=%s",
			c->socket, conn_state, c->error);

	uv_close((uv_handle_t*)(&c->alive), NULL);

	/* TODO: add a close callback and invoke dqlite__conn_close(conn) */
	uv_close((uv_handle_t*)(&c->tcp), NULL);
}
