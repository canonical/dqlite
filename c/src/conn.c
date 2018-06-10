#define _GNU_SOURCE

#include <assert.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>

#include <uv.h>
#include <sqlite3.h>

#include "conn.h"
#include "dqlite.h"
#include "error.h"
#include "fsm.h"
#include "gateway.h"
#include "lifecycle.h"
#include "log.h"
#include "request.h"
#include "response.h"

static void dqlite__conn_buf_init(struct dqlite__conn *c, uint8_t *buf,	size_t size)
{
	assert(c != NULL);
	assert(c->buf == NULL);
	assert(c->offset == 0);
	assert(c->pending == 0);

	c->buf = buf;
	c->offset = 0;
	c->pending = size;
}

static void dqlite__conn_buf_close(struct dqlite__conn *c)
{
	assert(c != NULL);
	assert(c->buf != NULL);
	assert(c->pending == 0);

	c->buf = NULL;
	c->offset = 0;
}

#define DQLITE__CONN_NULL DQLITE__FSM_NULL

#define DQLITE__CONN_HANDSHAKE 0
#define DQLITE__CONN_HEADER   1
#define DQLITE__CONN_BODY      2

static struct dqlite__fsm_state dqlite__conn_states[] = {
	{DQLITE__CONN_HANDSHAKE, "handshake"},
	{DQLITE__CONN_HEADER,   "message" },
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

static int dqlite__conn_handshake_alloc_hdlr(void *arg)
{
	struct dqlite__conn *c;
	uint8_t *buf;
	size_t len;

	assert(arg != NULL);

	c = (struct dqlite__conn*)arg;

	buf = (uint8_t*)(&c->version);
	len = sizeof(c->version);

	dqlite__debugf(c, "handshake alloc", "socket=%d len=%ld", c->socket, len);

	dqlite__conn_buf_init(c, buf, len);

	return 0;
}

static int dqlite__conn_handshake_read_hdlr(void *arg)
{
	int err;
	struct dqlite__conn *c;

	assert(arg != NULL);

	c = (struct dqlite__conn*)arg;

	/* The buffer must point to our version field */
	assert(c->buf == (uint8_t*)(&c->version));

	c->version = dqlite__message_flip32(c->version);

	dqlite__debugf(c, "handshake read", "socket=%d version=%x", c->socket, c->version);

	if (c->version != DQLITE_PROTOCOL_VERSION) {
		err = DQLITE_PROTO;
		dqlite__error_printf(&c->error, "unknown protocol version: %d", c->version);
		return err;
	}

	return 0;
}

static struct dqlite__fsm_transition dqlite__conn_transitions_handshake[] = {
	{DQLITE__CONN_ALLOC, dqlite__conn_handshake_alloc_hdlr, DQLITE__CONN_HANDSHAKE},
	{DQLITE__CONN_READ,  dqlite__conn_handshake_read_hdlr,  DQLITE__CONN_HEADER},
};

static int dqlite__conn_header_alloc_hdlr(void *arg)
{
	struct dqlite__conn *c;
	uint8_t *buf;
	size_t len;

	assert(arg != NULL);

	c = (struct dqlite__conn*)arg;

	dqlite__request_header_buf(&c->request, &buf, &len);

	dqlite__debugf(c, "header alloc", "socket=%d len=%ld", c->socket, len);

	dqlite__conn_buf_init(c, buf, len);

	return 0;
}

static int dqlite__conn_header_read_hdlr(void *arg)
{
	int err;
	struct dqlite__conn *c;

	assert(arg != NULL);

	c = (struct dqlite__conn*)arg;

	dqlite__debugf(c, "header read", "socket=%d", c->socket);

	err = dqlite__request_header_received(&c->request);
	if (err != 0) {
		dqlite__error_wrapf(&c->error, &c->request.error, "failed to parse message header");
		return err;
	}

	return 0;
}

static struct dqlite__fsm_transition dqlite__conn_transitions_header[] = {
	{DQLITE__CONN_ALLOC, dqlite__conn_header_alloc_hdlr, DQLITE__CONN_HEADER},
	{DQLITE__CONN_READ,  dqlite__conn_header_read_hdlr,  DQLITE__CONN_BODY},
};

static int dqlite__conn_body_alloc_hdlr(void *arg)
{
	struct dqlite__conn *c;
	uint8_t *buf;
	size_t len;

	assert(arg != NULL);

	c = (struct dqlite__conn*)arg;

	dqlite__request_body_buf(&c->request, &buf, &len);

	dqlite__debugf(c, "header alloc", "socket=%d len=%ld", c->socket, len);

	dqlite__conn_buf_init(c, buf, len);

	return 0;
}

/* Context attached to an uv_write_t write request */
struct dqlite__conn_write_ctx {
	struct dqlite__conn     *conn;
	struct dqlite__response *response;
};

static void dqlite__conn_write_cb(uv_write_t* req, int status);

static int dqlite__conn_write(struct dqlite__conn *c, struct dqlite__response *response)
{
	int err;
	struct dqlite__conn_write_ctx *ctx;
	uv_write_t *req;
	uv_buf_t bufs[2];

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

	dqlite__response_header_buf(response, (uint8_t**)(&bufs[0].base), &bufs[0].len);
	dqlite__response_body_buf(response, (uint8_t**)(&bufs[1].base), &bufs[1].len);

	assert(bufs[0].base != NULL);
	assert(bufs[0].len > 0);

	assert(bufs[1].base != NULL);
	assert(bufs[1].len > 0);

	dqlite__debugf(c, "writing response", "socket=%d len=%ld", c->socket, bufs[0].len + bufs[1].len);

	err = uv_write(req, (uv_stream_t*)(&c->tcp), bufs, 2, dqlite__conn_write_cb);
	if (err != 0) {
		sqlite3_free(req);
		dqlite__error_uv(&c->error, err, "failed to write response");
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
		dqlite__gateway_finish(&c->gateway, response);
	}

	sqlite3_free(req);
}

static int dqlite__conn_body_read_hdlr(void *arg)
{
	int err;
	struct dqlite__conn *c;
	struct dqlite__response *response;

	assert(arg != NULL);

	c = (struct dqlite__conn*)arg;

	dqlite__debugf(c, "body read", "socket=%d", c->socket);

	err = dqlite__message_body_received(&c->request.message);
	if (err != 0) {
		assert(err == DQLITE_PROTO);
		dqlite__error_wrapf(&c->error, &c->request.error, "failed to parse message body");
		return err;
	}

	c->request.timestamp = uv_now(c->loop);

	err = dqlite__gateway_handle(&c->gateway, &c->request, &response);

	/* Release any resources allocated by the request object and reset it to
	 * its initial state, so a new request can be started (if there was no
	 * error). */
	dqlite__request_processed(&c->request);

	if (err != 0) {
		assert(response == NULL);
		dqlite__error_wrapf(&c->error, &c->gateway.error, "failed to handle request");
		return err;
	}

	/* Currently all requests require a response */
	assert(response != NULL);

	err = dqlite__conn_write(c, response);
	if (err != 0) {
		/* NOTE: no need to set c->error since that's done by
		 * dqlite__conn_write. */
		dqlite__gateway_abort(&c->gateway, response);
		return err;
	}

	return 0;
}

static struct dqlite__fsm_transition dqlite__conn_transitions_body[] = {
	{DQLITE__CONN_ALLOC, dqlite__conn_body_alloc_hdlr, DQLITE__CONN_BODY},
	{DQLITE__CONN_READ,  dqlite__conn_body_read_hdlr,  DQLITE__CONN_HEADER},
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

	c = (struct dqlite__conn*)tcp->data;

	/* If this is the first read of the handshake or of a new message
	 * header, or of a message body, give the relevant FSM a chance
	 * initialize the read buffer. */
	if (c->buf == NULL) {

		assert(c->offset == 0);
		assert(c->pending == 0);

		err = dqlite__fsm_step(&c->fsm, DQLITE__CONN_ALLOC, (void*)c);
		if (err != 0) {
			dqlite__conn_abort(c);
			return;
		}

		assert(c->offset == 0);
	}

	buf->base = (char*)(c->buf + c->offset);
	buf->len = c->pending;
}

static void dqlite__conn_alive_cb(uv_timer_t *alive)
{
	uint64_t elapsed;
	struct dqlite__conn *c;

	assert(alive != NULL);

	c = (struct dqlite__conn*)alive->data;

	assert(c != NULL);

	dqlite__debugf(c, "check heartbeat", "socket=%d", c->socket);

	elapsed = uv_now(c->loop) - c->gateway.heartbeat;

	/* If the last successful heartbeat happened more than heartbeat_timeout
	 * milliseconds ago, abort the connection. */
	if (elapsed > c->gateway.heartbeat_timeout) {
		dqlite__error_printf(&c->error, "no heartbeat since %ld milliseconds", elapsed);
		dqlite__conn_abort(c);
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
		assert(n <= c->pending);

		/* Advance the counters */
		c->pending -= n;
		c->offset += n;

		/* If there's more data to read in order to fill the current
		 * read buffer, just return, we'll be invoked again. */
		if (c->pending > 0) {
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
		dqlite__debugf(c, "empty read", "socket=%d", c->socket);
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

	c->log = log;
	c->socket = socket;
	c->cluster = cluster;
	c->loop = loop;

	dqlite__fsm_init(&c->fsm, dqlite__conn_states, dqlite__conn_events, dqlite__transitions);

	c->buf = NULL;
	c->offset = 0;
	c->pending = 0;

	dqlite__request_init(&c->request);

	dqlite__gateway_init(&c->gateway, log, c->cluster);
}

void dqlite__conn_close(struct dqlite__conn *c)
{
	assert(c != NULL);

	dqlite__gateway_close(&c->gateway);
	dqlite__request_close(&c->request);
	dqlite__fsm_close(&c->fsm);

	dqlite__error_close(&c->error);

	dqlite__lifecycle_close(DQLITE__LIFECYCLE_CONN);
}

int dqlite__conn_start(struct dqlite__conn *c)
{
	int err;
	uint64_t heartbeat_timeout;

	assert(c != NULL);

	dqlite__debugf(c, "start connection", "socket=%d", c->socket);

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
	const char *gateway_state;

	assert(c != NULL);

	if (uv_is_closing((uv_handle_t*)(&c->tcp))) {
		/* It might happen that a connection error occurs at the same time
		** the loop gets stopped, and dqlite__conn_abort is called twice in
		** the same loop iteration. We just ignore the second call in that
		** case.
		*/
		dqlite__debugf(c, "skip abort closing connection", "socket=%d", c->socket);
		return;
	}

	conn_state = dqlite__fsm_state(&c->fsm);
	gateway_state = dqlite__fsm_state(&c->gateway.fsm);

	/* If the error is due to a client disconnection, log a debug
	 * message. Otherwise, log an error message */
	if (dqlite__error_is_disconnect(&c->error))
		dqlite__debugf(c,
			"aborting connection", "socket=%d conn_state=%s gateway_state=%s msg=%s"
			, c->socket, conn_state, gateway_state, c->error);
	else
		dqlite__errorf(c,
			"aborting connection", "socket=%d conn_state=%s gateway_state=%s msg=%s",
			c->socket, conn_state, gateway_state, c->error);

	uv_close((uv_handle_t*)(&c->alive), NULL);

	/* TODO: add a close callback and invoke dqlite__conn_close(conn) */
	uv_close((uv_handle_t*)(&c->tcp), NULL);
}
