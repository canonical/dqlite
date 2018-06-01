#define _GNU_SOURCE

#include <assert.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>

#include <uv.h>
#include <sqlite3.h>
#include <capnp_c.h>

#include "conn.h"
#include "dqlite.h"
#include "error.h"
#include "fsm.h"
#include "gateway.h"
#include "lifecycle.h"
#include "log.h"
#include "request.h"
#include "response.h"

static int dqlite__conn_buf_alloc(struct dqlite__conn *c, size_t size)
{
	assert(c != NULL);
	assert(c->buffer.data == NULL);

	c->buffer.data = (uint8_t*)sqlite3_malloc(size);
	if (c->buffer.data == NULL){
		dqlite__error_oom(&c->error, "failed to allocate read buffer");
		return DQLITE_NOMEM;
	}
	memset(c->buffer.data, 0, size);

	c->buffer.offset = 0;
	c->buffer.pending = size;

	return 0;
}

static void dqlite__conn_buf_free(struct dqlite__conn *c)
{
	assert(c != NULL);
	assert(c->buffer.data != NULL);

	sqlite3_free(c->buffer.data);

	c->buffer.data = 0;
	c->buffer.offset = 0;
	c->buffer.pending = 0;
}

#define DQLITE__CONN_NULL DQLITE__FSM_NULL

#define DQLITE__CONN_HANDSHAKE 0
#define DQLITE__CONN_PREAMBLE  1
#define DQLITE__CONN_HEADER    2
#define DQLITE__CONN_DATA      3

static struct dqlite__fsm_state dqlite__conn_states[] = {
	{DQLITE__CONN_HANDSHAKE, "handshake"},
	{DQLITE__CONN_PREAMBLE,  "preamble" },
	{DQLITE__CONN_HEADER,    "header"   },
	{DQLITE__CONN_DATA,      "data"     },
	{DQLITE__CONN_NULL,      NULL       },
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
	int err;
	struct dqlite__conn *c;
	size_t size = sizeof(uint32_t); /* The version is a 32-bit number */

	assert(arg != NULL);

	c = (struct dqlite__conn*)arg;

	dqlite__debugf(c, "handshake alloc", "socket=%d size=%ld", c->socket, size);

	err = dqlite__conn_buf_alloc(c, size);
	if (err != 0)
		return err;

	return 0;
}

static int dqlite__conn_handshake_read_hdlr(void *arg)
{
	int err;
	uint32_t version;
	struct dqlite__conn *c;

	assert(arg != NULL);

	c = (struct dqlite__conn*)arg;

	assert(c->buffer.data != NULL);

	memcpy(&version, c->buffer.data, sizeof(uint32_t));
	version = capn_flip32(version);

	dqlite__debugf(c, "handshake read", "socket=%d version=%x", c->socket, version);

	if (version != DQLITE_PROTOCOL_VERSION)	{
		err = DQLITE_PROTO;
		dqlite__error_printf(&c->error, "unknown protocol version: %d", version);
		return err;
	}

	return 0;
}

static struct dqlite__fsm_transition dqlite__conn_transitions_handshake[] = {
	{DQLITE__CONN_ALLOC, dqlite__conn_handshake_alloc_hdlr, DQLITE__CONN_HANDSHAKE},
	{DQLITE__CONN_READ,  dqlite__conn_handshake_read_hdlr,  DQLITE__CONN_PREAMBLE},
};

static int dqlite__conn_preamble_alloc_hdlr(void *arg)
{
	int err;
	struct dqlite__conn *c;
	size_t size;

	assert(arg != NULL);

	c = (struct dqlite__conn*)arg;

	size = dqlite__request_preamble_size(&c->request);

	dqlite__debugf(c, "preamble alloc", "socket=%d size=%ld", c->socket, size);

	err = dqlite__conn_buf_alloc(c, size);
	if (err != 0)
		return err;

	return 0;
}

static int dqlite__conn_preamble_read_hdlr(void *arg)
{
	int err;
	struct dqlite__conn *c;

	assert(arg != NULL);

	c = (struct dqlite__conn*)arg;

	assert(c->buffer.data != NULL);

	dqlite__debugf(c, "preamble read", "socket=%d", c->socket);

	err = dqlite__request_preamble(&c->request, c->buffer.data);
	if (err != 0) {
		dqlite__error_wrapf(&c->error, &c->request.error, "failed to parse request preamble");
		return err;
	}

	return 0;
}

static struct dqlite__fsm_transition dqlite__conn_transitions_preamble[] = {
	{DQLITE__CONN_ALLOC, dqlite__conn_preamble_alloc_hdlr, DQLITE__CONN_PREAMBLE},
	{DQLITE__CONN_READ,  dqlite__conn_preamble_read_hdlr,  DQLITE__CONN_HEADER},
};

static int dqlite__conn_header_alloc_hdlr(void *arg)
{
	int err;
	struct dqlite__conn *c;
	size_t size;

	assert(arg != NULL);

	c = (struct dqlite__conn*)arg;

	size = dqlite__request_header_size(&c->request);

	dqlite__debugf(c, "header alloc", "socket=%d size=%ld", c->socket, size);

	err = dqlite__conn_buf_alloc(c, size);
	if (err != 0)
		return err;

	return 0;
}

static int dqlite__conn_header_read_hdlr(void *arg)
{
	int err;
	struct dqlite__conn *c;

	assert(arg != NULL);

	c = (struct dqlite__conn*)arg;

	assert(c->buffer.data != NULL);

	dqlite__debugf(c, "header read", "socket=%d", c->socket);

	err = dqlite__request_header(&c->request, c->buffer.data);
	if (err != 0) {
		dqlite__error_wrapf(&c->error, &c->request.error, "failed to parse request header");
		return err;
	}

	return 0;
}

static struct dqlite__fsm_transition dqlite__conn_transitions_header[] = {
	{DQLITE__CONN_ALLOC, dqlite__conn_header_alloc_hdlr, DQLITE__CONN_HEADER},
	{DQLITE__CONN_READ,  dqlite__conn_header_read_hdlr, DQLITE__CONN_DATA  },
};

static int dqlite__conn_data_alloc_hdlr(void *arg)
{
	int err;
	struct dqlite__conn *c;
	size_t size;

	assert(arg != NULL);

	c = (struct dqlite__conn*)arg;

	assert(&c->request != NULL);

	size = dqlite__request_data_size(&c->request);

	dqlite__debugf(c, "data alloc", "socket=%d size=%ld", c->socket, size);

	err = dqlite__conn_buf_alloc(c, size);
	if (err != 0)
		return err;

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
	uv_buf_t buf;

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

	buf.base = (char*)dqlite__response_data(response);
	buf.len = dqlite__response_size(response);

	assert(buf.base != 0);
	assert(buf.len > 0);

	dqlite__debugf(c, "writing response", "socket=%d size=%ld", c->socket, buf.len);

	err = uv_write(req, (uv_stream_t*)(&c->tcp), &buf, 1, dqlite__conn_write_cb);
	if (err != 0) {
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

	// FIXME: there's a race condition, the write dqlite__conn_write_cb gets
	//        invoked after the connection has aborted.
	if( c->aborted)
		return;

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

static int dqlite__conn_data_read_hdlr(void *arg)
{
	int err;
	struct dqlite__conn *c;
	struct dqlite__response *response;

	assert(arg != NULL);

	c = (struct dqlite__conn*)arg;

	assert(c->buffer.data != NULL);

	dqlite__debugf(c, "data read", "socket=%d", c->socket);

	err = dqlite__request_data(&c->request, c->buffer.data);
	if (err != 0) {
		assert(err == DQLITE_PROTO);
		dqlite__error_wrapf(&c->error, &c->request.error, "failed to parse request data");
		return err;
	}

	err = dqlite__gateway_handle(&c->gateway, &c->request, &response);
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

static struct dqlite__fsm_transition dqlite__conn_transitions_data[] = {
	{DQLITE__CONN_ALLOC, dqlite__conn_data_alloc_hdlr, DQLITE__CONN_DATA},
	{DQLITE__CONN_READ,  dqlite__conn_data_read_hdlr,  DQLITE__CONN_PREAMBLE},
};

static struct dqlite__fsm_transition *dqlite__transitions[] = {
	dqlite__conn_transitions_handshake,
	dqlite__conn_transitions_preamble,
	dqlite__conn_transitions_header,
	dqlite__conn_transitions_data,
};

static void dqlite__conn_close_cb(uv_handle_t* tcp)
{
	struct dqlite__conn *c;

	assert(tcp != NULL);

	c = (struct dqlite__conn*)tcp->data;

	assert(c != NULL);

	dqlite__debugf(c, "connection aborted", "socket=%d", c->socket);
}

static void dqlite__conn_alloc_cb(uv_handle_t *tcp, size_t suggested_size, uv_buf_t *buf)
{
	int err;
	struct dqlite__conn *c;

	assert(tcp != NULL);
	assert(buf != NULL);

	c = (struct dqlite__conn*)tcp->data;

	if (c->buffer.data == NULL) {
		/* This is the first read for a handshake or a new request */
		err = dqlite__fsm_step(&c->fsm, DQLITE__CONN_ALLOC, (void*)c);

		if (err != 0) {
			assert(c->buffer.data == NULL);
			dqlite__conn_abort(c);
			return;
		}

		assert(c->buffer.data != NULL);
		assert(c->buffer.offset == 0);
		assert(c->buffer.pending > 0);
	}

	buf->base = (char*)c->buffer.data;
	buf->len = c->buffer.pending;
}

static void dqlite__conn_read_cb(uv_stream_t *tcp, ssize_t nread, const uv_buf_t *buf)
{
	int err;
	struct dqlite__conn *c;

	assert(tcp != NULL);
	assert(buf != NULL);

	c = (struct dqlite__conn*)tcp->data;

	if (nread > 0) {
		size_t n = (size_t)nread;

		/* We shouldn't have read more data than the pending amount. */
		assert(n <= c->buffer.pending);

		/* If there's more data to read, update the counters and return. */
		if (n < c->buffer.pending) {
			c->buffer.offset += n;
			c->buffer.pending -= n;
			goto out;
		}

		/* Read completed, advance the FSM. */
		err = dqlite__fsm_step(&c->fsm, DQLITE__CONN_READ, (void*)c);

		/* If an error occurred, abort the connection. */
		if (err != 0) {
			goto abort;
		}

		dqlite__conn_buf_free(c);

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

	dqlite__error_uv(&c->error, nread, "read error");

 abort:
	dqlite__conn_abort(c);
 out:
	return;
}

void dqlite__conn_init(struct dqlite__conn *c,
		FILE *log,
		int socket,
		dqlite_cluster *cluster)
{
	assert(c != NULL );

	dqlite__lifecycle_init(DQLITE__LIFECYCLE_CONN);

	dqlite__error_init(&c->error);

	c->log = log;
	c->socket = socket;
	c->cluster = cluster;

	dqlite__fsm_init(&c->fsm, dqlite__conn_states, dqlite__conn_events, dqlite__transitions);

	c->buffer.data = NULL;
	c->buffer.offset = 0;
	c->buffer.pending = 0;

	c->aborted = 0;

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

int dqlite__conn_read_start(struct dqlite__conn *c, uv_loop_t *loop)
{
	int err = 0;

	assert( c );
	assert( loop );

	dqlite__debugf(c, "start reading", "socket=%d", c->socket);

	err = uv_tcp_init(loop, &c->tcp);
	if (err != 0) {
		dqlite__error_uv(&c->error, err, "failed to init tcp stream");
		return DQLITE_ERROR;
	}

	err = uv_tcp_open(&c->tcp, c->socket);
	if (err != 0) {
		dqlite__error_uv(&c->error, err, "failed to open tcp stream");
		return DQLITE_ERROR;
	}
	c->tcp.data = (void*)c;

	err = uv_read_start((uv_stream_t*)(&c->tcp), dqlite__conn_alloc_cb, dqlite__conn_read_cb);
	if (err != 0) {
		dqlite__error_uv(&c->error, err, "failed to start reading tcp stream");
		return DQLITE_ERROR;
	}

	return err;
}

/* Abort the connection, realeasing any memory allocated by the read buffer, and
 * closing the UV handle (which closes the underlying socket as well) */
void dqlite__conn_abort(struct dqlite__conn *c)
{
	const char *conn_state;
	const char *gateway_state;

	assert(c != NULL);

	c->aborted = 1;

	if (uv_is_closing((uv_handle_t*)(&c->tcp))) {
		/* It might happen that a connection error occurs at the same time
		** the loop gets stopped, and dqlite__conn_abort is called twice in
		** the same loop iteration. We just ignore the second call in that
		** case.
		*/
		dqlite__debugf(c, "skip abort closing connection", "socket=%d", c->socket);
		return;
	}

	if( c->buffer.data ){
		dqlite__conn_buf_free(c);
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

	// TODO: understand if we actually need a close callback (e.g. do we
	// have resources that must be released after close?). Probably not,
	// since all the work can be done here.
	// uv_close((uv_handle_t*)(&c->tcp), dqlite__conn_close_cb);
	uv_close((uv_handle_t*)(&c->tcp), NULL);
}
