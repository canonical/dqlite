#include "conn.h"
#include "message.h"
#include "protocol.h"
#include "request.h"
#include "tracing.h"
#include "transport.h"

/* Initialize the given buffer for reading, ensure it has the given size. */
static int init_read(struct conn *c, uv_buf_t *buf, size_t size)
{
	buffer__reset(&c->read);
	buf->base = buffer__advance(&c->read, size);
	if (buf->base == NULL) {
		return DQLITE_NOMEM;
	}
	buf->len = size;
	return 0;
}

static int read_message(struct conn *c);
static void conn_write_cb(struct transport *transport, int status)
{
	struct conn *c = transport->data;
	bool finished;
	int rv;
	if (status != 0) {
		tracef("write cb status %d", status);
		goto abort;
	}

	buffer__reset(&c->write);
	buffer__advance(&c->write, message__sizeof(&c->response)); /* Header */

	rv = gateway__resume(&c->gateway, &finished);
	if (rv != 0) {
		goto abort;
	}
	if (!finished) {
		return;
	}

	/* Start reading the next request */
	rv = read_message(c);
	if (rv != 0) {
		goto abort;
	}
	return;
abort:
	conn__stop(c);
}

static void gateway_handle_cb(struct handle *req,
			      int status,
			      uint8_t type,
			      uint8_t schema)
{
	struct conn *c = req->data;
	size_t n;
	void *cursor;
	uv_buf_t buf;
	int rv;

	assert(schema <= req->schema);

	/* Ignore results firing after we started closing. TODO: instead, we
	 * should make gateway__close() asynchronous. */
	if (c->closed) {
		tracef("gateway handle cb closed");
		return;
	}

	if (status != 0) {
		tracef("gateway handle cb status %d", status);
		goto abort;
	}

	n = buffer__offset(&c->write) - message__sizeof(&c->response);
	assert(n % 8 == 0);

	c->response.type = type;
	c->response.words = (uint32_t)(n / 8);
	c->response.schema = schema;
	c->response.extra = 0;

	cursor = buffer__cursor(&c->write, 0);
	message__encode(&c->response, &cursor);

	buf.base = buffer__cursor(&c->write, 0);
	buf.len = buffer__offset(&c->write);

	rv = transport__write(&c->transport, &buf, conn_write_cb);
	if (rv != 0) {
		tracef("transport write failed %d", rv);
		goto abort;
	}
	return;
abort:
	conn__stop(c);
}

static void closeCb(struct transport *transport)
{
	struct conn *c = transport->data;
	buffer__close(&c->write);
	buffer__close(&c->read);
	if (c->close_cb != NULL) {
		c->close_cb(c);
	}
}

static void raft_connect(struct conn *c)
{
	struct cursor *cursor = &c->handle.cursor;
	struct request_connect request;
	int rv;
	tracef("raft_connect");
	rv = request_connect__decode(cursor, &request);
	if (rv != 0) {
		tracef("request connect decode failed %d", rv);
		conn__stop(c);
		return;
	}
	raftProxyAccept(c->uv_transport, request.id, request.address,
			c->transport.stream);
	/* Close the connection without actually closing the transport, since
	 * the stream will be used by raft */
	c->closed = true;
	closeCb(&c->transport);
}

static void read_request_cb(struct transport *transport, int status)
{
	struct conn *c = transport->data;
	struct cursor *cursor = &c->handle.cursor;
	int rv;

	if (status != 0) {
		tracef("read error %d", status);
		// errorf(c->logger, "read error");
		conn__stop(c);
		return;
	}

	cursor->p = buffer__cursor(&c->read, 0);
	cursor->cap = buffer__offset(&c->read);

	buffer__reset(&c->write);
	buffer__advance(&c->write, message__sizeof(&c->response)); /* Header */

	switch (c->request.type) {
		case DQLITE_REQUEST_CONNECT:
			raft_connect(c);
			return;
	}

	rv = gateway__handle(&c->gateway, &c->handle, c->request.type,
			     c->request.schema, &c->write, gateway_handle_cb);
	if (rv != 0) {
		tracef("read gateway handle error %d", rv);
		conn__stop(c);
	}
}

/* Start reading the body of the next request */
static int read_request(struct conn *c)
{
	uv_buf_t buf;
	int rv;
	if (UINT64_C(8) * (uint64_t)c->request.words > (uint64_t)UINT32_MAX) {
		return DQLITE_ERROR;
	}
	rv = init_read(c, &buf, c->request.words * 8);
	if (rv != 0) {
		tracef("init read failed %d", rv);
		return rv;
	}
	if (c->request.words == 0) {
		return 0;
	}
	rv = transport__read(&c->transport, &buf, read_request_cb);
	if (rv != 0) {
		tracef("transport read failed %d", rv);
		return rv;
	}
	return 0;
}

static void read_message_cb(struct transport *transport, int status)
{
	struct conn *c = transport->data;
	struct cursor cursor;
	int rv;

	if (status != 0) {
		// errorf(c->logger, "read error");
		tracef("read error %d", status);
		conn__stop(c);
		return;
	}

	cursor.p = buffer__cursor(&c->read, 0);
	cursor.cap = buffer__offset(&c->read);

	rv = message__decode(&cursor, &c->request);
	assert(rv == 0); /* Can't fail, we know we have enough bytes */

	rv = read_request(c);
	if (rv != 0) {
		tracef("read request error %d", rv);
		conn__stop(c);
		return;
	}
}

/* Start reading metadata about the next message */
static int read_message(struct conn *c)
{
	uv_buf_t buf;
	int rv;
	rv = init_read(c, &buf, message__sizeof(&c->request));
	if (rv != 0) {
		tracef("init read failed %d", rv);
		return rv;
	}
	rv = transport__read(&c->transport, &buf, read_message_cb);
	if (rv != 0) {
		tracef("transport read failed %d", rv);
		return rv;
	}
	return 0;
}

static void read_protocol_cb(struct transport *transport, int status)
{
	struct conn *c = transport->data;
	struct cursor cursor;
	int rv;

	if (status != 0) {
		// errorf(c->logger, "read error");
		tracef("read error %d", status);
		goto abort;
	}

	cursor.p = buffer__cursor(&c->read, 0);
	cursor.cap = buffer__offset(&c->read);

	rv = uint64__decode(&cursor, &c->protocol);
	assert(rv == 0); /* Can't fail, we know we have enough bytes */

	if (c->protocol != DQLITE_PROTOCOL_VERSION &&
	    c->protocol != DQLITE_PROTOCOL_VERSION_LEGACY) {
		/* errorf(c->logger, "unknown protocol version: %lx", */
		/* c->protocol); */
		/* TODO: instead of closing the connection we should return
		 * error messages */
		tracef("unknown protocol version %" PRIu64, c->protocol);
		goto abort;
	}
	c->gateway.protocol = c->protocol;

	rv = read_message(c);
	if (rv != 0) {
		goto abort;
	}

	return;
abort:
	conn__stop(c);
}

/* Start reading the protocol format version */
static int read_protocol(struct conn *c)
{
	uv_buf_t buf;
	int rv;
	rv = init_read(c, &buf, sizeof c->protocol);
	if (rv != 0) {
		tracef("init read failed %d", rv);
		return rv;
	}
	rv = transport__read(&c->transport, &buf, read_protocol_cb);
	if (rv != 0) {
		tracef("transport read failed %d", rv);
		return rv;
	}
	return 0;
}

int conn__start(struct conn *c,
		struct config *config,
		struct uv_loop_s *loop,
		struct registry *registry,
		struct raft *raft,
		struct uv_stream_s *stream,
		struct raft_uv_transport *uv_transport,
		struct id_state seed,
		conn_close_cb close_cb)
{
	int rv;
	(void)loop;
	tracef("conn start");
	rv = transport__init(&c->transport, stream);
	if (rv != 0) {
		tracef("conn start - transport init failed %d", rv);
		goto err;
	}
	c->config = config;
	c->transport.data = c;
	c->uv_transport = uv_transport;
	c->close_cb = close_cb;
	gateway__init(&c->gateway, config, registry, raft, seed);
	rv = buffer__init(&c->read);
	if (rv != 0) {
		goto err_after_transport_init;
	}
	rv = buffer__init(&c->write);
	if (rv != 0) {
		goto err_after_read_buffer_init;
	}
	c->handle.data = c;
	c->closed = false;
	/* First, we expect the client to send us the protocol version. */
	rv = read_protocol(c);
	if (rv != 0) {
		goto err_after_write_buffer_init;
	}
	return 0;

err_after_write_buffer_init:
	buffer__close(&c->write);
err_after_read_buffer_init:
	buffer__close(&c->read);
err_after_transport_init:
	transport__close(&c->transport, NULL);
err:
	return rv;
}

void conn__stop(struct conn *c)
{
	tracef("conn stop");
	if (c->closed) {
		return;
	}
	c->closed = true;
	gateway__close(&c->gateway);
	transport__close(&c->transport, closeCb);
}
