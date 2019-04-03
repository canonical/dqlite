#include "conn.h"

/* Initialize the given buffer for reading, ensure it has the given size. */
static int init_read(struct conn *c, uv_buf_t *buf, size_t size)
{
	buf->base = buffer__advance(&c->read, size);
	if (buf->base == NULL) {
		return DQLITE_NOMEM;
	}
	buf->len = size;
	return 0;
}

static void read_protocol_cb(struct transport *transport, int status)
{
	struct conn *c = transport->data;
	struct cursor cursor;
	int rv;

	if (status != 0) {
	}

	cursor.p = buffer__cursor(&c->read, 0);
	cursor.cap = buffer__offset(&c->read);
	rv = uint64__decode(&cursor, &c->protocol);
	assert(rv == 0); /* Can't fail, we know we have enough bytes */
}

/* Start reading the protocol format version */
static int read_protocol(struct conn *c)
{
	uv_buf_t buf;
	int rv;
	rv = init_read(c, &buf, sizeof c->protocol);
	if (rv != 0) {
		return rv;
	}
	rv = transport__read(&c->transport, &buf, read_protocol_cb);
	if (rv != 0) {
		return rv;
	}
	return 0;
}

int conn__start(struct conn *c,
		struct dqlite_logger *logger,
		struct uv_loop_s *loop,
		struct options *options,
		struct registry *registry,
		struct raft *raft,
		int fd,
		struct raft_io_uv_transport *uv_transport,
		conn_close_cb close_cb)
{
	int rv;
	c->logger = logger;
	rv = transport__init(&c->transport, loop, fd);
	if (rv != 0) {
		goto err;
	}
	c->transport.data = c;
	c->uv_transport = uv_transport;
	c->close_cb = close_cb;
	gateway__init(&c->gateway, logger, options, registry, raft);
	rv = buffer__init(&c->read);
	if (rv != 0) {
		goto err_after_transport_init;
	}
	rv = buffer__init(&c->write);
	if (rv != 0) {
		goto err_after_read_buffer_init;
	}
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

static void close_cb(struct transport *transport) {
	struct conn *c = transport->data;
	buffer__close(&c->write);
	buffer__close(&c->read);
}

void conn__stop(struct conn *c)
{
	transport__close(&c->transport, close_cb);
}
