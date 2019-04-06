#include <sqlite3.h>

#include "../include/dqlite.h"

#include "transport.h"

struct impl
{
	struct uv_loop_s *loop;
	unsigned id;
	const char *address;
	raft_io_uv_accept_cb accept_cb;
};

static int impl_init(struct raft_io_uv_transport *transport,
		     unsigned id,
		     const char *address)
{
	struct impl *i = transport->impl;
	i->id = id;
	i->address = address;
	return 0;
}

static int impl_listen(struct raft_io_uv_transport *transport,
		       raft_io_uv_accept_cb cb)
{
	struct impl *i = transport->impl;
	i->accept_cb = cb;
	return 0;
}

static int impl_connect(struct raft_io_uv_transport *transport,
			struct raft_io_uv_connect *req,
			unsigned id,
			const char *address,
			raft_io_uv_connect_cb cb)
{
	(void)transport;
	(void)req;
	(void)address;
	(void)cb;
	(void)id;
	return 0;
}

static void impl_close(struct raft_io_uv_transport *transport,
		       raft_io_uv_transport_close_cb cb)
{
	cb(transport);
}

int raft_uv_proxy__init(struct raft_io_uv_transport *transport,
			struct uv_loop_s *loop)
{
	struct impl *i = sqlite3_malloc(sizeof *i);
	if (i == NULL) {
		return DQLITE_NOMEM;
	}
	i->loop = loop;
	transport->impl = i;
	transport->init = impl_init;
	transport->listen = impl_listen;
	transport->connect = impl_connect;
	transport->close = impl_close;
	return 0;
}

void raft_uv_proxy__close(struct raft_io_uv_transport *transport) {
	struct impl *i = transport->impl;
	sqlite3_free(i);
}

void raft_uv_proxy__accept(struct raft_io_uv_transport *transport,
			   unsigned id,
			   const char *address,
			   struct uv_stream_s *stream)
{
	struct impl *i = transport->impl;
	i->accept_cb(transport, id, address, stream);
}
