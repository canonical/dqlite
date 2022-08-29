#include "lib/transport.h"

#include <raft.h>
#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "client.h"
#include "lib/addr.h"
#include "message.h"
#include "request.h"
#include "tracing.h"
#include "transport.h"

struct impl
{
	struct uv_loop_s *loop;
	struct
	{
		int (*f)(void *arg, const char *address, int *fd);
		void *arg;

	} connect;
	raft_id id;
	const char *address;
	raft_uv_accept_cb accept_cb;
};

struct connect
{
	struct impl *impl;
	struct raft_uv_connect *req;
	struct uv_work_s work;
	raft_id id;
	const char *address;
	int fd;
	int status;
};

static int impl_init(struct raft_uv_transport *transport,
		     raft_id id,
		     const char *address)
{
        tracef("impl init");
	struct impl *i = transport->impl;
	i->id = id;
	i->address = address;
	return 0;
}

static int impl_listen(struct raft_uv_transport *transport,
		       raft_uv_accept_cb cb)
{
        tracef("impl listen");
	struct impl *i = transport->impl;
	i->accept_cb = cb;
	return 0;
}

static void connect_work_cb(uv_work_t *work)
{
        tracef("connect work cb");
	struct connect *r = work->data;
	struct impl *i = r->impl;
	struct message message = {0};
	struct request_connect request = {0};
	uint64_t protocol;
	void *buf;
	void *cursor;
	size_t n;
	size_t n1;
	size_t n2;
	int rv;

	/* Establish a connection to the other node using the provided connect
	 * function. */
	rv = i->connect.f(i->connect.arg, r->address, &r->fd);
	if (rv != 0) {
                tracef("connect failed to %llu@%s", r->id, r->address);
		rv = RAFT_NOCONNECTION;
		goto err;
	}

	/* Send the initial dqlite protocol handshake. */
	protocol = ByteFlipLe64(DQLITE_PROTOCOL_VERSION);
	rv = (int)write(r->fd, &protocol, sizeof protocol);
	if (rv != sizeof protocol) {
                tracef("write failed");
		rv = RAFT_NOCONNECTION;
		goto err_after_connect;
	}

	/* Send a CONNECT dqlite protocol command, which will transfer control
	 * to the underlying raft UV backend. */
	request.id = i->id;
	request.address = i->address;

	n1 = message__sizeof(&message);
	n2 = request_connect__sizeof(&request);

	message.type = DQLITE_REQUEST_CONNECT;
	message.words = (uint32_t)(n2 / 8);

	n = n1 + n2;

	buf = sqlite3_malloc64(n);
	if (buf == NULL) {
                tracef("malloc failed");
		rv = RAFT_NOCONNECTION;
		goto err_after_connect;
	}

	cursor = buf;
	message__encode(&message, &cursor);
	request_connect__encode(&request, &cursor);

	rv = (int)write(r->fd, buf, n);
	sqlite3_free(buf);

	if (rv != (int)n) {
                tracef("write failed");
		rv = RAFT_NOCONNECTION;
		goto err_after_connect;
	}

	r->status = 0;
	return;

err_after_connect:
	close(r->fd);
err:
	r->status = rv;
	return;
}

static void connect_after_work_cb(uv_work_t *work, int status)
{
        tracef("connect after work cb status %d", status);
	struct connect *r = work->data;
	struct impl *i = r->impl;
	struct uv_stream_s *stream = NULL;
	int rv;

	assert(status == 0);

	if (r->status != 0) {
		goto out;
	}

	rv = transport__stream(i->loop, r->fd, &stream);
	if (rv != 0) {
                tracef("transport stream failed %d", rv);
		r->status = RAFT_NOCONNECTION;
		close(r->fd);
		goto out;
	}
out:
	r->req->cb(r->req, stream, r->status);
	sqlite3_free(r);
}

static int impl_connect(struct raft_uv_transport *transport,
			struct raft_uv_connect *req,
			raft_id id,
			const char *address,
			raft_uv_connect_cb cb)
{
        tracef("impl connect id:%llu address:%s", id, address);
	struct impl *i = transport->impl;
	struct connect *r;
	int rv;

	r = sqlite3_malloc(sizeof *r);
	if (r == NULL) {
                tracef("malloc failed");
		rv = DQLITE_NOMEM;
		goto err;
	}

	r->impl = i;
	r->req = req;
	r->work.data = r;
	r->id = id;
	r->address = address;

	req->cb = cb;

	rv = uv_queue_work(i->loop, &r->work, connect_work_cb,
			   connect_after_work_cb);
	if (rv != 0) {
                tracef("queue work failed");
		rv = RAFT_NOCONNECTION;
		goto err_after_connect_alloc;
	}

	return 0;

err_after_connect_alloc:
	sqlite3_free(r);
err:
	return rv;
}

static void impl_close(struct raft_uv_transport *transport,
		       raft_uv_transport_close_cb cb)
{
        tracef("impl close");
	cb(transport);
}

static int default_connect(void *arg, const char *address, int *fd)
{
	struct sockaddr_in addr_in;
	struct sockaddr *addr = (struct sockaddr *)&addr_in;
	socklen_t addr_len = sizeof addr_in;
	int rv;
	(void)arg;

	rv = AddrParse(address, addr, &addr_len, "8080", 0);
	if (rv != 0) {
		return RAFT_NOCONNECTION;
	}

	assert(addr->sa_family == AF_INET || addr->sa_family == AF_INET6);
	*fd = socket(addr->sa_family, SOCK_STREAM, 0);
	if (*fd == -1) {
		return RAFT_NOCONNECTION;
	}

	rv = connect(*fd, addr, addr_len);
	if (rv == -1) {
		close(*fd);
		return RAFT_NOCONNECTION;
	}

	return 0;
}

int raftProxyInit(struct raft_uv_transport *transport, struct uv_loop_s *loop)
{
        tracef("raft proxy init");
	struct impl *i = sqlite3_malloc(sizeof *i);
	if (i == NULL) {
		return DQLITE_NOMEM;
	}
	i->loop = loop;
	i->connect.f = default_connect;
	i->connect.arg = NULL;
	i->accept_cb = NULL;
	transport->impl = i;
	transport->init = impl_init;
	transport->listen = impl_listen;
	transport->connect = impl_connect;
	transport->close = impl_close;
	return 0;
}

void raftProxyClose(struct raft_uv_transport *transport)
{
        tracef("raft proxy close");
	struct impl *i = transport->impl;
	sqlite3_free(i);
}

void raftProxyAccept(struct raft_uv_transport *transport,
		     raft_id id,
		     const char *address,
		     struct uv_stream_s *stream)
{
        tracef("raft proxy accept");
	struct impl *i = transport->impl;
	/* If the accept callback is NULL it means we were stopped. */
	if (i->accept_cb == NULL) {
                tracef("raft proxy accept closed");
		uv_close((struct uv_handle_s *)stream, (uv_close_cb)raft_free);
	} else {
		i->accept_cb(transport, id, address, stream);
	}
}

void raftProxySetConnectFunc(struct raft_uv_transport *transport,
			     int (*f)(void *arg, const char *address, int *fd),
			     void *arg)
{
	struct impl *i = transport->impl;
	i->connect.f = f;
	i->connect.arg = arg;
}
