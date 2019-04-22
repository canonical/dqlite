#include <raft.h>
#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../include/dqlite.h"

#include "lib/transport.h"

#include "client.h"
#include "request.h"
#include "transport.h"

struct impl
{
	struct uv_loop_s *loop;
	void *data;             /* Passed back to connect */
	dqlite_connect connect; /* Connect function */
	unsigned id;
	const char *address;
	raft_uv_accept_cb accept_cb;
};

struct connect
{
	struct impl *impl;
	struct raft_uv_connect *req;
	struct uv_work_s work;
	unsigned id;
	const char *address;
	int fd;
	int status;
};

static int impl_init(struct raft_uv_transport *transport,
		     unsigned id,
		     const char *address)
{
	struct impl *i = transport->impl;
	i->id = id;
	i->address = address;
	return 0;
}

static int impl_listen(struct raft_uv_transport *transport,
		       raft_uv_accept_cb cb)
{
	struct impl *i = transport->impl;
	i->accept_cb = cb;
	return 0;
}

static void connect_work_cb(uv_work_t *work)
{
	struct connect *r = work->data;
	struct impl *i = r->impl;
	struct dqlite_server server;
	struct client client;
	int rv;

	server.id = r->id;           /* Remote server ID */
	server.address = r->address; /* Remote server address */

	rv = i->connect(i->data, &server, &r->fd);
	if (rv != 0) {
		rv = RAFT_NOCONNECTION;
		goto err;
	}
	rv = client__init(&client, r->fd);
	if (rv != 0) {
		assert(rv == DQLITE_NOMEM);
		rv = RAFT_NOMEM;
		goto err_after_connect;
	}

	rv = client__send_handshake(&client);
	if (rv != 0) {
		rv = RAFT_NOCONNECTION;
		goto err_after_client_init;
	}

	rv = client__send_connect(&client, i->id, i->address);
	if (rv != 0) {
		rv = RAFT_NOCONNECTION;
		goto err_after_client_init;
	}

	client__close(&client);

	r->status = 0;
	return;

err_after_client_init:
	client__close(&client);
err_after_connect:
	close(r->fd);
err:
	r->status = rv;
	return;
}

static void connect_after_work_cb(uv_work_t *work, int status)
{
	struct connect *r = work->data;
	struct impl *i = r->impl;
	struct transport transport;
	struct uv_stream_s *stream = NULL;
	int rv;

	assert(status == 0);

	if (r->status != 0) {
		goto out;
	}

	/* TODO: extract the logic to guess the handle type */
	rv = transport__init(&transport, i->loop, r->fd);
	if (rv != 0) {
		r->status = RAFT_NOCONNECTION;
		close(r->fd);
		goto out;
	}

	stream = transport.stream;
out:
	r->req->cb(r->req, stream, r->status);
	sqlite3_free(r);
}

static int impl_connect(struct raft_uv_transport *transport,
			struct raft_uv_connect *req,
			unsigned id,
			const char *address,
			raft_uv_connect_cb cb)
{
	struct impl *i = transport->impl;
	struct connect *r;
	int rv;

	r = sqlite3_malloc(sizeof *r);
	if (r == NULL) {
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
	cb(transport);
}

static int parse_address(const char *address, struct sockaddr_in *addr)
{
	char buf[256];
	char *host;
	char *port;
	char *colon = ":";
	int rv;

	/* TODO: turn this poor man parsing into proper one */
	strcpy(buf, address);
	host = strtok(buf, colon);
	port = strtok(NULL, ":");
	if (port == NULL) {
		port = "8080";
	}

	rv = uv_ip4_addr(host, atoi(port), addr);
	if (rv != 0) {
		return RAFT_NOCONNECTION;
	}

	return 0;
}

static int default_connect(void *data, const dqlite_server *server, int *fd)
{
	struct sockaddr_in addr;
	int rv;
	(void)data;

	rv = parse_address(server->address, &addr);
	if (rv != 0) {
		return RAFT_NOCONNECTION;
	}

	*fd = socket(AF_INET, SOCK_STREAM, 0);
	if (*fd == -1) {
		return RAFT_NOCONNECTION;
	}

	rv = connect(*fd, &addr, sizeof addr);
	if (rv == -1) {
		close(*fd);
		return RAFT_NOCONNECTION;
	}

	return 0;
}

int raft_uv_proxy__init(struct raft_uv_transport *transport,
			struct uv_loop_s *loop)
{
	struct impl *i = sqlite3_malloc(sizeof *i);
	if (i == NULL) {
		return DQLITE_NOMEM;
	}
	i->loop = loop;
	i->data = NULL;
	i->connect = default_connect;
	transport->impl = i;
	transport->init = impl_init;
	transport->listen = impl_listen;
	transport->connect = impl_connect;
	transport->close = impl_close;
	return 0;
}

void raft_uv_proxy__close(struct raft_uv_transport *transport)
{
	struct impl *i = transport->impl;
	sqlite3_free(i);
}

void raft_uv_proxy__accept(struct raft_uv_transport *transport,
			   unsigned id,
			   const char *address,
			   struct uv_stream_s *stream)
{
	struct impl *i = transport->impl;
	i->accept_cb(transport, id, address, stream);
}
