#include "uv_tcp.h"
#include "uv_ip.h"

#include <string.h>

#include "../raft.h"
#include "assert.h"
#include "err.h"
#include "heap.h"

/* Implementation of raft_uv_transport->init. */
static int uvTcpInit(struct raft_uv_transport *transport,
		     raft_id id,
		     const char *address)
{
	struct UvTcp *t = transport->impl;
	assert(id > 0);
	assert(address != NULL);
	t->id = id;
	t->address = address;
	return 0;
}

/* Implementation of raft_uv_transport->close. */
static void uvTcpClose(struct raft_uv_transport *transport,
		       raft_uv_transport_close_cb cb)
{
	struct UvTcp *t = transport->impl;
	assert(!t->closing);
	t->closing = true;
	t->close_cb = cb;
	UvTcpListenClose(t);
	UvTcpConnectClose(t);
	UvTcpMaybeFireCloseCb(t);
}

void UvTcpMaybeFireCloseCb(struct UvTcp *t)
{
	if (!t->closing) {
		return;
	}

	assert(QUEUE_IS_EMPTY(&t->accepting));
	assert(QUEUE_IS_EMPTY(&t->connecting));
	if (!QUEUE_IS_EMPTY(&t->aborting)) {
		return;
	}

	if (t->listeners != NULL) {
		return;
	}

	if (t->close_cb != NULL) {
		t->close_cb(t->transport);
	}
}

int raft_uv_tcp_init(struct raft_uv_transport *transport,
		     struct uv_loop_s *loop)
{
	struct UvTcp *t;
	void *data = transport->data;
	int version = transport->version;
	if (version != 1) {
		ErrMsgPrintf(transport->errmsg, "Invalid version: %d", version);
		return RAFT_INVALID;
	}

	memset(transport, 0, sizeof *transport);
	transport->data = data;
	transport->version = version;
	t = raft_malloc(sizeof *t);
	if (t == NULL) {
		ErrMsgOom(transport->errmsg);
		return RAFT_NOMEM;
	}
	t->transport = transport;
	t->loop = loop;
	t->id = 0;
	t->address = NULL;
	t->bind_address = NULL;
	t->listeners = NULL;
	t->n_listeners = 0;
	t->accept_cb = NULL;
	QUEUE_INIT(&t->accepting);
	QUEUE_INIT(&t->connecting);
	QUEUE_INIT(&t->aborting);
	t->closing = false;
	t->close_cb = NULL;

	transport->impl = t;
	transport->init = uvTcpInit;
	transport->close = uvTcpClose;
	transport->listen = UvTcpListen;
	transport->connect = UvTcpConnect;

	return 0;
}

void raft_uv_tcp_close(struct raft_uv_transport *transport)
{
	struct UvTcp *t = transport->impl;
	raft_free(t->bind_address);
	raft_free(t);
}

int raft_uv_tcp_set_bind_address(struct raft_uv_transport *transport,
				 const char *address)
{
	struct UvTcp *t = transport->impl;
	char hostname[NI_MAXHOST];
	char service[NI_MAXSERV];
	int rv;

	rv = uvIpAddrSplit(address, hostname, sizeof(hostname), service,
			   sizeof(service));
	if (rv != 0) {
		return RAFT_INVALID;
	}

	t->bind_address = raft_malloc(strlen(address) + 1);
	if (t->bind_address == NULL) {
		return RAFT_NOMEM;
	}
	strcpy(t->bind_address, address);
	return 0;
}
