#ifndef UV_TCP_H_
#define UV_TCP_H_

#include "../raft.h"
#include "../lib/queue.h"

/* Protocol version. */
#define UV__TCP_HANDSHAKE_PROTOCOL 1

struct UvTcp
{
	struct raft_uv_transport *transport; /* Interface object we implement */
	struct uv_loop_s *loop;              /* Event loop */
	raft_id id;                          /* ID of this raft server */
	const char *address;                 /* Address of this raft server */
	unsigned n_listeners;                /* Number of listener sockets */
	struct uv_tcp_s *listeners;          /* Listener sockets */
	raft_uv_accept_cb accept_cb; /* Call after accepting a connection */
	queue accepting;             /* Connections being accepted */
	queue connecting;            /* Pending connection requests */
	queue aborting;              /* Connections being aborted */
	bool closing;                /* True after close() is called */
	raft_uv_transport_close_cb
	    close_cb;       /* Call when it's safe to free us */
	char *bind_address; /* Optional address:port to bind to */
};

/* Implementation of raft_uv_transport->listen. */
int UvTcpListen(struct raft_uv_transport *transport, raft_uv_accept_cb cb);

/* Stop accepting new connection and close all connections being accepted. */
void UvTcpListenClose(struct UvTcp *t);

/* Implementation of raft_uv_transport->connect. */
int UvTcpConnect(struct raft_uv_transport *transport,
		 struct raft_uv_connect *req,
		 raft_id id,
		 const char *address,
		 raft_uv_connect_cb cb);

/* Abort all pending connection requests. */
void UvTcpConnectClose(struct UvTcp *t);

/* Fire the transport close callback if the transport is closing and there's no
 * more pending callback. */
void UvTcpMaybeFireCloseCb(struct UvTcp *t);

#endif /* UV_TCP_H_ */
