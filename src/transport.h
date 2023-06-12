/* Implementation of the raft_uv_transport interface, proxied by a dqlite
 * connection.
 *
 * Instead of having raft instances connect to each other directly, we pass a custom
 * connect function that causes dqlite to send a CONNECT request to the dqlite
 * server where the destination raft instance is running. That server responds to
 * the CONNECT request by forwarding the dqlite connection to its raft instance,
 * after which the raft-to-raft connection is transparent. */
#ifndef TRANSPORT_H_
#define TRANSPORT_H_

#include <raft/uv.h>

#include "../include/dqlite.h"

int raftProxyInit(struct raft_uv_transport *transport, struct uv_loop_s *loop);

void raftProxyClose(struct raft_uv_transport *transport);

/* Invoke the accept callback configured on the transport object. */
void raftProxyAccept(struct raft_uv_transport *transport,
		     raft_id id,
		     const char *address,
		     struct uv_stream_s *stream);

/* Set a custom connect function. */
void raftProxySetConnectFunc(struct raft_uv_transport *transport,
			     int (*f)(void *arg, const char *address, int *fd),
			     void *arg);

#endif /* TRANSPORT_H_*/
