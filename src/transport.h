/**
 * Implementation of the raft_io_uv_transport interface, proxied by a dqlite
 * connection.
 */
#ifndef TRANSPORT_H_
#define TRANSPORT_H_

#include <raft/io_uv.h>

#include "../include/dqlite.h"

int raft_uv_proxy__init(struct raft_io_uv_transport *transport,
			struct uv_loop_s *loop);

void raft_uv_proxy__close(struct raft_io_uv_transport *transport);

/**
 * Invoke the accept callback configured on the transport object.
 */
void raft_uv_proxy__accept(struct raft_io_uv_transport *transport,
			   unsigned id,
			   const char *address,
			   struct uv_stream_s *stream);

/**
 * Set a custom connect function.
 */
void raft_uv_proxy__set_connect_func(struct raft_io_uv_transport *transport,
				     dqlite_connect connect);

#endif /* TRANSPORT_H_*/
