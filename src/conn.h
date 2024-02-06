/**
 * Handle a single client connection.
 */

#ifndef DQLITE_CONN_H_
#define DQLITE_CONN_H_

#include "lib/buffer.h"
#include "lib/queue.h"
#include "lib/transport.h"

#include "gateway.h"
#include "id.h"
#include "message.h"
#include "raft.h"

/**
 * Callbacks.
 */
struct conn;
typedef void (*conn_close_cb)(struct conn *c);

struct conn
{
	struct config *config;
	struct raft_uv_transport *uv_transport; /* Raft transport */
	conn_close_cb close_cb;                 /* Close callback */
	struct transport transport;             /* Async network read/write */
	struct gateway gateway;                 /* Request handler */
	struct buffer read;                     /* Read buffer */
	struct buffer write;                    /* Write buffer */
	uint64_t protocol;                      /* Protocol format version */
	struct message request;                 /* Request message meta data */
	struct message response;                /* Response message meta data */
	struct handle handle;
	bool closed;
	queue queue;
};

/**
 * Initialize and start a connection.
 *
 * If no error is returned, the connection should be considered started. Any
 * error occurring after this point will trigger the @close_cb callback.
 */
int conn__start(struct conn *c,
		struct config *config,
		struct uv_loop_s *loop,
		struct registry *registry,
		struct raft *raft,
		struct uv_stream_s *stream,
		struct raft_uv_transport *uv_transport,
		struct id_state seed,
		conn_close_cb close_cb);

/**
 * Force closing the connection. The close callback will be invoked when it's
 * safe to release the memory of the connection object.
 */
void conn__stop(struct conn *c);

#endif /* DQLITE_CONN_H_ */
