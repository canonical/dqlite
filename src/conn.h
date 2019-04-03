/**
 * Handle a single client connection.
 */

#ifndef DQLITE_CONN_H_
#define DQLITE_CONN_H_

#include <raft/io_uv.h>

#include "gateway.h"
#include "lib/buffer.h"
#include "lib/transport.h"
#include "message.h"

/**
 * Callbacks.
 */
struct conn;
typedef void (*conn_close_cb)(struct conn *c);

struct conn
{
	struct dqlite_logger *logger;
	struct raft_io_uv_transport *uv_transport; /* Raft transport */
	conn_close_cb close_cb;			   /* Close callback */
	struct transport transport; /* Async network read/write */
	struct gateway gateway;     /* Request handler */
	struct buffer read;	 /* Read buffer */
	struct buffer write;	/* Write buffer */
	uint64_t protocol;	  /* Protocol format version */
	struct message request;     /* Request message meta data */
	struct message response;    /* Response message meta data */
	struct handle handle;
	bool closed;
};

/**
 * Initialize and start a connection.
 *
 * If no error is returned, the connection should be considered started. Any
 * error occurring after this point will trigger the @close_cb callback.
 */
int conn__start(struct conn *c,
		struct dqlite_logger *logger,
		struct uv_loop_s *loop,
		struct options *options,
		struct registry *registry,
		struct raft *raft,
		int fd,
		struct raft_io_uv_transport *uv_transport,
		conn_close_cb close_cb);

/**
 * Force closing the connection. The close callback will be invoked when it's
 * safe to release the memory of the connection object.
 */
void conn__stop(struct conn *c);

#endif /* DQLITE_CONN_H_ */
