#ifndef DQLITE_CONN_H
#define DQLITE_CONN_H

#include <stdint.h>
#include <stdio.h>
#include <uv.h>

#include "../include/dqlite.h"

#include "error.h"
#include "fsm.h"
#include "gateway.h"
#include "metrics.h"
#include "options.h"
#include "request.h"

/* The size of pre-allocated read buffer for holding the payload of incoming
 * requests. This should generally fit in a single IP packet, given typical MTU
 * sizes, and request payloads usually are short enough to fit here.
 *
 * If the request payload is larger than this amount, memory will be allocated
 * on the heap.
 **/
#define DQLITE__CONN_BUF_SIZE 1024

/* Serve requests from a single connected client. */
struct dqlite__conn {
	/* public */
	dqlite_logger *logger; /* Optional logger implementation */

	/* read-only */
	dqlite__error error;    /* Last error occurred, if any */
	uint64_t      protocol; /* Protocol version */

	/* private */
	struct dqlite__metrics *metrics;  /* Operational metrics */
	struct dqlite__options *options;  /* Connection state machine */
	struct dqlite__fsm      fsm;      /* Connection state machine */
	struct dqlite__gateway  gateway;  /* Client state and request handler */
	struct dqlite__request  request;  /* Incoming request */
	struct dqlite__response response; /* Response for internal failures */

	int        fd;   /* File descriptor of client stream */
	uv_loop_t *loop; /* UV loop */
	union {
		uv_tcp_t    tcp;
		uv_pipe_t   pipe;
		uv_stream_t stream;
	};                /* UV stream handle */
	uv_timer_t alive; /* Check that the client is still alive */
	uv_buf_t   buf;   /* Read buffer */

	uint64_t timestamp; /* Time at which the current request started. */
	int      aborting;  /* True if we started to abort the connetion */
	int      paused;    /* True if we have paused reading from the stream */
};

/* Initialize a connection object */
void dqlite__conn_init(struct dqlite__conn *   c,
                       int                     fd,
                       dqlite_logger *         logger,
                       dqlite_cluster *        cluster,
                       uv_loop_t *             loop,
                       struct dqlite__options *options,
                       struct dqlite__metrics *metrics);

/* Close a connection object, releasing all associated resources. */
void dqlite__conn_close(struct dqlite__conn *c);

/* Start reading data from the client and processing requests. */
int dqlite__conn_start(struct dqlite__conn *c);

/* Immediately close the connection with the client. */
void dqlite__conn_abort(struct dqlite__conn *c);

#endif /* DQLITE_CONN_H */
