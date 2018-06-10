#ifndef DQLITE_CONN_H
#define DQLITE_CONN_H

#include <uv.h>
#include <stdio.h>
#include <stdint.h>

#include "error.h"
#include "dqlite.h"
#include "fsm.h"
#include "request.h"
#include "gateway.h"

/* The size of pre-allocated read buffer for holding the payload of incoming
 * requests. This should generally fit in a single IP packet, given typical MTU
 * sizes, and request payloads usually are short enough to fit here.
 *
 * If the request payload is larger than this amount, memory will be allocated
 * on the heap.
 **/
#define DQLITE__CONN_BUF_SIZE 1024

struct dqlite__conn {
	/* read-only */
	dqlite__error           error;    /* Last error occurred, if any */
	uint32_t                version;  /* Protocol version TODO: change to 64 bit */

	/* private */
	struct dqlite__fsm      fsm;      /* Connection state machine */
	struct dqlite__request  request;  /* Request parser */
	struct dqlite__gateway  gateway;  /* Client state and request handler */
	dqlite_cluster         *cluster;  /* Cluster interface implementation */
	FILE                   *log;      /* Log output stream */
	int                     socket;   /* Socket file descriptor of client connection */
	uv_loop_t              *loop;     /* UV loop */
	uv_tcp_t                tcp;      /* UV TCP handle */
	uv_timer_t              alive;    /* Check that the client is still alive */
	uint8_t                *buf;      /* Read buffer */
	size_t                  offset;   /* Number of bytes in buf that have been read */
	size_t                  pending;  /* Number of bytes in buf that haven't been read yet */
};

void dqlite__conn_init(
	struct dqlite__conn *c,
	FILE *log,
	int socket,
	dqlite_cluster *cluster,
	uv_loop_t *loop);

void dqlite__conn_close(struct dqlite__conn* c);

int dqlite__conn_start(struct dqlite__conn* c);
void dqlite__conn_abort(struct dqlite__conn* c);

#endif /* DQLITE_CONN_H */
