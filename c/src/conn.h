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

/* Request read buffer */
struct dqlite__conn_buf {
	uint8_t *data;    /* Hold the data of a single request */
	size_t   offset;  /* Number of bytes in buf that have been read */
	size_t   pending; /* Number of bytes in buf that haven't been read yet */
};

struct dqlite__conn {
	/* read-only */
	dqlite__error           error;    /* Last error occurred, if any */

	/* private */
	FILE                   *log;      /* Log output stream */
	int                     socket;   /* Socket file descriptor of client connection */
	dqlite_cluster         *cluster;  /* Cluster interface implementation */
	uv_loop_t              *loop;     /* UV loop */
	struct dqlite__fsm      fsm;      /* Connection state machine */
	struct dqlite__conn_buf buffer;   /* Read buffer */
	struct dqlite__request  request;  /* Request parser */
	struct dqlite__gateway  gateway;  /* Client state and request handler */
	uv_tcp_t                tcp;      /* UV TCP handle */
	uv_timer_t              alive;    /* Check that the client is still alive */
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
