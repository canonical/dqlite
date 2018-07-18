#ifndef DQLITE_GATEWAY_H
#define DQLITE_GATEWAY_H

#include <stdint.h>
#include <stdio.h>
#include <time.h>

#include "../include/dqlite.h"

#include "db.h"
#include "error.h"
#include "fsm.h"
#include "request.h"
#include "response.h"

/* Maximum number of requests that can be served concurrently.
 *
 * TODO: this should be reduced to 5 or 3. The problem is that some new request
 * might come in before the response for the last request has been completely
 * written out.
 */
#define DQLITE__GATEWAY_MAX_REQUESTS 20

/* Context for the gateway request handlers */
struct dqlite__gateway_ctx {
	struct dqlite__request *request;
	struct dqlite__response response;
};

/*
 * Handle requests from a single connected client and forward them to SQLite.
 */
struct dqlite__gateway {
	/* public */
	uint16_t heartbeat_timeout; /* Abort after this many milliseconds with no
	                               heartbeat */

	/* read-only */
	uint64_t      client_id;
	uint64_t      heartbeat; /* Last successful heartbeat from the client */
	dqlite__error error;     /* Last error occurred, if any */

	/* private */
	dqlite_cluster *cluster; /* Cluster interface implementation */

	/*
	 * Clients are expected to issue one SQL request at a time and wait for
	 * the response, plus possibly some concurrent requests such as
	 * Heartbeat or Interrupt. So we don't need a lot of concurrency.
	 */
	struct dqlite__gateway_ctx ctxs[DQLITE__GATEWAY_MAX_REQUESTS];

	struct dqlite__db_registry dbs; /* Registry of open databases */
};

void dqlite__gateway_init(struct dqlite__gateway *g, dqlite_cluster *cluster);

void dqlite__gateway_close(struct dqlite__gateway *g);

/* Handle a new client request */
int dqlite__gateway_handle(struct dqlite__gateway *  g,
                           struct dqlite__request *  request,
                           struct dqlite__response **response);

/* Continue serving a request after the first write (for result sets) */
int dqlite__gateway_continue(struct dqlite__gateway * g,
                             struct dqlite__response *response);

/* Complete a request after the response has been written */
void dqlite__gateway_finish(struct dqlite__gateway * g,
                            struct dqlite__response *response);

/* Abort a request */
void dqlite__gateway_abort(struct dqlite__gateway * g,
                           struct dqlite__response *response);

#endif /* DQLITE_GATEWAY_H */
