/******************************************************************************
 *
 * Core dqlite server engine, calling out SQLite for serving client requests.
 *
 *****************************************************************************/

#ifndef DQLITE_GATEWAY_H
#define DQLITE_GATEWAY_H

#include <stdint.h>
#include <stdio.h>
#include <time.h>

#ifdef DQLITE_EXPERIMENTAL

#include <libco.h>

#endif /* DQLITE_EXPERIMENTAL */

#include "../include/dqlite.h"

#include "../include/dqlite.h"

#include "db.h"
#include "error.h"
#include "fsm.h"
#include "options.h"
#include "request.h"
#include "response.h"

#define DQLITE__GATEWAY_MAX_REQUESTS 2

/* Cleanup code indicating that the request does not require any special logic
 * upon completion. */
#define DQLITE__GATEWAY_CLEANUP_NONE 0

/* Cleanup code indicating that in order to complete the request the associated
 * statement needs to be finalized. */
#define DQLITE__GATEWAY_CLEANUP_FINALIZE 1

/* Context for the gateway request handlers */
struct dqlite__gateway_ctx {
	struct dqlite__request *request;
	struct dqlite__response response;
	struct dqlite__db *     db;      /* For multi-response queries */
	struct dqlite__stmt *   stmt;    /* For multi-response queries */
	int                     cleanup; /* Code indicating how to cleanup */
};

/* Callbacks that the gateway will invoke during the various phases of request
 * handling. */
struct dqlite__gateway_cbs {
	void *ctx; /* Context to pass to the callbacks. */

	/* Invoked when a respone is available. User code is expected to invoke
	 * dqlite__gateway_flushed() to indicate that it has completed sending
	 * the response data to the client and that the response buffer can be
	 * used for another request. */
	void (*xFlush)(void *ctx, struct dqlite__response *response);
};

/*
 * Handle requests from a single connected client and forward them to
 * SQLite.
 */
struct dqlite__gateway {
	/* read-only */
	uint64_t      client_id;
	uint64_t      heartbeat; /* Last successful heartbeat from the client */
	dqlite__error error;     /* Last error occurred, if any */

	/* private */
	struct dqlite__gateway_cbs callbacks; /* User callbacks */
	dqlite_cluster *           cluster;   /* Cluster API implementation  */
	struct dqlite__options *   options;   /* Configuration options */

	/* Buffer holding responses for in-progress requests. Clients are
	 * expected to issue one SQL request at a time and wait for the
	 * response, plus possibly some concurrent control requests such as an
	 * heartbeat or interrupt. */
	struct dqlite__gateway_ctx ctxs[DQLITE__GATEWAY_MAX_REQUESTS];

	struct dqlite__request *next;

	struct dqlite__db *db; /* Open database */

#ifdef DQLITE_EXPERIMENTAL

	cothread_t main_coroutine; /* Main coroutine ID */
	cothread_t loop_coroutine; /* Coroutine ID of the gateway main loop. */

#endif /* DQLITE_EXPERIMENTAL */
};

void dqlite__gateway_init(struct dqlite__gateway *    g,
                          struct dqlite__gateway_cbs *callbacks,
                          struct dqlite_cluster *     cluster,
                          struct dqlite__options *    options);

void dqlite__gateway_close(struct dqlite__gateway *g);

/* Start the gateway.
 *
 * This function will kick off the gateway loop which runs in its own coroutine
 * and has its own stack. Whenever blocking I/O is required (for example when
 * applying a new raft entry) control will be passed back to the main thread
 * loop, and the gateway loop will resume when the relevant I/O is completed.
 *
 * The 'now' parameter holds the current time, and it's used to set the initial
 * heartbeat timestamp.
 *
 * It's a separate function from dqlite__gateway_init() since it must be called
 * from the main loop thread. */
int dqlite__gateway_start(struct dqlite__gateway *g, uint64_t now);

/* Start handling a new client request.
 *
 * Responses for requests that can be handled synchronously will be generated
 * immediately and the xFlush() callback will be invoked inline, before this
 * function returns.
 *
 * Responses for requests that need to perform network or disk I/O will be
 * generated asynchronously and xFlush() will be invoked when done.
 *
 * Some requests might generate more than one response (for example when a
 * SELECT query yields a large number of rows). In that case xFlush() will be
 * invoked more than once.
 *
 * Generally at most one request can be outstanding at any given time. This
 * function will return an error if user code calls it and there's already a
 * request in progress. The only exceptions to this rule are heartbeat and
 * interrupt requests, that will be handled synchronously regardless of whether
 * there's already a request in progress.
 *
 * User code can check whether the gateway would currently accept a request of a
 * certain type by calling dqlite__gateway_ctx_for.
 */
int dqlite__gateway_handle(struct dqlite__gateway *g,
                           struct dqlite__request *request);

/* Return the request ctx index that the gateway will use to handle a request of
 * the given type at this moment, or -1 if the gateway can't handle a request of
 * that type right now. */
int dqlite__gateway_ctx_for(struct dqlite__gateway *g, int type);

/* Notify the gateway that a response has been completely flushed and its data
 * sent to the client. */
void dqlite__gateway_flushed(struct dqlite__gateway * g,
                             struct dqlite__response *response);

/* Notify the gateway that this response has been aborted due to errors
 * (e.g. the client disconnected). */
void dqlite__gateway_aborted(struct dqlite__gateway * g,
                             struct dqlite__response *response);

#endif /* DQLITE_GATEWAY_H */
