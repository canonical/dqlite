/**
 * Core dqlite server engine, calling out SQLite for serving client requests.
 */

#ifndef DQLITE_GATEWAY_H
#define DQLITE_GATEWAY_H

#include "../include/dqlite.h"

#include "db_.h"
#include "error.h"
#include "options.h"
#include "request.h"
#include "response.h"
#include "registry.h"
#include "leader.h"

#define GATEWAY__MAX_REQUESTS 2

/* Cleanup code indicating that the request does not require any special logic
 * upon completion. */
#define GATEWAY__CLEANUP_NONE 0

/* Cleanup code indicating that in order to complete the request the associated
 * statement needs to be finalized. */
#define GATEWAY__CLEANUP_FINALIZE 1

/* Context for the gateway request handlers */
struct gateway__ctx
{
	struct request *request;
	struct response response;
	struct db_ *db;     /* For multi-response queries */
	struct stmt *stmt; /* For multi-response queries */
	int cleanup;       /* Code indicating how to cleanup */
};

/* Callbacks that the gateway will invoke during the various phases of request
 * handling. */
struct gateway__cbs
{
	void *ctx; /* Context to pass to the callbacks. */

	/* Invoked when a respone is available. User code is expected to invoke
	 * gateway__flushed() to indicate that it has completed sending
	 * the response data to the client and that the response buffer can be
	 * used for another request. */
	void (*xFlush)(void *ctx, struct response *response);
};

/*
 * Handle requests from a single connected client and forward them to
 * SQLite.
 */
struct gateway_
{
	/* read-only */
	uint64_t client_id;
	uint64_t heartbeat;  /* Last successful heartbeat from the client */
	dqlite__error error; /* Last error occurred, if any */

	/* private */
	struct gateway__cbs callbacks;   /* User callbacks */
	struct options *options; /* Configuration options */
	struct dqlite_logger *logger;    /* Logger to use */

	/* Buffer holding responses for in-progress requests. Clients are
	 * expected to issue one SQL request at a time and wait for the
	 * response, plus possibly some concurrent control requests such as an
	 * heartbeat or interrupt. */
	struct gateway__ctx ctxs[GATEWAY__MAX_REQUESTS];

	struct request *next;

	struct db_ *db; /* Open database */

	struct registry *registry;
	struct leader *leader;
};

void gateway__init_(struct gateway_ *g,
		   struct gateway__cbs *callbacks,
		   struct dqlite_logger *logger,
		   struct options *options);

void gateway__close_(struct gateway_ *g);

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
 * certain type by calling gateway__ctx_for.
 */
int gateway__handle(struct gateway_ *g, struct request *request);

/* Return the request ctx index that the gateway will use to handle a request of
 * the given type at this moment, or -1 if the gateway can't handle a request of
 * that type right now. */
int gateway__ctx_for(struct gateway_ *g, int type);

/* Notify the gateway that a response has been completely flushed and its data
 * sent to the client. */
void gateway__flushed(struct gateway_ *g, struct response *response);

/* Notify the gateway that this response has been aborted due to errors
 * (e.g. the client disconnected). */
void gateway__aborted(struct gateway_ *g, struct response *response);

#endif /* DQLITE_GATEWAY_H */
