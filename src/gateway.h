/**
 * Core dqlite server engine, calling out SQLite for serving client requests.
 */

#ifndef DQLITE_GATEWAY_H_
#define DQLITE_GATEWAY_H_

#include "../include/dqlite.h"

#include "lib/buffer.h"
#include "lib/serialize.h"

#include "config.h"
#include "leader.h"
#include "raft.h"
#include "registry.h"
#include "stmt.h"
#include "tuple.h"

struct handle;
struct gateway;

typedef void (*gateway_close_cb)(struct gateway *g);

/**
 * Handle requests from a single connected client and forward them to
 * SQLite.
 */
struct gateway {
	struct config *config;       /* Configuration */
	struct registry *registry;   /* Register of existing databases */
	struct raft *raft;           /* Raft instance */
	struct leader *leader;       /* Leader connection to the database */
	struct handle *req;          /* Asynchronous request being handled */
	struct stmt__registry stmts; /* Registry of prepared statements */
	uint64_t protocol;           /* Protocol format version */
	uint64_t client_id;
	gateway_close_cb close_cb;   /* Callback to close the gateway */
};

void gateway__init(struct gateway *g,
		   struct config *config,
		   struct registry *registry,
		   struct raft *raft);

void gateway__close(struct gateway *g, gateway_close_cb cb);

/**
 * Asynchronous request to handle a client command.
 *
 * We also use the handle as a place to save request-scoped data that we need
 * to access from a callback.
 */
typedef void (*handle_cb)(struct handle *req,
			  int status,
			  uint8_t type,
			  uint8_t schema);
struct handle {
	/* User data. */
	void *data;
	/* Type code for this request. */
	int type;
	/* Schema version for this request. */
	int schema;
	/* Buffer where the response to this request will be written. */
	struct buffer *buffer;
	/* Cursor for reading the request. */
	struct cursor cursor;
	/* Database ID parsed from this request.
	 *
	 * This is used by handle_prepare. */
	size_t db_id;
	/* Set to true when a cancellation has been requested. */
	bool cancellation_requested;
	/* Set to true when the parameters for the current query have been bound */
	bool parameters_bound;
	/* Tuple decoder for the parameters in this request. */
	struct tuple_decoder decoder;
	/* Callback that will be invoked at the end of request processing to
	 * write the response. */
	handle_cb cb;
	/* A link into thread pool's queues. */
	pool_work_t work;
};

/**
 * Start handling a new client request.
 *
 * At most one request can be outstanding at any given time. This function will
 * return an error if user code calls it and there's already a request in
 * progress.
 *
 * The @type parameter holds the request type code (e.g. #REQUEST_LEADER), and
 * the @buffer parameter is a buffer for writing the response.
 */
int gateway__handle(struct gateway *g,
		    struct handle *req,
		    int type,
		    int schema,
		    struct buffer *buffer,
		    handle_cb cb);

/**
 * Resume execution of a query that was yielding a lot of rows and has been
 * interrupted in order to start sending a first batch of rows. The response
 * write buffer associated with the request must have been reset.
 */
int gateway__resume(struct gateway *g, bool *finished);

#endif /* DQLITE_GATEWAY_H_ */
