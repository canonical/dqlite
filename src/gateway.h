/**
 * Core dqlite server engine, calling out SQLite for serving client requests.
 */

#ifndef DQLITE_GATEWAY_H_
#define DQLITE_GATEWAY_H_

#include <raft.h>

#include "../include/dqlite.h"

#include "./lib/buffer.h"
#include "./lib/serialize.h"

#include "leader.h"
#include "options.h"
#include "registry.h"

struct handle;

/**
 * Handle requests from a single connected client and forward them to
 * SQLite.
 */
struct gateway
{
	struct dqlite_logger *logger; /* Logger to use */
	struct options *options;      /* Configuration options */
	struct registry *registry;    /* Register of existing databases */
	struct raft *raft;	    /* Raft instance */
	struct leader *leader;	/* Leader connection to the database */
	struct handle *req;	   /* Asynchronous request being handled */
};

void gateway__init(struct gateway *g,
		   struct dqlite_logger *logger,
		   struct options *options,
		   struct registry *registry,
		   struct raft *raft);

void gateway__close(struct gateway *g);

/**
 * Asynchronous request to handle a client command.
 */
typedef void (*handle_cb)(struct handle *req, int status, int type);
struct handle
{
	void *data; /* User data */
	struct gateway *gateway;
	struct buffer *buffer;
	handle_cb cb;
};

/**
 * Start handling a new client request.
 *
 * At most one request can be outstanding at any given time. This function will
 * return an error if user code calls it and there's already a request in
 * progress.
 *
 * The @type parameter holds the request type code (e.g. #REQUEST_LEADER), the
 * @cursor parameter holds a cursor for reading the request payload, and the
 * @buffer parameter is a buffer for writing the response.
 */
int gateway__handle(struct gateway *g,
		    struct handle *req,
		    int type,
		    struct cursor *cursor,
		    struct buffer *buffer,
		    handle_cb cb);

#endif /* DQLITE_GATEWAY_H_ */
