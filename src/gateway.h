/**
 * Core dqlite server engine, calling out SQLite for serving client requests.
 */

#ifndef DQLITE_GATEWAY_H_
#define DQLITE_GATEWAY_H_

#include <raft.h>

#include "../include/dqlite.h"

#include "lib/buffer.h"
#include "lib/serialize.h"

#include "config.h"
#include "leader.h"
#include "registry.h"
#include "stmt.h"

struct handle;

/**
 * Handle requests from a single connected client and forward them to
 * SQLite.
 */
struct gateway
{
	struct config *config;       /* Configuration */
	struct registry *registry;   /* Register of existing databases */
	struct raft *raft;           /* Raft instance */
	struct leader *leader;       /* Leader connection to the database */
	struct handle *req;          /* Asynchronous request being handled */
	sqlite3_stmt *stmt;          /* Statement being processed */
	bool stmt_finalize;          /* Whether to finalize the statement */
	struct exec exec;            /* Low-level exec async request */
	/* FIXME store this in the req */
	const char *sql;             /* SQL query for exec_sql requests */
	struct stmt__registry stmts; /* Registry of prepared statements */
	struct barrier barrier;      /* Barrier for query requests */
	uint64_t protocol;           /* Protocol format version */
};

void gateway__init(struct gateway *g,
		   struct config *config,
		   struct registry *registry,
		   struct raft *raft);

void gateway__close(struct gateway *g);

/**
 * Closes the leader connection to the database, reason should contain a raft
 * error code.
 */
void gateway__leader_close(struct gateway *g, int reason);

/**
 * Asynchronous request to handle a client command.
 *
 * We also use the handle as a place to save request-scoped data that we need
 * to access from a callback.
 */
typedef void (*handle_cb)(struct handle *req, int status, int type);
struct handle
{
	void *data;              /* User data */
	int type;                /* Request type */
	int schema;              /* Request schema version */
	struct gateway *gateway;
	struct buffer *buffer;
	struct cursor cursor;
	size_t db_id;            /* For use by prepare callback */
	size_t stmt_id;          /* For use by prepare callback */
	handle_cb cb;
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
