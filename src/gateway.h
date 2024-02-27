/**
 * Core dqlite server engine, calling out SQLite for serving client requests.
 */

#ifndef DQLITE_GATEWAY_H_
#define DQLITE_GATEWAY_H_

#include "../include/dqlite.h"

#include "lib/buffer.h"
#include "lib/serialize.h"

#include "config.h"
#include "id.h"
#include "leader.h"
#include "raft.h"
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
	struct exec exec;            /* Low-level exec async request */
	struct stmt__registry stmts; /* Registry of prepared statements */
	struct barrier barrier;      /* Barrier for query requests */
	uint64_t protocol;           /* Protocol format version */
	uint64_t client_id;
	struct id_state random_state; /* For generating IDs */
};

void gateway__init(struct gateway *g,
		   struct config *config,
		   struct registry *registry,
		   struct raft *raft,
		   struct id_state seed);

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
typedef void (*handle_cb)(struct handle *req,
			  int status,
			  uint8_t type,
			  uint8_t schema);
struct handle
{
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
	/* ID of the statement associated with this request.
	 *
	 * This is used by handle_prepare. */
	size_t stmt_id;
	/* SQL string associated with this request.
	 *
	 * This is used by handle_prepare, handle_query_sql, and handle_exec_sql
	 * to save the provided SQL string across calls to leader__barrier and
	 * leader__exec, since there's no prepared statement that can be saved
	 * instead. In the case of handle_exec_sql, after preparing each
	 * statement we update this field to point to the "tail" that has not
	 * been prepared yet. */
	const char *sql;
	/* Prepared statement that will be queried to process this request.
	 *
	 * This is used by handle_query and handle_query_sql. */
	sqlite3_stmt *stmt;
	/* Number of times a statement parsed from this request has been
	 * executed.
	 *
	 * This is used by handle_exec_sql, which parses zero or more statements
	 * from the provided SQL string and executes them successively. Only if
	 * at least one statement was executed should we fill the RESULT
	 * response using sqlite3_last_insert_rowid and sqlite3_changes. */
	unsigned exec_count;
	/* Callback that will be invoked at the end of request processing to
	 * write the response. */
	handle_cb cb;
	/* TP_TODO! */
	pool_work_t work;
	int rc;
	struct gateway *gw;
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
