#include <assert.h>
#include <float.h>
#include <stdio.h>

#ifdef DQLITE_EXPERIMENTAL

#include <libco.h>

#endif /* DQLITE_EXPERIMENTAL */

#include "../include/dqlite.h"

#include "error.h"
#include "format.h"
#include "fsm.h"
#include "gateway.h"
#include "lifecycle.h"
#include "log.h"
#include "request.h"
#include "response.h"

/* Perform a distributed checkpoint if the size of the WAL has reached the
 * configured threshold and there are no reading transactions in progress (there
 * can't be writing transaction because this helper gets called after a
 * successful commit). */
static int dqlite__gateway_maybe_checkpoint(void *      ctx,
                                            sqlite3 *   db,
                                            const char *schema,
                                            int         pages)
{
	struct dqlite__gateway *g;
	struct sqlite3_file *   file;
	volatile void *         region;
	uint32_t                mx_frame;
	uint32_t                read_marks[DQLITE__FORMAT_WAL_NREADER];
	int                     rc;
	int                     i;

	(void)schema;

	assert(ctx != NULL);
	assert(db != NULL);

	g = ctx;

	/* Check if the size of the WAL is beyond the threshold. */
	if ((unsigned)pages < g->options->checkpoint_threshold) {
		/* Nothing to do yet. */
		return SQLITE_OK;
	}

	/* Get the database file associated with this connection */
	rc = sqlite3_file_control(db, "main", SQLITE_FCNTL_FILE_POINTER, &file);
	assert(rc == SQLITE_OK); /* Should never fail */

	/* Get the first SHM region, which contains the WAL header. */
	rc = file->pMethods->xShmMap(file, 0, 0, 0, &region);
	assert(rc == SQLITE_OK); /* Should never fail */

	/* Get the current value of mxFrame. */
	dqlite__format_get_mx_frame((const uint8_t *)region, &mx_frame);

	/* Get the content of the read marks. */
	dqlite__format_get_read_marks((const uint8_t *)region, read_marks);

	/* Check each mark and associated lock. This logic is similar to the one
	 * in the walCheckpoint function of wal.c, in the SQLite code. */
	for (i = 0; i < SQLITE_SHM_NLOCK; i++) {
		int flags = SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE;

		rc = file->pMethods->xShmLock(file, i, 1, flags);
		if (rc == SQLITE_BUSY) {
			/* It's locked. Let's postpone the checkpoint
			 * for now. */
			return SQLITE_OK;
		}

		/* Not locked. Let's release the lock we just
		 * acquired. */
		flags = SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE;
		file->pMethods->xShmLock(file, i, 1, flags);
	}

	/* Attempt to perfom a checkpoint across all nodes.
	 *
	 * TODO: reason about if it's indeed fine to ignore all kind of
	 * errors. */
	g->cluster->xCheckpoint(g->cluster->ctx, db);

	return SQLITE_OK;
}

/* Release dynamically allocated data attached to a response after it has been
 * flushed. */
static void dqlite__gateway_response_reset(struct dqlite__response *r)
{
	int i;

	/* TODO: we use free() instead of sqlite3_free() below because Go's
	 * C.CString() will allocate strings using malloc. Once we switch to a
	 * pure C implementation, we can use sqlite3_free instead. */
	switch (r->type) {

	case DQLITE_RESPONSE_SERVER:
		assert(r->server.address != NULL);

		free((char *)r->server.address);
		r->server.address = NULL;

		break;

	case DQLITE_RESPONSE_SERVERS:
		assert(r->servers.servers != NULL);

		for (i = 0; r->servers.servers[i].address != NULL; i++) {
			free((char *)r->servers.servers[i].address);
		}

		free(r->servers.servers);
		r->servers.servers = NULL;

		break;
	}
}

/* Render a failure response. */
static void dqlite__gateway_failure(struct dqlite__gateway *    g,
                                    struct dqlite__gateway_ctx *ctx,
                                    int                         code)
{
	ctx->response.type            = DQLITE_RESPONSE_FAILURE;
	ctx->response.failure.code    = code;
	ctx->response.failure.message = g->error;
}

static void dqlite__gateway_leader(struct dqlite__gateway *    g,
                                   struct dqlite__gateway_ctx *ctx)
{
	const char *address;

	address = g->cluster->xLeader(g->cluster->ctx);

	if (address == NULL) {
		dqlite__error_oom(&g->error, "failed to get cluster leader");
		dqlite__gateway_failure(g, ctx, SQLITE_NOMEM);
		return;
	}
	ctx->response.type           = DQLITE_RESPONSE_SERVER;
	ctx->response.server.address = address;
}

static void dqlite__gateway_client(struct dqlite__gateway *    g,
                                   struct dqlite__gateway_ctx *ctx)
{
	/* TODO: handle client registrations */

	ctx->response.type                      = DQLITE_RESPONSE_WELCOME;
	ctx->response.welcome.heartbeat_timeout = g->options->heartbeat_timeout;
}

static void dqlite__gateway_heartbeat(struct dqlite__gateway *    g,
                                      struct dqlite__gateway_ctx *ctx)
{
	int                        rc;
	struct dqlite_server_info *servers;

	/* Get the current list of servers in the cluster */
	rc = g->cluster->xServers(g->cluster->ctx, &servers);
	if (rc != SQLITE_OK) {
		dqlite__error_printf(&g->error,
		                     "failed to get cluster servers");
		dqlite__gateway_failure(g, ctx, rc);
		return;
	}

	assert(servers != NULL);

	ctx->response.type            = DQLITE_RESPONSE_SERVERS;
	ctx->response.servers.servers = servers;

	/* Refresh the heartbeat timestamp. */
	g->heartbeat = ctx->request->timestamp;
}

static void dqlite__gateway_open(struct dqlite__gateway *    g,
                                 struct dqlite__gateway_ctx *ctx)
{
	int rc;

	assert(g != NULL);

	if (g->db != NULL) {
		dqlite__error_printf(
		    &g->error,
		    "a database for this connection is already open");
		dqlite__gateway_failure(g, ctx, SQLITE_BUSY);
		return;
	}

	g->db = sqlite3_malloc(sizeof *g->db);
	if (g->db == NULL) {
		dqlite__error_oom(&g->error, "unable to create database");
		dqlite__gateway_failure(g, ctx, SQLITE_NOMEM);
		return;
	}

	dqlite__db_init(g->db);

	g->db->id = 0;

	rc = dqlite__db_open(g->db,
	                     ctx->request->open.name,
	                     ctx->request->open.flags,
	                     g->options->vfs,
	                     g->options->page_size,
	                     g->options->wal_replication);

	if (rc != 0) {
		dqlite__error_printf(&g->error, g->db->error);
		dqlite__gateway_failure(g, ctx, rc);
		dqlite__db_close(g->db);
		sqlite3_free(g->db);
		g->db = NULL;
		return;
	}

	sqlite3_wal_hook(g->db->db, dqlite__gateway_maybe_checkpoint, g);

	ctx->response.type  = DQLITE_RESPONSE_DB;
	ctx->response.db.id = (uint32_t)g->db->id;

	/* Notify the cluster implementation about the new connection. */
	g->cluster->xRegister(g->cluster->ctx, g->db->db);
	g->db->cluster = g->cluster;
}

/* Ensure that there are no raft logs pending. */
#define DQLITE__GATEWAY_BARRIER                                                \
	rc = g->cluster->xBarrier(g->cluster->ctx);                            \
	if (rc != 0) {                                                         \
		dqlite__error_printf(&g->error, "raft barrier failed");        \
		dqlite__gateway_failure(g, ctx, rc);                           \
		return;                                                        \
	}

/* Lookup the database with the given ID. */
#define DQLITE__GATEWAY_LOOKUP_DB(ID)                                          \
	db = g->db;                                                            \
	if (db == NULL || db->id != ID) {                                      \
		dqlite__error_printf(&g->error, "no db with id %d", ID);       \
		dqlite__gateway_failure(g, ctx, SQLITE_NOTFOUND);              \
		return;                                                        \
	}

/* Lookup the statement with the given ID. */
#define DQLITE__GATEWAY_LOOKUP_STMT(ID)                                        \
	stmt = dqlite__db_stmt(db, ID);                                        \
	if (stmt == NULL) {                                                    \
		dqlite__error_printf(&g->error, "no stmt with id %d", ID);     \
		dqlite__gateway_failure(g, ctx, SQLITE_NOTFOUND);              \
		return;                                                        \
	}

static void dqlite__gateway_prepare(struct dqlite__gateway *    g,
                                    struct dqlite__gateway_ctx *ctx)
{
	struct dqlite__db *  db;
	struct dqlite__stmt *stmt;
	int                  rc;

	DQLITE__GATEWAY_BARRIER;
	DQLITE__GATEWAY_LOOKUP_DB(ctx->request->prepare.db_id);

	rc = dqlite__db_prepare(db, ctx->request->prepare.sql, &stmt);
	if (rc != SQLITE_OK) {
		dqlite__error_printf(&g->error, db->error);
		dqlite__gateway_failure(g, ctx, rc);
		return;
	}

	ctx->response.type        = DQLITE_RESPONSE_STMT;
	ctx->response.stmt.db_id  = ctx->request->prepare.db_id;
	ctx->response.stmt.id     = stmt->id;
	ctx->response.stmt.params = sqlite3_bind_parameter_count(stmt->stmt);
}

static void dqlite__gateway_exec(struct dqlite__gateway *    g,
                                 struct dqlite__gateway_ctx *ctx)
{
	int                  rc;
	struct dqlite__db *  db;
	struct dqlite__stmt *stmt;
	uint64_t             last_insert_id;
	uint64_t             rows_affected;

	DQLITE__GATEWAY_BARRIER;
	DQLITE__GATEWAY_LOOKUP_DB(ctx->request->exec.db_id);
	DQLITE__GATEWAY_LOOKUP_STMT(ctx->request->exec.stmt_id);

	assert(stmt != NULL);

	rc = dqlite__stmt_bind(stmt, &ctx->request->message);
	if (rc != SQLITE_OK) {
		dqlite__error_printf(&g->error, stmt->error);
		dqlite__gateway_failure(g, ctx, rc);
		return;
	}

	rc = dqlite__stmt_exec(stmt, &last_insert_id, &rows_affected);
	if (rc == SQLITE_OK) {
		ctx->response.type                  = DQLITE_RESPONSE_RESULT;
		ctx->response.result.last_insert_id = last_insert_id;
		ctx->response.result.rows_affected  = rows_affected;
	} else {
		dqlite__error_printf(&g->error, stmt->error);
		dqlite__gateway_failure(g, ctx, rc);
		sqlite3_reset(stmt->stmt);
	}
}

/* Step through the tiven statement and populate the response of the given
 * context with a single batch of rows.
 *
 * A single batch of rows is typically about the size of the static response
 * message body. */
static void dqlite__gateway_query_batch(struct dqlite__gateway *    g,
                                        struct dqlite__db *         db,
                                        struct dqlite__stmt *       stmt,
                                        struct dqlite__gateway_ctx *ctx)
{
	int rc;

	rc = dqlite__stmt_query(stmt, &ctx->response.message);
	if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
		sqlite3_reset(stmt->stmt);

		/* Finalize the statement if needed. */
		if (ctx->cleanup == DQLITE__GATEWAY_CLEANUP_FINALIZE) {
			dqlite__db_finalize(db, stmt);
		}

		/* TODO: reset what was written in the message */
		dqlite__error_printf(&g->error, stmt->error);
		dqlite__gateway_failure(g, ctx, rc);

		ctx->db      = NULL;
		ctx->stmt    = NULL;
		ctx->cleanup = DQLITE__GATEWAY_CLEANUP_NONE;
	} else {
		ctx->response.type = DQLITE_RESPONSE_ROWS;
		assert(rc == SQLITE_ROW || rc == SQLITE_DONE);
		if (rc == SQLITE_ROW) {
			ctx->response.rows.eof = DQLITE_RESPONSE_ROWS_PART;
			ctx->db                = db;
			ctx->stmt              = stmt;
		} else {
			/* Finalize the statement if needed. */
			if (ctx->cleanup == DQLITE__GATEWAY_CLEANUP_FINALIZE) {
				dqlite__db_finalize(db, stmt);
			}

			/* Reset the multi-response info and the cleanup code */
			ctx->db      = NULL;
			ctx->stmt    = NULL;
			ctx->cleanup = DQLITE__GATEWAY_CLEANUP_NONE;

			ctx->response.rows.eof = DQLITE_RESPONSE_ROWS_DONE;
		}
	}
}

static void dqlite__gateway_query(struct dqlite__gateway *    g,
                                  struct dqlite__gateway_ctx *ctx)
{
	int                  rc;
	struct dqlite__db *  db;
	struct dqlite__stmt *stmt;

	DQLITE__GATEWAY_BARRIER;
	DQLITE__GATEWAY_LOOKUP_DB(ctx->request->query.db_id);
	DQLITE__GATEWAY_LOOKUP_STMT(ctx->request->query.stmt_id);

	assert(stmt != NULL);

	rc = dqlite__stmt_bind(stmt, &ctx->request->message);
	if (rc != SQLITE_OK) {
		dqlite__error_printf(&g->error, stmt->error);
		dqlite__gateway_failure(g, ctx, rc);
		return;
	}

	/* Set the cleanup code to none, since there's nothing to do
	 * once the request is done. */
	ctx->cleanup = DQLITE__GATEWAY_CLEANUP_NONE;

	dqlite__gateway_query_batch(g, db, stmt, ctx);
}

static void dqlite__gateway_finalize(struct dqlite__gateway *    g,
                                     struct dqlite__gateway_ctx *ctx)
{
	int                  rc;
	struct dqlite__db *  db;
	struct dqlite__stmt *stmt;

	DQLITE__GATEWAY_BARRIER;
	DQLITE__GATEWAY_LOOKUP_DB(ctx->request->finalize.db_id);
	DQLITE__GATEWAY_LOOKUP_STMT(ctx->request->finalize.stmt_id);

	rc = dqlite__db_finalize(db, stmt);
	if (rc == SQLITE_OK) {
		ctx->response.type = DQLITE_RESPONSE_EMPTY;
	} else {
		dqlite__error_printf(&g->error, db->error);
		dqlite__gateway_failure(g, ctx, rc);
	}
}

static void dqlite__gateway_exec_sql(struct dqlite__gateway *    g,
                                     struct dqlite__gateway_ctx *ctx)
{
	int                  rc;
	struct dqlite__db *  db;
	const char *         sql;
	struct dqlite__stmt *stmt = NULL;
	uint64_t             last_insert_id;
	uint64_t             rows_affected;

	DQLITE__GATEWAY_BARRIER;
	DQLITE__GATEWAY_LOOKUP_DB(ctx->request->exec_sql.db_id);

	assert(db != NULL);

	sql = ctx->request->exec_sql.sql;

	while (sql != NULL && strcmp(sql, "") != 0) {
		rc = dqlite__db_prepare(db, sql, &stmt);
		if (rc != SQLITE_OK) {
			dqlite__error_printf(&g->error, db->error);
			dqlite__gateway_failure(g, ctx, rc);
			return;
		}

		if (stmt->stmt == NULL) {
			return;
		}

		/* TODO: what about bindings for multi-statement SQL text? */
		rc = dqlite__stmt_bind(stmt, &ctx->request->message);
		if (rc != SQLITE_OK) {
			dqlite__error_printf(&g->error, stmt->error);
			dqlite__gateway_failure(g, ctx, rc);
			goto err;
			return;
		}

		rc = dqlite__stmt_exec(stmt, &last_insert_id, &rows_affected);
		if (rc == SQLITE_OK) {
			ctx->response.type = DQLITE_RESPONSE_RESULT;
			ctx->response.result.last_insert_id = last_insert_id;
			ctx->response.result.rows_affected  = rows_affected;
		} else {
			dqlite__error_printf(&g->error, stmt->error);
			dqlite__gateway_failure(g, ctx, rc);
			goto err;
		}

		/* Ignore errors here. TODO: can this fail? */
		dqlite__db_finalize(db, stmt);

		sql = stmt->tail;
	}

	return;

err:
	/* Ignore errors here. TODO: emit a warning instead */
	dqlite__db_finalize(db, stmt);
}

static void dqlite__gateway_query_sql(struct dqlite__gateway *    g,
                                      struct dqlite__gateway_ctx *ctx)
{
	int                  rc;
	struct dqlite__db *  db;
	struct dqlite__stmt *stmt;

	DQLITE__GATEWAY_BARRIER;
	DQLITE__GATEWAY_LOOKUP_DB(ctx->request->query_sql.db_id);

	assert(db != NULL);

	rc = dqlite__db_prepare(db, ctx->request->query_sql.sql, &stmt);
	if (rc != SQLITE_OK) {
		dqlite__error_printf(&g->error, db->error);
		dqlite__gateway_failure(g, ctx, rc);
		return;
	}

	rc = dqlite__stmt_bind(stmt, &ctx->request->message);
	if (rc != SQLITE_OK) {
		dqlite__error_printf(&g->error, stmt->error);
		dqlite__gateway_failure(g, ctx, rc);
		return;
	}

	/* When the request is completed, the statement needs to be
	 * finalized. */
	ctx->cleanup = DQLITE__GATEWAY_CLEANUP_FINALIZE;

	dqlite__gateway_query_batch(g, db, stmt, ctx);
}

static void dqlite__gateway_interrupt(struct dqlite__gateway *    g,
                                      struct dqlite__gateway_ctx *ctx)
{
	assert(g != NULL);
	assert(ctx != NULL);

	/* If there's no ongoing database request there's really nothing to
	 * do. */
	if (g->ctxs[0].request == NULL) {
		goto out;
	}

	assert(g->ctxs[0].cleanup == DQLITE__GATEWAY_CLEANUP_NONE ||
	       g->ctxs[0].cleanup == DQLITE__GATEWAY_CLEANUP_FINALIZE);

	/* Take appropriate action depending on the cleanup code. */
	switch (g->ctxs[0].cleanup) {
	case DQLITE__GATEWAY_CLEANUP_NONE:
		/* Nothing to do */
		break;
	case DQLITE__GATEWAY_CLEANUP_FINALIZE:
		/* Finalize the statempt */
		dqlite__db_finalize(g->ctxs[0].db, g->ctxs[0].stmt);
		break;
	}

out:
	g->ctxs[0].request = NULL;
	g->ctxs[0].db      = NULL;
	g->ctxs[0].stmt    = NULL;
	g->ctxs[0].cleanup = DQLITE__GATEWAY_CLEANUP_NONE;

	ctx->response.type = DQLITE_RESPONSE_EMPTY;
}

/* Dispatch a request to the appropriate request handler. */
static void dqlite__gateway_dispatch(struct dqlite__gateway *    g,
                                     struct dqlite__gateway_ctx *ctx)
{
	switch (ctx->request->type) {

#define DQLITE__GATEWAY_HANDLE(CODE, STRUCT, NAME, _)                          \
	case CODE:                                                             \
		dqlite__gateway_##NAME(g, ctx);                                \
		break;

		DQLITE__REQUEST_SCHEMA_TYPES(DQLITE__GATEWAY_HANDLE, );

	default:
		dqlite__error_printf(
		    &g->error, "invalid request type %d", ctx->request->type);
		dqlite__gateway_failure(g, ctx, SQLITE_ERROR);
		break;
	}

	g->callbacks.xFlush(g->callbacks.ctx, &ctx->response);
}

#ifdef DQLITE_EXPERIMENTAL

/* Use a global variable to pass a gateway object as argument of the main loop
 * coroutine entry point. */
static struct dqlite__gateway *dqlite__gateway_loop_arg;

static void dqlite__gateway_loop()
{
	struct dqlite__gateway *g = dqlite__gateway_loop_arg;

	/* Pass control back to the main coroutine, as all we need to do
	 * initially is to set the gateway local variable above */
	co_switch(g->main_coroutine);

	while (1) {
		struct dqlite__gateway_ctx *ctx = &g->ctxs[0];

		assert(ctx->request != NULL);

		dqlite__gateway_dispatch(g, ctx);

		co_switch(g->main_coroutine);
	}
}

#endif /* DQLITE_EXPERIMENTAL */

void dqlite__gateway_init(struct dqlite__gateway *    g,
                          struct dqlite__gateway_cbs *callbacks,
                          struct dqlite_cluster *     cluster,
                          struct dqlite_logger *      logger,
                          struct dqlite__options *    options)
{
	int i;

	assert(g != NULL);
	assert(cluster != NULL);
	assert(logger != NULL);
	assert(options != NULL);
	assert(callbacks != NULL);
	assert(callbacks->xFlush != NULL);

	dqlite__lifecycle_init(DQLITE__LIFECYCLE_GATEWAY);

	g->client_id = 0;

	dqlite__error_init(&g->error);

	/* Make a copy of the callbacks passed as argument. */
	memcpy(&g->callbacks, callbacks, sizeof *callbacks);

	g->cluster = cluster;
	g->logger  = logger;
	g->options = options;

	/* Reset all request contexts in the buffer */
	for (i = 0; i < DQLITE__GATEWAY_MAX_REQUESTS; i++) {
		g->ctxs[i].request = NULL;
		g->ctxs[i].db      = NULL;
		g->ctxs[i].stmt    = NULL;
		g->ctxs[i].cleanup = DQLITE__GATEWAY_CLEANUP_NONE;
		dqlite__response_init(&g->ctxs[i].response);
	}

	g->db = NULL;

#ifdef DQLITE_EXPERIMENTAL
	g->main_coroutine = NULL;
	g->loop_coroutine = NULL;
#endif /* DQLITE_EXPERIMENTAL */
}

#ifdef DQLITE_EXPERIMENTAL

int dqlite__gateway_start(struct dqlite__gateway *g, uint64_t now)
{
	g->heartbeat      = now;
	g->main_coroutine = co_active();
	g->loop_coroutine =
	    co_create(1024 * 1024 * sizeof(void *), dqlite__gateway_loop);

	if (g->loop_coroutine == NULL) {
		return DQLITE_NOMEM;
	}

	dqlite__gateway_loop_arg = g;

	/* Kick off the gateway loop coroutine, which will initialize itself by
	 * saving a reference to this gateway object and then immediately switch
	 * back here. */
	co_switch(g->loop_coroutine);

	return 0;
}

#endif /* DQLITE_EXPERIMENTAL */

void dqlite__gateway_close(struct dqlite__gateway *g)
{
	int i;

	assert(g != NULL);

	if (g->db != NULL) {
		dqlite__db_close(g->db);
		sqlite3_free(g->db);
	}

#ifdef DQLITE_EXPERIMENTAL

	if (g->loop_coroutine != NULL) {
		co_delete(g->loop_coroutine);
	}

#endif /* DQLITE_EXPERIMENTAL */

	for (i = 0; i < DQLITE__GATEWAY_MAX_REQUESTS; i++) {
		dqlite__response_close(&g->ctxs[i].response);
	}

	dqlite__error_close(&g->error);

	dqlite__lifecycle_close(DQLITE__LIFECYCLE_GATEWAY);
}

int dqlite__gateway_ctx_for(struct dqlite__gateway *g, int type)
{
	int idx;
	assert(g != NULL);

	/* The first slot is reserved for database requests, and the second for
	 * control ones. A control request can be served concurrently with a
	 * database request, but not the other way round. */
	switch (type) {
	case DQLITE_REQUEST_HEARTBEAT:
	case DQLITE_REQUEST_INTERRUPT:
		idx = 1;
		break;
	default:
		if (g->ctxs[1].request != NULL) {
			return -1;
		}
		idx = 0;
		break;
	}

	if (g->ctxs[idx].request == NULL) {
		return idx;
	}

	return -1;
}

int dqlite__gateway_handle(struct dqlite__gateway *g,
                           struct dqlite__request *request)
{
	struct dqlite__gateway_ctx *ctx;
	int                         i;
	int                         err;

	assert(g != NULL);
	assert(request != NULL);

	/* Abort if we can't accept the request at this time */
	i = dqlite__gateway_ctx_for(g, request->type);
	if (i == -1) {
		dqlite__error_printf(&g->error,
		                     "concurrent request limit exceeded");
		err = DQLITE_PROTO;
		goto err;
	}

	/* Save the request in the context object. */
	ctx          = &g->ctxs[i];
	ctx->request = request;

	if (i == 1) {
		/* Heartbeat and interrupt requests are handled synchronously.
		 */
		dqlite__gateway_dispatch(g, ctx);
	} else {

#ifdef DQLITE_EXPERIMENTAL
		/* Database requests are handled asynchronously by the gateway
		 * coroutine. */
		co_switch(g->loop_coroutine);
#else
		dqlite__gateway_dispatch(g, ctx);
#endif /* DQLITE_EXPERIMENTAL */
	}

	return 0;
err:
	assert(err != 0);

	return err;
}

/* Resume stepping through a query and send a new follow-up response with more
 * rows. */
static void dqlite__gateway_query_resume(struct dqlite__gateway *    g,
                                         struct dqlite__gateway_ctx *ctx)
{
	assert(ctx->db != NULL);
	assert(ctx->stmt != NULL);

	dqlite__gateway_query_batch(g, ctx->db, ctx->stmt, ctx);

	/* Notify user code that a response is available. */
	g->callbacks.xFlush(g->callbacks.ctx, &ctx->response);
}

void dqlite__gateway_flushed(struct dqlite__gateway * g,
                             struct dqlite__response *response)
{
	int i;

	assert(g != NULL);
	assert(response != NULL);

	/* Reset the request context associated with this response */
	for (i = 0; i < DQLITE__GATEWAY_MAX_REQUESTS; i++) {
		struct dqlite__gateway_ctx *ctx = &g->ctxs[i];
		if (&ctx->response == response) {
			dqlite__gateway_response_reset(response);
			if (ctx->stmt != NULL) {
				dqlite__gateway_query_resume(g, ctx);
			} else {
				ctx->request = NULL;
			}
			break;
		}
	}

	/* Assert that an associated request was indeed found */
	assert(i < DQLITE__GATEWAY_MAX_REQUESTS);
}

void dqlite__gateway_aborted(struct dqlite__gateway * g,
                             struct dqlite__response *response)
{
	assert(g != NULL);
	assert(response != NULL);
}
