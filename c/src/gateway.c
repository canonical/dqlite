#include <assert.h>
#include <float.h>
#include <stdio.h>

#include "../include/dqlite.h"

#include "error.h"
#include "format.h"
#include "fsm.h"
#include "gateway.h"
#include "lifecycle.h"
#include "log.h"
#include "request.h"
#include "response.h"
#include "vfs.h"

/* Render a failure response. */
static void dqlite__gateway_failure(struct dqlite__gateway *    g,
                                    struct dqlite__gateway_ctx *ctx,
                                    int                         code) {
	ctx->response.type            = DQLITE_RESPONSE_FAILURE;
	ctx->response.failure.code    = code;
	ctx->response.failure.message = g->error;
}

static void dqlite__gateway_leader(struct dqlite__gateway *    g,
                                   struct dqlite__gateway_ctx *ctx) {
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
                                   struct dqlite__gateway_ctx *ctx) {
	/* TODO: handle client registrations */

	ctx->response.type                      = DQLITE_RESPONSE_WELCOME;
	ctx->response.welcome.heartbeat_timeout = g->options->heartbeat_timeout;
}

static void dqlite__gateway_heartbeat(struct dqlite__gateway *    g,
                                      struct dqlite__gateway_ctx *ctx) {
	int                        rc;
	struct dqlite_server_info *servers;

	/* Get the current list of servers in the cluster */
	rc = g->cluster->xServers(g->cluster->ctx, &servers);
	if (rc != SQLITE_OK) {
		dqlite__error_printf(&g->error, "failed to get cluster servers");
		dqlite__gateway_failure(g, ctx, rc);
		return;
	}

	assert(servers != NULL);

	/* Encode the response */
	ctx->response.type            = DQLITE_RESPONSE_SERVERS;
	ctx->response.servers.servers = servers;

	/* Refresh the heartbeat timestamp. */
	g->heartbeat = ctx->request->timestamp;
}

static void dqlite__gateway_open(struct dqlite__gateway *    g,
                                 struct dqlite__gateway_ctx *ctx) {
	int                err;
	int                rc;
	const char *       replication;
	struct dqlite__db *db;

	err = dqlite__db_registry_add(&g->dbs, &db);
	if (err != 0) {
		assert(err == DQLITE_NOMEM);
		dqlite__error_oom(&g->error, "unable to register database");
		dqlite__gateway_failure(g, ctx, SQLITE_NOMEM);
		return;
	}

	assert(db != NULL);

	replication = g->cluster->xReplication(g->cluster->ctx);

	assert(replication != NULL);

	rc = dqlite__db_open(db,
	                     ctx->request->open.name,
	                     ctx->request->open.flags,
	                     replication,
	                     g->options->page_size);

	if (rc != 0) {
		dqlite__error_printf(&g->error, db->error);
		dqlite__gateway_failure(g, ctx, rc);
		dqlite__db_registry_del(&g->dbs, db);
		return;
	}

	ctx->response.type  = DQLITE_RESPONSE_DB;
	ctx->response.db.id = (uint32_t)db->id;

	/* Notify the cluster implementation about the new connection. */
	g->cluster->xRegister(g->cluster->ctx, db->db);
}

/* Ensure that there are no raft logs pending. */
#define DQLITE__GATEWAY_BARRIER                                                     \
	rc = g->cluster->xBarrier(g->cluster->ctx);                                 \
	if (rc != 0) {                                                              \
		dqlite__error_printf(&g->error, "raft barrier failed");             \
		dqlite__gateway_failure(g, ctx, rc);                                \
		return;                                                             \
	}

/* Lookup the database with the given ID. */
#define DQLITE__GATEWAY_LOOKUP_DB(ID)                                               \
	db = dqlite__db_registry_get(&g->dbs, ID);                                  \
	if (db == NULL) {                                                           \
		dqlite__error_printf(&g->error, "no db with id %d", ID);            \
		dqlite__gateway_failure(g, ctx, SQLITE_NOTFOUND);                   \
		return;                                                             \
	}

/* Lookup the statement with the given ID. */
#define DQLITE__GATEWAY_LOOKUP_STMT(ID)                                             \
	stmt = dqlite__db_stmt(db, ID);                                             \
	if (stmt == NULL) {                                                         \
		dqlite__error_printf(&g->error, "no stmt with id %d", ID);          \
		dqlite__gateway_failure(g, ctx, SQLITE_NOTFOUND);                   \
		return;                                                             \
	}

/* Check that there's an in progress transaction. */
#define DQLITE__GATEWAY_CHECK_DB_IN_A_TX                                            \
	if (!db->in_a_tx) {                                                         \
		dqlite__error_printf(&g->error, "no transaction in progress");      \
		dqlite__gateway_failure(g, ctx, SQLITE_ERROR);                      \
		return;                                                             \
	}

static void dqlite__gateway_prepare(struct dqlite__gateway *    g,
                                    struct dqlite__gateway_ctx *ctx) {
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
                                 struct dqlite__gateway_ctx *ctx) {
	int                  rc;
	struct dqlite__db *  db;
	struct dqlite__stmt *stmt;
	uint64_t             last_insert_id;
	uint64_t             rows_affected;

	DQLITE__GATEWAY_BARRIER;
	DQLITE__GATEWAY_LOOKUP_DB(ctx->request->exec.db_id);
	DQLITE__GATEWAY_LOOKUP_STMT(ctx->request->exec.stmt_id);
	DQLITE__GATEWAY_CHECK_DB_IN_A_TX;

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
	}
}

static void dqlite__gateway_query(struct dqlite__gateway *    g,
                                  struct dqlite__gateway_ctx *ctx) {
	int                  rc;
	struct dqlite__db *  db;
	struct dqlite__stmt *stmt;

	DQLITE__GATEWAY_BARRIER;
	DQLITE__GATEWAY_LOOKUP_DB(ctx->request->query.db_id);
	DQLITE__GATEWAY_LOOKUP_STMT(ctx->request->query.stmt_id);
	DQLITE__GATEWAY_CHECK_DB_IN_A_TX;

	assert(stmt != NULL);

	rc = dqlite__stmt_bind(stmt, &ctx->request->message);
	if (rc != SQLITE_OK) {
		dqlite__error_printf(&g->error, stmt->error);
		dqlite__gateway_failure(g, ctx, rc);
		return;
	}

	rc = dqlite__stmt_query(stmt, &ctx->response.message);
	if (rc != SQLITE_DONE) {
		/* TODO: reset what was written in the message */
		dqlite__error_printf(&g->error, stmt->error);
		dqlite__gateway_failure(g, ctx, rc);
	} else {
		ctx->response.type     = DQLITE_RESPONSE_ROWS;
		ctx->response.rows.eof = DQLITE_RESPONSE_ROWS_EOF;
	}
}

static void dqlite__gateway_finalize(struct dqlite__gateway *    g,
                                     struct dqlite__gateway_ctx *ctx) {
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
                                     struct dqlite__gateway_ctx *ctx) {
	int                  rc;
	struct dqlite__db *  db;
	const char *         sql;
	struct dqlite__stmt *stmt = NULL;
	uint64_t             last_insert_id;
	uint64_t             rows_affected;

	DQLITE__GATEWAY_BARRIER;
	DQLITE__GATEWAY_LOOKUP_DB(ctx->request->exec_sql.db_id);
	DQLITE__GATEWAY_CHECK_DB_IN_A_TX;

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
			goto out;
		}

		/* TODO: what about bindings for multi-statement SQL text? */
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
			goto out;
		}

		sql = stmt->tail;
	}

out:
	/* Ignore errors here. TODO: emit a warning instead */
	dqlite__db_finalize(db, stmt);
}

static void dqlite__gateway_query_sql(struct dqlite__gateway *    g,
                                      struct dqlite__gateway_ctx *ctx) {
	int                  rc;
	struct dqlite__db *  db;
	struct dqlite__stmt *stmt;

	DQLITE__GATEWAY_BARRIER;
	DQLITE__GATEWAY_LOOKUP_DB(ctx->request->query_sql.db_id);
	DQLITE__GATEWAY_CHECK_DB_IN_A_TX;

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

	rc = dqlite__stmt_query(stmt, &ctx->response.message);
	if (rc != SQLITE_DONE) {
		/* TODO: reset what was written in the message */
		dqlite__error_printf(&g->error, stmt->error);
		dqlite__gateway_failure(g, ctx, rc);
	} else {
		ctx->response.type     = DQLITE_RESPONSE_ROWS;
		ctx->response.rows.eof = DQLITE_RESPONSE_ROWS_EOF;
	}
}

static void dqlite__gateway_begin(struct dqlite__gateway *    g,
                                  struct dqlite__gateway_ctx *ctx) {
	int                rc;
	struct dqlite__db *db;

	DQLITE__GATEWAY_BARRIER;
	DQLITE__GATEWAY_LOOKUP_DB(ctx->request->begin.db_id);

	assert(db != NULL);

	rc = dqlite__db_begin(db);

	if (rc != SQLITE_OK) {
		dqlite__error_printf(&g->error, db->error);
		dqlite__gateway_failure(g, ctx, rc);
	} else {
		ctx->response.type = DQLITE_RESPONSE_EMPTY;
	}
}

/* Perform a distributed checkpoint if the size of the WAL has reached the
 * configured threshold and there are no reading transactions in progress (there
 * can't be writing transaction because this helper gets called after a
 * successful commit). */
static void dqlite__gateway_maybe_checkpoint(struct dqlite__gateway *g,
                                             struct dqlite__db *     db) {
	struct sqlite3_file *file;
	volatile void *      region;
	uint32_t             mx_frame;
	uint32_t             read_marks[DQLITE__FORMAT_WAL_NREADER];
	sqlite_int64         size;
	unsigned             pages;
	int                  rc;
	int                  i;

	assert(g != NULL);
	assert(db != NULL);

	/* Get the WAL file for this connection */
	rc = sqlite3_file_control(
	    db->db, "main", SQLITE_FCNTL_JOURNAL_POINTER, &file);
	assert(rc == SQLITE_OK); /* Should never fail */

	rc = file->pMethods->xFileSize(file, &size);
	assert(rc == SQLITE_OK); /* Should never fail */

	/* Check if the size of the WAL is beyond the threshold. */
	pages = dqlite__format_wal_calc_pages(g->options->page_size, size);
	if (pages < g->options->checkpoint_threshold) {
		/* Nothing to do yet. */
		return;
	}

	/* Get the database file associated with this connection */
	rc = sqlite3_file_control(db->db, "main", SQLITE_FCNTL_FILE_POINTER, &file);
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
	for (i = 1; i < DQLITE__FORMAT_WAL_NREADER; i++) {
		if (mx_frame > read_marks[i]) {
			/* This read mark is set, let's check if it's also
			 * locked. */
			int flags = SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE;

			rc = file->pMethods->xShmLock(file, i, 1, flags);
			if (rc == SQLITE_BUSY) {
				/* It's locked. Let's postpone the checkpoint
				 * for now. */
				return;
			}

			/* Not locked. Let's release the lock we just
			 * acquired. */
			flags = SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE;
			file->pMethods->xShmLock(file, i, 1, flags);
		}
	}

	/* Attempt to perfom a checkpoint across all nodes.
	 *
	 * TODO: reason about if it's indeed fine to ignore all kind of
	 * errors. */
	g->cluster->xCheckpoint(g->cluster->ctx, db->db);
}

static void dqlite__gateway_commit(struct dqlite__gateway *    g,
                                   struct dqlite__gateway_ctx *ctx) {
	int                rc;
	struct dqlite__db *db;

	DQLITE__GATEWAY_BARRIER;
	DQLITE__GATEWAY_LOOKUP_DB(ctx->request->commit.db_id);

	assert(db != NULL);

	rc = dqlite__db_commit(db);

	if (rc != SQLITE_OK) {
		dqlite__error_printf(&g->error, db->error);
		dqlite__gateway_failure(g, ctx, rc);
	} else {
		dqlite__gateway_maybe_checkpoint(g, db);
		ctx->response.type = DQLITE_RESPONSE_EMPTY;
	}
}

static void dqlite__gateway_rollback(struct dqlite__gateway *    g,
                                     struct dqlite__gateway_ctx *ctx) {
	int                rc;
	struct dqlite__db *db;

	DQLITE__GATEWAY_BARRIER;
	DQLITE__GATEWAY_LOOKUP_DB(ctx->request->rollback.db_id);

	assert(db != NULL);

	rc = dqlite__db_rollback(db);

	if (rc != SQLITE_OK) {
		dqlite__error_printf(&g->error, db->error);
		dqlite__gateway_failure(g, ctx, rc);
	} else {
		ctx->response.type = DQLITE_RESPONSE_EMPTY;
	}
}

void dqlite__gateway_init(struct dqlite__gateway *g,
                          struct dqlite_cluster * cluster,
                          struct dqlite__options *options) {
	int i;

	assert(g != NULL);
	assert(cluster != NULL);
	assert(options != NULL);

	dqlite__lifecycle_init(DQLITE__LIFECYCLE_GATEWAY);

	g->client_id = 0;

	dqlite__error_init(&g->error);

	g->cluster = cluster;
	g->options = options;

	/* Reset all request contexts in the buffer */
	for (i = 0; i < DQLITE__GATEWAY_MAX_REQUESTS; i++) {
		g->ctxs[i].request = NULL;
		dqlite__response_init(&g->ctxs[i].response);
	}

	dqlite__db_registry_init(&g->dbs);
}

void dqlite__gateway_close(struct dqlite__gateway *g) {
	int i;

	assert(g != NULL);

	dqlite__db_registry_close(&g->dbs);

	for (i = 0; i < DQLITE__GATEWAY_MAX_REQUESTS; i++) {
		dqlite__response_close(&g->ctxs[i].response);
	}

	dqlite__error_close(&g->error);

	dqlite__lifecycle_close(DQLITE__LIFECYCLE_GATEWAY);
}

int dqlite__gateway_handle(struct dqlite__gateway *  g,
                           struct dqlite__request *  request,
                           struct dqlite__response **response) {
	int                         err;
	int                         i;
	struct dqlite__gateway_ctx *ctx;

	assert(g != NULL);
	assert(request != NULL);
	assert(response != NULL);

	/* Look for an available request context buffer */
	for (i = 0; i < DQLITE__GATEWAY_MAX_REQUESTS; i++) {
		if (g->ctxs[i].request == NULL)
			break;
	}

	/* Abort if we reached the maximum number of concurrent requests */
	if (i == DQLITE__GATEWAY_MAX_REQUESTS) {
		dqlite__error_printf(&g->error, "concurrent request limit exceeded");
		err = DQLITE_PROTO;
		goto err;
	}

	ctx          = &g->ctxs[i];
	ctx->request = request;

	switch (request->type) {

#define DQLITE__GATEWAY_HANDLE(CODE, STRUCT, NAME, _)                               \
	case CODE:                                                                  \
		dqlite__gateway_##NAME(g, ctx);                                     \
		break;

		DQLITE__REQUEST_SCHEMA_TYPES(DQLITE__GATEWAY_HANDLE, );

	default:
		dqlite__error_printf(
		    &g->error, "invalid request type %d", request->type);
		dqlite__gateway_failure(g, ctx, SQLITE_ERROR);
		break;
	}

	*response = &ctx->response;

	return 0;

err:
	assert(err != 0);

	*response = 0;

	return err;
}

int dqlite__gateway_continue(struct dqlite__gateway * g,
                             struct dqlite__response *response) {
	assert(g != NULL);
	assert(response != NULL);

	return 0;
}

void dqlite__gateway_finish(struct dqlite__gateway * g,
                            struct dqlite__response *response) {
	int i;

	assert(g != NULL);
	assert(response != NULL);

	/* Reset the request associated with this response */
	for (i = 0; i < DQLITE__GATEWAY_MAX_REQUESTS; i++) {
		if (&g->ctxs[i].response == response) {
			g->ctxs[i].request = NULL;
			break;
		}
	}

	/* Assert that an associated request was indeed found */
	assert(i < DQLITE__GATEWAY_MAX_REQUESTS);
}

void dqlite__gateway_abort(struct dqlite__gateway * g,
                           struct dqlite__response *response) {
	assert(g != NULL);
	assert(response != NULL);
}
