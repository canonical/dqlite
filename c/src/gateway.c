#include <assert.h>
#include <stdio.h>
#include <float.h>

#include "gateway.h"
#include "dqlite.h"
#include "error.h"
#include "fsm.h"
#include "lifecycle.h"
#include "log.h"
#include "request.h"
#include "response.h"

/* Default heartbeat timeout in milliseconds.
 *
 * Clients will be disconnected if we don't send a heartbeat
 * message within this time. */
#define DQLITE__GATEWAY_DEFAULT_HEARTBEAT_TIMEOUT 15000

static int dqlite__gateway_helo(struct dqlite__gateway *g, struct dqlite__gateway_ctx *ctx)
{
	const char *leader;

	leader = g->cluster->xLeader(g->cluster->ctx);

	if (leader == NULL) {
		dqlite__error_oom(&g->error, "failed to get cluster leader");
		return DQLITE_NOMEM;
	}

	ctx->response.type = DQLITE_RESPONSE_WELCOME;
	ctx->response.welcome.heartbeat_timeout = g->heartbeat_timeout;
	ctx->response.welcome.leader = leader;

	return 0;
}

static int dqlite__gateway_heartbeat(struct dqlite__gateway *g, struct dqlite__gateway_ctx *ctx)
{
	int err;
	const char **addresses;

	/* Get the current list of servers in the cluster */
	err = g->cluster->xServers(g->cluster->ctx, &addresses);
	if (err != 0) {
		/* TODO: handle the case where the error is due to the not not
		 * being the leader */
		dqlite__errorf(g, "failed to get cluster servers", "");
		return DQLITE_ERROR;
	}

	assert(addresses != NULL);

	/* Encode the response */
	ctx->response.type = DQLITE_RESPONSE_SERVERS;
	ctx->response.servers.addresses = addresses;

	/* Refresh the heartbeat timestamp. */
	g->heartbeat = ctx->request->timestamp;

	return 0;
}

/* Helper to fill the response with the details of a database error */
static void dqlite__gateway_db_error(struct dqlite__gateway_ctx *ctx, sqlite3 *db, int rc)
{
	assert(ctx != NULL);
	assert(db != NULL);
	assert(rc != SQLITE_OK);

	ctx->response.type = DQLITE_RESPONSE_DB_ERROR;
	ctx->response.db_error.code = sqlite3_errcode(db);
	ctx->response.db_error.extended_code = sqlite3_extended_errcode(db);
	ctx->response.db_error.description = sqlite3_errmsg(db);
}

static int dqlite__gateway_open(struct dqlite__gateway *g, struct dqlite__gateway_ctx *ctx)
{
	int err;
	int rc;
	size_t i;
	struct dqlite__db *db;

	err = dqlite__db_registry_add(&g->dbs, &db, &i);
	if (err != 0) {
		assert(err == DQLITE_NOMEM);
		dqlite__error_oom(&g->error, "failed to create db object");
		return err;
	}

	assert(db != NULL);

	rc = dqlite__db_open(
		db, ctx->request->open.name, ctx->request->open.flags, ctx->request->open.vfs);
	if (rc == SQLITE_OK) {
		ctx->response.type = DQLITE_RESPONSE_DB;
		ctx->response.db.id =  (uint32_t)i;
	} else {
		dqlite__db_registry_del(&g->dbs, i);
		dqlite__gateway_db_error(ctx, db->db, rc);
	}

	return 0;
}

#define DQLITE__GATEWAY_LOOKUP_DB(ID)					\
	db = dqlite__db_registry_get(&g->dbs, ID);			\
	if (db == NULL) {						\
		dqlite__error_printf(&g->error, "no db with id %d", ID); \
		return DQLITE_NOTFOUND;					\
	}

#define DQLITE__GATEWAY_LOOKUP_STMT(ID)					\
	stmt = dqlite__db_stmt(db, ID);					\
	if (stmt == NULL) {						\
		dqlite__error_printf(&g->error, "no stmt with id %d", ID); \
		return DQLITE_NOTFOUND;					\
	}

static int dqlite__gateway_prepare(struct dqlite__gateway *g, struct dqlite__gateway_ctx *ctx)
{
	int rc;

	struct dqlite__db *db;
	uint32_t stmt_id;

	DQLITE__GATEWAY_LOOKUP_DB(ctx->request->prepare.db_id);

	rc = dqlite__db_prepare(db, ctx->request->prepare.sql, &stmt_id);
	if (rc == SQLITE_OK) {
		ctx->response.type = DQLITE_RESPONSE_STMT;
		ctx->response.stmt.db_id =  ctx->request->prepare.db_id;
		ctx->response.stmt.id =  stmt_id;
	} else {
		dqlite__gateway_db_error(ctx, db->db, rc);
	}

	return 0;
}

static int dqlite__gateway_exec(struct dqlite__gateway *g, struct dqlite__gateway_ctx *ctx)
{
	int err;
	int rc;
	struct dqlite__db *db;
	struct dqlite__stmt *stmt;
	uint64_t last_insert_id;
	uint64_t rows_affected;

	DQLITE__GATEWAY_LOOKUP_DB(ctx->request->exec.db_id);
	DQLITE__GATEWAY_LOOKUP_STMT(ctx->request->exec.stmt_id);

	assert(stmt != NULL);

	err = dqlite__stmt_bind(stmt, &ctx->request->message, &rc);
	if (err != 0) {
		assert(err == DQLITE_PROTO || err == DQLITE_ENGINE);

		if (err == DQLITE_PROTO) {
			dqlite__error_wrapf(&g->error, &stmt->error, "invalid bindings");
			return err;
		}

		assert(err == DQLITE_ENGINE);
		assert(rc != SQLITE_OK);

		dqlite__gateway_db_error(ctx, db->db, rc);

		return 0;
	}

	rc = dqlite__stmt_exec(stmt, &last_insert_id, &rows_affected);
	if (rc == 0) {
		ctx->response.type = DQLITE_RESPONSE_RESULT;
		ctx->response.result.last_insert_id = last_insert_id;
		ctx->response.result.rows_affected = rows_affected;
	} else {
		dqlite__gateway_db_error(ctx, db->db, rc);
	}

	return 0;
}

static int dqlite__gateway_query(struct dqlite__gateway *g, struct dqlite__gateway_ctx *ctx)
{
	int err;
	int rc;
	struct dqlite__db *db;
	struct dqlite__stmt *stmt;

	DQLITE__GATEWAY_LOOKUP_DB(ctx->request->query.db_id);
	DQLITE__GATEWAY_LOOKUP_STMT(ctx->request->query.stmt_id);

	assert(stmt != NULL);

	err = dqlite__stmt_bind(stmt, &ctx->request->message, &rc);
	if (err != 0) {
		assert(err == DQLITE_PROTO || err == DQLITE_ENGINE);

		if (err == DQLITE_PROTO) {
			dqlite__error_wrapf(&g->error, &stmt->error, "invalid bindings");
			return err;
		}

		assert(err == DQLITE_ENGINE);
		assert(rc != SQLITE_OK);

		dqlite__gateway_db_error(ctx, db->db, rc);

		return 0;
	}

	err = dqlite__stmt_query(stmt, &ctx->response.message, &rc);
	if (err != 0) {
		dqlite__error_wrapf(&g->error, &stmt->error, "failed to fetch rows");
		return err;
	}

	if (rc != SQLITE_DONE) {
		/* TODO: reset what was written in the message */
		dqlite__gateway_db_error(ctx, db->db, rc);
	} else {
		ctx->response.type = DQLITE_RESPONSE_ROWS;
	}

	return 0;
}

static int dqlite__gateway_finalize(struct dqlite__gateway *g, struct dqlite__gateway_ctx *ctx)
{
	int rc;
	struct dqlite__db *db;
	struct dqlite__stmt *stmt;

	DQLITE__GATEWAY_LOOKUP_DB(ctx->request->finalize.db_id);
	DQLITE__GATEWAY_LOOKUP_STMT(ctx->request->finalize.stmt_id);

	rc = dqlite__db_finalize(db, stmt, ctx->request->finalize.stmt_id);
	if (rc == SQLITE_OK) {
		ctx->response.type = DQLITE_RESPONSE_EMPTY;
	} else {
		dqlite__gateway_db_error(ctx, db->db, rc);
	}

	return 0;
}

void dqlite__gateway_init(
	struct dqlite__gateway *g,
	FILE *log,
	struct dqlite_cluster *cluster)
{
	int i;

	assert(g != NULL);

	dqlite__lifecycle_init(DQLITE__LIFECYCLE_GATEWAY);

	g->client_id = 0;
	g->heartbeat_timeout = DQLITE__GATEWAY_DEFAULT_HEARTBEAT_TIMEOUT;

	dqlite__error_init(&g->error);

	g->log = log;
	g->cluster = cluster;

	/* Reset all request contexts in the buffer */
	for (i = 0; i < DQLITE__GATEWAY_MAX_REQUESTS; i++) {
		g->ctxs[i].request = NULL;
	}

	dqlite__db_registry_init(&g->dbs);
}

void dqlite__gateway_close(struct dqlite__gateway *g)
{
	assert(g != NULL);

	dqlite__error_close(&g->error);
	dqlite__db_registry_close(&g->dbs);

	dqlite__lifecycle_close(DQLITE__LIFECYCLE_GATEWAY);
}

int dqlite__gateway_handle(
	struct dqlite__gateway *g,
	struct dqlite__request *request,
	struct dqlite__response **response)
{
	int err;
	int i;
	struct dqlite__gateway_ctx *ctx;

	assert(g != NULL);
	assert(request != NULL );
	assert(response != NULL );

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

	ctx = &g->ctxs[i];
	ctx->request = request;

	switch (request->type) {

#define DQLITE__GATEWAY_HANDLE(CODE, STRUCT, NAME, _)			\
		case CODE:						\
			dqlite__debugf(g, "handle request", "type=%s", #NAME); \
									\
			err = dqlite__gateway_ ## NAME(g, ctx);		\
			if (err != 0) {					\
				dqlite__error_wrapf(			\
					&g->error, &g->error,		\
					"failed to handle %s", #NAME);	\
				goto err;				\
			}						\
			break;

		DQLITE__REQUEST_SCHEMA_TYPES(DQLITE__GATEWAY_HANDLE,);

	default:
		dqlite__error_printf(&g->error, "unexpected Request %d", request->type);
		err = DQLITE_PROTO;
		goto err;

	}

	*response = &ctx->response;

	return 0;

 err:
	assert(err != 0);

	*response = 0;

	return err;
}

int dqlite__gateway_continue(struct dqlite__gateway *g, struct dqlite__response *response)
{
	assert(g != NULL);
	assert(response != NULL);

	return 0;
}

void dqlite__gateway_finish(struct dqlite__gateway *g, struct dqlite__response *response)
{
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

	dqlite__debugf(g, "request finished", "index=%d", i);
}

void dqlite__gateway_abort(struct dqlite__gateway *g, struct dqlite__response *response){
	assert(g != NULL);
	assert(response != NULL);

	dqlite__debugf(g, "abort request", "");
}
