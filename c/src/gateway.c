#include <assert.h>
#include <stdio.h>

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

	ctx->response.type = DQLITE_WELCOME;
	ctx->response.welcome.heartbeat_timeout = g->heartbeat_timeout;
	ctx->response.welcome.leader = leader;

	return 0;
}

static int dqlite__gateway_heartbeat(struct dqlite__gateway *g, struct dqlite__gateway_ctx *ctx)
{
	const char **addresses;

	/* Get the current list of servers in the cluster */
	addresses = g->cluster->xServers(g->cluster->ctx);
	if (addresses == NULL ) {
		dqlite__errorf(g, "failed to get cluster servers", "");
		return DQLITE_ERROR;
	}

	/* Encode the response */
	ctx->response.type = DQLITE_SERVERS;
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

	ctx->response.type = DQLITE_DB_ERROR;
	ctx->response.db_error.code = sqlite3_errcode(db);
	ctx->response.db_error.extended_code = sqlite3_extended_errcode(db);
	ctx->response.db_error.message = sqlite3_errmsg(db);
}

#define DQLITE__GATEWAY_DB_ERROR					\
	assert(rc != SQLITE_OK);					\
	ctx->response.type = DQLITE_DB_ERROR;				\
	ctx->response.db_error.code = sqlite3_errcode(db->db);		\
	ctx->response.db_error.extended_code = sqlite3_extended_errcode(db->db); \
	ctx->response.db_error.message = sqlite3_errmsg(db->db)

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
		ctx->response.type = DQLITE_DB;
		ctx->response.db.id =  (uint64_t)i;
	} else {
		dqlite__db_registry_del(&g->dbs, i);
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

	dqlite__debugf(g, "handle request", "type=%d", request->type);

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

#define DQLITE__GATEWAY_HANDLE(CODE, STRUCT, NAME, _)		\
		case CODE:					\
			err = dqlite__gateway_ ## NAME(g, ctx);	\
			break;

		DQLITE__REQUEST_SCHEMA_TYPES(DQLITE__GATEWAY_HANDLE,);

	default:
		dqlite__error_printf(&g->error, "unexpected Request %d", request->type);
		err = DQLITE_PROTO;
		goto err;

	}

	if (err != 0) {
		dqlite__error_wrapf(&g->error, &ctx->response.error, "failed to render response");
		goto err;
	}

	*response = &ctx->response;

	return 0;

 err:
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
	assert(g != NULL);
	assert(response != NULL);

	dqlite__debugf(g, "request finished", "");
}

void dqlite__gateway_abort(struct dqlite__gateway *g, struct dqlite__response *response){
	assert(g != NULL);
	assert(response != NULL);

	dqlite__debugf(g, "abort request", "");
}
