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

#define DQLITE__GATEWAY_NULL DQLITE__FSM_NULL

#define DQLITE__GATEWAY_CONNECT  0 /* Initial state after a fresh connection */
#define DQLITE__GATEWAY_OPERATE  1 /* Operating state after registration */

static struct dqlite__fsm_state dqlite__gateway_states[] = {
	{DQLITE__GATEWAY_CONNECT, "connect"},
	{DQLITE__GATEWAY_OPERATE, "operate"},
	{DQLITE__GATEWAY_NULL,  NULL },
};

#define DQLITE__GATEWAY_REQUEST 0

static struct dqlite__fsm_event dqlite__gateway_events[] = {
	{DQLITE__GATEWAY_REQUEST, "request"},
	{DQLITE__GATEWAY_NULL,    NULL     },
};

/* Helper for common request FSM handler initialization code */
#define DQLITE__GATEWAY_REQUEST_HDLR_INIT \
	int err = 0; \
	int type; \
	struct dqlite__gateway_ctx *ctx; \
	struct dqlite__gateway *g; \
	struct dqlite__request *request; \
	struct dqlite__response *response; \
	const char *name; \
						\
	assert(arg != NULL); \
			     \
	ctx = (struct dqlite__gateway_ctx*)arg; \
	g = ctx->gateway; \
	request = ctx->request; \
	response = &ctx->response; \
	dqlite__response_init(&ctx->response);	\
	type = dqlite__request_type(request); \
	name = dqlite__request_type_name(request)

/* Helper for common request FSM handler cleanup code */
#define DQLITE__GATEWAY_REQUEST_HDLR_CLOSE \
	ctx->request = NULL; \
	dqlite__response_close(&ctx->response); \
	return err

static int dqlite__gateway_connect_request_hdlr(void *arg)
{
	DQLITE__GATEWAY_REQUEST_HDLR_INIT;

	const char *leader;

	dqlite__debugf(g, "handle connect", "name=%s", name);

	if (type != DQLITE_HELO) {
		dqlite__error_printf(&g->error, "expected Helo, got %s", name);
		err = DQLITE_PROTO;
		goto out;
	}

	leader = g->cluster->xLeader(g->cluster->ctx);

	err = dqlite__response_welcome(response, leader, g->heartbeat_timeout);
	if (err != 0) {
		dqlite__error_wrapf(&g->error, &response->error, "failed to render response");
		goto out;
	}

 out:
	DQLITE__GATEWAY_REQUEST_HDLR_CLOSE;
}

static struct dqlite__fsm_transition dqlite__conn_transitions_connect[] = {
	{DQLITE__GATEWAY_REQUEST, dqlite__gateway_connect_request_hdlr, DQLITE__GATEWAY_OPERATE},
};

static int dqlite__gateway_heartbeat(
	struct dqlite__gateway *g,
	struct dqlite__request *request,
	struct dqlite__response *response)
{
	int err;
	const char **addresses;

	/* Get the current list of servers in the cluster */
	addresses = g->cluster->xServers(g->cluster->ctx);
	if (addresses == NULL ) {
		dqlite__errorf(g, "failed to get cluster servers", "");
		return DQLITE_ERROR;
	}

	/* Encode the response */
	err = dqlite__response_servers(response, addresses);
	if (err != 0) {
		dqlite__error_wrapf(&g->error, &response->error, "failed to render response");
		return err;
	}

	/* Refresh the heartbeat timestamp. */
	g->heartbeat = request->timestamp;

	return 0;
}

static int dqlite__gateway_open(
	struct dqlite__gateway *g,
	struct dqlite__request *request,
	struct dqlite__response *response)
{
	int err;
	const char **addresses;

	/* Get the current list of servers in the cluster */
	addresses = g->cluster->xServers(g->cluster->ctx);
	if (addresses == NULL ) {
		dqlite__errorf(g, "failed to get cluster servers", "");
		return DQLITE_ERROR;
	}

	/* Encode the response */
	err = dqlite__response_servers(response, addresses);
	if (err != 0) {
		dqlite__error_wrapf(&g->error, &response->error, "failed to render response");
		return err;
	}

	/* Refresh the heartbeat timestamp. */
	g->heartbeat = request->timestamp;

	return 0;
}

static int dqlite__gateway_operate_request_hdlr(void *arg)
{
	DQLITE__GATEWAY_REQUEST_HDLR_INIT;

	dqlite__debugf(g, "handle operate", "name=%s", name);

	switch (type) {

	case DQLITE_HEARTBEAT:
		err = dqlite__gateway_heartbeat(g, request, response);
		break;

	case DQLITE_OPEN:
		err = dqlite__gateway_open(g, request, response);
		break;

	default:
		dqlite__error_printf(&g->error, "unexpected Request %s", name);
		err = DQLITE_PROTO;
		goto out;

	}
	if (err != 0) {
		dqlite__error_wrapf(&g->error, &response->error, "failed to render response");
		goto out;
	}

 out:
	DQLITE__GATEWAY_REQUEST_HDLR_CLOSE;
}

static struct dqlite__fsm_transition dqlite__conn_transitions_operate[] = {
	{DQLITE__GATEWAY_REQUEST, dqlite__gateway_operate_request_hdlr, DQLITE__GATEWAY_OPERATE},
};

static struct dqlite__fsm_transition *dqlite__gateway_transitions[] = {
	dqlite__conn_transitions_connect,
	dqlite__conn_transitions_operate,
};

static void dqlite__gateway_ctx_init(struct dqlite__gateway_ctx *ctx, struct dqlite__gateway *g)
{
	ctx->gateway = g;
	ctx->request = NULL;
}

void dqlite__gateway_init(
	struct dqlite__gateway *g,
	FILE *log,
	struct dqlite_cluster *cluster)
{
	int i;

	assert(g != NULL);

	dqlite__lifecycle_init(DQLITE__LIFECYCLE_GATEWAY);

	g->heartbeat_timeout = DQLITE__GATEWAY_DEFAULT_HEARTBEAT_TIMEOUT;

	dqlite__error_init(&g->error);

	g->log = log;
	g->cluster = cluster;

	dqlite__fsm_init(&g->fsm,
		dqlite__gateway_states,
		dqlite__gateway_events,
		dqlite__gateway_transitions
		);

	/* Reset all request contexts in the buffer */
	for (i = 0; i < DQLITE__GATEWAY_MAX_REQUESTS; i++) {
		dqlite__gateway_ctx_init(&g->ctxs[i], g);
	}
}

void dqlite__gateway_close(struct dqlite__gateway *g)
{
	assert(g != NULL);

	dqlite__error_close(&g->error);
	dqlite__fsm_close(&g->fsm);

	dqlite__lifecycle_close(DQLITE__LIFECYCLE_GATEWAY);
}

int dqlite__gateway_handle(struct dqlite__gateway *g,
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

	err = dqlite__fsm_step(&g->fsm, DQLITE__GATEWAY_REQUEST, (void*)ctx);
	if (err != 0) {
		const char *state = dqlite__fsm_state(&g->fsm);
		dqlite__debugf(g, state, "msg=%s", g->error);
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
