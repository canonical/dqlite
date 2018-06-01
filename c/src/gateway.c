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

#define DQLITE__GATEWAY_NULL DQLITE__FSM_NULL

#define DQLITE__GATEWAY_CONNECT 0 /* Initial state after a fresh connection */

static struct dqlite__fsm_state dqlite__gateway_states[] = {
	{DQLITE__GATEWAY_CONNECT, "connect"},
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
					 \
	assert(arg != NULL); \
			     \
	ctx = (struct dqlite__gateway_ctx*)arg; \
	g = ctx->gateway; \
	request = ctx->request; \
	response = &ctx->response; \
	dqlite__response_init(&ctx->response);	\
	type = dqlite__request_type(request)

/* Helper for common request FSM handler cleanup code */
#define DQLITE__GATEWAY_REQUEST_HDLR_CLOSE \
	ctx->request = NULL; \
	dqlite__response_close(&ctx->response); \
	return err

static int dqlite__gateway_connect_request_hdlr(void *arg)
{
	DQLITE__GATEWAY_REQUEST_HDLR_INIT;

	const char *address;

	dqlite__debugf(g, "handle connect", "");

	if (type != DQLITE_REQUEST_LEADER) {
		const char *name = dqlite__request_type_name(request);
		dqlite__error_printf(&g->error, "expected Leader, got %s", name);
		err = DQLITE_PROTO;
		goto out;
	}

	address = g->cluster->xLeader(g->cluster->ctx);

	err = dqlite__response_server(response, address);
	if (err != 0) {
		dqlite__error_wrapf(&g->error, &response->error, "failed to render response");
		goto out;
	}

 out:
	DQLITE__GATEWAY_REQUEST_HDLR_CLOSE;
}

static struct dqlite__fsm_transition dqlite__conn_transitions_connect[] = {
	{DQLITE__GATEWAY_REQUEST, dqlite__gateway_connect_request_hdlr, 0},
};

static struct dqlite__fsm_transition *dqlite__gateway_transitions[] = {
	dqlite__conn_transitions_connect,
};

static void dqlite__gateway_ctx_init(struct dqlite__gateway_ctx *ctx, struct dqlite__gateway *g)
{
	ctx->gateway = g;
	ctx->request = NULL;
}

void dqlite__gateway_init(struct dqlite__gateway *g,
			FILE *log,
			dqlite_cluster *cluster)
{
	int i;

	assert(g != NULL);

	dqlite__lifecycle_init(DQLITE__LIFECYCLE_GATEWAY);

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
