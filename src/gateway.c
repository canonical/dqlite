#include "gateway.h"
#include "request.h"
#include "response.h"

void gateway__init(struct gateway *g,
		   struct dqlite_logger *logger,
		   struct options *options,
		   struct registry *registry,
		   struct raft *raft)
{
	g->logger = logger;
	g->options = options;
	g->registry = registry;
	g->raft = raft;
	g->leader = NULL;
}

void gateway__close(struct gateway *g)
{
	if (g->leader != NULL) {
		leader__close(g->leader);
		sqlite3_free(g->leader);
	}
}

/* Declare a request struct and a response struct of the appropriate types and
 * decode the request. */
#define HANDLE_START(REQ, RES)                                   \
	struct request_##REQ request;                            \
	struct response_##RES response;                          \
	{                                                        \
		int rc2;                                         \
		rc2 = request_##REQ##__decode(cursor, &request); \
		if (rc2 != 0) {                                  \
			return rc2;                              \
		}                                                \
	}

/* Encode the given response and invoke the request callback */
#define HANDLE_END(LOWER, UPPER)                                  \
	{                                                         \
		size_t n = response_##LOWER##__sizeof(&response); \
		void *cursor;                                     \
		assert(n % 8 == 0);                               \
		cursor = buffer__advance(req->buffer, n);         \
		if (cursor == NULL) {                             \
			return DQLITE_NOMEM;                      \
		}                                                 \
		response_##LOWER##__encode(&response, &cursor);   \
		req->cb(req, 0, DQLITE_RESPONSE_##UPPER);         \
	}

static int handle_leader(struct handle *req, struct cursor *cursor)
{
	HANDLE_START(leader, server);
	unsigned id;
	raft_leader(req->gateway->raft, &id, &response.address);
	if (response.address == NULL) {
		response.address = "";
	}
	HANDLE_END(server, SERVER);
	return 0;
}

static int handle_client(struct handle *req, struct cursor *cursor)
{
	HANDLE_START(client, welcome);
	response.heartbeat_timeout = req->gateway->options->heartbeat_timeout;
	HANDLE_END(welcome, WELCOME);
	return 0;
}

int gateway__handle(struct gateway *g,
		    struct handle *req,
		    int type,
		    struct cursor *cursor,
		    struct buffer *buffer,
		    handle_cb cb)
{
	int rc;

	/* Abort if we can't accept the request at this time */
	if (g->req != NULL) {
		return DQLITE_PROTO;
	}

	req->gateway = g;
	req->cb = cb;
	req->buffer = buffer;

	switch (type) {
#define DISPATCH(LOWER, UPPER, _)                 \
	case DQLITE_REQUEST_##UPPER:              \
		rc = handle_##LOWER(req, cursor); \
		break;
		REQUEST__TYPES(DISPATCH);
	}

	return rc;
}
