#include "gateway.h"
#include "request.h"

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

static int handle_leader(struct gateway *g,
			 struct handle *req,
			 struct cursor *cursor,
			 struct buffer *buffer)
{
	(void)g;
	(void)req;
	(void)cursor;
	(void)buffer;
	return 0;
}

int gateway__handle(struct gateway *g,
		    struct handle *req,
		    int type,
		    struct cursor *cursor,
		    struct buffer *buffer)
{
	int rc;

	/* Abort if we can't accept the request at this time */
	if (g->req != NULL) {
		return DQLITE_PROTO;
	}

	switch (type) {
#define HANDLE(LOWER, UPPER, _)                              \
	case DQLITE_REQUEST_##UPPER:                         \
		rc = handle_##LOWER(g, req, cursor, buffer); \
		break;
		REQUEST__TYPES(HANDLE);
	}

	return rc;
}
