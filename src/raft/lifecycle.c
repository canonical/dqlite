#include "lifecycle.h"
#include "../tracing.h"
#include "queue.h"

#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

static bool reqIdIsSet(const struct request *req)
{
	return req->req_id[15] == (uint8_t)-1;
}

static uint64_t extractReqId(const struct request *req)
{
	uint64_t id;
	memcpy(&id, &req->req_id, sizeof(id));
	return id;
}

void lifecycleRequestStart(struct raft *r, struct request *req)
{
	if (reqIdIsSet(req)) {
		tracef("request start id:%" PRIu64, extractReqId(req));
	}
	QUEUE_PUSH(&r->leader_state.requests, &req->queue);
}

void lifecycleRequestEnd(struct raft *r, struct request *req)
{
	(void)r;
	if (reqIdIsSet(req)) {
		tracef("request end id:%" PRIu64, extractReqId(req));
	}
	QUEUE_REMOVE(&req->queue);
}
