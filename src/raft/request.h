#ifndef REQUEST_H_
#define REQUEST_H_

#include "../lib/sm.h" /* struct sm */
#include "../raft.h"

/**
 * State machine for a request to append an entry to the raft log.
 */
enum {
	REQUEST_START,
	REQUEST_COMPLETE,
	REQUEST_FAILED,
	REQUEST_NR,
};

static struct sm_conf request_states[REQUEST_NR] = {
	[REQUEST_START] = {
		.name = "start",
		.allowed = BITS(REQUEST_COMPLETE)|BITS(REQUEST_FAILED),
		.flags = SM_INITIAL,
	},
	[REQUEST_COMPLETE] = {
		.name = "complete",
		.flags = SM_FINAL,
	},
	[REQUEST_FAILED] = {
		.name = "failed",
		.flags = SM_FAILURE|SM_FINAL,
	},
};

static inline bool request_invariant(const struct sm *sm, int prev)
{
	/* The next line exists because otherwise request_states would
	 * be flagged as unused in translation units that include this
	 * header but don't reference the static (i.e. that don't call
	 * sm_init). We could avoid this by just declaring the static
	 * in this header and defining it in request.c, but that splits
	 * the details of the state machine up in an ugly way. */
	(void)request_states;
	(void)sm;
	(void)prev;
	return true;
}

/* Abstract request type */
struct request {
	/* Must be kept in sync with RAFT__REQUEST in raft.h */
	void *data;
	int type;
	raft_index index;
	queue queue;
	struct sm sm;
	uint8_t req_id[16];
	uint8_t client_id[16];
	uint8_t unique_id[16];
	uint64_t reserved[4];
};

#endif /* REQUEST_H_ */
