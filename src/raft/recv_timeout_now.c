#include "recv_timeout_now.h"

#include "../tracing.h"
#include "assert.h"
#include "configuration.h"
#include "convert.h"
#include "log.h"
#include "recv.h"

int recvTimeoutNow(struct raft *r,
		   const raft_id id,
		   const char *address,
		   const struct raft_timeout_now *args)
{
	const struct raft_server *local_server;
	raft_index local_last_index;
	raft_term local_last_term;
	int match;
	int rv;

	assert(r != NULL);
	assert(id > 0);
	assert(args != NULL);

	(void)address;

	tracef(
	    "self:%llu from:%llu@%s last_log_index:%llu last_log_term:%llu "
	    "term:%llu",
	    r->id, id, address, args->last_log_index, args->last_log_term,
	    args->term);
	/* Ignore the request if we are not voters. */
	local_server = configurationGet(&r->configuration, r->id);
	if (local_server == NULL || local_server->role != RAFT_VOTER) {
		tracef("non-voter");
		return 0;
	}

	/* Ignore the request if we are not follower, or we have different
	 * leader. */
	if (r->state != RAFT_FOLLOWER ||
	    r->follower_state.current_leader.id != id) {
		tracef("Ignore - r->state:%d current_leader.id:%llu", r->state,
		       r->follower_state.current_leader.id);
		return 0;
	}

	/* Possibly update our term. Ignore the request if it turns out we have
	 * a higher term. */
	rv = recvEnsureMatchingTerms(r, args->term, &match);
	if (rv != 0) {
		return rv;
	}
	if (match < 0) {
		return 0;
	}

	/* Ignore the request if we our log is not up-to-date. */
	local_last_index = logLastIndex(r->log);
	local_last_term = logLastTerm(r->log);
	if (local_last_index != args->last_log_index ||
	    local_last_term != args->last_log_term) {
		return 0;
	}

	/* Finally, ignore the request if we're working on persisting some
	 * entries. */
	if (r->follower_state.append_in_flight_count > 0) {
		return 0;
	}

	/* Convert to candidate and start a new election. */
	rv = convertToCandidate(r, true /* disrupt leader */);
	if (rv != 0) {
		return rv;
	}

	return 0;
}

