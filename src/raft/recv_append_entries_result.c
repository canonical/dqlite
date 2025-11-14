#include "../lib/assert.h"
#include "../tracing.h"
#include "configuration.h"
#include "recv_append_entries_result.h"
#include "recv.h"
#include "replication.h"

int recvAppendEntriesResult(struct raft *r,
			    const raft_id id,
			    const char *address,
			    const struct raft_append_entries_result *result)
{
	int match;
	const struct raft_server *server;
	int rv;

	dqlite_assert(r != NULL);
	dqlite_assert(id > 0);
	dqlite_assert(address != NULL);
	dqlite_assert(result != NULL);

	tracef(
	    "self: %" PRIu64 " from: %" PRIu64 "@%s last_log_index: %" PRIu64 " rejected: %" PRIu64 " "
	    "term: %" PRIu64,
	    r->id, id, address, result->last_log_index, result->rejected,
	    result->term);

	if (r->state != RAFT_LEADER) {
		tracef("local server is not leader -> ignore");
		return 0;
	}

	rv = recvEnsureMatchingTerms(r, result->term, &match);
	if (rv != 0) {
		return rv;
	}

	if (match < 0) {
		tracef("local term is higher -> ignore ");
		return 0;
	}

	/* If we have stepped down, abort here.
	 *
	 * From Figure 3.1:
	 *
	 *   [Rules for Servers] All Servers: If RPC request or response
	 * contains term T > currentTerm: set currentTerm = T, convert to
	 * follower.
	 */
	if (match > 0) {
		dqlite_assert(r->state == RAFT_FOLLOWER);
		return 0;
	}

	dqlite_assert(result->term == r->current_term);

	/* Ignore responses from servers that have been removed */
	server = configurationGet(&r->configuration, id);
	if (server == NULL) {
		tracef("unknown server -> ignore");
		return 0;
	}

	/* Update the progress of this server, possibly sending further entries.
	 */
	rv = replicationUpdate(r, server, result);
	if (rv != 0) {
		return rv;
	}

	return 0;
}

