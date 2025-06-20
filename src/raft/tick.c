#include "../raft.h"
#include "../tracing.h"
#include "assert.h"
#include "configuration.h"
#include "convert.h"
#include "election.h"
#include "membership.h"
#include "progress.h"
#include "replication.h"

/* Apply time-dependent rules for followers (Figure 3.1). */
static int tickFollower(struct raft *r)
{
	const struct raft_server *server;
	int rv;

	assert(r != NULL);
	assert(r->state == RAFT_FOLLOWER);

	server = configurationGet(&r->configuration, r->id);

	/* If we have been removed from the configuration, or maybe we didn't
	 * receive one yet, just stay follower. */
	if (server == NULL) {
		return 0;
	}

	/* Check if we need to start an election.
	 *
	 * From Section 3.3:
	 *
	 *   If a follower receives no communication over a period of time
	 * called the election timeout, then it assumes there is no viable
	 * leader and begins an election to choose a new leader.
	 *
	 * Figure 3.1:
	 *
	 *   If election timeout elapses without receiving AppendEntries RPC
	 * from current leader or granting vote to candidate, convert to
	 * candidate.
	 */
	if (electionTimerExpired(r) && server->role == RAFT_VOTER) {
		if (replicationInstallSnapshotBusy(r)) {
			tracef(
			    "installing snapshot -> don't convert to "
			    "candidate");
			electionResetTimer(r);
			return 0;
		}
		if (r->follower_state.append_in_flight_count > 0) {
			tracef(
			    "append in progress -> don't convert to candidate");
			electionResetTimer(r);
			return 0;
		}
		tracef("convert to candidate and start new election");
		rv = convertToCandidate(r, false /* disrupt leader */);
		if (rv != 0) {
			tracef("convert to candidate: %s", raft_strerror(rv));
			return rv;
		}
	}

	return 0;
}

/* Apply time-dependent rules for candidates (Figure 3.1). */
static int tickCandidate(struct raft *r)
{
	assert(r != NULL);
	assert(r->state == RAFT_CANDIDATE);

	/* Check if we need to start an election.
	 *
	 * From Section 3.4:
	 *
	 *   The third possible outcome is that a candidate neither wins nor
	 * loses the election: if many followers become candidates at the same
	 * time, votes could be split so that no candidate obtains a majority.
	 * When this happens, each candidate will time out and start a new
	 * election by incrementing its term and initiating another round of
	 * RequestVote RPCs
	 */
	if (electionTimerExpired(r)) {
		tracef("start new election");
		return electionStart(r);
	}

	return 0;
}

/* Return true if we received an AppendEntries RPC result from a majority of
 * voting servers since we became leaders or since the last time this function
 * was called.
 *
 * For each server the function checks the recent_recv flag of the associated
 * progress object, and resets the flag after the check. It returns true if a
 * majority of voting server had the flag set to true. */
static bool checkContactQuorum(struct raft *r)
{
	unsigned i;
	unsigned contacts = 0;
	assert(r->state == RAFT_LEADER);

	for (i = 0; i < r->configuration.n; i++) {
		struct raft_server *server = &r->configuration.servers[i];
		bool recent_recv = progressResetRecentRecv(r, i);
		if ((server->role == RAFT_VOTER && recent_recv) ||
		    server->id == r->id) {
			contacts++;
		}
	}
	r->leader_state.voter_contacts = contacts;

	return contacts > configurationVoterCount(&r->configuration) / 2;
}

/* Apply time-dependent rules for leaders (Figure 3.1). */
static int tickLeader(struct raft *r)
{
	raft_time now = r->io->time(r->io);
	assert(r->state == RAFT_LEADER);

	/* Check if we still can reach a majority of servers.
	 *
	 * From Section 6.2:
	 *
	 *   A leader in Raft steps down if an election timeout elapses without
	 * a successful round of heartbeats to a majority of its cluster; this
	 *   allows clients to retry their requests with another server.
	 */
	if (now - r->election_timer_start >= r->election_timeout) {
		if (!checkContactQuorum(r)) {
			tracef(
			    "unable to contact majority of cluster -> step "
			    "down");
			convertToFollower(r);
			return 0;
		}
		r->election_timer_start = r->io->time(r->io);
	}

	/* Possibly send heartbeats.
	 *
	 * From Figure 3.1:
	 *
	 *   Send empty AppendEntries RPC during idle periods to prevent
	 * election timeouts.
	 */
	replicationHeartbeat(r);

	/* If a server is being promoted, increment the timer of the current
	 * round or abort the promotion.
	 *
	 * From Section 4.2.1:
	 *
	 *   The algorithm waits a fixed number of rounds (such as 10). If the
	 * last round lasts less than an election timeout, then the leader adds
	 * the new server to the cluster, under the assumption that there are
	 * not enough unreplicated entries to create a significant availability
	 *   gap. Otherwise, the leader aborts the configuration change with an
	 *   error.
	 */
	if (r->leader_state.promotee_id != 0) {
		raft_id id = r->leader_state.promotee_id;
		unsigned server_index;
		raft_time round_duration = now - r->leader_state.round_start;
		bool is_too_slow;
		bool is_unresponsive;

		/* If a promotion is in progress, we expect that our
		 * configuration contains an entry for the server being
		 * promoted, and that the server is not yet considered as
		 * voting. */
		server_index = configurationIndexOf(&r->configuration, id);
		assert(server_index < r->configuration.n);
		assert(r->configuration.servers[server_index].role !=
		       RAFT_VOTER);

		is_too_slow =
		    (r->leader_state.round_number == r->max_catch_up_rounds &&
		     round_duration > r->election_timeout);
		is_unresponsive =
		    round_duration > r->max_catch_up_round_duration;

		/* Abort the promotion if we are at the 10'th round and it's
		 * still taking too long, or if the server is unresponsive. */
		if (is_too_slow || is_unresponsive) {
			tracef(
			    "server_index:%d is_too_slow:%d is_unresponsive:%d",
			    server_index, is_too_slow, is_unresponsive);
			struct raft_change *change;

			r->leader_state.promotee_id = 0;

			r->leader_state.round_index = 0;
			r->leader_state.round_number = 0;
			r->leader_state.round_start = 0;

			change = r->leader_state.change;
			r->leader_state.change = NULL;
			if (change != NULL && change->cb != NULL) {
				change->cb(change, RAFT_NOCONNECTION);
			}
		}
	}

	return 0;
}

static int tick(struct raft *r)
{
	int rv = -1;

	assert(r->state == RAFT_UNAVAILABLE || r->state == RAFT_FOLLOWER ||
	       r->state == RAFT_CANDIDATE || r->state == RAFT_LEADER);

	/* If we are not available, let's do nothing. */
	if (r->state == RAFT_UNAVAILABLE) {
		return 0;
	}

	switch (r->state) {
		case RAFT_FOLLOWER:
			rv = tickFollower(r);
			break;
		case RAFT_CANDIDATE:
			rv = tickCandidate(r);
			break;
		case RAFT_LEADER:
			rv = tickLeader(r);
			break;
	}

	return rv;
}

void tickCb(struct raft_io *io)
{
	struct raft *r;
	int rv;
	r = io->data;
	rv = tick(r);
	if (rv != 0) {
		convertToUnavailable(r);
		return;
	}

	/* For all states: if there is a leadership transfer request in
	 * progress, check if it's expired. */
	if (r->transfer != NULL) {
		raft_time now = r->io->time(r->io);
		if (now - r->transfer->start >= r->election_timeout) {
			membershipLeadershipTransferClose(r);
		}
	}
}

