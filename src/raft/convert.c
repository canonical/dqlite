#include "convert.h"

#include "../raft.h"
#include "../tracing.h"
#include "assert.h"
#include "callbacks.h"
#include "configuration.h"
#include "election.h"
#include "log.h"
#include "membership.h"
#include "progress.h"
#include "../lib/queue.h"
#include "replication.h"
#include "request.h"

/* Convenience for setting a new state value and asserting that the transition
 * is valid. */
static void convertSetState(struct raft *r, unsigned short new_state)
{
	/* Check that the transition is legal, see Figure 3.3. Note that with
	 * respect to the paper we have an additional "unavailable" state, which
	 * is the initial or final state. */
	unsigned short old_state = r->state;
	tracef("old_state:%u new_state:%u", old_state, new_state);
	assert((r->state == RAFT_UNAVAILABLE && new_state == RAFT_FOLLOWER) ||
	       (r->state == RAFT_FOLLOWER && new_state == RAFT_CANDIDATE) ||
	       (r->state == RAFT_CANDIDATE && new_state == RAFT_FOLLOWER) ||
	       (r->state == RAFT_CANDIDATE && new_state == RAFT_LEADER) ||
	       (r->state == RAFT_LEADER && new_state == RAFT_FOLLOWER) ||
	       (r->state == RAFT_FOLLOWER && new_state == RAFT_UNAVAILABLE) ||
	       (r->state == RAFT_CANDIDATE && new_state == RAFT_UNAVAILABLE) ||
	       (r->state == RAFT_LEADER && new_state == RAFT_UNAVAILABLE));
	r->state = new_state;
	if (r->state == RAFT_LEADER) {
		r->leader_state.voter_contacts = 1;
	}

	struct raft_callbacks *cbs = raftGetCallbacks(r);
	if (cbs != NULL && cbs->state_cb != NULL) {
		cbs->state_cb(r, old_state, new_state);
	}
}

/* Clear follower state. */
static void convertClearFollower(struct raft *r)
{
	tracef("clear follower state");
	r->follower_state.current_leader.id = 0;
	if (r->follower_state.current_leader.address != NULL) {
		raft_free(r->follower_state.current_leader.address);
	}
	r->follower_state.current_leader.address = NULL;
}

/* Clear candidate state. */
static void convertClearCandidate(struct raft *r)
{
	tracef("clear candidate state");
	if (r->candidate_state.votes != NULL) {
		raft_free(r->candidate_state.votes);
		r->candidate_state.votes = NULL;
	}
}

static void convertFailApply(struct raft_apply *req)
{
	if (req != NULL && req->cb != NULL) {
		req->cb(req, RAFT_LEADERSHIPLOST, NULL);
	}
}

static void convertFailBarrier(struct raft_barrier *req)
{
	if (req != NULL && req->cb != NULL) {
		req->cb(req, RAFT_LEADERSHIPLOST);
	}
}

static void convertFailChange(struct raft_change *req)
{
	if (req != NULL && req->cb != NULL) {
		req->cb(req, RAFT_LEADERSHIPLOST);
	}
}

/* Clear leader state. */
static void convertClearLeader(struct raft *r)
{
	tracef("clear leader state");
	if (r->leader_state.progress != NULL) {
		raft_free(r->leader_state.progress);
		r->leader_state.progress = NULL;
	}

	/* Fail all outstanding requests */
	while (!queue_empty(&r->leader_state.requests)) {
		struct request *req;
		queue *head;
		head = queue_head(&r->leader_state.requests);
		queue_remove(head);
		req = QUEUE_DATA(head, struct request, queue);
		assert(req->type == RAFT_COMMAND || req->type == RAFT_BARRIER);
		switch (req->type) {
			case RAFT_COMMAND:
				convertFailApply((struct raft_apply *)req);
				break;
			case RAFT_BARRIER:
				convertFailBarrier((struct raft_barrier *)req);
				break;
		};
	}

	/* Fail any promote request that is still outstanding because the server
	 * is still catching up and no entry was submitted. */
	if (r->leader_state.change != NULL) {
		convertFailChange(r->leader_state.change);
		r->leader_state.change = NULL;
	}
}

/* Clear the current state */
static void convertClear(struct raft *r)
{
	assert(r->state == RAFT_UNAVAILABLE || r->state == RAFT_FOLLOWER ||
	       r->state == RAFT_CANDIDATE || r->state == RAFT_LEADER);
	switch (r->state) {
		case RAFT_FOLLOWER:
			convertClearFollower(r);
			break;
		case RAFT_CANDIDATE:
			convertClearCandidate(r);
			break;
		case RAFT_LEADER:
			convertClearLeader(r);
			break;
	}
}

void convertToFollower(struct raft *r)
{
	convertClear(r);
	convertSetState(r, RAFT_FOLLOWER);

	/* Reset election timer. */
	electionResetTimer(r);

	r->follower_state.current_leader.id = 0;
	r->follower_state.current_leader.address = NULL;
	r->follower_state.append_in_flight_count = 0;
}

int convertToCandidate(struct raft *r, bool disrupt_leader)
{
	const struct raft_server *server;
	size_t n_voters = configurationVoterCount(&r->configuration);
	int rv;

	(void)server; /* Only used for assertions. */

	convertClear(r);
	convertSetState(r, RAFT_CANDIDATE);

	/* Allocate the votes array. */
	r->candidate_state.votes = raft_malloc(n_voters * sizeof(bool));
	if (r->candidate_state.votes == NULL) {
		return RAFT_NOMEM;
	}
	r->candidate_state.disrupt_leader = disrupt_leader;
	r->candidate_state.in_pre_vote = disrupt_leader ? false : r->pre_vote;

	/* Fast-forward to leader if we're the only voting server in the
	 * configuration. */
	server = configurationGet(&r->configuration, r->id);
	assert(server != NULL);
	assert(server->role == RAFT_VOTER);

	if (n_voters == 1) {
		tracef("self elect and convert to leader");
		return convertToLeader(r);
	}

	/* Start a new election round */
	rv = electionStart(r);
	if (rv != 0) {
		r->state = RAFT_FOLLOWER;
		raft_free(r->candidate_state.votes);
		return rv;
	}

	return 0;
}

void convertInitialBarrierCb(struct raft_barrier *req, int status)
{
	(void)status;
	raft_free(req);
}

int convertToLeader(struct raft *r)
{
	int rv;

	tracef("become leader for term %llu", r->current_term);

	convertClear(r);
	convertSetState(r, RAFT_LEADER);

	/* Reset timers */
	r->election_timer_start = r->io->time(r->io);

	/* Reset apply requests queue */
	queue_init(&r->leader_state.requests);

	/* Allocate and initialize the progress array. */
	rv = progressBuildArray(r);
	if (rv != 0) {
		return rv;
	}

	r->leader_state.change = NULL;

	/* Reset promotion state. */
	r->leader_state.promotee_id = 0;
	r->leader_state.round_number = 0;
	r->leader_state.round_index = 0;
	r->leader_state.round_start = 0;

	/* By definition, all entries until the last_stored entry will be
	 * committed if we are the only voter around. */
	size_t n_voters = configurationVoterCount(&r->configuration);
	if (n_voters == 1 && (r->last_stored > r->commit_index)) {
		tracef("apply log entries after self election %llu %llu",
		       r->last_stored, r->commit_index);
		r->commit_index = r->last_stored;
		rv = replicationApply(r);
	} else if (n_voters > 1) {
		/* Raft Dissertation, paragraph 6.4:
		 * The Leader Completeness Property guarantees that a leader has
		 * all committed entries, but at the start of its term, it may
		 * not know which those are. To find out, it needs to commit an
		 * entry from its term. Raft handles this by having each leader
		 * commit a blank no-op entry into the log at the start of its
		 * term. */
		struct raft_barrier *req = raft_malloc(sizeof(*req));
		if (req == NULL) {
			return RAFT_NOMEM;
		}
		rv = raft_barrier(r, req, convertInitialBarrierCb);
		if (rv != 0) {
			tracef(
			    "failed to send no-op barrier entry after leader "
			    "conversion: "
			    "%d",
			    rv);
		}
	}

	return rv;
}

void convertToUnavailable(struct raft *r)
{
	/* Abort any pending leadership transfer request. */
	if (r->transfer != NULL) {
		membershipLeadershipTransferClose(r);
	}
	convertClear(r);
	convertSetState(r, RAFT_UNAVAILABLE);
}

